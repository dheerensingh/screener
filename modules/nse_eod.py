"""
Module: NSE end-of-day backup for Yahoo gaps

Yahoo Finance sometimes has no bar for a session that NSE traded — on
2026-10-09, 130 of 431 Nifty500 stocks still had no 2026-10-08 bar at 13:39
IST. NSE's own end-of-day files on archives.nseindia.com (the subdomain this
project already uses for the universe and delivery %) have every stock's and
index's OHLC for the day:

  - sec_bhavdata_full_DDMMYYYY.csv — all stocks (EQ series): OPEN/HIGH/LOW/CLOSE_PRICE, TTL_TRD_QNTY
  - ind_close_all_DDMMYYYY.csv     — all indices, incl. "Nifty 500"

fill_stock_gaps() / fill_index_gaps() add a bar from these files for any of the
last RECENT_SESSIONS trading days that Yahoo is missing. Only recent gaps are
filled: the point is a correct latest session, not repairing old history.

NSE's prices are unadjusted while Yahoo's history is split/dividend-adjusted,
and on 2026-10-09 a few stocks' Yahoo history turned out to be on a different
scale altogether. Each filled bar is therefore rescaled by Yahoo's last close /
NSE's PREV_CLOSE whenever those disagree, so the series never jumps at the seam.
"""

import io
import logging
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

BHAV_URL = "https://archives.nseindia.com/products/content/sec_bhavdata_full_{d}.csv"
INDEX_URL = "https://archives.nseindia.com/content/indices/ind_close_all_{d}.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"}
RECENT_SESSIONS = 5
MAX_UNSCALED_GAP = 0.005  # Yahoo last close vs NSE PREV_CLOSE; beyond this the NSE bar is rescaled

_bhav_cache: dict[date, Optional[pd.DataFrame]] = {}
FILLED: dict[str, set] = {}  # session ISO date -> tickers filled this process (for main.py's feed log)
_index_cache: dict[date, Optional[pd.DataFrame]] = {}


def _get_csv(url: str) -> Optional[pd.DataFrame]:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
    except requests.RequestException as exc:
        logger.debug("NSE fetch failed %s: %s", url, exc)
        return None
    if resp.status_code != 200:
        return None  # 404 = not a trading day, or not published yet
    try:
        df = pd.read_csv(io.StringIO(resp.text))
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as exc:
        logger.debug("NSE parse failed %s: %s", url, exc)
        return None


def _file_date_ok(df: pd.DataFrame, column: str, d: date) -> bool:
    """
    NSE's archive answers a holiday's URL with a file anyway (the 2026-10-09 audit
    found a "bhavcopy" for all 14 weekday holidays of the past year), so a file
    only counts if the date printed inside it is the date asked for.
    """
    if column not in df or df.empty:
        return False
    stamped = pd.to_datetime(df[column].astype(str).str.strip().iloc[0], dayfirst=True, errors="coerce")
    if pd.isna(stamped) or stamped.date() != d:
        logger.info("NSE file for %s is dated %s — not a trading day, or not published yet", d,
                    df[column].iloc[0])
        return False
    return True


def bhavcopy(d: date) -> Optional[pd.DataFrame]:
    """EQ-series OHLCV for every NSE stock on `d`, indexed by symbol; None if unavailable."""
    if d not in _bhav_cache:
        raw = _get_csv(BHAV_URL.format(d=d.strftime("%d%m%Y")))
        out = None
        if raw is not None and "SERIES" in raw and _file_date_ok(raw, "DATE1", d):
            eq = raw[raw["SERIES"].astype(str).str.strip() == "EQ"].copy()
            eq["SYMBOL"] = eq["SYMBOL"].astype(str).str.strip()
            out = pd.DataFrame({
                "Open": pd.to_numeric(eq["OPEN_PRICE"], errors="coerce").values,
                "High": pd.to_numeric(eq["HIGH_PRICE"], errors="coerce").values,
                "Low": pd.to_numeric(eq["LOW_PRICE"], errors="coerce").values,
                "Close": pd.to_numeric(eq["CLOSE_PRICE"], errors="coerce").values,
                "Volume": pd.to_numeric(eq["TTL_TRD_QNTY"], errors="coerce").values,
                "PrevClose": pd.to_numeric(eq["PREV_CLOSE"], errors="coerce").values,
            }, index=eq["SYMBOL"].values).dropna(subset=["Close"])
        _bhav_cache[d] = out
    return _bhav_cache[d]


def index_close(d: date, name: str = "Nifty 500") -> Optional[float]:
    if d not in _index_cache:
        _index_cache[d] = _get_csv(INDEX_URL.format(d=d.strftime("%d%m%Y")))
    df = _index_cache[d]
    if df is None or "Index Name" not in df or not _file_date_ok(df, "Index Date", d):
        return None
    row = df[df["Index Name"].astype(str).str.strip().str.lower() == name.lower()]
    if row.empty:
        return None
    return float(pd.to_numeric(row["Closing Index Value"], errors="coerce").iloc[0])


def recent_sessions(n: int = RECENT_SESSIONS, now: Optional[datetime] = None) -> list[date]:
    """The last `n` sessions up to the latest completed one (holiday list permitting)."""
    from .market_calendar import load_holidays, previous_trading_day
    from .stock_fetcher import latest_completed_session

    holidays = load_holidays()
    d = date.fromisoformat(latest_completed_session(now, holidays))
    out = [d]
    while len(out) < n:
        d = previous_trading_day(d, holidays)
        out.append(d)
    return sorted(out)


def _stamp(idx: pd.DatetimeIndex, d: date) -> pd.Timestamp:
    ts = pd.Timestamp(d)
    return ts.tz_localize(idx.tz) if getattr(idx, "tz", None) is not None else ts


def _has_day(idx: pd.DatetimeIndex, d: date) -> bool:
    if getattr(idx, "tz", None) is not None:
        idx = idx.tz_localize(None)
    return pd.Timestamp(d) in set(idx.normalize())


def fill_stock_gaps(data: dict[str, pd.DataFrame], sessions: Optional[list[date]] = None) -> dict[str, int]:
    """
    In place: adds an NSE bhavcopy bar to each stock for any recent session Yahoo
    lacks. Returns {session ISO date: number of stocks filled}.
    """
    sessions = sessions or recent_sessions()
    filled: dict[str, int] = {}
    for d in sessions:
        missing = [t for t, df in data.items() if len(df) and df.index[0] < _stamp(df.index, d)
                   and not _has_day(df.index, d)]
        if not missing:
            continue
        bhav = bhavcopy(d)
        if bhav is None:
            logger.info("NSE bhavcopy for %s not available; %d stock(s) stay without that bar", d, len(missing))
            continue
        n, rescaled, absent = 0, [], []
        for t in missing:
            if t not in bhav.index:
                absent.append(t)
                continue
            df = data[t]
            before = df[df.index < _stamp(df.index, d)]
            nse_prev = float(bhav.at[t, "PrevClose"])
            if before.empty or not nse_prev > 0:
                continue
            # Put NSE's bar on Yahoo's price basis. Yahoo's history is adjusted
            # for splits/bonuses (and sometimes just wrong), so the two can differ
            # by a constant factor; appending NSE's raw price would then create a
            # fake jump. NSE's own PREV_CLOSE vs Yahoo's last close gives the factor.
            ratio = float(before["Close"].iloc[-1]) / nse_prev
            row = bhav.loc[[t], ["Open", "High", "Low", "Close", "Volume"]].copy()
            if abs(ratio - 1) > MAX_UNSCALED_GAP:
                row[["Open", "High", "Low", "Close"]] *= ratio
                rescaled.append(f"{t} x{ratio:.3f}")
            row.index = [_stamp(df.index, d)]
            data[t] = pd.concat([df, row[df.columns.intersection(row.columns)]]).sort_index()
            n += 1
        if absent:
            logger.warning("NSE bhavcopy for %s has no EQ row for %d stock(s) Yahoo is missing — left "
                           "without that bar: %s", d, len(absent), ", ".join(sorted(absent)))
        if rescaled:
            logger.warning("NSE bars for %s rescaled to Yahoo's price basis (Yahoo last close != NSE previous "
                           "close): %s", d, ", ".join(rescaled))
        if n:
            filled[d.isoformat()] = n
            FILLED.setdefault(d.isoformat(), set()).update(t for t in missing if t in bhav.index)
            logger.info("Filled %d stock bar(s) for %s from NSE bhavcopy (Yahoo was missing %d)", n, d, len(missing))
    return filled


def fill_index_gaps(series: pd.Series, name: str = "Nifty 500",
                    sessions: Optional[list[date]] = None) -> pd.Series:
    """Returns `series` with NSE's close added for any recent session Yahoo lacks."""
    if series.empty:
        return series
    sessions = sessions or recent_sessions()
    for d in sessions:
        if series.index[0] >= _stamp(series.index, d) or _has_day(series.index, d):
            continue
        close = index_close(d, name)
        if close is None:
            continue
        series = pd.concat([series, pd.Series([close], index=[_stamp(series.index, d)])]).sort_index()
        logger.info("Filled %s close for %s from NSE (%.2f)", name, d, close)
    return series
