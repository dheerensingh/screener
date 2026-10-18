"""
Trade Check: daily bars from yfinance

One instrument at a time, cached as parquet under trade_check/cache/
(git-ignored). Today's bar is dropped while it is still forming: NSE before
16:00 IST, crypto always (Yahoo's BTC daily bar is the current UTC day).
"""

import logging
from datetime import datetime, time as dtime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from .config import Instrument

logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).parent / "cache"
IST = ZoneInfo("Asia/Kolkata")
NSE_SETTLED_AT = dtime(16, 0)


def drop_unsettled_bar(df: pd.DataFrame, inst: Instrument, now: datetime | None = None) -> pd.DataFrame:
    if df.empty:
        return df
    if inst.trades_weekends:
        today = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).date()
        return df[df.index.normalize() < pd.Timestamp(today)]
    now_ist = (now or datetime.now(IST)).astimezone(IST)
    if now_ist.time() >= NSE_SETTLED_AT:
        return df
    return df[df.index.normalize() < pd.Timestamp(now_ist.date())]


def _clean(raw: pd.DataFrame) -> pd.DataFrame:
    if isinstance(raw.columns, pd.MultiIndex):
        raw = raw.droplevel(1, axis=1) if raw.columns.nlevels > 1 else raw
    df = raw[["Open", "High", "Low", "Close"]].copy()
    if getattr(df.index, "tz", None) is not None:
        df.index = df.index.tz_localize(None)
    df.index = pd.DatetimeIndex(df.index).normalize()
    df = df[~df.index.duplicated(keep="last")].dropna()
    # Yahoo occasionally prints a zero-range or inverted bar; drop the impossible ones.
    df = df[(df["High"] >= df["Low"]) & (df["Low"] > 0)]
    return df.sort_index()


def fetch(inst: Instrument, period: str = "10y", use_cache: bool = True) -> pd.DataFrame:
    import yfinance as yf

    CACHE_DIR.mkdir(exist_ok=True)
    path = CACHE_DIR / f"{inst.key}_{period}.parquet"
    if use_cache and path.exists():
        age_h = (datetime.now().timestamp() - path.stat().st_mtime) / 3600
        if age_h < 12:
            return pd.read_parquet(path)
    raw = yf.download(inst.yahoo, period=period, interval="1d", auto_adjust=True,
                      progress=False, threads=False)
    if raw is None or raw.empty:
        raise RuntimeError(f"yfinance returned no data for {inst.yahoo}")
    df = drop_unsettled_bar(_clean(raw), inst)
    df.to_parquet(path)
    logger.info("%s (%s): %d bars, %s → %s", inst.key, inst.yahoo, len(df),
                df.index[0].date(), df.index[-1].date())
    return df
