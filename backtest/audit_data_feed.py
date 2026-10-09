"""
Backtest: Yahoo vs NSE data audit

    python -m backtest.audit_data_feed [--days 365]

Compares the last year of Yahoo Finance daily bars (what the screener and paper
account use) with NSE's own end-of-day files from archives.nseindia.com:

  - sec_bhavdata_full_DDMMYYYY.csv — every stock's open/high/low/close that day
    (the same file delivery_analyzer.py reads). Its existence also tells us which
    weekdays were trading days: a holiday 404s.
  - ind_close_all_DDMMYYYY.csv — every NSE index's close, incl. Nifty 500.

Reports: trading days Yahoo is missing (market-wide and per stock), bars Yahoo
has on non-trading days, and daily returns that disagree with NSE's by more
than MISMATCH_PP. Returns are compared rather than prices because Yahoo's
history is split/dividend-adjusted and NSE's is not.

What it can't tell: how *late* Yahoo published a day. Yahoo's history is
backfilled, so a day that arrived hours late looks complete today. main.py's
docs/data/feed_log.csv records that going forward.
"""

import argparse
import io
import logging
import os
import sys
import time
from datetime import date, timedelta

import pandas as pd
import requests

from modules.sector_mapper import build_universe
from modules.sector_rotation import BENCHMARK_TICKER, _fetch_benchmark
from modules.stock_fetcher import fetch_stock_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("backtest.audit")

BHAV_URL = "https://archives.nseindia.com/products/content/sec_bhavdata_full_{d}.csv"
INDEX_URL = "https://archives.nseindia.com/content/indices/ind_close_all_{d}.csv"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"}
MISMATCH_PP = 1.0  # flag a day when Yahoo's and NSE's daily return differ by more than this


def _get_csv(url: str) -> pd.DataFrame | None:
    for attempt in range(3):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
        except requests.RequestException:
            time.sleep(2 * (attempt + 1))
            continue
        if resp.status_code == 404:
            return None
        if resp.status_code == 200:
            df = pd.read_csv(io.StringIO(resp.text))
            df.columns = [c.strip() for c in df.columns]
            return df
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"Could not fetch {url}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=365)
    args = parser.parse_args()

    tickers = sorted({u.ticker for u in build_universe()})
    yahoo = fetch_stock_data(tickers, period="2y", fill_from_nse=False)  # 2y so the first audited day has a previous close
    bench = _fetch_benchmark("2y", fill_from_nse=False)

    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=args.days)
    weekdays = [start + timedelta(days=i) for i in range((end - start).days + 1)
                if (start + timedelta(days=i)).weekday() < 5]

    nse_close: dict[date, pd.Series] = {}      # ticker -> close, per trading day
    nse_prev: dict[date, pd.Series] = {}       # ticker -> previous close
    nse_index: dict[date, float] = {}
    holidays: list[date] = []
    for i, d in enumerate(weekdays):
        bhav = _get_csv(BHAV_URL.format(d=d.strftime("%d%m%Y")))
        if bhav is None:
            holidays.append(d)
            continue
        eq = bhav[bhav["SERIES"].str.strip() == "EQ"].copy()
        eq["SYMBOL"] = eq["SYMBOL"].str.strip()
        eq = eq.set_index("SYMBOL")
        nse_close[d] = pd.to_numeric(eq["CLOSE_PRICE"], errors="coerce")
        nse_prev[d] = pd.to_numeric(eq["PREV_CLOSE"], errors="coerce")
        idx = _get_csv(INDEX_URL.format(d=d.strftime("%d%m%Y")))
        if idx is not None:
            row = idx[idx["Index Name"].str.strip().str.lower() == "nifty 500"]
            if len(row):
                nse_index[d] = float(row["Closing Index Value"].iloc[0])
        if i % 25 == 0:
            logger.info("NSE files: %d/%d weekdays checked", i + 1, len(weekdays))
        time.sleep(0.3)

    trading_days = sorted(nse_close)
    universe = [t for t in tickers if t in yahoo]

    # ── Missing / extra bars ────────────────────────────────────────────────
    missing_by_day: dict[date, int] = {}
    missing_by_ticker: dict[str, int] = {}
    mismatches = []
    pairs = 0
    for t in universe:
        df = yahoo[t]
        ydates = {ts.date(): i for i, ts in enumerate(df.index)}
        closes = df["Close"].astype(float).values
        for d in trading_days:
            if t not in nse_close[d].index:
                continue  # not trading on NSE that day (e.g. listed later, suspended)
            if d not in ydates:
                missing_by_day[d] = missing_by_day.get(d, 0) + 1
                missing_by_ticker[t] = missing_by_ticker.get(t, 0) + 1
                continue
            i = ydates[d]
            if i == 0:
                continue
            nse_c, nse_p = nse_close[d].get(t), nse_prev[d].get(t)
            if not (nse_c and nse_p):
                continue
            pairs += 1
            y_ret = (closes[i] / closes[i - 1] - 1) * 100.0
            n_ret = (nse_c / nse_p - 1) * 100.0
            if abs(y_ret - n_ret) > MISMATCH_PP:
                mismatches.append((d, t, round(y_ret, 2), round(n_ret, 2)))

    yahoo_days = pd.Series([ts.date() for df in yahoo.values() for ts in df.index]).value_counts()
    extra_days = sorted(d for d, n in yahoo_days.items()
                        if start <= d <= end and d not in nse_close and n > len(universe) / 2)
    market_wide_missing = sorted(d for d, n in missing_by_day.items() if n > len(universe) / 2)

    bench_dates = {ts.date(): float(v) for ts, v in bench.items()}
    bench_missing = [d for d in trading_days if d not in bench_dates]
    bench_mismatch = []
    prev = None
    for d in trading_days:
        if prev and d in nse_index and prev in nse_index and d in bench_dates and prev in bench_dates:
            y = (bench_dates[d] / bench_dates[prev] - 1) * 100.0
            n = (nse_index[d] / nse_index[prev] - 1) * 100.0
            if abs(y - n) > MISMATCH_PP / 2:
                bench_mismatch.append((d, round(y, 2), round(n, 2)))
        prev = d

    total_missing = sum(missing_by_ticker.values())
    lines = [
        f"### Yahoo vs NSE audit — {trading_days[0]} to {trading_days[-1]}",
        "",
        f"- Weekdays checked: **{len(weekdays)}** · NSE trading days: **{len(trading_days)}** · "
        f"weekday holidays (no bhavcopy): **{len(holidays)}**",
        f"- Stocks compared: **{len(universe)}** (Nifty500 today) · stock-days compared: **{pairs:,}**",
        f"- Trading days Yahoo is missing **market-wide** (>50% of stocks): **{len(market_wide_missing)}** "
        + (f"— {', '.join(map(str, market_wide_missing[:10]))}" if market_wide_missing else ""),
        f"- Individual stock-days missing on Yahoo: **{total_missing:,}** "
        f"({total_missing / max(pairs + total_missing, 1) * 100:.2f}%)",
        f"- Days Yahoo has bars but NSE was closed: **{len(extra_days)}** "
        + (f"— {', '.join(map(str, extra_days[:10]))}" if extra_days else ""),
        f"- Stock-days where Yahoo's daily return differs from NSE's by >{MISMATCH_PP:.0f}pp: "
        f"**{len(mismatches):,}** ({len(mismatches) / max(pairs, 1) * 100:.2f}%)",
        f"- Benchmark {BENCHMARK_TICKER}: missing **{len(bench_missing)}** trading days"
        + (f" ({', '.join(map(str, bench_missing[:10]))})" if bench_missing else "")
        + f"; return off by >{MISMATCH_PP / 2:.1f}pp vs NSE's Nifty 500 on **{len(bench_mismatch)}** days",
        "",
        f"NSE weekday holidays found: {', '.join(map(str, holidays)) or 'none'}",
    ]
    if missing_by_ticker:
        worst = sorted(missing_by_ticker.items(), key=lambda kv: -kv[1])[:10]
        lines += ["", "Stocks with the most missing days: " + ", ".join(f"{t} ({n})" for t, n in worst)]
    if mismatches:
        worst = sorted(mismatches, key=lambda m: -abs(m[2] - m[3]))[:10]
        lines += ["", "| Date | Stock | Yahoo return % | NSE return % |", "|---|---|---|---|"]
        lines += [f"| {d} | {t} | {y:+.2f} | {n:+.2f} |" for d, t, y, n in worst]
    text = "\n".join(lines)
    print(text)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as f:
            f.write(text + "\n")
    pd.DataFrame(mismatches, columns=["date", "ticker", "yahoo_ret_pct", "nse_ret_pct"]).to_csv(
        "audit_mismatches.csv", index=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())
