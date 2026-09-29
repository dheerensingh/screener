"""
Backtest: Delivery Percentage Cache

Backfills ~5 years of NSE delivery% (modules/delivery_analyzer.fetch_delivery_data)
into a single local parquet cache, following backtest/data_cache.py's exact pattern
(long format, git-ignored backtest/cache/ dir). One bhavcopy request covers ALL NSE
stocks for that day, so a 5-year backfill is ~1,250 requests (one per trading day),
not one per ticker.

Uses the already-cached benchmark index (backtest/cache/benchmark_5y.parquet) as the
authoritative trading-day calendar — avoids reimplementing weekday/holiday logic, and
guarantees exact alignment with what simulator.py's day-loop iterates over.

Resumable: on each run, loads whatever's already cached and only requests the missing
dates; checkpoints to disk every CHECKPOINT_EVERY successfully-fetched days so an
interrupted run loses only a small tail, not the whole backfill.
"""

import logging
import os
import time

import pandas as pd

from modules.delivery_analyzer import fetch_delivery_data

from .data_cache import BENCHMARK_CACHE_PATH, CACHE_DIR

logger = logging.getLogger(__name__)

DELIVERY_CACHE_PATH = os.path.join(CACHE_DIR, "delivery_5y.parquet")
REQUEST_SLEEP_SEC = 0.4
CHECKPOINT_EVERY = 20

# Confirmed live, 2026-09-28 (binary-searched via direct curl against
# archives.nseindia.com): NSE's combined "sec_bhavdata_full_DDMMYYYY.csv" format —
# the one with DELIV_PER — only exists from 30-Sep-2019 onward. Every date before
# that 404s regardless of whether it was a trading day (spot-checked 2017/2018/most
# of 2019 — all 404; 2020 onward — all 200 on real trading days). This means the
# delivery% cache/backtest can only ever cover ~6 years, not the full ~10-year
# benchmark window (2016-09-26 onward) — stated explicitly here and in STRATEGY.md
# §13 rather than silently truncated.
DELIVERY_DATA_AVAILABLE_FROM = pd.Timestamp("2019-09-30")


def _trading_calendar() -> pd.DatetimeIndex:
    """The authoritative list of trading days — reuses the already-cached benchmark
    index rather than guessing weekday/holiday logic independently. Restricted to
    DELIVERY_DATA_AVAILABLE_FROM onward — see the constant's comment above."""
    if not os.path.exists(BENCHMARK_CACHE_PATH):
        raise FileNotFoundError(
            f"{BENCHMARK_CACHE_PATH} not found — run backtest.data_cache.fetch_and_cache() first."
        )
    benchmark = pd.read_parquet(BENCHMARK_CACHE_PATH)["Close"]
    trading_days = benchmark.dropna().index
    return trading_days[trading_days >= DELIVERY_DATA_AVAILABLE_FROM]


def load_delivery_cache() -> pd.DataFrame:
    """Returns the full cached long-format DataFrame (date, ticker, deliv_per), or an
    empty one if nothing's been backfilled yet."""
    if not os.path.exists(DELIVERY_CACHE_PATH):
        return pd.DataFrame(columns=["date", "ticker", "deliv_per"])
    return pd.read_parquet(DELIVERY_CACHE_PATH)


def pivot_delivery_by_ticker(df: pd.DataFrame) -> dict[str, pd.Series]:
    """Long-format (date, ticker, deliv_per) -> {ticker: deliv_per Series indexed by date},
    the shape backtest/simulator.py's run_backtest(delivery_data=...) expects."""
    if df.empty:
        return {}
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    return {
        ticker: group.set_index("date")["deliv_per"].sort_index()
        for ticker, group in df.groupby("ticker")
    }


def backfill_delivery_cache(force: bool = False) -> pd.DataFrame:
    os.makedirs(CACHE_DIR, exist_ok=True)
    trading_days = _trading_calendar()

    existing = pd.DataFrame(columns=["date", "ticker", "deliv_per"]) if force else load_delivery_cache()
    already_have = set(pd.to_datetime(existing["date"]).unique()) if len(existing) else set()
    missing_days = [d for d in trading_days if d not in already_have]

    if not missing_days:
        logger.info("Delivery cache already complete: %d trading days cached.", len(already_have))
        return existing

    logger.info(
        "Backfilling delivery%% for %d missing trading day(s) (of %d total) ...",
        len(missing_days), len(trading_days),
    )

    new_frames = [existing] if len(existing) else []
    fetched_since_checkpoint = []
    ok_count, skip_count = 0, 0

    for i, day in enumerate(missing_days):
        df = fetch_delivery_data(day.to_pydatetime())
        if df is not None and len(df):
            df = df.copy()
            df["date"] = day
            fetched_since_checkpoint.append(df[["date", "ticker", "deliv_per"]])
            ok_count += 1
        else:
            skip_count += 1  # non-trading day (shouldn't happen given the calendar source) or fetch failure

        if (i + 1) % CHECKPOINT_EVERY == 0 or i == len(missing_days) - 1:
            if fetched_since_checkpoint:
                new_frames.extend(fetched_since_checkpoint)
                combined = pd.concat(new_frames, ignore_index=True)
                combined.to_parquet(DELIVERY_CACHE_PATH)
                new_frames = [combined]
                fetched_since_checkpoint = []
            logger.info(
                "  Progress: %d/%d days processed (%d ok, %d skipped) — checkpointed.",
                i + 1, len(missing_days), ok_count, skip_count,
            )

        if i < len(missing_days) - 1:
            time.sleep(REQUEST_SLEEP_SEC)

    result = load_delivery_cache()
    logger.info(
        "Delivery backfill complete: %d trading days cached (%d newly fetched, %d skipped).",
        result["date"].nunique() if len(result) else 0, ok_count, skip_count,
    )
    return result
