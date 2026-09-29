"""
Backtest: Data Cache

Fetches 5y OHLCV for the full universe + benchmark ONCE, caches to local
parquet files (git-ignored) so every backtest/tuning iteration re-reads the
cache instead of re-hitting yfinance. Reuses stock_fetcher.fetch_stock_data so
backtest data comes through the exact same fetch/clean path as the live
pipeline — same MultiIndex handling, same minimum-history filtering.
"""

import logging
import os

import pandas as pd
import yfinance as yf

from modules.sector_mapper import build_universe
from modules.sector_rotation import BENCHMARK_TICKER
from modules.stock_fetcher import fetch_stock_data

logger = logging.getLogger(__name__)

CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")
UNIVERSE_CACHE_PATH = os.path.join(CACHE_DIR, "universe_5y.parquet")
BENCHMARK_CACHE_PATH = os.path.join(CACHE_DIR, "benchmark_5y.parquet")


def _fetch_benchmark(period: str) -> pd.Series:
    df = yf.download(BENCHMARK_TICKER, period=period, interval="1d", auto_adjust=True, progress=False)
    close = df["Close"]
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]
    return close.dropna()


def _tickers_to_parquet(data: dict[str, pd.DataFrame], path: str) -> None:
    """Stacks {ticker: df} into one long-format parquet file (ticker column + OHLCV + date index)."""
    frames = []
    for ticker, df in data.items():
        tmp = df.copy()
        tmp["ticker"] = ticker
        frames.append(tmp)
    pd.concat(frames).to_parquet(path)


def _parquet_to_tickers(path: str) -> dict[str, pd.DataFrame]:
    combined = pd.read_parquet(path)
    return {
        ticker: group.drop(columns=["ticker"]).sort_index()
        for ticker, group in combined.groupby("ticker")
    }


def fetch_and_cache(period: str = "5y", force: bool = False) -> tuple[dict[str, pd.DataFrame], pd.Series]:
    """
    Returns (universe_data, benchmark_series). Uses the local parquet cache if
    present, unless `force=True` (re-fetch everything from yfinance). This is a
    one-time (or occasional-refresh) operation, not part of the live pipeline.
    """
    os.makedirs(CACHE_DIR, exist_ok=True)

    if not force and os.path.exists(UNIVERSE_CACHE_PATH) and os.path.exists(BENCHMARK_CACHE_PATH):
        logger.info("Loading cached 5y universe + benchmark data from %s", CACHE_DIR)
        universe_data = _parquet_to_tickers(UNIVERSE_CACHE_PATH)
        benchmark = pd.read_parquet(BENCHMARK_CACHE_PATH)["Close"]
        return universe_data, benchmark

    logger.info("No cache found (or force=True) — fetching %s of data from yfinance ...", period)
    universe = build_universe()
    all_tickers = sorted({u.ticker for u in universe})
    universe_data = fetch_stock_data(all_tickers, period=period)
    benchmark = _fetch_benchmark(period)

    _tickers_to_parquet(universe_data, UNIVERSE_CACHE_PATH)
    benchmark.to_frame(name="Close").to_parquet(BENCHMARK_CACHE_PATH)

    logger.info("Cached %d tickers + benchmark to %s", len(universe_data), CACHE_DIR)
    return universe_data, benchmark
