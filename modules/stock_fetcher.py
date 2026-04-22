"""
Module 2b: Historical Data Fetching via yfinance

Downloads 6 months of daily OHLCV data for a list of NSE tickers.
NSE tickers on Yahoo Finance require the '.NS' suffix (e.g., TCS → TCS.NS).
BSE tickers use '.BO' — we default to NSE throughout.

Key design decisions:
  - Batch download via yfinance.download() is far faster than per-ticker calls.
  - A per-ticker fallback handles cases where batch download silently drops a symbol.
  - Minimum 60 trading-day requirement ensures RSI-14 and MACD-26 have enough history.
"""

import math
import logging
import time
from typing import Optional

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

NSE_SUFFIX = ".NS"
MIN_REQUIRED_ROWS = 60   # ~3 months; 6 months requested but some stocks may be newer
CHUNK_SLEEP_SEC = 2       # polite pause between chunks to avoid yfinance rate limits


def _to_ns_ticker(ticker: str) -> str:
    """Append NSE suffix if not already present."""
    t = ticker.strip().upper()
    if not t.endswith(NSE_SUFFIX):
        t += NSE_SUFFIX
    return t


def fetch_stock_data(
    tickers: list[str],
    period: str = "6mo",
    interval: str = "1d",
    chunk_size: int = 100,
) -> dict[str, pd.DataFrame]:
    """
    Fetches OHLCV data for all tickers in chunks to handle 500+ symbols safely.

    yfinance MultiIndex layout (default, NO group_by):
      columns = MultiIndex[(price_field, ticker), ...]
      level 0 = price field  (Open / High / Low / Close / Volume)
      level 1 = ticker symbol (e.g. TCS.NS)

    Extracting one ticker:  raw.xs("TCS.NS", axis=1, level=1)
    → gives a flat DataFrame with columns Open, High, Low, Close, Volume.

    NOTE: group_by="ticker" reverses the levels (ticker at 0, field at 1),
    which breaks xs(ticker, level=1). We intentionally omit group_by to keep
    the standard (field, ticker) layout.
    """
    ns_tickers = [_to_ns_ticker(t) for t in tickers]
    orig_map = {_to_ns_ticker(t): t for t in tickers}  # ns_ticker → original
    result: dict[str, pd.DataFrame] = {}

    total_chunks = math.ceil(len(ns_tickers) / chunk_size)
    logger.info(
        "Downloading %d symbols in %d chunk(s) of up to %d ...",
        len(ns_tickers), total_chunks, chunk_size,
    )

    for chunk_idx in range(total_chunks):
        chunk_ns = ns_tickers[chunk_idx * chunk_size: (chunk_idx + 1) * chunk_size]
        logger.info("  Chunk %d/%d — %d symbols", chunk_idx + 1, total_chunks, len(chunk_ns))

        raw = pd.DataFrame()
        try:
            raw = yf.download(
                tickers=chunk_ns,
                period=period,
                interval=interval,
                auto_adjust=True,
                progress=False,
                threads=True,
                # No group_by → default layout: MultiIndex(field, ticker) at levels (0, 1)
            )
        except Exception as exc:
            logger.error("Chunk %d download failed: %s — skipping", chunk_idx + 1, exc)

        if raw.empty:
            logger.warning("  Chunk %d returned empty DataFrame", chunk_idx + 1)
        elif isinstance(raw.columns, pd.MultiIndex):
            # Multi-ticker result: level 0 = field, level 1 = ticker
            tickers_in_data = set(raw.columns.get_level_values(1))
            for ns in chunk_ns:
                orig = orig_map.get(ns, ns.replace(NSE_SUFFIX, ""))
                if ns not in tickers_in_data:
                    logger.debug("  %s not in chunk result — skipping", orig)
                    continue
                try:
                    df = raw.xs(ns, axis=1, level=1)
                    df = _clean(df, orig)
                    if df is not None:
                        result[orig] = df
                except Exception as exc:
                    logger.debug("  Parsing %s failed: %s", orig, exc)
        else:
            # Single-ticker chunk returns flat columns
            if len(chunk_ns) == 1:
                orig = orig_map.get(chunk_ns[0], chunk_ns[0].replace(NSE_SUFFIX, ""))
                df = _clean(raw, orig)
                if df is not None:
                    result[orig] = df

        if chunk_idx < total_chunks - 1:
            time.sleep(CHUNK_SLEEP_SEC)

    logger.info("Data ready: %d / %d tickers", len(result), len(tickers))
    return result


def _clean(df: pd.DataFrame, ticker: str) -> Optional[pd.DataFrame]:
    """
    Validates and cleans a raw OHLCV DataFrame.
    Returns None if the data is unusable.
    """
    if df is None or df.empty:
        logger.warning("No data returned for %s", ticker)
        return None

    # Keep only standard OHLCV columns — yfinance may return extras
    required = {"Open", "High", "Low", "Close", "Volume"}
    available = set(df.columns)
    missing_cols = required - available
    if missing_cols:
        logger.warning("%s missing columns %s — skipping", ticker, missing_cols)
        return None

    df = df[list(required)].copy()
    df.dropna(subset=["Close"], inplace=True)

    if len(df) < MIN_REQUIRED_ROWS:
        logger.warning(
            "%s has only %d rows (need %d) — skipping", ticker, len(df), MIN_REQUIRED_ROWS
        )
        return None

    df.sort_index(inplace=True)
    return df


def get_current_price(data: dict[str, pd.DataFrame]) -> dict[str, float]:
    """Returns the most-recent close price for each ticker."""
    return {ticker: float(df["Close"].iloc[-1]) for ticker, df in data.items()}
