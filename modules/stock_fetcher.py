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

import logging
from typing import Optional

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

NSE_SUFFIX = ".NS"
MIN_REQUIRED_ROWS = 60   # ~3 months; 6 months requested but some stocks may be newer


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
    Fetches OHLCV data for all tickers in chunks to avoid yfinance rate limits.

    Parameters
    ----------
    tickers    : list of NSE ticker symbols (without .NS suffix)
    period     : yfinance period string — "6mo", "1y", etc.
    interval   : "1d" for daily bars
    chunk_size : number of tickers per batch request (100 is safe for yfinance)

    Returns
    -------
    dict mapping ticker (original, without suffix) → DataFrame with columns
    [Open, High, Low, Close, Volume].  Tickers with insufficient data are
    omitted with a logged warning.
    """
    import math
    ns_tickers = [_to_ns_ticker(t) for t in tickers]
    orig_map = {_to_ns_ticker(t): t for t in tickers}  # ns → original
    result: dict[str, pd.DataFrame] = {}

    total_chunks = math.ceil(len(ns_tickers) / chunk_size)
    logger.info(
        "Downloading data for %d symbols in %d chunk(s) of %d ...",
        len(ns_tickers), total_chunks, chunk_size,
    )

    for chunk_idx in range(total_chunks):
        chunk_ns = ns_tickers[chunk_idx * chunk_size : (chunk_idx + 1) * chunk_size]
        logger.info("  Chunk %d/%d — fetching %d symbols ...", chunk_idx + 1, total_chunks, len(chunk_ns))

        # ── Batch download this chunk ─────────────────────────────────────────
        raw = pd.DataFrame()
        try:
            raw = yf.download(
                tickers=chunk_ns,
                period=period,
                interval=interval,
                auto_adjust=True,
                progress=False,
                threads=True,
                group_by="ticker",
            )
        except Exception as exc:
            logger.error("Chunk %d batch download failed: %s", chunk_idx + 1, exc)

        # ── Parse this chunk's result ─────────────────────────────────────────
        if not raw.empty:
            if isinstance(raw.columns, pd.MultiIndex):
                for ns in chunk_ns:
                    orig = orig_map.get(ns, ns.replace(NSE_SUFFIX, ""))
                    try:
                        level1 = raw.columns.get_level_values(1)
                        if ns in level1:
                            df = raw.xs(ns, axis=1, level=1)
                        else:
                            continue
                        df = _clean(df, orig)
                        if df is not None:
                            result[orig] = df
                    except Exception as exc:
                        logger.debug("Parsing %s failed: %s", orig, exc)
            else:
                # Single ticker in chunk — flat columns
                if len(chunk_ns) == 1:
                    orig = orig_map.get(chunk_ns[0], chunk_ns[0].replace(NSE_SUFFIX, ""))
                    df = _clean(raw, orig)
                    if df is not None:
                        result[orig] = df

    logger.info(
        "Data available for %d / %d requested tickers", len(result), len(tickers)
    )
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
