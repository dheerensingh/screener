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
) -> dict[str, pd.DataFrame]:
    """
    Fetches OHLCV data for all tickers.

    Parameters
    ----------
    tickers : list of NSE ticker symbols (without .NS suffix)
    period  : yfinance period string — "6mo", "1y", etc.
    interval: "1d" for daily bars

    Returns
    -------
    dict mapping ticker (original, without suffix) → DataFrame with columns
    [Open, High, Low, Close, Volume].  Tickers with insufficient data are
    omitted with a logged warning.
    """
    ns_tickers = [_to_ns_ticker(t) for t in tickers]
    result: dict[str, pd.DataFrame] = {}

    # ── Batch download ────────────────────────────────────────────────────────
    try:
        raw = yf.download(
            tickers=ns_tickers,
            period=period,
            interval=interval,
            auto_adjust=True,
            progress=False,
            threads=True,
            group_by="ticker",
        )
        logger.info("Batch download complete for %d symbols", len(ns_tickers))
    except Exception as exc:
        logger.error("yfinance batch download failed: %s — attempting per-ticker fallback", exc)
        raw = pd.DataFrame()

    # ── Parse batch result ────────────────────────────────────────────────────
    if not raw.empty:
        if isinstance(raw.columns, pd.MultiIndex):
            # Multi-ticker download returns a MultiIndex: (field, ticker)
            for orig, ns in zip(tickers, ns_tickers):
                try:
                    df = raw[ns] if ns in raw.columns.get_level_values(1) else raw.xs(ns, axis=1, level=1)
                    df = _clean(df, orig)
                    if df is not None:
                        result[orig] = df
                except Exception as exc:
                    logger.debug("Parsing batch data for %s failed: %s", orig, exc)
        else:
            # Single-ticker batch returns flat columns
            if len(tickers) == 1:
                df = _clean(raw, tickers[0])
                if df is not None:
                    result[tickers[0]] = df

    # ── Per-ticker fallback for any missing symbols ────────────────────────────
    missing = [t for t in tickers if t not in result]
    if missing:
        logger.info("Fetching %d symbols individually (batch miss)", len(missing))
        for orig in missing:
            ns = _to_ns_ticker(orig)
            try:
                ticker_obj = yf.Ticker(ns)
                df = ticker_obj.history(period=period, interval=interval, auto_adjust=True)
                df = _clean(df, orig)
                if df is not None:
                    result[orig] = df
                    logger.debug("Per-ticker fetch succeeded: %s", orig)
            except Exception as exc:
                logger.warning("Per-ticker fetch failed for %s: %s", orig, exc)

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
