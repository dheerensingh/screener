"""
Module: Minervini Trend Template + Relative Strength Rating

Implements Mark Minervini's published 8-point "Trend Template" for confirming a
genuine Stage-2 uptrend, plus a Relative Strength (RS) Rating — a percentile rank
of each stock's blended trailing return against the *full universe* (distinct
from sector_rotation.py's relative strength, which compares a sector's synthetic
index to the Nifty500 benchmark, not one stock to every other stock).

Research note (2026-09-27): reviewed `marco-hui-95/vcp_screener`, a Python
screener implementing these same published rules (two-time U.S. Investing
Championship winner — a real, documented track record, not a blog strategy).
See CLAUDE.md for the full reasoning on what was adopted here vs deliberately
deferred (VCP pattern-timing).

This is a REQUIRED gate alongside RSI/MACD in technical_analyzer.screen_stocks(),
not a replacement: RSI/MACD are the short-term timing trigger; Trend Template
confirms the longer-term structural trend is real before that trigger is trusted.
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

RS_RATING_THRESHOLD = 70.0
PCT_ABOVE_52W_LOW_MIN = 30.0
PCT_BELOW_52W_HIGH_MAX = 25.0
SMA_TREND_LOOKBACK_DAYS = 21  # ~1 month — "200-day SMA rising for >=1 month"
MIN_HISTORY_ROWS = 230  # 200-day SMA + SMA_TREND_LOOKBACK_DAYS margin


@dataclass
class TrendTemplateResult:
    ticker: str
    passes: bool
    criteria: dict[str, bool] = field(default_factory=dict)  # name -> pass/fail, for report transparency
    rs_rating: Optional[float] = None


def _sma(close: pd.Series, period: int) -> pd.Series:
    return close.rolling(period, min_periods=period).mean()


def evaluate(ticker: str, df: pd.DataFrame, rs_rating: Optional[float] = None) -> TrendTemplateResult:
    """
    Evaluates Minervini's 8-point Trend Template on a single stock's OHLCV.
    `rs_rating` (0-100 percentile vs the full universe) must come from
    `compute_rs_ratings()` below, computed once per run across the whole universe.
    """
    close = df["Close"].astype(float).dropna()
    if len(close) < MIN_HISTORY_ROWS:
        return TrendTemplateResult(ticker=ticker, passes=False, criteria={"insufficient_history": False})

    sma50 = _sma(close, 50)
    sma150 = _sma(close, 150)
    sma200 = _sma(close, 200)

    price = float(close.iloc[-1])
    s50, s150, s200 = float(sma50.iloc[-1]), float(sma150.iloc[-1]), float(sma200.iloc[-1])
    s200_prev = float(sma200.iloc[-1 - SMA_TREND_LOOKBACK_DAYS])

    window = close.iloc[-252:]
    week52_high, week52_low = float(window.max()), float(window.min())
    pct_above_low = (price / week52_low - 1.0) * 100.0 if week52_low else float("nan")
    pct_below_high = (1.0 - price / week52_high) * 100.0 if week52_high else float("nan")

    criteria = {
        "price_above_150_200_sma": price > s150 and price > s200,
        "sma150_above_sma200": s150 > s200,
        "sma200_trending_up": s200 > s200_prev,
        "sma50_above_150_200": s50 > s150 and s50 > s200,
        "price_above_sma50": price > s50,
        "price_30pct_above_52w_low": pct_above_low >= PCT_ABOVE_52W_LOW_MIN,
        "price_within_25pct_of_52w_high": pct_below_high <= PCT_BELOW_52W_HIGH_MAX,
        "rs_rating_ge_70": (rs_rating is not None) and (rs_rating >= RS_RATING_THRESHOLD),
    }

    return TrendTemplateResult(
        ticker=ticker, passes=all(criteria.values()), criteria=criteria, rs_rating=rs_rating,
    )


def _cum_return(close: pd.Series, lookback_days: int) -> Optional[float]:
    if len(close) <= lookback_days:
        return None
    start, end = float(close.iloc[-lookback_days - 1]), float(close.iloc[-1])
    return (end / start - 1.0) * 100.0 if start else None


def compute_rs_ratings(data: dict[str, pd.DataFrame]) -> dict[str, float]:
    """
    Percentile rank (0-100) of each ticker's blended trailing return against
    every other ticker in `data` — a universe-wide Relative Strength Rating.
    Blend weights recent performance more heavily (40/30/30 over 3M/6M/~11M),
    in the spirit of IBD's own emphasis on the most recent quarter.

    Longest lookback is 230 sessions, not a full 252 (~12mo): a "1y" yfinance
    fetch returns exactly 252 rows, and `_cum_return` needs *more* rows than
    its lookback (to have a valid "before" price) — 252 would silently return
    None for every single ticker. 230 leaves real margin inside a 1y fetch.
    """
    returns: dict[str, float] = {}
    for ticker, df in data.items():
        close = df["Close"].astype(float).dropna()
        r3, r6, r11 = _cum_return(close, 63), _cum_return(close, 126), _cum_return(close, 230)
        if r3 is None or r6 is None or r11 is None:
            continue
        returns[ticker] = 0.4 * r3 + 0.3 * r6 + 0.3 * r11

    if not returns:
        return {}
    ranked = pd.Series(returns).rank(pct=True) * 100.0
    return ranked.to_dict()
