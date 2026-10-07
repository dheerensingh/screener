"""
Module 1: Sector Rotation

Ranks NSE sectors by relative strength vs the broad market, to decide which
sector(s) the rest of the pipeline should screen for momentum stocks in.

Official NSE sector-index tickers are mostly stale on yfinance (confirmed in a
Phase 0 spike, 2026-09-27 — only ^CNXIT / ^CNXPHARMA / ^NSEBANK returned live
multi-day data out of 13 tried; see CLAUDE.md). Instead of depending on those,
each sector's relative strength is computed from a *synthetic* sector index:
an equal-weight average of the normalized close prices of that sector's
member stocks (sector_mapper.build_universe(), sourced from NSE's own Nifty500
+ thematic index CSVs — see sector_mapper.py), using the same yfinance
pipeline already used for individual stocks (stock_fetcher.fetch_stock_data).

Every sector's members are fetched in ONE bulk call (not one call per sector)
and then ranked by liquidity (average daily traded value) and capped at
sector_mapper.MAX_STOCKS_PER_SECTOR — both for predictable runtime across a
~500-stock universe and because a "momentum signal" on an illiquid stock
isn't realistically tradeable on a ₹5L account (see CLAUDE.md).
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd
import yfinance as yf

from . import trend_template
from .sector_mapper import MAX_STOCKS_PER_SECTOR, build_universe, group_by_sector
from .stock_fetcher import drop_unsettled_bar, fetch_stock_data

logger = logging.getLogger(__name__)

BENCHMARK_TICKER = "^CRSLDX"  # Nifty 500 — confirmed live on yfinance in the Phase 0 spike
LOOKBACKS: dict[str, int] = {"1M": 21, "3M": 63, "6M": 126}
LOOKBACK_WEIGHTS: dict[str, float] = {"1M": 0.5, "3M": 0.3, "6M": 0.2}
TOP_N_SECTORS = 4

# Minimum average daily traded value (₹) for a stock to be considered at all —
# below this, a signal isn't realistically executable at any meaningful size.
MIN_AVG_DAILY_VALUE_INR = 2_00_00_000  # ₹2 crore/day
LIQUIDITY_LOOKBACK_DAYS = 20


@dataclass
class SectorScore:
    sector: str
    returns: dict[str, float]              # lookback label -> sector return %
    relative_strength: dict[str, float]    # lookback label -> sector return - benchmark return
    score: float                           # weighted composite relative-strength score
    num_stocks: int
    data: dict[str, pd.DataFrame] = field(repr=False)       # ticker -> OHLCV, reused downstream
    synthetic_index: pd.Series = field(repr=False)          # equal-weight normalized sector series
    cap_bands: dict[str, str] = field(default_factory=dict, repr=False)  # ticker -> Large/Mid/Small


@dataclass
class RotationResult:
    scores: list[SectorScore]
    rs_ratings: dict[str, float] = field(default_factory=dict, repr=False)  # universe-wide, from trend_template
    benchmark: pd.Series = field(default_factory=lambda: pd.Series(dtype=float), repr=False)
    breadth_pct: float = float("nan")  # % of universe above their own 50-day SMA today — see market_regime.py


def _cumulative_return(series: pd.Series, lookback_days: int) -> float:
    """% change from `lookback_days` sessions ago to the latest session."""
    clean = series.dropna()
    if len(clean) <= lookback_days:
        return float("nan")
    start = float(clean.iloc[-lookback_days - 1])
    end = float(clean.iloc[-1])
    if start == 0:
        return float("nan")
    return (end / start - 1.0) * 100.0


def _avg_daily_value(df: pd.DataFrame, lookback: int = LIQUIDITY_LOOKBACK_DAYS) -> float:
    """Trailing average of Close x Volume (₹) — a proxy for how tradeable a stock is."""
    value = (df["Close"].astype(float) * df["Volume"].astype(float)).dropna()
    if value.empty:
        return 0.0
    return float(value.iloc[-lookback:].mean())


def _synthetic_sector_series(data: dict[str, pd.DataFrame]) -> pd.Series:
    """Equal-weight average of each basket member's normalized close price."""
    normalized = []
    for df in data.values():
        close = df["Close"].dropna()
        if close.empty:
            continue
        normalized.append(close / close.iloc[0])
    if not normalized:
        return pd.Series(dtype=float)
    return pd.concat(normalized, axis=1).mean(axis=1)


def _compute_breadth(all_data: dict[str, pd.DataFrame], sma_period: int = 50) -> float:
    """
    % of the universe currently trading above their own `sma_period`-day SMA —
    an informational market-breadth signal for market_regime.py (see its
    docstring: tested against a known chop episode, found noisier than the
    SMA rate-of-change check, so kept informational rather than a hard gate).
    """
    above = 0
    total = 0
    for df in all_data.values():
        close = df["Close"].astype(float)
        if len(close) < sma_period:
            continue
        sma = close.rolling(sma_period, min_periods=sma_period).mean()
        if sma.iloc[-1] != sma.iloc[-1]:  # NaN
            continue
        total += 1
        if close.iloc[-1] > sma.iloc[-1]:
            above += 1
    return (above / total * 100.0) if total else float("nan")


def _fetch_benchmark(period: str) -> pd.Series:
    df = yf.download(BENCHMARK_TICKER, period=period, interval="1d", auto_adjust=True, progress=False)
    if df.empty:
        raise RuntimeError(f"No benchmark data returned for {BENCHMARK_TICKER}")
    close = df["Close"]
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]
    return drop_unsettled_bar(close.dropna())


def rank_sectors(period: str = "1y") -> RotationResult:
    """
    Builds the full NSE-sourced universe, fetches 1y OHLCV for every member in
    ONE bulk call, ranks each sector's stocks by liquidity and caps at
    MAX_STOCKS_PER_SECTOR, builds a synthetic sector index from the surviving
    members, scores relative strength vs the Nifty500 benchmark over 1M/3M/6M
    lookbacks (weighted 50/30/20, favouring recent momentum), and returns
    every sector ranked best-first — plus the universe-wide RS Rating for every
    stock (trend_template.compute_rs_ratings, computed once here from the full
    bulk-fetched `all_data` before per-sector capping — an RS Rating must be
    against the whole universe, not just a 40-stock sector subset) and the raw
    benchmark series (for market_regime.py's own trend check on the benchmark).
    """
    logger.info("Building sector universe from NSE (Nifty500 + thematic overrides) ...")
    universe = build_universe()
    grouped = group_by_sector(universe)
    cap_band_by_ticker = {u.ticker: u.cap_band for u in universe}

    all_tickers = sorted({u.ticker for u in universe})
    logger.info(
        "Fetching 1y OHLCV for %d unique tickers across %d sectors (one bulk fetch) ...",
        len(all_tickers), len(grouped),
    )
    all_data = fetch_stock_data(all_tickers, period=period)

    logger.info("Computing universe-wide RS Ratings (Minervini-style) ...")
    rs_ratings = trend_template.compute_rs_ratings(all_data)

    breadth_pct = _compute_breadth(all_data)

    logger.info("Fetching Nifty500 benchmark (%s) ...", BENCHMARK_TICKER)
    benchmark = _fetch_benchmark(period)
    benchmark_returns = {label: _cumulative_return(benchmark, days) for label, days in LOOKBACKS.items()}

    scores: list[SectorScore] = []
    for sector, members in grouped.items():
        candidates = [m.ticker for m in members if m.ticker in all_data]
        if not candidates:
            continue

        ranked = sorted(candidates, key=lambda t: _avg_daily_value(all_data[t]), reverse=True)
        liquid = [t for t in ranked if _avg_daily_value(all_data[t]) >= MIN_AVG_DAILY_VALUE_INR]
        if not liquid:
            logger.warning(
                "No liquid stocks in %s (min ₹%.1fcr/day avg traded value) — skipping",
                sector, MIN_AVG_DAILY_VALUE_INR / 1e7,
            )
            continue
        selected = liquid[:MAX_STOCKS_PER_SECTOR]

        data = {t: all_data[t] for t in selected}
        synthetic = _synthetic_sector_series(data)
        if synthetic.empty:
            logger.warning("Could not build synthetic index for %s — skipping", sector)
            continue

        returns = {label: _cumulative_return(synthetic, days) for label, days in LOOKBACKS.items()}
        rel_strength = {
            label: returns[label] - benchmark_returns[label]
            for label in LOOKBACKS
            if not pd.isna(returns[label]) and not pd.isna(benchmark_returns[label])
        }
        if not rel_strength:
            logger.warning("Insufficient history to score %s — skipping", sector)
            continue

        weight_sum = sum(LOOKBACK_WEIGHTS[label] for label in rel_strength)
        composite = sum(rel_strength[label] * LOOKBACK_WEIGHTS[label] for label in rel_strength) / weight_sum

        scores.append(SectorScore(
            sector=sector,
            returns=returns,
            relative_strength=rel_strength,
            score=composite,
            num_stocks=len(data),
            data=data,
            synthetic_index=synthetic,
            cap_bands={t: cap_band_by_ticker.get(t, "Mid") for t in selected},
        ))

    scores.sort(key=lambda s: s.score, reverse=True)
    logger.info("Sector ranking complete: %s", [f"{s.sector}({s.score:+.1f}, n={s.num_stocks})" for s in scores])
    return RotationResult(scores=scores, rs_ratings=rs_ratings, benchmark=benchmark, breadth_pct=breadth_pct)


def leading_sectors(scores: list[SectorScore], top_n: int = TOP_N_SECTORS) -> list[SectorScore]:
    """Top-N sectors by composite relative-strength score."""
    return scores[:top_n]
