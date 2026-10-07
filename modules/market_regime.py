"""
Module: Market Regime Filter

Minervini (and trend-following practice generally) ties new-entry aggressiveness
to the *general market's* health, not just the individual stock's or sector's —
momentum/trend systems are well documented to get chopped up in choppy or
declining broad markets. This module reuses the same `^CRSLDX` (Nifty500)
benchmark series `sector_rotation.py` already fetches for relative-strength math.

**v2 (2026-09-27)**: the original price-vs-200-SMA check missed a real 5-month
chop (Nov 2022 - Apr 2023) that drove the backtest's worst drawdown — the
Nifty500 stayed above a *rising* 200-day SMA the entire period, so the v1
filter said "uptrend" throughout, while price still whipsawed sideways and
stopped out a string of entries. Investigated with the cached 5y data before
changing anything: during that chop, the 200-SMA's own 21-day rate of change
averaged only 0.5% (max 1.2%); during a genuinely healthy trending period
(Nov 2023-Feb 2024) it never dropped below 1.0%. That's a clean separation, so
a minimum SMA rate-of-change is now a second required condition, not just
price > SMA. Market breadth (% of the universe above their own 50-day SMA) was
also tested on the same chop window — real signal, but noisier (swung from
~24% to ~81% within the same episode) — kept as an informational/logged value
only, not a hard gate, to avoid overfitting a filter to one historical episode
on a signal that didn't cleanly separate the two windows.

**Informational only since 2026-10-01 — no longer gates entries** (STRATEGY.md
§16). On the 8.8-year backtest, turning the gate off nearly doubled CAGR (+7.3%
-> +13.9%) for the live Trend Template paradigm while max drawdown barely moved
(-48.0% -> -48.9%): each stock must already pass its own Trend Template, which
does the per-stock version of this job. Still evaluated and shown in the report
as market context. Those figures came from a backtest with an equity-accounting
bug; re-measured on 2026-10-07 (STRATEGY.md §17) the gate costs ~4pp of CAGR
(+12.6% -> +8.5%) and saves only ~2-7pp of drawdown, so off still stands.
"""

import logging
from dataclasses import dataclass
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

REGIME_SMA_PERIOD = 200
ROC_LOOKBACK_DAYS = 21
MIN_SMA_ROC_PCT = 1.0  # % rise in the 200-SMA itself over ROC_LOOKBACK_DAYS — see module docstring


@dataclass
class MarketRegime:
    in_uptrend: bool
    benchmark_price: float
    benchmark_sma: float
    sma_roc_pct: float = float("nan")
    breadth_pct: Optional[float] = None
    reason: str = ""


def evaluate(
    benchmark_close: pd.Series,
    sma_period: int = REGIME_SMA_PERIOD,
    breadth_pct: Optional[float] = None,
) -> MarketRegime:
    """
    Uptrend requires BOTH: the benchmark's latest close above its own trailing
    `sma_period`-day SMA, AND that SMA itself rising at least `MIN_SMA_ROC_PCT`
    over the last `ROC_LOOKBACK_DAYS` sessions — filters out the "price
    technically above a flat/barely-rising SMA" chop case the v1 filter missed.

    `breadth_pct` (optional): % of the stock universe trading above their own
    50-day SMA, computed by the caller (e.g. from sector_rotation's already-
    fetched bulk data — see main.py) — logged for visibility but not gating,
    per the module docstring's reasoning.
    """
    close = benchmark_close.dropna()
    min_rows = sma_period + ROC_LOOKBACK_DAYS
    if len(close) < min_rows:
        return MarketRegime(
            in_uptrend=True,  # fail open: don't block entries over insufficient history
            benchmark_price=float(close.iloc[-1]) if len(close) else float("nan"),
            benchmark_sma=float("nan"),
            breadth_pct=breadth_pct,
            reason=f"Insufficient benchmark history ({len(close)} rows < {min_rows}) — regime check skipped",
        )

    price = float(close.iloc[-1])
    sma_series = close.rolling(sma_period, min_periods=sma_period).mean()
    sma = float(sma_series.iloc[-1])
    sma_prev = float(sma_series.iloc[-1 - ROC_LOOKBACK_DAYS])
    sma_roc_pct = (sma / sma_prev - 1.0) * 100.0 if sma_prev else 0.0

    price_above_sma = price > sma
    sma_rising_enough = sma_roc_pct >= MIN_SMA_ROC_PCT
    in_uptrend = price_above_sma and sma_rising_enough

    reason = (
        f"price {'>' if price_above_sma else '<='} {sma_period}d SMA ({price:.1f} vs {sma:.1f}); "
        f"SMA {ROC_LOOKBACK_DAYS}d RoC {sma_roc_pct:+.2f}% ({'meets' if sma_rising_enough else 'below'} "
        f"{MIN_SMA_ROC_PCT:.1f}% threshold)"
    )
    if breadth_pct is not None:
        reason += f"; breadth {breadth_pct:.0f}% of universe above own 50d SMA (informational only)"
    reason += " — " + ("broad market in an uptrend" if in_uptrend else "broad market not in a clean uptrend")

    return MarketRegime(
        in_uptrend=in_uptrend, benchmark_price=price, benchmark_sma=sma,
        sma_roc_pct=sma_roc_pct, breadth_pct=breadth_pct, reason=reason,
    )
