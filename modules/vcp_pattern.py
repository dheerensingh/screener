"""
Module: VCP (Volatility Contraction Pattern) Entry Timing

Minervini's precise entry-timing technique: a stock already in an established
uptrend forms a "base" of progressively tighter pullbacks ("contractions") on
progressively lighter volume, and the buy trigger is a breakout above the
base's resistance ("pivot") on above-average volume — not just "RSI crossed
55." The point: a precisely-timed entry (buying the actual breakout, not an
already-extended move) plus a naturally tight stop (the base's own low).

Research note (2026-09-28): this is a stated, pragmatic simplification of
"true" VCP, not full fractal swing-point detection (finding exact swing
highs/lows and merging them into a contraction sequence is meaningfully more
complex engineering — deliberately deferred in Task 3, see CLAUDE.md).
Instead: split the trailing base window into fixed-size consecutive
sub-windows and check each one's range% and average volume are decreasing —
captures the core "tightening base on lighter volume" mechanism at a
fraction of the engineering cost. Can be refined to genuine swing-point
detection later if this shows promise in the backtest.

Only meaningful for stocks that already pass trend_template.evaluate() — VCP
is a consolidation *within* an existing Stage-2 uptrend, not a standalone
signal (see backtest/simulator.py's "vcp_breakout" entry paradigm, which
requires both).
"""

import logging
from dataclasses import dataclass, field

import pandas as pd

logger = logging.getLogger(__name__)

LOOKBACK_DAYS = 45          # the base window, ending YESTERDAY (today is the potential breakout day)
NUM_CONTRACTIONS = 3
CONTRACTION_LEN = LOOKBACK_DAYS // NUM_CONTRACTIONS  # 15 trading days each
DEPTH_TOLERANCE_PCT = 10.0   # allow each contraction's range% to be up to this much *larger* than the prior
VOLUME_TOLERANCE_PCT = 10.0  # same tolerance for the volume-decline check
VOLUME_AVG_DAYS = 20


@dataclass
class VCPResult:
    ticker: str
    is_valid_base: bool
    contraction_depths_pct: list[float] = field(default_factory=list)
    contraction_volumes: list[float] = field(default_factory=list)
    pivot: float = float("nan")
    base_low: float = float("nan")
    breakout_today: bool = False


def evaluate(ticker: str, df: pd.DataFrame) -> VCPResult:
    """
    Evaluates whether the base ending yesterday is a valid, tightening VCP,
    and whether today's bar is a valid breakout above it. `df` must include
    today as its last row — the base itself deliberately excludes it (a
    pivot computed from a window that includes today's own high would make
    "today breaks above the pivot" circular).
    """
    min_rows = LOOKBACK_DAYS + VOLUME_AVG_DAYS + 1
    if len(df) < min_rows:
        return VCPResult(ticker=ticker, is_valid_base=False)

    base = df.iloc[-(LOOKBACK_DAYS + 1):-1]
    today = df.iloc[-1]

    chunks = [base.iloc[i * CONTRACTION_LEN:(i + 1) * CONTRACTION_LEN] for i in range(NUM_CONTRACTIONS)]

    depths: list[float] = []
    volumes: list[float] = []
    for chunk in chunks:
        high = float(chunk["High"].astype(float).max())
        low = float(chunk["Low"].astype(float).min())
        depths.append((high - low) / high * 100.0 if high else float("nan"))
        volumes.append(float(chunk["Volume"].astype(float).mean()))

    depths_decreasing = all(
        depths[i] <= depths[i - 1] * (1 + DEPTH_TOLERANCE_PCT / 100.0) for i in range(1, len(depths))
    ) and depths[-1] < depths[0]
    volumes_decreasing = all(
        volumes[i] <= volumes[i - 1] * (1 + VOLUME_TOLERANCE_PCT / 100.0) for i in range(1, len(volumes))
    ) and volumes[-1] < volumes[0]
    is_valid_base = depths_decreasing and volumes_decreasing

    final_chunk = chunks[-1]
    pivot = float(final_chunk["High"].astype(float).max())
    base_low = float(final_chunk["Low"].astype(float).min())

    today_close = float(today["Close"])
    today_volume = float(today["Volume"])
    avg_volume_20 = float(df["Volume"].astype(float).iloc[-(VOLUME_AVG_DAYS + 1):-1].mean())

    breakout_today = (
        is_valid_base
        and today_close > pivot
        and avg_volume_20 == avg_volume_20  # not NaN
        and today_volume >= avg_volume_20
    )

    return VCPResult(
        ticker=ticker, is_valid_base=is_valid_base,
        contraction_depths_pct=depths, contraction_volumes=volumes,
        pivot=pivot, base_low=base_low, breakout_today=breakout_today,
    )
