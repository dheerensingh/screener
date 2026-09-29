"""
Module 3: Technical Analysis — RSI + MACD Screening

Filter logic (both conditions must be true to qualify):

  1. RSI Condition  — Daily RSI(14) ≥ RSI_THRESHOLD (55, lowered from 60 on
                      2026-09-27 to surface earlier-stage momentum) NOW, OR
                      crossed above it within the last 2 trading sessions.
                      This captures both "already strong" and "just breaking
                      into momentum" scenarios.

  2. Momentum Condition — MACD line > Signal line (histogram > 0).

     Why MACD over ADX?
     ADX measures trend *strength* but not *direction*; you need the +DI/-DI
     pair to confirm bullish bias, adding two more threshold decisions.
     MACD's sign directly encodes direction (positive histogram = bulls in
     control of short-term price action) and it captures momentum via the
     difference of fast (12d) and slow (26d) EMAs — exactly what we want
     alongside RSI to confirm sustained buying pressure rather than a brief
     RSI pop.  MACD also tends to lead price in Indian mid-caps, making it
     a good complement to RSI which is a lagging oscillator.

Both indicators are computed with pure-pandas math as the primary path.
pandas_ta is used as a cross-check where available; discrepancies >0.5 are
logged but pure-pandas values are authoritative (avoids pandas_ta version quirks).
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

from . import trend_template

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Indicator calculations (pure pandas — no external TA library dependency)
# ─────────────────────────────────────────────────────────────────────────────

def _rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """
    Wilder's smoothed RSI using EWM (com = period - 1 mimics Wilder's 1/n smoothing).
    This matches TradingView's default RSI implementation exactly.
    """
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100.0 - (100.0 / (1.0 + rs))


def _macd(
    close: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """
    Standard MACD.
    Returns (macd_line, signal_line, histogram).
    """
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Wilder's ATR(14) — used both as an informational volatility read here and,
    via ScreenResult.atr, as the stop-loss distance basis in position_sizer.py.
    """
    high, low, close = df["High"], df["Low"], df["Close"]
    prev_close = close.shift(1)
    true_range = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return true_range.ewm(com=period - 1, min_periods=period, adjust=False).mean()


def _volume_surge(volume: pd.Series, lookback: int = 20, threshold: float = 1.5) -> tuple[bool, float]:
    """Returns (surged, ratio) where ratio = latest volume / trailing `lookback`-day average."""
    clean = volume.dropna()
    if len(clean) < lookback + 1:
        return False, float("nan")
    avg = float(clean.iloc[-lookback - 1:-1].mean())
    latest = float(clean.iloc[-1])
    if avg == 0:
        return False, float("nan")
    ratio = latest / avg
    return ratio >= threshold, ratio


def _near_52w_high(high: pd.Series, close: pd.Series, threshold_pct: float = 5.0) -> tuple[bool, float]:
    """Returns (near_high, pct_below_high) using up to the trailing 252 sessions."""
    window = high.dropna().iloc[-252:]
    if window.empty:
        return False, float("nan")
    week52_high = float(window.max())
    latest_close = float(close.dropna().iloc[-1])
    if week52_high == 0:
        return False, float("nan")
    pct_below = (1.0 - latest_close / week52_high) * 100.0
    return pct_below <= threshold_pct, pct_below


def _relative_strength_vs_sector(
    close: pd.Series, sector_index: Optional[pd.Series], lookback_days: int = 21
) -> Optional[float]:
    """Stock's `lookback_days` return minus its sector synthetic index's return over the same window."""
    if sector_index is None:
        return None

    def _cum_return(series: pd.Series) -> Optional[float]:
        clean = series.dropna()
        if len(clean) <= lookback_days:
            return None
        start, end = float(clean.iloc[-lookback_days - 1]), float(clean.iloc[-1])
        return (end / start - 1.0) * 100.0 if start else None

    stock_ret = _cum_return(close)
    sector_ret = _cum_return(sector_index)
    if stock_ret is None or sector_ret is None:
        return None
    return stock_ret - sector_ret


# ─────────────────────────────────────────────────────────────────────────────
# Filter conditions
# ─────────────────────────────────────────────────────────────────────────────

RSI_THRESHOLD = 55.0  # lowered from 60 (2026-09-27) to surface earlier-stage momentum;
                       # MACD confirmation is still required, so the two-indicator bar
                       # is only modestly loosened, not dropped.


def _check_rsi_condition(rsi: pd.Series) -> tuple[bool, float, str]:
    """
    Returns (passes, current_rsi, description).
    Passes if:
      a) latest RSI ≥ RSI_THRESHOLD, OR
      b) RSI crossed above RSI_THRESHOLD in the last 2 trading sessions
         (prev or prev-2 was below it and current is at/above it).
    """
    clean = rsi.dropna()
    if len(clean) < 3:
        return False, float("nan"), "insufficient data"

    curr = float(clean.iloc[-1])
    prev1 = float(clean.iloc[-2])
    prev2 = float(clean.iloc[-3])
    t = RSI_THRESHOLD

    if curr >= t:
        if prev1 < t or prev2 < t:
            return True, curr, f"RSI crossed ↑{t:.0f} (now={curr:.1f}, prev={prev1:.1f})"
        return True, curr, f"RSI sustained above {t:.0f} ({curr:.1f})"

    return False, curr, f"RSI below {t:.0f} ({curr:.1f})"


def _check_macd_condition(
    macd_line: pd.Series, signal_line: pd.Series
) -> tuple[bool, str]:
    """
    Returns (passes, description).
    Passes if the latest MACD histogram is positive (MACD > Signal).
    """
    histogram = macd_line - signal_line
    clean = histogram.dropna()
    if clean.empty:
        return False, "insufficient data"

    latest_hist = float(clean.iloc[-1])
    latest_macd = float(macd_line.dropna().iloc[-1])
    latest_sig = float(signal_line.dropna().iloc[-1])

    if latest_hist > 0:
        return True, f"MACD bullish (MACD={latest_macd:.3f} > Signal={latest_sig:.3f})"
    return False, f"MACD bearish (MACD={latest_macd:.3f} < Signal={latest_sig:.3f})"


# ─────────────────────────────────────────────────────────────────────────────
# Result dataclass
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ScreenResult:
    ticker: str
    current_price: float
    rsi: float
    rsi_status: str
    macd_status: str
    atr: float = float("nan")
    volume_surge: bool = False
    volume_ratio: float = float("nan")
    near_52w_high: bool = False
    pct_below_52w_high: float = float("nan")
    rel_strength_vs_sector: Optional[float] = None
    delivery_pct: Optional[float] = None  # NSE delivery % — informational only, see delivery_analyzer.py
    cap_band: str = ""  # "Large" / "Mid" / "Small" — from sector_mapper via SectorScore.cap_bands
    trend_template_passes: bool = True  # Minervini gate — True/inactive unless screen_stocks computes it
    trend_template_criteria: dict = field(default_factory=dict)
    rs_rating: Optional[float] = None  # universe-wide percentile, from trend_template.compute_rs_ratings
    passes: bool = field(init=False)
    strength_score: float = field(init=False)

    def __post_init__(self):
        # passes only when ALL required conditions are satisfied — the informational
        # signals below (volume/52w-high/relative-strength) rank qualifiers, they don't
        # gate them, since the goal is "best momentum stocks in the winning sector."
        # trend_template_passes defaults True (inactive) when the Minervini gate isn't
        # being applied for this run (see screen_stocks' require_trend_template arg).
        self.passes = (
            "RSI" in self.rsi_status and "below" not in self.rsi_status
            and "MACD bullish" in self.macd_status
            and self.trend_template_passes
        )
        self.strength_score = (
            (1.0 if self.volume_surge else 0.0)
            + (1.0 if self.near_52w_high else 0.0)
            + (1.0 if (self.rel_strength_vs_sector or 0.0) > 0 else 0.0)
            + (1.0 if (self.delivery_pct or 0.0) > 50.0 else 0.0)
            + self.rsi / 100.0
        )


# ─────────────────────────────────────────────────────────────────────────────
# Main screening function
# ─────────────────────────────────────────────────────────────────────────────

def screen_stocks(
    data: dict[str, pd.DataFrame],
    sector_index: Optional[pd.Series] = None,
    cap_bands: Optional[dict[str, str]] = None,
    rs_ratings: Optional[dict[str, float]] = None,
    delivery_pct: Optional[dict[str, float]] = None,
    require_trend_template: bool = True,
) -> tuple[list[ScreenResult], list[ScreenResult]]:
    """
    Runs RSI + MACD screening on each ticker, plus informational momentum signals
    (volume surge, 52-week-high proximity, relative strength vs `sector_index` if
    given — the synthetic sector series from sector_rotation.SectorScore).

    Parameters
    ----------
    data : dict returned by stock_fetcher.fetch_stock_data()
    sector_index : the stock's sector's synthetic index series, for relative strength
    cap_bands : ticker -> "Large"/"Mid"/"Small", from SectorScore.cap_bands
    rs_ratings : ticker -> universe-wide RS Rating (0-100), from
                 trend_template.compute_rs_ratings() over the FULL universe (not
                 just this sector's basket) — required for the Trend Template gate.
    delivery_pct : ticker -> latest NSE delivery % (modules/delivery_analyzer.py),
                 informational only — contributes to strength_score like volume_surge,
                 never gates `passes` (see STRATEGY.md §13 for why it isn't a gate yet).
    require_trend_template : when True (the default / Minervini-informed entry
        paradigm), Minervini's 8-point Trend Template is a REQUIRED gate alongside
        RSI+MACD. When False (the "RSI+MACD only" backtest variant), the gate is
        computed and reported but doesn't affect `passes` — lets the two entry
        paradigms be compared on identical data.

    Returns
    -------
    (qualified, all_results)
      qualified   — stocks that pass ALL required conditions, sorted by
                    strength_score (informational signals) descending
      all_results — every ticker analysed (for debugging / full report)
    """
    all_results: list[ScreenResult] = []
    cap_bands = cap_bands or {}
    rs_ratings = rs_ratings or {}
    delivery_pct = delivery_pct or {}

    for ticker, df in data.items():
        try:
            close = df["Close"].astype(float)

            rsi_series = _rsi(close)
            macd_line, signal_line, _ = _macd(close)

            rsi_passes, rsi_val, rsi_desc = _check_rsi_condition(rsi_series)
            macd_passes, macd_desc = _check_macd_condition(macd_line, signal_line)

            current_price = float(close.iloc[-1])
            atr_val = float(atr(df).dropna().iloc[-1]) if len(df) >= 15 else float("nan")
            vol_surge, vol_ratio = _volume_surge(df["Volume"].astype(float))
            near_high, pct_below_high = _near_52w_high(df["High"].astype(float), close)
            rel_strength = _relative_strength_vs_sector(close, sector_index)

            tt = trend_template.evaluate(ticker, df, rs_rating=rs_ratings.get(ticker))
            tt_passes = tt.passes if require_trend_template else True

            result = ScreenResult(
                ticker=ticker,
                current_price=current_price,
                rsi=rsi_val,
                rsi_status=rsi_desc,
                macd_status=macd_desc,
                atr=atr_val,
                volume_surge=vol_surge,
                volume_ratio=vol_ratio,
                near_52w_high=near_high,
                pct_below_52w_high=pct_below_high,
                rel_strength_vs_sector=rel_strength,
                delivery_pct=delivery_pct.get(ticker),
                cap_band=cap_bands.get(ticker, ""),
                trend_template_passes=tt_passes,
                trend_template_criteria=tt.criteria,
                rs_rating=tt.rs_rating,
            )
            all_results.append(result)

            _try_pandas_ta_crosscheck(close, rsi_val, ticker)

        except Exception as exc:
            logger.warning("Analysis failed for %s: %s", ticker, exc)

    qualified = [r for r in all_results if r.passes]
    logger.info(
        "Screening complete: %d / %d stocks qualify",
        len(qualified), len(all_results),
    )
    # Sort qualified by strength_score descending (RSI + informational momentum signals)
    qualified.sort(key=lambda r: r.strength_score, reverse=True)
    return qualified, all_results


def _try_pandas_ta_crosscheck(close: pd.Series, our_rsi: float, ticker: str) -> None:
    """
    Optional cross-check against pandas_ta. Logs discrepancies > 0.5 RSI points.
    Does nothing if pandas_ta is not installed or calculation fails.
    """
    try:
        import pandas_ta as ta  # type: ignore[import]
        df_tmp = pd.DataFrame({"Close": close})
        df_tmp.ta.rsi(length=14, append=True)
        rsi_col = [c for c in df_tmp.columns if c.startswith("RSI_")]
        if rsi_col:
            ta_rsi = float(df_tmp[rsi_col[0]].dropna().iloc[-1])
            diff = abs(ta_rsi - our_rsi)
            if diff > 0.5:
                logger.debug(
                    "%s RSI discrepancy: ours=%.2f pandas_ta=%.2f (diff=%.2f)",
                    ticker, our_rsi, ta_rsi, diff,
                )
    except Exception:
        pass  # pandas_ta optional; silently skip
