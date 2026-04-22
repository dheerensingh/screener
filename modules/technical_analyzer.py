"""
Module 3: Technical Analysis — RSI + MACD Screening

Filter logic (both conditions must be true to qualify):

  1. RSI Condition  — Daily RSI(14) ≥ 60 NOW, OR crossed above 60 within
                      the last 2 trading sessions (i.e., previous or 2-ago
                      session had RSI < 60 and current session has RSI ≥ 60).
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


# ─────────────────────────────────────────────────────────────────────────────
# Filter conditions
# ─────────────────────────────────────────────────────────────────────────────

RSI_THRESHOLD = 60.0


def _check_rsi_condition(rsi: pd.Series) -> tuple[bool, float, str]:
    """
    Returns (passes, current_rsi, description).
    Passes if:
      a) latest RSI ≥ 60, OR
      b) RSI crossed above 60 in the last 2 trading sessions
         (prev or prev-2 was < 60 and current is ≥ 60).
    """
    clean = rsi.dropna()
    if len(clean) < 3:
        return False, float("nan"), "insufficient data"

    curr = float(clean.iloc[-1])
    prev1 = float(clean.iloc[-2])
    prev2 = float(clean.iloc[-3])

    if curr >= RSI_THRESHOLD:
        if prev1 < RSI_THRESHOLD or prev2 < RSI_THRESHOLD:
            return True, curr, f"RSI crossed ↑60 (now={curr:.1f}, prev={prev1:.1f})"
        return True, curr, f"RSI sustained above 60 ({curr:.1f})"

    return False, curr, f"RSI below 60 ({curr:.1f})"


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
    passes: bool = field(init=False)

    def __post_init__(self):
        # passes only when BOTH conditions are satisfied
        self.passes = (
            "RSI" in self.rsi_status and "below" not in self.rsi_status
            and "MACD bullish" in self.macd_status
        )


# ─────────────────────────────────────────────────────────────────────────────
# Main screening function
# ─────────────────────────────────────────────────────────────────────────────

def screen_stocks(
    data: dict[str, pd.DataFrame],
) -> tuple[list[ScreenResult], list[ScreenResult]]:
    """
    Runs RSI + MACD screening on each ticker.

    Parameters
    ----------
    data : dict returned by stock_fetcher.fetch_stock_data()

    Returns
    -------
    (qualified, all_results)
      qualified   — stocks that pass BOTH conditions
      all_results — every ticker analysed (for debugging / full report)
    """
    all_results: list[ScreenResult] = []

    for ticker, df in data.items():
        try:
            close = df["Close"].astype(float)

            rsi_series = _rsi(close)
            macd_line, signal_line, _ = _macd(close)

            rsi_passes, rsi_val, rsi_desc = _check_rsi_condition(rsi_series)
            macd_passes, macd_desc = _check_macd_condition(macd_line, signal_line)

            current_price = float(close.iloc[-1])

            result = ScreenResult(
                ticker=ticker,
                current_price=current_price,
                rsi=rsi_val,
                rsi_status=rsi_desc,
                macd_status=macd_desc,
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
    # Sort qualified by RSI descending (strongest momentum first)
    qualified.sort(key=lambda r: r.rsi, reverse=True)
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
