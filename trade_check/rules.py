"""
Trade Check: the 5 rules as code

`plan_trade()` does what the doc's prompt asks an LLM to do, deterministically:
given the last two daily candles and the account state, it returns TAKE or SKIP
with entry, stop, target, risk, size, reward-to-risk and a per-rule check.
`format_check()` prints it in the doc's exact output format. The backtest, the
daily check and the paper account all call `plan_trade()`, so they apply the
same rules.
"""

import math
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from . import config
from .config import Instrument


@dataclass(frozen=True)
class Params:
    trend_filter: str = config.DEFAULT_TREND_FILTER
    target_r: float = config.DEFAULT_TARGET_R
    allow_short: bool = True
    max_hold: int = config.DEFAULT_MAX_HOLD
    sizing: str = "lots"           # "lots" (real contract sizes) | "fractional" (ignore lots)

    def label(self) -> str:
        side = "long+short" if self.allow_short else "long only"
        return f"{self.trend_filter}, {self.target_r:g}R, {side}"


@dataclass
class TradeCheck:
    verdict: str                    # "TAKE" | "SKIP"
    why: str
    next_step: str = ""
    direction: str = ""             # "LONG" | "SHORT"
    entry: float = float("nan")
    stop: float = float("nan")
    target: float = float("nan")
    risk_budget: float = float("nan")
    risk_amount: float = float("nan")   # actual ₹ at risk after rounding to lots
    units: float = 0.0
    lot: float | None = None
    value: float = 0.0
    reward_to_risk: float = float("nan")
    rules: dict[int, bool] = field(default_factory=dict)


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Wilder's ATR — same formula as modules/technical_analyzer.atr (copied, not imported)."""
    high, low, close = df["High"], df["Low"], df["Close"]
    prev_close = close.shift(1)
    tr = pd.concat([high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    return tr.ewm(com=period - 1, min_periods=period, adjust=False).mean()


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["ATR"] = atr(out)
    for n in (20, 50, 200):
        out[f"SMA{n}"] = out["Close"].rolling(n).mean()
    return out


def lot_size(inst: Instrument, price: float) -> float:
    if inst.lot is not None:
        return inst.lot
    # NSE stock futures: smallest lot worth at least ₹15L, in steps of 25 units.
    return float(math.ceil(config.NSE_MIN_CONTRACT_INR / price / 25.0) * 25)


def direction(bars: pd.DataFrame, trend_filter: str, allow_short: bool) -> str | None:
    """LONG / SHORT / None from the bars up to and including the prior day."""
    last = bars.iloc[-1]
    if trend_filter == "prior_candle":
        d = "LONG" if last["Close"] > last["Open"] else "SHORT" if last["Close"] < last["Open"] else None
    else:
        col = trend_filter.upper()
        sma = bars[col]
        lb = config.SMA_SLOPE_LOOKBACK
        if len(sma.dropna()) <= lb:
            return None
        rising = sma.iloc[-1] > sma.iloc[-1 - lb]
        if last["Close"] > sma.iloc[-1] and rising:
            d = "LONG"
        elif last["Close"] < sma.iloc[-1] and not rising:
            d = "SHORT"
        else:
            d = None
    if d == "SHORT" and not allow_short:
        return None
    return d


def plan_trade(
    bars: pd.DataFrame,
    inst: Instrument,
    equity: float,
    params: Params,
    losses_in_row: int = 0,
    pause_left: int = 0,
    in_position: bool = False,
) -> TradeCheck:
    """
    Plans the next session's trade from `bars` (indicators added, last row = the
    prior day). Follows the doc's order: hard SKIPs first (rules 4, 3, 5), then
    the setup.
    """
    rules = {1: True, 2: True, 3: True, 4: True, 5: True}

    if losses_in_row >= config.LOSS_STREAK_PAUSE or pause_left > 0:
        rules[4] = False
        left = pause_left or config.PAUSE_SESSIONS
        return TradeCheck("SKIP", f"{config.LOSS_STREAK_PAUSE} losses in a row.",
                          f"{left} session(s) off.", rules=rules)
    if in_position:
        rules[3] = False
        return TradeCheck("SKIP", "Already in a trade in this market. One position at a time; never add to it.",
                          "Manage the open trade.", rules=rules)

    prior = bars.iloc[-1]
    d = direction(bars, params.trend_filter, params.allow_short)
    if d is None or not np.isfinite(prior["ATR"]):
        return TradeCheck("SKIP", "No clean setup: the trend filter gives no direction.",
                          "Wait for tomorrow's candle.", rules=rules)

    buf = config.STOP_BUFFER_ATR * prior["ATR"]
    if d == "LONG":
        entry, stop = prior["High"] + buf, prior["Low"] - buf
    else:
        entry, stop = prior["Low"] - buf, prior["High"] + buf
    per_unit = abs(entry - stop)
    if per_unit <= 0:
        return TradeCheck("SKIP", "No clean setup: zero-range candle.", "Wait for tomorrow's candle.", rules=rules)
    target = entry + params.target_r * per_unit if d == "LONG" else entry - params.target_r * per_unit

    budget = equity * config.RISK_PCT / 100.0
    max_value = equity * config.MAX_NOTIONAL_PCT / 100.0
    units = budget / per_unit
    note = ""
    if units * entry > max_value:
        units = max_value / entry
        note = " Size cut to the account size (rule 5)."

    lot = None
    if params.sizing == "lots":
        lot = lot_size(inst, entry)
        units = round(math.floor(units / lot + 1e-9) * lot, 8)
        if units <= 0:
            one_lot_risk = lot * per_unit
            one_lot_value = lot * entry
            if one_lot_value > max_value:
                rules[5] = False
                why = (f"One lot ({lot:g} units) is worth Rs {one_lot_value:,.0f}, above the "
                       f"Rs {max_value:,.0f} account. No leverage above the account.")
            else:
                rules[1] = False
                why = (f"One lot risks Rs {one_lot_risk:,.0f}, more than the Rs {budget:,.0f} "
                       f"({config.RISK_PCT:g}%) allowed.")
            return TradeCheck("SKIP", why, "No trade: the contract is too big for this account.",
                              direction=d, entry=entry, stop=stop, target=target, risk_budget=budget,
                              lot=lot, rules=rules)

    return TradeCheck(
        "TAKE",
        f"{d.title()} with the {params.trend_filter} trend; breakout of the prior day's "
        f"{'high' if d == 'LONG' else 'low'}, stop past its {'low' if d == 'LONG' else 'high'}.{note}",
        direction=d, entry=entry, stop=stop, target=target, risk_budget=budget,
        risk_amount=units * per_unit, units=units, lot=lot, value=units * entry,
        reward_to_risk=params.target_r, rules=rules,
    )


def format_check(c: TradeCheck) -> str:
    """The doc's output format, exactly."""
    if c.verdict == "SKIP":
        lines = ["VERDICT: SKIP", f"WHY: {c.why}"]
        if c.next_step:
            lines.append(f"NEXT: {c.next_step}")
        return "\n".join(lines)
    lots = f" ({c.units / c.lot:g} lots of {c.lot:g})" if c.lot else ""
    checks = ", ".join(f"Rule {k} {'OK' if v else 'FAIL'}" for k, v in sorted(c.rules.items()))
    return "\n".join([
        "VERDICT: TAKE",
        f"DIRECTION: {c.direction}",
        f"ENTRY: {c.entry:,.2f} (stop order; valid for the next session only)",
        f"STOP: {c.stop:,.2f}",
        f"TARGET: {c.target:,.2f}",
        f"RISK AMOUNT: Rs {c.risk_amount:,.0f} (budget Rs {c.risk_budget:,.0f})",
        f"POSITION SIZE: {c.units:g} units{lots} = Rs {c.value:,.0f}",
        f"REWARD-TO-RISK: {c.reward_to_risk:g}",
        f"RULES CHECK: {checks}",
        f"WHY: {c.why}",
    ])
