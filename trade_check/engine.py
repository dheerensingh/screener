"""
Trade Check: day-by-day simulator

One position at a time on one instrument's daily bars. Shared by the backtest
and the paper account, so both follow the same fills and exits:

  - At day T's close, plan_trade() uses bars up to T to place a stop order for
    day T+1 only (max one new trade a day).
  - Day T+1: the order fills if price trades through the entry (at the open if
    it gapped past). If the same bar also reaches the stop, the trade is
    stopped out that day: daily bars can't show which came first, so the
    simulator assumes the worse case.
  - Open trade: gap through the stop or target at the open exits at the open;
    otherwise the stop is checked before the target. After `max_hold` sessions
    it exits at that session's close.
  - Costs: cost_per_side_pct of notional on entry and on exit. Carry: a long
    pays carry_annual_pct a year on notional (futures basis / perp funding),
    a short receives it.
  - Rule 4: after 3 losses in a row, no orders for the next 3 sessions.
"""

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from . import config
from .config import Instrument
from .rules import Params, TradeCheck, add_indicators, plan_trade

WARMUP_BARS = 210   # SMA200 + slope lookback


@dataclass
class Trade:
    direction: str
    signal_date: pd.Timestamp
    entry_date: pd.Timestamp
    entry: float
    stop: float
    target: float
    units: float
    risk_amount: float
    exit_date: pd.Timestamp | None = None
    exit: float = float("nan")
    exit_reason: str = ""
    pnl: float = 0.0            # net of costs and carry
    sessions: int = 0

    @property
    def r_multiple(self) -> float:
        return self.pnl / self.risk_amount if self.risk_amount else 0.0


@dataclass
class SimResult:
    trades: list[Trade]
    equity: pd.Series
    skips: dict[str, int]                  # SKIP reason bucket -> count of sessions
    open_trade: Trade | None
    next_check: TradeCheck | None          # the plan for the session after the last bar
    losses_in_row: int = 0
    pause_left: int = 0
    plans: list[tuple[pd.Timestamp, TradeCheck]] = field(default_factory=list)


def _sign(direction: str) -> int:
    return 1 if direction == "LONG" else -1


def _skip_bucket(c: TradeCheck) -> str:
    if not c.rules.get(4, True):
        return "rule 4 pause"
    if not c.rules.get(5, True):
        return "lot above account"
    if not c.rules.get(1, True):
        return "lot risk above 0.5%"
    if not c.rules.get(3, True):
        return "in a trade"
    return "no setup"


def simulate(
    df: pd.DataFrame,
    inst: Instrument,
    params: Params,
    start_equity: float = config.ACCOUNT_SIZE_INR,
    start: pd.Timestamp | None = None,
    keep_plans: bool = False,
) -> SimResult:
    """
    `df`: daily OHLC, DatetimeIndex ascending. Trading starts at `start` (or
    after the warm-up), using earlier bars only for indicators.
    """
    bars = add_indicators(df)
    idx = bars.index
    first = WARMUP_BARS if start is None else max(int(idx.searchsorted(start)), 1)

    o, h, l, c = (bars[k].to_numpy(dtype=float) for k in ("Open", "High", "Low", "Close"))
    cash = start_equity
    trades: list[Trade] = []
    skips: dict[str, int] = {}
    plans: list[tuple[pd.Timestamp, TradeCheck]] = []
    equity_points: dict[pd.Timestamp, float] = {}
    pos: Trade | None = None
    order: TradeCheck | None = None
    order_date: pd.Timestamp | None = None
    losses_in_row = 0
    pause_left = 0
    cost = inst.cost_per_side_pct / 100.0
    check: TradeCheck | None = None

    def close_trade(t: Trade, date: pd.Timestamp, price: float, reason: str) -> None:
        nonlocal cash, losses_in_row, pause_left
        gross = _sign(t.direction) * (price - t.entry) * t.units
        exit_cost = abs(price * t.units) * cost
        cash += gross - exit_cost
        t.pnl += gross - exit_cost
        t.exit_date, t.exit, t.exit_reason = date, price, reason
        trades.append(t)
        if t.pnl < 0:
            losses_in_row += 1
            if losses_in_row >= config.LOSS_STREAK_PAUSE:
                pause_left = config.PAUSE_SESSIONS
                losses_in_row = 0
        else:
            losses_in_row = 0

    for i in range(first, len(bars)):
        date = idx[i]

        # Carry on the position held overnight into today.
        if pos is not None:
            days = max((date - idx[i - 1]).days, 1)
            carry = _sign(pos.direction) * abs(c[i - 1] * pos.units) * inst.carry_annual_pct / 100.0 * days / 365.0
            cash -= carry
            pos.pnl -= carry

        # 1. Manage the open trade.
        if pos is not None:
            pos.sessions += 1
            s = _sign(pos.direction)
            if s * (o[i] - pos.stop) <= 0:
                close_trade(pos, date, o[i], "gap through stop")
            elif s * (o[i] - pos.target) >= 0:
                close_trade(pos, date, o[i], "gap through target")
            elif s * ((l[i] if s > 0 else h[i]) - pos.stop) <= 0:
                close_trade(pos, date, pos.stop, "stop")
            elif s * ((h[i] if s > 0 else l[i]) - pos.target) >= 0:
                close_trade(pos, date, pos.target, "target")
            elif pos.sessions >= params.max_hold:
                close_trade(pos, date, c[i], "time stop")
            if pos is not None and pos.exit_date is not None:
                pos = None

        # 2. Today's stop order (placed at yesterday's close).
        if order is not None and pos is None:
            s = _sign(order.direction)
            reach = h[i] if s > 0 else l[i]
            if s * (reach - order.entry) >= 0:
                fill = max(o[i], order.entry) if s > 0 else min(o[i], order.entry)
                t = Trade(order.direction, order_date, date, fill, order.stop, order.target,
                          order.units, order.risk_amount)
                entry_cost = abs(fill * t.units) * cost
                cash -= entry_cost
                t.pnl -= entry_cost
                pos = t
                adverse = l[i] if s > 0 else h[i]
                favourable = h[i] if s > 0 else l[i]
                if s * (adverse - t.stop) <= 0:
                    close_trade(t, date, t.stop, "stop (entry day)")
                    pos = None
                elif s * (favourable - t.target) >= 0:
                    close_trade(t, date, t.target, "target (entry day)")
                    pos = None
        order = None

        # 3. Mark to market at the close.
        unreal = _sign(pos.direction) * (c[i] - pos.entry) * pos.units if pos is not None else 0.0
        equity = cash + unreal
        equity_points[date] = equity

        # 4. Plan tomorrow.
        if pause_left > 0 and pos is None:
            check = plan_trade(bars.iloc[: i + 1], inst, equity, params, pause_left=pause_left)
            pause_left -= 1
        else:
            check = plan_trade(bars.iloc[: i + 1], inst, equity, params,
                               losses_in_row=losses_in_row, in_position=pos is not None)
        if keep_plans:
            plans.append((date, check))
        if check.verdict == "TAKE":
            order, order_date = check, date
        else:
            bucket = _skip_bucket(check)
            skips[bucket] = skips.get(bucket, 0) + 1

    return SimResult(
        trades=trades,
        equity=pd.Series(equity_points, dtype=float),
        skips=skips,
        open_trade=pos,
        next_check=check,
        losses_in_row=losses_in_row,
        pause_left=pause_left,
        plans=plans,
    )


# --- metrics (copied from backtest/metrics.py so this package stands alone) ---

def max_drawdown(equity: pd.Series) -> float:
    if equity.empty:
        return 0.0
    return float(((equity / equity.cummax()) - 1.0).min() * 100.0)


def cagr(equity: pd.Series) -> float:
    if len(equity) < 2 or equity.iloc[0] <= 0 or equity.iloc[-1] <= 0:
        return 0.0
    years = (equity.index[-1] - equity.index[0]).days / 365.25
    return ((equity.iloc[-1] / equity.iloc[0]) ** (1 / years) - 1.0) * 100.0 if years > 0 else 0.0


def sharpe(equity: pd.Series, periods_per_year: int = 252) -> float:
    r = equity.pct_change().dropna()
    if len(r) < 2 or r.std() == 0:
        return 0.0
    return float(r.mean() / r.std() * np.sqrt(periods_per_year))


def summarize(trades: list[Trade], equity: pd.Series, periods_per_year: int = 252) -> dict:
    n = len(trades)
    rs = [t.r_multiple for t in trades]
    wins = sum(1 for r in rs if r > 0)
    gross_win = sum(t.pnl for t in trades if t.pnl > 0)
    gross_loss = -sum(t.pnl for t in trades if t.pnl < 0)
    return {
        "trades": n,
        "win_pct": wins / n * 100.0 if n else 0.0,
        "avg_r": float(np.mean(rs)) if n else 0.0,
        "profit_factor": gross_win / gross_loss if gross_loss > 0 else float("inf") if gross_win > 0 else 0.0,
        "return_pct": (equity.iloc[-1] / equity.iloc[0] - 1.0) * 100.0 if len(equity) > 1 else 0.0,
        "cagr_pct": cagr(equity),
        "max_dd_pct": max_drawdown(equity),
        "sharpe": sharpe(equity, periods_per_year),
    }
