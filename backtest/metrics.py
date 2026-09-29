"""
Backtest: Metrics

Performance metrics computed from a closed-trade list + daily equity curve.
"""

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from .simulator import Trade


@dataclass
class BacktestMetrics:
    total_trades: int
    win_rate_pct: float
    avg_r_multiple: float
    total_return_pct: float
    cagr_pct: float
    max_drawdown_pct: float
    sharpe_ratio: float
    avg_holding_days: float
    per_sector: dict[str, dict] = field(default_factory=dict)


def _max_drawdown(equity: pd.Series) -> float:
    running_max = equity.cummax()
    drawdown = (equity / running_max - 1.0) * 100.0
    return float(drawdown.min()) if len(drawdown) else 0.0


def _cagr(equity: pd.Series) -> float:
    if len(equity) < 2:
        return 0.0
    years = (equity.index[-1] - equity.index[0]).days / 365.25
    if years <= 0 or equity.iloc[0] <= 0:
        return 0.0
    return (float(equity.iloc[-1] / equity.iloc[0]) ** (1 / years) - 1.0) * 100.0


def _sharpe(equity: pd.Series) -> float:
    daily_returns = equity.pct_change().dropna()
    if daily_returns.std() == 0 or len(daily_returns) < 2:
        return 0.0
    return float(daily_returns.mean() / daily_returns.std() * np.sqrt(252))


def compute_metrics(trades: list[Trade], equity: pd.Series) -> BacktestMetrics:
    if not trades:
        return BacktestMetrics(
            total_trades=0, win_rate_pct=0.0, avg_r_multiple=0.0, total_return_pct=0.0,
            cagr_pct=0.0, max_drawdown_pct=0.0, sharpe_ratio=0.0, avg_holding_days=0.0,
        )

    wins = [t for t in trades if t.r_multiple > 0]
    win_rate = len(wins) / len(trades) * 100.0
    avg_r = sum(t.r_multiple for t in trades) / len(trades)
    holding_days = [(t.exit_date - t.entry_date).days for t in trades if t.exit_date]
    avg_holding = sum(holding_days) / len(holding_days) if holding_days else 0.0

    total_return = (equity.iloc[-1] / equity.iloc[0] - 1.0) * 100.0 if len(equity) else 0.0

    per_sector: dict[str, dict] = {}
    for t in trades:
        s = per_sector.setdefault(t.sector, {"trades": 0, "wins": 0, "total_r": 0.0})
        s["trades"] += 1
        s["wins"] += 1 if t.r_multiple > 0 else 0
        s["total_r"] += t.r_multiple
    for s, stats in per_sector.items():
        stats["win_rate_pct"] = stats["wins"] / stats["trades"] * 100.0
        stats["avg_r"] = stats["total_r"] / stats["trades"]

    return BacktestMetrics(
        total_trades=len(trades),
        win_rate_pct=win_rate,
        avg_r_multiple=avg_r,
        total_return_pct=total_return,
        cagr_pct=_cagr(equity),
        max_drawdown_pct=_max_drawdown(equity),
        sharpe_ratio=_sharpe(equity),
        avg_holding_days=avg_holding,
        per_sector=per_sector,
    )
