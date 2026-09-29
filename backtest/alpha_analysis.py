"""
Backtest: Alpha/Beta Regression Diagnostic

Every prior comparison against the Nifty500 benchmark (STRATEGY.md §8b) has been a
head-to-head CAGR/Sharpe/Calmar/MaxDD comparison — never a real decomposition of the
strategy's own daily returns into alpha (stock-picking skill) and beta (market
exposure) via regression. This module does that: a CAPM-style OLS,

    strategy_daily_return = alpha + beta * benchmark_daily_return + residual

computed with plain numpy (this repo has no scipy/statsmodels — see requirements.txt —
and a ~30-line hand-rolled OLS needs neither).

This is a diagnostic, not a fix. Two outcomes are equally valid and reportable:
significant alpha with beta driving the drawdowns (real stock-picking edge masked by
market exposure — points toward a future hedging investigation), or alpha ~= 0 (the
benchmark-comparison finding in STRATEGY.md §8b wasn't noise, and no amount of
entry-timing tuning will close the gap).
"""

import math
from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class AlphaBetaResult:
    n_obs: int
    alpha_daily: float
    alpha_annualized_pct: float
    beta: float
    r_squared: float
    alpha_se: float
    alpha_tstat: float
    alpha_pvalue: float
    significant_at_5pct: bool


def _standard_normal_cdf(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def compute_alpha_beta(equity: pd.Series, benchmark: pd.Series) -> AlphaBetaResult:
    """OLS regression of daily strategy returns on daily benchmark returns.

    Equity curves have flat stretches when no positions are open, while the benchmark
    trades every day, so the two return series are inner-joined on their shared dates
    before regressing rather than assumed to already line up.
    """
    strategy_ret = equity.pct_change().dropna()
    bench_ret = benchmark.pct_change().dropna()
    strategy_ret, bench_ret = strategy_ret.align(bench_ret, join="inner")

    n = len(strategy_ret)
    if n < 30:
        return AlphaBetaResult(
            n_obs=n, alpha_daily=0.0, alpha_annualized_pct=0.0, beta=0.0,
            r_squared=0.0, alpha_se=0.0, alpha_tstat=0.0, alpha_pvalue=1.0,
            significant_at_5pct=False,
        )

    y = strategy_ret.values
    x = bench_ret.values
    X = np.column_stack([np.ones(n), x])

    coef, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
    alpha_daily, beta = float(coef[0]), float(coef[1])

    resid = y - X @ coef
    sigma2 = float(resid @ resid) / (n - 2)
    xtx_inv = np.linalg.inv(X.T @ X)
    cov = sigma2 * xtx_inv
    alpha_se = float(np.sqrt(cov[0, 0]))
    alpha_tstat = alpha_daily / alpha_se if alpha_se > 0 else 0.0
    alpha_pvalue = 2.0 * (1.0 - _standard_normal_cdf(abs(alpha_tstat)))

    ss_res = float(resid @ resid)
    ss_tot = float(((y - y.mean()) ** 2).sum())
    r_squared = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

    return AlphaBetaResult(
        n_obs=n,
        alpha_daily=alpha_daily,
        alpha_annualized_pct=alpha_daily * 252 * 100.0,
        beta=beta,
        r_squared=r_squared,
        alpha_se=alpha_se,
        alpha_tstat=alpha_tstat,
        alpha_pvalue=alpha_pvalue,
        significant_at_5pct=alpha_pvalue < 0.05,
    )
