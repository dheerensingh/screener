"""
Backtest: Periodic-Rebalance Momentum Factor Strategy

A deliberately *separate* strategy from the daily-signal swing-trading system
in `backtest/simulator.py` — built to answer a specific question: real
momentum strategies that reach 20-25% CAGR (NSE's own Nifty200 Momentum 30 /
Nifty Midcap150 Momentum 50 indices) do it with a periodic-rebalance,
no-active-stop-loss design that tolerates ~70% drawdowns, not by tuning a
daily swing-trading system's risk dials (tested directly on our own data —
see CLAUDE.md/STRATEGY.md: removing every risk control barely moved CAGR).
This module replicates that different paradigm honestly, on our own data.

Momentum score — matches Nifty200 Momentum 30's own disclosed methodology
(research, 2026-09-28), not a guess: 6-month and 12-month price return, each
divided by trailing 12-month annualized daily-return volatility (risk-
adjustment), then cross-sectionally z-scored across the eligible universe on
each rebalance date ("Normalised Momentum Score" in the index's own
terminology) and averaged into one composite.

Stated deviations from the official index (disclosed, not hidden):
  - Universe is the full Nifty500 (already cached), not the official Nifty200 — unless
    `FactorConfig.universe_scope="midcap_only"` (added 2026-09-30, see STRATEGY.md §15),
    which restricts to `cap_band=="Mid"` tickers and 50 holdings, a closer mechanical
    match to Nifty Midcap150 Momentum 50 specifically.
  - Equal-weighted, not free-float-market-cap x momentum-score weighted — this
    project has no *historical* market-cap data cached, only OHLCV (current market cap
    is scrapeable from Screener.in, but using it to weight a historical backtest would
    itself be a new look-ahead bias, so this deviation stays even in the midcap variant).

IMPORTANT (STRATEGY.md §8d, §15): any CAGR this module produces — including the
midcap_only variant — is backtested on sector_mapper.build_universe(), today's
Nifty500/cap-band membership applied retroactively. This is the same survivorship-bias
mechanism §8d proved (a momentum-selected 2018 portfolio's forward return was
statistically identical to a random sample from the same universe) — restricting to
midcap-only plausibly makes it *worse*, not better, since smaller companies have a
higher real-world base rate of delisting. Don't report any number from here as
validated performance; see STRATEGY.md §15 for the honest framing.

No active stop-loss between rebalances — the defining difference from
`simulator.py`. An optional absolute-momentum/cash overlay reuses the
already-validated `modules.market_regime` (not a new signal) to reduce
equity exposure when the benchmark isn't in a clean uptrend, the same
mechanism that gives Dual Momentum (Antonacci) a much better drawdown than
raw relative-momentum indices.
"""

import logging
from dataclasses import dataclass, field
from typing import Literal, Optional

import numpy as np
import pandas as pd

from modules.market_regime import evaluate as evaluate_regime
from modules.sector_rotation import MIN_AVG_DAILY_VALUE_INR

logger = logging.getLogger(__name__)

RET_6M_DAYS = 126
RET_12M_DAYS = 252
VOL_LOOKBACK_DAYS = 252
MIN_HISTORY_ROWS = VOL_LOOKBACK_DAYS + 10
NUM_HOLDINGS = 30
MIDCAP_NUM_HOLDINGS = 50  # Nifty Midcap150 Momentum 50's actual constituent count
TRANSACTION_COST_PCT = 0.1

RebalanceFreq = Literal["semiannual", "quarterly"]
Overlay = Literal["none", "half_defensive", "full_defensive"]
UniverseScope = Literal["full_nifty500", "midcap_only"]


@dataclass
class FactorConfig:
    rebalance_freq: RebalanceFreq = "semiannual"
    overlay: Overlay = "none"
    num_holdings: int = NUM_HOLDINGS
    account_size: float = 500_000.0
    apply_liquidity_filter: bool = True
    universe_scope: UniverseScope = "full_nifty500"


def _per_ticker_factor_signals(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    if len(df) < MIN_HISTORY_ROWS:
        return None
    close = df["Close"].astype(float)
    volume = df["Volume"].astype(float)

    daily_ret = close.pct_change()
    vol_ann_pct = daily_ret.rolling(VOL_LOOKBACK_DAYS, min_periods=VOL_LOOKBACK_DAYS).std() * np.sqrt(252) * 100.0
    ret_6m = close.pct_change(RET_6M_DAYS) * 100.0
    ret_12m = close.pct_change(RET_12M_DAYS) * 100.0

    risk_adj_6m = ret_6m / vol_ann_pct
    risk_adj_12m = ret_12m / vol_ann_pct
    avg_dollar_vol = (close * volume).rolling(20, min_periods=20).mean()

    return pd.DataFrame({
        "close": close, "open": df["Open"].astype(float),
        "risk_adj_6m": risk_adj_6m, "risk_adj_12m": risk_adj_12m,
        "avg_dollar_vol": avg_dollar_vol,
    })


def _rebalance_dates(trading_days: pd.DatetimeIndex, freq: RebalanceFreq) -> list[pd.Timestamp]:
    months = {4, 10} if freq == "semiannual" else {1, 4, 7, 10}
    years = range(trading_days[0].year, trading_days[-1].year + 2)
    targets = [pd.Timestamp(year=y, month=m, day=1) for y in years for m in months]
    dates = []
    for t in sorted(targets):
        candidates = trading_days[trading_days >= t]
        if len(candidates):
            dates.append(candidates[0])
    return sorted(set(d for d in dates if trading_days[0] < d <= trading_days[-1]))


def _month_start_dates(trading_days: pd.DatetimeIndex) -> set[pd.Timestamp]:
    seen_months = set()
    result = set()
    for d in trading_days:
        key = (d.year, d.month)
        if key not in seen_months:
            seen_months.add(key)
            result.add(d)
    return result


def _select_top_holdings(
    signals: dict[str, pd.DataFrame], date: pd.Timestamp, num_holdings: int, apply_liquidity_filter: bool,
) -> list[str]:
    rows = []
    for ticker, sig in signals.items():
        if date not in sig.index:
            continue
        row = sig.loc[date]
        if row["risk_adj_6m"] != row["risk_adj_6m"] or row["risk_adj_12m"] != row["risk_adj_12m"]:
            continue
        if apply_liquidity_filter:
            adv = row["avg_dollar_vol"]
            if adv != adv or adv < MIN_AVG_DAILY_VALUE_INR:
                continue
        rows.append((ticker, row["risk_adj_6m"], row["risk_adj_12m"]))

    if len(rows) < num_holdings:
        return [t for t, _, _ in rows]

    df = pd.DataFrame(rows, columns=["ticker", "risk_adj_6m", "risk_adj_12m"]).set_index("ticker")
    z6 = (df["risk_adj_6m"] - df["risk_adj_6m"].mean()) / df["risk_adj_6m"].std()
    z12 = (df["risk_adj_12m"] - df["risk_adj_12m"].mean()) / df["risk_adj_12m"].std()
    composite = (z6 + z12) / 2.0
    return composite.sort_values(ascending=False).head(num_holdings).index.tolist()


def run_factor_backtest(
    universe_data: dict[str, pd.DataFrame], benchmark: pd.Series, config: FactorConfig,
    cap_band_by_ticker: Optional[dict[str, str]] = None,
) -> pd.Series:
    """Returns the daily equity curve for the periodic-rebalance factor strategy.

    cap_band_by_ticker : ticker -> "Large"/"Mid"/"Small" (modules.sector_mapper.
        build_universe()). Required when config.universe_scope == "midcap_only", to
        restrict the eligible pool to a closer mechanical match for Nifty Midcap150
        Momentum 50 — see the module docstring for the caveat on what that number
        does and doesn't prove.
    """
    if config.universe_scope == "midcap_only":
        if not cap_band_by_ticker:
            raise ValueError("universe_scope='midcap_only' requires cap_band_by_ticker")
        universe_data = {
            t: df for t, df in universe_data.items() if cap_band_by_ticker.get(t) == "Mid"
        }
        logger.info("midcap_only scope: restricted to %d 'Mid' cap-band tickers", len(universe_data))

    logger.info("Precomputing factor signals for %d tickers ...", len(universe_data))
    signals: dict[str, pd.DataFrame] = {}
    for ticker, df in universe_data.items():
        sig = _per_ticker_factor_signals(df)
        if sig is not None:
            signals[ticker] = sig

    benchmark = benchmark.dropna()
    trading_days = benchmark.index
    trading_days = trading_days[MIN_HISTORY_ROWS:] if len(trading_days) > MIN_HISTORY_ROWS else trading_days

    rebalance_dates = set(_rebalance_dates(trading_days, config.rebalance_freq))
    month_starts = _month_start_dates(trading_days) if config.overlay != "none" else set()
    overlay_target = {"none": 1.0, "half_defensive": 0.5, "full_defensive": 0.0}[config.overlay]

    holdings: dict[str, int] = {}  # ticker -> quantity
    cash = config.account_size
    equity_exposure_pct = 1.0
    pending_selection: Optional[list[str]] = None
    pending_exposure: Optional[float] = None
    equity_curve: list[tuple[pd.Timestamp, float]] = []

    def _portfolio_value(day: pd.Timestamp) -> float:
        return cash + sum(
            qty * float(signals[t]["close"].get(day, signals[t]["close"].iloc[-1]))
            for t, qty in holdings.items() if t in signals
        )

    for day in trading_days:
        # ── Execute a selection change decided yesterday, at today's open ──
        if pending_selection is not None:
            value_before = _portfolio_value(day)
            target_value_each = (value_before * equity_exposure_pct / len(pending_selection)) if pending_selection else 0.0
            new_holdings: dict[str, int] = {}
            turnover_value = 0.0
            for ticker in pending_selection:
                sig = signals.get(ticker)
                if sig is None or day not in sig.index:
                    continue
                px = float(sig.loc[day, "open"])
                if px <= 0:
                    continue
                qty = int(target_value_each / px)
                if qty > 0:
                    new_holdings[ticker] = qty
                    prev_qty = holdings.get(ticker, 0)
                    turnover_value += abs(qty - prev_qty) * px
            for ticker, prev_qty in holdings.items():
                if ticker not in new_holdings:
                    sig = signals.get(ticker)
                    px = float(sig.loc[day, "open"]) if sig is not None and day in sig.index else 0.0
                    turnover_value += prev_qty * px
            cash = value_before - sum(q * float(signals[t].loc[day, "open"]) for t, q in new_holdings.items())
            cash -= turnover_value * TRANSACTION_COST_PCT / 100.0
            holdings = new_holdings
            pending_selection = None

        # ── Execute an exposure change (overlay) decided yesterday, at today's open ──
        # (not `elif` — both a selection change and an exposure change can be pending
        # on the same execution day; selection runs first, using the exposure level in
        # effect at that moment, then this normalizes to the new exposure afterward.)
        if pending_exposure is not None and holdings:
            value_before = _portfolio_value(day)
            target_total_equity = value_before * pending_exposure
            target_value_each = target_total_equity / len(holdings) if holdings else 0.0
            turnover_value = 0.0
            new_holdings = {}
            for ticker in holdings:
                sig = signals.get(ticker)
                if sig is None or day not in sig.index:
                    continue
                px = float(sig.loc[day, "open"])
                if px <= 0:
                    continue
                qty = int(target_value_each / px)
                turnover_value += abs(qty - holdings[ticker]) * px
                if qty > 0:
                    new_holdings[ticker] = qty
            cash = value_before - sum(q * float(signals[t].loc[day, "open"]) for t, q in new_holdings.items())
            cash -= turnover_value * TRANSACTION_COST_PCT / 100.0
            holdings = new_holdings
            equity_exposure_pct = pending_exposure
            pending_exposure = None

        # ── Decide today (executed tomorrow) ──
        if day in rebalance_dates:
            pending_selection = _select_top_holdings(signals, day, config.num_holdings, config.apply_liquidity_filter)
        if day in month_starts:
            regime = evaluate_regime(benchmark.loc[:day])
            new_target = overlay_target if not regime.in_uptrend else 1.0
            if new_target != equity_exposure_pct:
                pending_exposure = new_target

        equity_curve.append((day, _portfolio_value(day)))

    return pd.Series({d: v for d, v in equity_curve})
