"""
Backtest: Simulator

Day-by-day trade simulation reusing this repo's own indicator/sizing logic
(technical_analyzer.py, trend_template.py, position_sizer.py) rather than a
re-implementation — see CLAUDE.md for why. Two stages:

  1. Precompute (vectorized, once per ticker): RSI, MACD-bullish, ATR, 20d avg
     traded value, SMA50/150/200 stack, 52w high/low, blended-return series —
     everything screen_stocks()/trend_template.py compute per-day-live, but as
     full historical Series so the day-loop below is cheap lookups, not
     per-day recomputation.
  2. Simulate (a plain day-by-day loop — only this part is sequential, since
     portfolio state genuinely depends on prior days): sector rotation ranking
     that day, entries from qualifying leading-sector stocks (via
     portfolio_allocator's rules), and exit checks for open positions.

No look-ahead: a signal computed from data through day T enters at day T+1's
open. Resting stop/target levels (known in advance) are checked against day
T's own intraday high/low, same as a real resting order would fill.

Methodology simplification, stated explicitly (see STRATEGY.md limitations):
sector synthetic indices are built from ALL of a sector's members with
sufficient history, not the liquidity-capped 40 used for live stock selection
— the liquidity/cap filter here only gates which stocks are eligible to become
positions, not which shape the sector-level rotation ranking. This keeps daily
rotation ranking stable and is a reasonable simplification: an equal-weight
average of a full sector basket correlates highly with its most-liquid subset.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Optional

import numpy as np
import pandas as pd

from modules.technical_analyzer import _macd, _rsi, atr as compute_atr
from modules.trend_template import PCT_ABOVE_52W_LOW_MIN, PCT_BELOW_52W_HIGH_MAX, RS_RATING_THRESHOLD
from modules.vcp_pattern import (
    CONTRACTION_LEN, DEPTH_TOLERANCE_PCT, NUM_CONTRACTIONS, VOLUME_AVG_DAYS, VOLUME_TOLERANCE_PCT,
)

logger = logging.getLogger(__name__)

# ── Strategy parameters (mirrors the live modules' defaults) ─────────────────
RSI_THRESHOLD = 55.0
TOP_N_SECTORS = 4
MAX_STOCKS_PER_SECTOR = 40
MIN_AVG_DAILY_VALUE_INR = 2_00_00_000
MAX_POSITIONS_PER_SECTOR = 2
MAX_DEPLOYED_CAPITAL_PCT = 85.0
MAX_PORTFOLIO_HEAT_PCT = 8.0
CAP_BAND_RISK_PCT_DEFAULT = 0.75
ATR_STOP_MULTIPLIER = 2.0
MAX_STOP_PCT = 8.0
CHANDELIER_MULTIPLIER = 2.5
RSI_FAILURE_FLOOR = 45.0
TIME_BACKSTOP_DAYS = 26
FIXED_TARGET_R = 2.0
SCALE_OUT_R = 1.5
TRANSACTION_COST_PCT = 0.1  # round-trip, % of trade value
REGIME_SMA_PERIOD = 200
REGIME_ROC_LOOKBACK_DAYS = 21
REGIME_MIN_SMA_ROC_PCT = 1.0  # must match modules/market_regime.py's MIN_SMA_ROC_PCT

LOOKBACK_1M, LOOKBACK_3M, LOOKBACK_6M = 21, 63, 126
LOOKBACK_WEIGHTS = {"1M": 0.5, "3M": 0.3, "6M": 0.2}

EntryParadigm = Literal["trend_template", "rsi_macd_only", "vcp_breakout"]
ExitParadigm = Literal["trailing", "fixed_target", "scale_out"]


@dataclass
class BacktestConfig:
    entry_paradigm: EntryParadigm = "trend_template"
    exit_paradigm: ExitParadigm = "trailing"
    account_size: float = 500_000.0
    apply_market_regime_filter: bool = False  # off live since 2026-10-01 (STRATEGY.md §16) — kept testable
    apply_liquidity_filter: bool = True
    apply_drawdown_circuit_breaker: bool = False
    drawdown_halt_pct: float = 15.0    # halt NEW entries once strategy equity is this far below its own peak
    drawdown_halt_days: int = 20       # resume after a fixed cooldown — NOT equity recovery (see simulator note:
                                        # halting entries freezes equity at flat cash, so an equity-recovery resume
                                        # condition can never trigger — a self-locking trap, found empirically)
    require_delivery_above_median: bool = False  # additional entry filter — see STRATEGY.md §13; tested, not assumed
    delivery_median_lookback_days: int = 20


@dataclass
class Trade:
    ticker: str
    sector: str
    cap_band: str
    entry_date: pd.Timestamp
    entry_price: float
    quantity: int
    initial_stop: float
    exit_date: Optional[pd.Timestamp] = None
    exit_price: Optional[float] = None
    exit_reason: str = ""
    pnl_rupees: float = 0.0
    r_multiple: float = 0.0  # pnl / initial risk


@dataclass
class OpenPosition:
    ticker: str
    sector: str
    cap_band: str
    entry_date: pd.Timestamp
    entry_price: float
    quantity: int
    initial_risk_per_share: float
    stop: float
    highest_close: float
    risk_amount: float
    scaled_out: bool = False


def _per_ticker_signals(ticker: str, df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Precomputes every daily signal needed by the simulator for one ticker."""
    if len(df) < 60:
        return None
    close = df["Close"].astype(float)
    high = df["High"].astype(float)
    low = df["Low"].astype(float)
    volume = df["Volume"].astype(float)

    rsi = _rsi(close)
    macd_line, signal_line, _ = _macd(close)
    macd_bullish = macd_line > signal_line
    atr_series = compute_atr(df)
    avg_dollar_vol = (close * volume).rolling(20, min_periods=20).mean()

    sma50 = close.rolling(50, min_periods=50).mean()
    sma150 = close.rolling(150, min_periods=150).mean()
    sma200 = close.rolling(200, min_periods=200).mean()
    sma200_rising = sma200 > sma200.shift(21)

    week52_high = close.rolling(252, min_periods=1).max()
    week52_low = close.rolling(252, min_periods=1).min()
    pct_above_low = (close / week52_low - 1.0) * 100.0
    pct_below_high = (1.0 - close / week52_high) * 100.0

    trend_template_ok = (
        (close > sma150) & (close > sma200) & (sma150 > sma200) & sma200_rising
        & (sma50 > sma150) & (sma50 > sma200) & (close > sma50)
        & (pct_above_low >= PCT_ABOVE_52W_LOW_MIN) & (pct_below_high <= PCT_BELOW_52W_HIGH_MAX)
    )

    ret_1m = close.pct_change(LOOKBACK_1M) * 100.0
    ret_3m = close.pct_change(LOOKBACK_3M) * 100.0
    ret_6m = close.pct_change(LOOKBACK_6M) * 100.0
    ret_blend = 0.4 * close.pct_change(63) * 100.0 + 0.3 * close.pct_change(126) * 100.0 + 0.3 * close.pct_change(230) * 100.0

    # VCP (Volatility Contraction Pattern) — vectorized replica of modules/vcp_pattern.py's
    # single-day evaluate(), for every historical day at once (see that module's docstring
    # for the algorithm/tolerances; constants imported from there so live and backtest can't drift).
    # Base ends YESTERDAY (shift(1)) so a day's own high/low never inflates its own pivot.
    roll_high = high.rolling(CONTRACTION_LEN).max()
    roll_low = low.rolling(CONTRACTION_LEN).min()
    roll_vol = volume.rolling(CONTRACTION_LEN).mean()

    chunk_high, chunk_low, chunk_vol = [], [], []
    for c in range(NUM_CONTRACTIONS):  # c=0 -> oldest chunk, c=NUM_CONTRACTIONS-1 -> most recent (ends yesterday)
        shift_amount = 1 + (NUM_CONTRACTIONS - 1 - c) * CONTRACTION_LEN
        chunk_high.append(roll_high.shift(shift_amount))
        chunk_low.append(roll_low.shift(shift_amount))
        chunk_vol.append(roll_vol.shift(shift_amount))

    depths = [(chunk_high[c] - chunk_low[c]) / chunk_high[c] * 100.0 for c in range(NUM_CONTRACTIONS)]
    depths_decreasing = pd.Series(True, index=close.index)
    volumes_decreasing = pd.Series(True, index=close.index)
    for c in range(1, NUM_CONTRACTIONS):
        depths_decreasing &= depths[c] <= depths[c - 1] * (1 + DEPTH_TOLERANCE_PCT / 100.0)
        volumes_decreasing &= chunk_vol[c] <= chunk_vol[c - 1] * (1 + VOLUME_TOLERANCE_PCT / 100.0)
    depths_decreasing &= depths[-1] < depths[0]
    volumes_decreasing &= chunk_vol[-1] < chunk_vol[0]
    vcp_valid_base = depths_decreasing & volumes_decreasing

    vcp_pivot = chunk_high[-1]
    vcp_base_low = chunk_low[-1]
    avg_vol_20_yday = volume.rolling(VOLUME_AVG_DAYS).mean().shift(1)
    vcp_breakout = vcp_valid_base & (close > vcp_pivot) & (volume >= avg_vol_20_yday)

    out = pd.DataFrame({
        "close": close, "high": high, "low": low, "open": df["Open"].astype(float),
        "rsi": rsi, "macd_bullish": macd_bullish, "atr": atr_series, "avg_dollar_vol": avg_dollar_vol,
        "trend_template_ok": trend_template_ok, "ret_1m": ret_1m, "ret_3m": ret_3m, "ret_6m": ret_6m,
        "ret_blend": ret_blend,
        "vcp_breakout": vcp_breakout, "vcp_pivot": vcp_pivot, "vcp_base_low": vcp_base_low,
    })
    return out


def _rsi_condition(rsi_today: float, rsi_y1: float, rsi_y2: float) -> bool:
    t = RSI_THRESHOLD
    return rsi_today >= t and (rsi_today >= t)  # sustained-or-crossed both satisfy rsi_today>=t; kept explicit for clarity


def run_backtest(
    universe_data: dict[str, pd.DataFrame],
    benchmark: pd.Series,
    sector_by_ticker: dict[str, str],
    cap_band_by_ticker: dict[str, str],
    config: BacktestConfig,
    start: Optional[str] = None,
    end: Optional[str] = None,
    delivery_data: Optional[dict[str, pd.Series]] = None,
) -> tuple[list[Trade], pd.Series]:
    """
    Runs the full sector-rotation + momentum + portfolio strategy over history.
    Returns (closed_trades, daily_equity_curve).

    delivery_data : optional {ticker: deliv_per Series indexed by date}, from
        backtest/delivery_cache.py. Additive — merged onto each ticker's existing
        signals after the fact (no change to `_per_ticker_signals()` itself), and
        only used at all when `config.require_delivery_above_median` is True. See
        STRATEGY.md §13 for the tested hypothesis this exists to check.
    """
    logger.info("Precomputing signals for %d tickers ...", len(universe_data))
    signals: dict[str, pd.DataFrame] = {}
    for ticker, df in universe_data.items():
        sig = _per_ticker_signals(ticker, df)
        if sig is not None:
            signals[ticker] = sig

    for ticker, sig in signals.items():
        deliv = delivery_data.get(ticker) if delivery_data else None
        if deliv is None:
            sig["delivery_above_own_median"] = False
            continue
        deliv_aligned = deliv.reindex(sig.index)
        rolling_median = deliv_aligned.rolling(
            config.delivery_median_lookback_days, min_periods=config.delivery_median_lookback_days
        ).median()
        sig["delivery_above_own_median"] = (deliv_aligned > rolling_median).fillna(False)

    benchmark = benchmark.dropna()
    benchmark_sma200 = benchmark.rolling(REGIME_SMA_PERIOD, min_periods=REGIME_SMA_PERIOD).mean()
    benchmark_sma200_roc = (benchmark_sma200 / benchmark_sma200.shift(REGIME_ROC_LOOKBACK_DAYS) - 1.0) * 100.0
    benchmark_ret = {
        "1M": benchmark.pct_change(LOOKBACK_1M) * 100.0,
        "3M": benchmark.pct_change(LOOKBACK_3M) * 100.0,
        "6M": benchmark.pct_change(LOOKBACK_6M) * 100.0,
    }

    sector_members: dict[str, list[str]] = {}
    for ticker, sector in sector_by_ticker.items():
        if ticker in signals:
            sector_members.setdefault(sector, []).append(ticker)

    # Sector synthetic index: equal-weight mean of normalized close, over ALL members
    # with sufficient history (see module docstring — a stated simplification).
    sector_synthetic: dict[str, pd.Series] = {}
    for sector, tickers in sector_members.items():
        normalized = []
        for t in tickers:
            c = signals[t]["close"].dropna()
            if c.empty:
                continue
            normalized.append(c / c.iloc[0])
        if normalized:
            sector_synthetic[sector] = pd.concat(normalized, axis=1).mean(axis=1)

    sector_ret = {
        sector: {
            "1M": series.pct_change(LOOKBACK_1M) * 100.0,
            "3M": series.pct_change(LOOKBACK_3M) * 100.0,
            "6M": series.pct_change(LOOKBACK_6M) * 100.0,
        }
        for sector, series in sector_synthetic.items()
    }

    trading_days = benchmark.index
    if start:
        trading_days = trading_days[trading_days >= pd.Timestamp(start)]
    if end:
        trading_days = trading_days[trading_days <= pd.Timestamp(end)]
    # Need enough lookback history before the window starts for indicators to be valid.
    trading_days = trading_days[252:] if len(trading_days) > 252 else trading_days

    open_positions: dict[str, OpenPosition] = {}
    closed_trades: list[Trade] = []
    equity_curve: list[tuple[pd.Timestamp, float]] = []
    cash = config.account_size
    pending_entries: list[tuple[str, str]] = []  # (ticker, sector) decided on day T, entered T+1
    equity_peak = config.account_size
    entries_halted = False  # strategy-level drawdown circuit breaker state
    halted_since_idx: Optional[int] = None  # trading_days index when the halt started, for the cooldown timer

    for i, day in enumerate(trading_days):
        # ── 1. Process pending entries decided on the PREVIOUS day (T+1 open execution) ──
        if pending_entries and day in benchmark.index:
            deployed_pct = sum(
                p.quantity * signals[p.ticker]["close"].get(day, p.entry_price) for p in open_positions.values()
            ) / config.account_size * 100.0
            heat_pct = sum(p.risk_amount for p in open_positions.values()) / config.account_size * 100.0

            per_sector_open_count: dict[str, int] = {}
            for p in open_positions.values():
                per_sector_open_count[p.sector] = per_sector_open_count.get(p.sector, 0) + 1

            for ticker, sector in pending_entries:
                if ticker in open_positions:
                    continue
                sig = signals[ticker]
                if day not in sig.index:
                    continue
                entry_price = float(sig.loc[day, "open"])
                if entry_price != entry_price or entry_price <= 0:
                    continue

                atr_val = float(sig.loc[day, "atr"]) if day in sig.index else float("nan")
                if atr_val != atr_val or atr_val <= 0:
                    continue

                cap_band = cap_band_by_ticker.get(ticker, "Mid")
                risk_pct = {"Large": 1.0, "Mid": 0.75, "Small": 0.5}.get(cap_band, CAP_BAND_RISK_PCT_DEFAULT)

                if per_sector_open_count.get(sector, 0) >= MAX_POSITIONS_PER_SECTOR:
                    continue
                remaining_capital_pct = MAX_DEPLOYED_CAPITAL_PCT - deployed_pct
                remaining_heat_pct = MAX_PORTFOLIO_HEAT_PCT - heat_pct
                if remaining_capital_pct <= 0 or remaining_heat_pct <= 0 or risk_pct > remaining_heat_pct:
                    continue

                atr_stop_dist = ATR_STOP_MULTIPLIER * atr_val
                max_stop_dist = entry_price * MAX_STOP_PCT / 100.0
                stop_dist = min(atr_stop_dist, max_stop_dist)
                if config.entry_paradigm == "vcp_breakout":
                    # A valid base's own low is often tighter than the generic ATR/%
                    # caps — that's the whole point of VCP-derived stops (see plan).
                    vcp_base_low = sig.loc[day, "vcp_base_low"]
                    if vcp_base_low == vcp_base_low and 0 < vcp_base_low < entry_price:
                        stop_dist = min(stop_dist, entry_price - vcp_base_low)
                if stop_dist <= 0:
                    continue

                risk_amount = config.account_size * risk_pct / 100.0
                qty = int(risk_amount / stop_dist)
                max_alloc = config.account_size * min(20.0, remaining_capital_pct) / 100.0
                if qty * entry_price > max_alloc:
                    qty = int(max_alloc / entry_price)
                if qty <= 0:
                    continue

                open_positions[ticker] = OpenPosition(
                    ticker=ticker, sector=sector, cap_band=cap_band, entry_date=day,
                    entry_price=entry_price, quantity=qty, initial_risk_per_share=stop_dist,
                    stop=entry_price - stop_dist, highest_close=entry_price, risk_amount=risk_amount,
                )
                per_sector_open_count[sector] = per_sector_open_count.get(sector, 0) + 1
                deployed_pct += (qty * entry_price) / config.account_size * 100.0
                heat_pct += risk_pct

        pending_entries = []

        # ── 2. Exit checks for open positions (resting stop/target vs today's H/L) ──
        for ticker in list(open_positions.keys()):
            pos = open_positions[ticker]
            sig = signals[ticker]
            if day not in sig.index:
                continue
            o, h, l, c = (float(sig.loc[day, k]) for k in ("open", "high", "low", "close"))
            holding_days = trading_days.get_loc(day) - trading_days.get_loc(pos.entry_date) if pos.entry_date in trading_days else 0

            exit_price, exit_reason = None, ""

            if config.exit_paradigm == "fixed_target":
                target = pos.entry_price + FIXED_TARGET_R * pos.initial_risk_per_share
                if o < pos.stop:
                    exit_price, exit_reason = o, "Gap below stop"
                elif l <= pos.stop:
                    exit_price, exit_reason = pos.stop, "Stop hit"
                elif h >= target:
                    exit_price, exit_reason = target, f"Target {FIXED_TARGET_R:.1f}R hit"
                elif holding_days >= 15:
                    exit_price, exit_reason = o, "Time exit (15d)"
            else:  # trailing and scale_out both use the chandelier trail + RSI failure + time backstop
                if o < pos.stop:
                    exit_price, exit_reason = o, "Gap below stop"
                elif l <= pos.stop:
                    exit_price, exit_reason = pos.stop, "Stop hit"
                elif sig.loc[day, "rsi"] < RSI_FAILURE_FLOOR:
                    exit_price, exit_reason = o, "RSI momentum failure"
                elif holding_days >= TIME_BACKSTOP_DAYS:
                    exit_price, exit_reason = o, f"Time backstop ({TIME_BACKSTOP_DAYS}d)"

            if exit_price is not None:
                pnl = (exit_price - pos.entry_price) * pos.quantity * (1 - TRANSACTION_COST_PCT / 100.0)
                closed_trades.append(Trade(
                    ticker=ticker, sector=pos.sector, cap_band=pos.cap_band,
                    entry_date=pos.entry_date, entry_price=pos.entry_price, quantity=pos.quantity,
                    initial_stop=pos.entry_price - pos.initial_risk_per_share,
                    exit_date=day, exit_price=exit_price, exit_reason=exit_reason,
                    pnl_rupees=pnl, r_multiple=pnl / pos.risk_amount if pos.risk_amount else 0.0,
                ))
                cash += pnl
                del open_positions[ticker]
                continue

            # Still open: update trailing stop / highest close for tomorrow
            pos.highest_close = max(pos.highest_close, c)
            if config.exit_paradigm in ("trailing", "scale_out"):
                trail_stop = pos.highest_close - CHANDELIER_MULTIPLIER * float(sig.loc[day, "atr"])
                pos.stop = max(pos.stop, trail_stop)
            if config.exit_paradigm == "scale_out" and not pos.scaled_out:
                scale_target = pos.entry_price + SCALE_OUT_R * pos.initial_risk_per_share
                if h >= scale_target:
                    half_qty = pos.quantity // 2
                    if half_qty > 0:
                        pnl = (scale_target - pos.entry_price) * half_qty * (1 - TRANSACTION_COST_PCT / 100.0)
                        cash += pnl
                        pos.quantity -= half_qty
                        pos.risk_amount *= (pos.quantity / (pos.quantity + half_qty))
                        pos.scaled_out = True

        # ── 3. Market regime check — price above SMA AND the SMA itself rising
        #    at least REGIME_MIN_SMA_ROC_PCT (see modules/market_regime.py's
        #    docstring: a plain price>SMA check missed a real 5-month chop
        #    where price stayed above a barely-rising 200-SMA) ────────────────
        in_uptrend = True
        if config.apply_market_regime_filter and day in benchmark_sma200.index:
            sma_today = benchmark_sma200.get(day)
            roc_today = benchmark_sma200_roc.get(day)
            if sma_today == sma_today and roc_today == roc_today:  # neither NaN
                in_uptrend = (benchmark.loc[day] > sma_today) and (roc_today >= REGIME_MIN_SMA_ROC_PCT)

        # ── 4. Decide today's leading sectors + candidate entries (executed T+1) ──
        if in_uptrend and not entries_halted:
            today_sector_scores = {}
            for sector, rets in sector_ret.items():
                rs = {}
                for label, days in (("1M", LOOKBACK_1M), ("3M", LOOKBACK_3M), ("6M", LOOKBACK_6M)):
                    sv, bv = rets[label].get(day), benchmark_ret[label].get(day)
                    if sv == sv and bv == bv:
                        rs[label] = sv - bv
                if rs:
                    wsum = sum(LOOKBACK_WEIGHTS[k] for k in rs)
                    today_sector_scores[sector] = sum(rs[k] * LOOKBACK_WEIGHTS[k] for k in rs) / wsum

            leading = sorted(today_sector_scores, key=today_sector_scores.get, reverse=True)[:TOP_N_SECTORS]

            candidates_by_sector: dict[str, list[tuple[str, float]]] = {}
            for sector in leading:
                cands = []
                for ticker in sector_members.get(sector, []):
                    if ticker in open_positions:
                        continue
                    sig = signals[ticker]
                    if day not in sig.index:
                        continue
                    row = sig.loc[day]
                    if config.apply_liquidity_filter:
                        adv = row["avg_dollar_vol"]
                        if adv != adv or adv < MIN_AVG_DAILY_VALUE_INR:
                            continue
                    if config.require_delivery_above_median and not bool(row["delivery_above_own_median"]):
                        continue

                    if config.entry_paradigm == "vcp_breakout":
                        # Structural gate (Trend Template + RS Rating) still required — VCP is a
                        # consolidation *within* an existing uptrend, not a standalone signal —
                        # but the trigger is the VCP breakout itself, not RSI/MACD (kept isolated
                        # so VCP's own contribution is cleanly measurable, not confounded).
                        if not row["trend_template_ok"] or not row["vcp_breakout"]:
                            continue
                        rs_today = _cross_sectional_rank(signals, day, ticker)
                        if rs_today is None or rs_today < RS_RATING_THRESHOLD:
                            continue
                        strength = 1.0  # tiebreak order among same-day VCP breakouts doesn't matter much
                    else:
                        if row["rsi"] != row["rsi"] or row["rsi"] < RSI_THRESHOLD or not row["macd_bullish"]:
                            continue
                        if config.entry_paradigm == "trend_template":
                            if not row["trend_template_ok"]:
                                continue
                            rs_today = _cross_sectional_rank(signals, day, ticker)
                            if rs_today is None or rs_today < RS_RATING_THRESHOLD:
                                continue
                        strength = row["rsi"] / 100.0
                    cands.append((ticker, strength))
                cands.sort(key=lambda x: x[1], reverse=True)
                candidates_by_sector[sector] = [t for t, _ in cands[:MAX_POSITIONS_PER_SECTOR]]

            for sector, tickers in candidates_by_sector.items():
                for t in tickers:
                    pending_entries.append((t, sector))

        equity = cash + sum(
            p.quantity * float(signals[p.ticker]["close"].get(day, p.entry_price)) for p in open_positions.values()
        )
        equity_curve.append((day, equity))

        # Portfolio-level drawdown circuit breaker: halts NEW entries — via
        # `entries_halted`, checked in section 4 above — once *this strategy's
        # own* equity (not the benchmark's) falls `drawdown_halt_pct` below its
        # peak; resumes after a fixed `drawdown_halt_days` cooldown, NOT once
        # equity recovers. (Found empirically: an equity-recovery resume
        # condition self-locks, since halting entries freezes equity at flat
        # cash the moment existing positions finish closing — there is then no
        # mechanism left for equity to ever recover on its own.) This state
        # reflects TODAY's close and only affects TOMORROW's entry decision
        # (section 4 ran earlier this iteration), so it introduces no look-ahead.
        if config.apply_drawdown_circuit_breaker:
            equity_peak = max(equity_peak, equity)
            dd_pct = (equity / equity_peak - 1.0) * 100.0 if equity_peak else 0.0
            if not entries_halted and dd_pct <= -config.drawdown_halt_pct:
                entries_halted = True
                halted_since_idx = i
            elif entries_halted and halted_since_idx is not None and (i - halted_since_idx) >= config.drawdown_halt_days:
                entries_halted = False
                halted_since_idx = None

    equity_series = pd.Series({d: v for d, v in equity_curve})
    logger.info(
        "Backtest complete: %d closed trades, final equity ₹%.0f (started ₹%.0f)",
        len(closed_trades), equity_series.iloc[-1] if len(equity_series) else config.account_size,
        config.account_size,
    )
    return closed_trades, equity_series


_rank_cache: dict[pd.Timestamp, dict[str, float]] = {}


def _cross_sectional_rank(signals: dict[str, pd.DataFrame], day: pd.Timestamp, ticker: str) -> Optional[float]:
    """Universe-wide RS Rating for `ticker` on `day` — percentile rank of ret_blend across all tickers with data that day."""
    if day not in _rank_cache:
        values = {}
        for t, sig in signals.items():
            if day in sig.index:
                v = sig.loc[day, "ret_blend"]
                if v == v:
                    values[t] = v
        if values:
            ranked = pd.Series(values).rank(pct=True) * 100.0
            _rank_cache[day] = ranked.to_dict()
        else:
            _rank_cache[day] = {}
    return _rank_cache[day].get(ticker)
