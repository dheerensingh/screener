"""
Backtest: rule-change comparison (STRATEGY.md §17, §18)

    python -m backtest.run_portfolio_backtest [--period 10y] [--force-refetch]

Runs the live configuration (Trend Template entry, trailing exit) next to
variants that each change one rule, plus combinations, and prints one
comparison table next to Nifty500 buy & hold. §18's variants test the fixes
suggested by the first two weeks of paper trading: the 8% stop cap, adds to
winners, buying back a stock right after a loss, small-cap concentration and
sizing in a down market. A second table splits the live rules' own trades by
those same conditions. Both are written to $GITHUB_STEP_SUMMARY in Actions.
"""

import argparse
import logging
import os
import sys
from dataclasses import replace

import pandas as pd

from modules.sector_mapper import build_universe

from .data_cache import fetch_and_cache
from .metrics import _cagr, _max_drawdown, _sharpe, compute_metrics
from .simulator import BacktestConfig, run_backtest

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("backtest.portfolio")

BASE = BacktestConfig(entry_paradigm="trend_template", exit_paradigm="trailing")  # = live settings
OLD = replace(BASE, weighted_sector_slots=False, max_total_positions=8, allow_pyramiding=False, risk_scale=1.0)
FIX_A = dict(stop_rule="atr")
FIX_B = dict(add_min_open_r=1.0)
FIX_C = dict(reentry_cooldown_days=10)
FIX_D = dict(max_small_cap_heat_pct=4.0, regime_down_risk_scale=0.5)
VARIANTS = [
    ("Live rules", BASE),
    ("Old: 2/sector, max 8, no adds", OLD),
    ("A1 Stop = full 2xATR (fewer shares)", replace(BASE, **FIX_A)),
    ("A2 Skip stocks with 2xATR > 8%", replace(BASE, stop_rule="skip_wide")),
    ("B1 Adds only at first lot >= +1R", replace(BASE, **FIX_B)),
    ("B2 No adds", replace(BASE, allow_pyramiding=False)),
    ("C Wait 10 sessions after a loss", replace(BASE, **FIX_C)),
    ("D1 Small-cap risk <= 4%", replace(BASE, max_small_cap_heat_pct=4.0)),
    ("D2 Half risk when market is down", replace(BASE, regime_down_risk_scale=0.5)),
    ("A1+B1+C", replace(BASE, **FIX_A, **FIX_B, **FIX_C)),
    ("A1+B1+C+D1+D2", replace(BASE, **FIX_A, **FIX_B, **FIX_C, **FIX_D)),
    ("B1+D1", replace(BASE, **FIX_B, max_small_cap_heat_pct=4.0)),
    ("B1+D1+D2", replace(BASE, **FIX_B, **FIX_D)),
]
YEARLY = ("Live rules", "Old: 2/sector, max 8, no adds", "B1+D1")  # variants in the year-by-year table


def _avg_names_held(trades, equity: pd.Series) -> float:
    days = equity.index
    held = pd.Series(0.0, index=days)
    per_day: dict[pd.Timestamp, set] = {}
    for t in trades:
        for d in days[(days >= t.entry_date) & (days < t.exit_date)]:
            per_day.setdefault(d, set()).add(t.ticker)
    for d, names in per_day.items():
        held[d] = len(names)
    return float(held.mean())


def _cagr_since(equity: pd.Series, years: float) -> float:
    start = equity.index[-1] - pd.DateOffset(years=years)
    return _cagr(equity[equity.index >= start])


def _group(name: str, trades) -> str:
    if not trades:
        return f"| {name} | 0 | | | |"
    wins = sum(t.pnl_rupees > 0 for t in trades)
    avg_r = sum(t.r_multiple for t in trades) / len(trades)
    pnl = sum(t.pnl_rupees for t in trades)
    return f"| {name} | {len(trades)} | {wins / len(trades) * 100:.1f} | {avg_r:+.2f} | {pnl:+,.0f} |"


def _breakdown(trades) -> list[str]:
    """The live rules' trades split by the conditions §18's fixes target."""
    first = [t for t in trades if not t.is_add]
    adds = [t for t in trades if t.is_add]
    return [
        "", "#### Live rules: trades split by condition", "",
        "| Group | Trades | Win % | Avg R | P&L ₹ |", "|---|---|---|---|---|",
        _group("All trades", trades),
        _group("Stop at the 8% cap (2xATR wider)", [t for t in trades if t.stop_capped]),
        _group("Stop = 2xATR (under 8%)", [t for t in trades if not t.stop_capped]),
        _group("Bought within 10 sessions of a loss in the same stock", [t for t in trades if t.reentry_after_loss]),
        _group("Other buys", [t for t in trades if not t.reentry_after_loss]),
        _group("First buys", first),
        _group("Adds, first lot < +1R", [t for t in adds if t.add_open_r < 1.0]),
        _group("Adds, first lot >= +1R", [t for t in adds if t.add_open_r >= 1.0]),
        _group("Small caps", [t for t in trades if t.cap_band == "Small"]),
        _group("Mid + large caps", [t for t in trades if t.cap_band != "Small"]),
    ]


def _yearly_table(curves: dict[str, pd.Series], bench: pd.Series) -> list[str]:
    """Calendar-year % return of each equity curve next to Nifty500's."""
    curves = {**curves, "Nifty500": bench}
    years = sorted({d.year for d in bench.index})
    lines = ["", "#### Return by calendar year (%)", "",
             "| Year | " + " | ".join(curves) + " |", "|---" * (len(curves) + 1) + "|"]
    for y in years:
        cells = []
        for c in curves.values():
            prev, cur = c[c.index.year < y], c[c.index.year == y]
            start = prev.iloc[-1] if len(prev) else (cur.iloc[0] if len(cur) else float("nan"))
            cells.append(f"{(cur.iloc[-1] / start - 1) * 100:+.1f}" if len(cur) else "")
        lines.append(f"| {y} | " + " | ".join(cells) + " |")
    return lines


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--period", default="10y")
    parser.add_argument("--force-refetch", action="store_true")
    args = parser.parse_args()

    universe_data, benchmark = fetch_and_cache(period=args.period, force=args.force_refetch)
    universe = build_universe()
    sector_by_ticker = {u.ticker: u.sector for u in universe}
    cap_band_by_ticker = {u.ticker: u.cap_band for u in universe}

    rows = []
    window = None
    breakdown: list[str] = []
    yearly: dict[str, pd.Series] = {}
    for name, cfg in VARIANTS:
        logger.info("Running: %s", name)
        trades, equity = run_backtest(universe_data, benchmark, sector_by_ticker, cap_band_by_ticker, cfg)
        m = compute_metrics(trades, equity)
        window = window or (equity.index[0], equity.index[-1])
        adds = [t for t in trades if t.is_add]
        add_win = (sum(t.pnl_rupees > 0 for t in adds) / len(adds) * 100.0) if adds else float("nan")
        avg_r = sum(t.r_multiple for t in trades) / len(trades) if trades else 0.0
        rows.append((name, m.total_trades, len(adds), m.win_rate_pct, add_win, avg_r, _avg_names_held(trades, equity),
                     m.cagr_pct, _cagr_since(equity, 3), m.max_drawdown_pct, m.sharpe_ratio,
                     m.cagr_pct / abs(m.max_drawdown_pct) if m.max_drawdown_pct else 0.0))
        if cfg is BASE:
            breakdown = _breakdown(trades)
        if name in YEARLY:
            yearly[name] = equity

    bench = benchmark.dropna()
    bench = bench[(bench.index >= window[0]) & (bench.index <= window[1])]
    bh_cagr, bh_dd, bh_sharpe = _cagr(bench), _max_drawdown(bench), _sharpe(bench)
    bh_cagr3 = _cagr_since(bench, 3)

    lines = [
        f"### Portfolio rules backtest — {window[0]:%Y-%m-%d} to {window[1]:%Y-%m-%d}, "
        f"{len(universe_data)} stocks, Trend Template + trailing exit",
        "",
        "| Variant | Trades | Adds | Win % | Add win % | Avg R | Avg stocks held | CAGR % | CAGR last 3y % "
        "| Max DD % | Sharpe | Calmar |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        lines.append("| {} | {} | {} | {:.1f} | {:.1f} | {:+.2f} | {:.1f} | {:+.1f} | {:+.1f} | {:.1f} | {:.2f} | {:.2f} |"
                     .format(*r))
    lines.append(f"| *Nifty500 buy & hold* | | | | | | | {bh_cagr:+.1f} | {bh_cagr3:+.1f} | {bh_dd:.1f} | {bh_sharpe:.2f} | "
                 f"{bh_cagr / abs(bh_dd) if bh_dd else 0:.2f} |")
    lines += breakdown
    lines += _yearly_table(yearly, bench)
    text = "\n".join(lines)
    print(text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(text + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
