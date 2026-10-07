"""
Backtest: portfolio-shape comparison (STRATEGY.md §17)

    python -m backtest.run_portfolio_backtest [--period 10y] [--force-refetch]

Runs the live configuration (Trend Template entry, trailing exit) under the
old portfolio rules (2 stocks per leading sector, 8 max, no adds) and the new
ones (15 stocks, sector slots weighted by relative strength, adds to winners)
at a few risk levels, and prints one comparison table next to Nifty500 buy &
hold. Also written to $GITHUB_STEP_SUMMARY when run in GitHub Actions.
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

BASE = BacktestConfig(entry_paradigm="trend_template", exit_paradigm="trailing")
VARIANTS = [
    ("Old: 2/sector, max 8, no adds", replace(BASE, weighted_sector_slots=False, max_total_positions=8,
                                              allow_pyramiding=False)),
    ("15 weighted, no adds", replace(BASE, allow_pyramiding=False)),
    ("15 weighted + adds", BASE),
    ("15 weighted + adds, risk x0.75", replace(BASE, risk_scale=0.75)),
    ("15 weighted + adds, risk x0.6", replace(BASE, risk_scale=0.6)),
    ("15 weighted + adds, risk x0.6, heat 10%", replace(BASE, risk_scale=0.6, max_portfolio_heat_pct=10.0)),
    ("15 weighted, no adds, risk x0.6", replace(BASE, risk_scale=0.6, allow_pyramiding=False)),
]


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
    for name, cfg in VARIANTS:
        logger.info("Running: %s", name)
        trades, equity = run_backtest(universe_data, benchmark, sector_by_ticker, cap_band_by_ticker, cfg)
        m = compute_metrics(trades, equity)
        window = window or (equity.index[0], equity.index[-1])
        adds = [t for t in trades if t.is_add]
        add_win = (sum(t.pnl_rupees > 0 for t in adds) / len(adds) * 100.0) if adds else float("nan")
        rows.append((name, m.total_trades, len(adds), m.win_rate_pct, add_win, _avg_names_held(trades, equity),
                     m.cagr_pct, m.max_drawdown_pct, m.sharpe_ratio,
                     m.cagr_pct / abs(m.max_drawdown_pct) if m.max_drawdown_pct else 0.0))

    bench = benchmark.dropna()
    bench = bench[(bench.index >= window[0]) & (bench.index <= window[1])]
    bh_cagr, bh_dd, bh_sharpe = _cagr(bench), _max_drawdown(bench), _sharpe(bench)

    lines = [
        f"### Portfolio rules backtest — {window[0]:%Y-%m-%d} to {window[1]:%Y-%m-%d}, "
        f"{len(universe_data)} stocks, Trend Template + trailing exit",
        "",
        "| Variant | Trades | Adds | Win % | Add win % | Avg stocks held | CAGR % | Max DD % | Sharpe | Calmar |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        lines.append("| {} | {} | {} | {:.1f} | {:.1f} | {:.1f} | {:+.1f} | {:.1f} | {:.2f} | {:.2f} |".format(*r))
    lines.append(f"| *Nifty500 buy & hold* | | | | | | {bh_cagr:+.1f} | {bh_dd:.1f} | {bh_sharpe:.2f} | "
                 f"{bh_cagr / abs(bh_dd) if bh_dd else 0:.2f} |")
    text = "\n".join(lines)
    print(text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(text + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
