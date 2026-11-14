"""
Backtest: does the stock selection beat chance? (STRATEGY.md §21)

    python -m backtest.run_edge_test [--period 10y] [--seeds 12]

The strategy's 9-year backtest beats Nifty500, but the universe is today's
Nifty500 applied to the past (survivorship bias, CLAUDE.md), so a good number
could come from the universe rather than the rules. This compares the real
rules (stock rules only: no liquid fund, no gold) with random versions that
share the same universe, sizing, stops and exits:

- random_qualifier: same screen, same sectors, but qualifiers taken in random
  order — measures the ranking.
- random_member: same sectors, no screen — any liquid member with RSI >= 45 —
  measures the screen (Trend Template, RS rating, RSI/MACD).
- random sectors: the real screen inside 4 random sectors, redrawn every 21
  sessions — measures the sector rotation.

Survivorship bias helps the random runs exactly as much as the real one, so
the gap between them is the rules' own contribution. Also runs RSI ceilings
at entry and splits the real trades by RSI at the signal.
"""

import argparse
import logging
import os
import sys
from dataclasses import replace

import numpy as np

from modules.sector_mapper import build_universe

from .data_cache import fetch_and_cache
from .metrics import compute_metrics
from .run_portfolio_backtest import BASE
from .simulator import run_backtest

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("backtest.edge")

NULLS = [
    ("Random order among qualifiers", dict(pick_mode="random_qualifier")),
    ("Random liquid sector member (no screen)", dict(pick_mode="random_member")),
    ("Real screen, random sectors", dict(sector_mode="random")),
]
RSI_CEILINGS = (70.0, 75.0, 80.0)
RSI_BUCKETS = ((0, 60), (60, 65), (65, 70), (70, 75), (75, 80), (80, 101))


def _stats(trades, equity) -> dict:
    m = compute_metrics(trades, equity)
    return dict(trades=m.total_trades, win=m.win_rate_pct, cagr=m.cagr_pct, dd=m.max_drawdown_pct,
                sharpe=m.sharpe_ratio, avg_r=sum(t.r_multiple for t in trades) / len(trades) if trades else 0.0)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--period", default="10y")
    parser.add_argument("--seeds", type=int, default=12)
    args = parser.parse_args()

    universe_data, benchmark = fetch_and_cache(period=args.period)
    universe = build_universe()
    sector_by_ticker = {u.ticker: u.sector for u in universe}
    cap_band_by_ticker = {u.ticker: u.cap_band for u in universe}

    def run(cfg):
        return run_backtest(universe_data, benchmark, sector_by_ticker, cap_band_by_ticker, cfg)

    logger.info("Running: real rules")
    real_trades, real_equity = run(BASE)
    real = _stats(real_trades, real_equity)

    lines = [
        f"### Edge test — {real_equity.index[0]:%Y-%m-%d} to {real_equity.index[-1]:%Y-%m-%d}, "
        f"stock rules only, {args.seeds} random runs per null",
        "",
        "| Version | Runs | CAGR % (5th / median / 95th) | Sharpe (5th / median / 95th) | Avg R (median) "
        "| Max DD % (median) | Real rules beat (CAGR / Sharpe) |",
        "|---|---|---|---|---|---|---|",
        f"| **Real rules** | 1 | **{real['cagr']:+.1f}** | **{real['sharpe']:.2f}** | {real['avg_r']:+.2f} "
        f"| {real['dd']:.1f} | |",
    ]
    for name, change in NULLS:
        runs = []
        for seed in range(args.seeds):
            logger.info("Running: %s, seed %d", name, seed)
            runs.append(_stats(*run(replace(BASE, random_seed=seed, **change))))
        c = np.array([r["cagr"] for r in runs])
        s = np.array([r["sharpe"] for r in runs])
        p = lambda a: f"{np.percentile(a, 5):+.1f} / {np.median(a):+.1f} / {np.percentile(a, 95):+.1f}"
        q = lambda a: f"{np.percentile(a, 5):.2f} / {np.median(a):.2f} / {np.percentile(a, 95):.2f}"
        lines.append(
            f"| {name} | {len(runs)} | {p(c)} | {q(s)} | {np.median([r['avg_r'] for r in runs]):+.2f} "
            f"| {np.median([r['dd'] for r in runs]):.1f} | {(c < real['cagr']).mean():.0%} / {(s < real['sharpe']).mean():.0%} |"
        )

    lines += ["", "#### RSI at entry", "", "| Version | Trades | Win % | Avg R | CAGR % | Max DD % | Sharpe |",
              "|---|---|---|---|---|---|---|",
              f"| Real rules (no ceiling) | {real['trades']} | {real['win']:.1f} | {real['avg_r']:+.2f} "
              f"| {real['cagr']:+.1f} | {real['dd']:.1f} | {real['sharpe']:.2f} |"]
    for ceiling in RSI_CEILINGS:
        logger.info("Running: RSI ceiling %.0f", ceiling)
        r = _stats(*run(replace(BASE, max_entry_rsi=ceiling)))
        lines.append(f"| Skip RSI > {ceiling:.0f} | {r['trades']} | {r['win']:.1f} | {r['avg_r']:+.2f} "
                     f"| {r['cagr']:+.1f} | {r['dd']:.1f} | {r['sharpe']:.2f} |")
    lines += ["", "Real rules' trades by RSI on the signal day:", "",
              "| RSI | Trades | Win % | Avg R |", "|---|---|---|---|"]
    for lo, hi in RSI_BUCKETS:
        b = [t for t in real_trades if lo <= t.rsi_at_signal < hi]
        if b:
            lines.append(f"| {lo}-{min(hi, 100)} | {len(b)} | {sum(t.pnl_rupees > 0 for t in b) / len(b) * 100:.1f} "
                         f"| {sum(t.r_multiple for t in b) / len(b):+.2f} |")

    text = "\n".join(lines)
    print(text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(text + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
