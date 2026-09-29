"""
Backtest: CLI entry point

    python -m backtest.run_backtest [--start 2021-01-01] [--end 2026-09-27] [--force-refetch]

Runs all 6 entry x exit paradigm combinations over the cached 5y universe,
prints a comparison table, and renders an HTML report for the best-performing
variant (by CAGR/max-drawdown tradeoff — see backtest/report.py).
"""

import argparse
import logging
import sys

from modules.sector_mapper import build_universe

from .data_cache import fetch_and_cache
from .metrics import compute_metrics
from .report import build_backtest_report, publish_backtest_report
from .simulator import BacktestConfig, run_backtest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("backtest.run")

ENTRY_PARADIGMS = ["trend_template", "rsi_macd_only", "vcp_breakout"]
EXIT_PARADIGMS = ["trailing", "fixed_target", "scale_out"]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the sector-rotation swing strategy backtest.")
    parser.add_argument("--start", default=None, help="YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="YYYY-MM-DD")
    parser.add_argument("--force-refetch", action="store_true", help="Ignore the local data cache")
    parser.add_argument("--account-size", type=float, default=500_000.0)
    args = parser.parse_args()

    logger.info("Loading universe data (cached, or fetching if absent) ...")
    universe_data, benchmark = fetch_and_cache(period="5y", force=args.force_refetch)

    universe = build_universe()
    sector_by_ticker = {u.ticker: u.sector for u in universe}
    cap_band_by_ticker = {u.ticker: u.cap_band for u in universe}

    results = []
    for entry in ENTRY_PARADIGMS:
        for exit_p in EXIT_PARADIGMS:
            config = BacktestConfig(entry_paradigm=entry, exit_paradigm=exit_p, account_size=args.account_size)
            logger.info("Running variant: entry=%s exit=%s ...", entry, exit_p)
            trades, equity = run_backtest(
                universe_data, benchmark, sector_by_ticker, cap_band_by_ticker,
                config, start=args.start, end=args.end,
            )
            metrics = compute_metrics(trades, equity)
            results.append((config, trades, equity, metrics))

    print("\n" + "=" * 100)
    print(f"{'ENTRY':16s} {'EXIT':12s} {'TRADES':>7s} {'WIN%':>6s} {'AVG_R':>7s} "
          f"{'CAGR%':>7s} {'MAXDD%':>7s} {'SHARPE':>7s} {'AVG_DAYS':>9s}")
    print("-" * 100)
    for config, _, _, m in results:
        print(f"{config.entry_paradigm:16s} {config.exit_paradigm:12s} {m.total_trades:7d} "
              f"{m.win_rate_pct:6.1f} {m.avg_r_multiple:7.2f} {m.cagr_pct:7.1f} "
              f"{m.max_drawdown_pct:7.1f} {m.sharpe_ratio:7.2f} {m.avg_holding_days:9.1f}")
    print("=" * 100)

    # Pick the best by CAGR/max-drawdown ratio (Calmar-like) among variants with enough trades
    scored = [
        (config, trades, equity, m, m.cagr_pct / abs(m.max_drawdown_pct) if m.max_drawdown_pct else 0.0)
        for config, trades, equity, m in results if m.total_trades >= 10
    ]
    if scored:
        best_config, best_trades, best_equity, best_metrics, _ = max(scored, key=lambda x: x[4])
        logger.info(
            "Best variant by CAGR/MaxDD: entry=%s exit=%s", best_config.entry_paradigm, best_config.exit_paradigm,
        )
        html = build_backtest_report(best_config, best_trades, best_equity, best_metrics, results)
        path = publish_backtest_report(html)
        logger.info("Backtest report published: %s", path)
    else:
        logger.warning("No variant produced enough trades (>=10) to select a 'best' report.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
