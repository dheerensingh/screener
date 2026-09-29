"""
Backtest: Delivery% Filter — Before/After Comparison CLI

    python -m backtest.run_delivery_backtest

Tests the hypothesis that requiring delivery% above a stock's own trailing-median
at entry improves the strategy, on the current best-performing paradigm (Trend
Template entry + trailing-chandelier exit, per STRATEGY.md §8b) — baseline vs
filter-on, same rigor as the VCP comparison. Restricted to the window where NSE's
delivery data actually exists (2019-09-30 onward — see delivery_cache.py's
DELIVERY_DATA_AVAILABLE_FROM) so the comparison isn't confounded by years where the
filter has no data to work with. Only promoted to a live gate/default if this shows
a genuine, non-marginal improvement — see STRATEGY.md §13 for the verdict.
"""

import logging
import sys

from modules.sector_mapper import build_universe

from .data_cache import fetch_and_cache
from .delivery_cache import DELIVERY_DATA_AVAILABLE_FROM, load_delivery_cache, pivot_delivery_by_ticker
from .metrics import compute_metrics
from .simulator import BacktestConfig, run_backtest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("backtest.run_delivery")

BEST_ENTRY = "trend_template"
BEST_EXIT = "trailing"


def main() -> int:
    logger.info("Loading universe data (cached, or fetching if absent) ...")
    universe_data, benchmark = fetch_and_cache(period="5y")

    delivery_df = load_delivery_cache()
    if delivery_df.empty:
        logger.error(
            "No delivery cache found — run `python -m backtest.run_delivery_cache` first."
        )
        return 1
    delivery_data = pivot_delivery_by_ticker(delivery_df)
    logger.info("Loaded delivery%% cache: %d tickers, from %s", len(delivery_data), DELIVERY_DATA_AVAILABLE_FROM.date())

    universe = build_universe()
    sector_by_ticker = {u.ticker: u.sector for u in universe}
    cap_band_by_ticker = {u.ticker: u.cap_band for u in universe}

    start = DELIVERY_DATA_AVAILABLE_FROM.strftime("%Y-%m-%d")

    results = []
    for label, require_delivery in [("baseline (no delivery filter)", False), ("delivery% > own 20d median", True)]:
        config = BacktestConfig(
            entry_paradigm=BEST_ENTRY, exit_paradigm=BEST_EXIT,
            require_delivery_above_median=require_delivery,
        )
        logger.info("Running variant: %s ...", label)
        trades, equity = run_backtest(
            universe_data, benchmark, sector_by_ticker, cap_band_by_ticker, config,
            start=start, delivery_data=delivery_data,
        )
        metrics = compute_metrics(trades, equity)
        results.append((label, metrics))

    print("\n" + "=" * 100)
    print(f"Comparison window: {start} onward (delivery data availability limit)")
    print(f"{'VARIANT':32s} {'TRADES':>7s} {'WIN%':>6s} {'AVG_R':>7s} "
          f"{'CAGR%':>7s} {'MAXDD%':>7s} {'SHARPE':>7s} {'AVG_DAYS':>9s}")
    print("-" * 100)
    for label, m in results:
        print(f"{label:32s} {m.total_trades:7d} {m.win_rate_pct:6.1f} {m.avg_r_multiple:7.2f} "
              f"{m.cagr_pct:7.1f} {m.max_drawdown_pct:7.1f} {m.sharpe_ratio:7.2f} {m.avg_holding_days:9.1f}")
    print("=" * 100)

    return 0


if __name__ == "__main__":
    sys.exit(main())
