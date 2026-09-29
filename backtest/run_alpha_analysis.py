"""
Backtest: Alpha/Beta Regression Diagnostic CLI

    python -m backtest.run_alpha_analysis

Runs the same 9-combination entry x exit grid as run_backtest.py against the existing
cached 5y data (no new fetch), regresses each variant's daily equity returns on the
Nifty500 benchmark's daily returns, and prints annualized alpha / beta / R^2 / t-stat
per variant — see STRATEGY.md §11 for the full table and interpretation.
"""

import logging
import sys

from modules.sector_mapper import build_universe

from .alpha_analysis import compute_alpha_beta
from .data_cache import fetch_and_cache
from .run_backtest import ENTRY_PARADIGMS, EXIT_PARADIGMS
from .simulator import BacktestConfig, run_backtest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("backtest.run_alpha")


def main() -> int:
    logger.info("Loading universe data (cached, or fetching if absent) ...")
    universe_data, benchmark = fetch_and_cache(period="5y")

    universe = build_universe()
    sector_by_ticker = {u.ticker: u.sector for u in universe}
    cap_band_by_ticker = {u.ticker: u.cap_band for u in universe}

    rows = []
    for entry in ENTRY_PARADIGMS:
        for exit_p in EXIT_PARADIGMS:
            config = BacktestConfig(entry_paradigm=entry, exit_paradigm=exit_p)
            logger.info("Running variant: entry=%s exit=%s ...", entry, exit_p)
            _, equity = run_backtest(
                universe_data, benchmark, sector_by_ticker, cap_band_by_ticker, config,
            )
            if len(equity) < 30:
                logger.warning("  Skipping (too few equity points: %d)", len(equity))
                continue
            ab = compute_alpha_beta(equity, benchmark)
            rows.append((entry, exit_p, ab))

    print("\n" + "=" * 100)
    print(f"{'ENTRY':16s} {'EXIT':12s} {'N':>5s} {'ALPHA(ann%)':>12s} {'BETA':>7s} "
          f"{'R2':>6s} {'T-STAT':>8s} {'P-VALUE':>8s} {'SIG(5%)':>8s}")
    print("-" * 100)
    for entry, exit_p, ab in rows:
        print(f"{entry:16s} {exit_p:12s} {ab.n_obs:5d} {ab.alpha_annualized_pct:12.2f} "
              f"{ab.beta:7.2f} {ab.r_squared:6.2f} {ab.alpha_tstat:8.2f} "
              f"{ab.alpha_pvalue:8.3f} {'YES' if ab.significant_at_5pct else 'no':>8s}")
    print("=" * 100)

    return 0


if __name__ == "__main__":
    sys.exit(main())
