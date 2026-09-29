"""
Backtest: Periodic-Rebalance Factor Strategy CLI

    python -m backtest.run_factor_backtest
    python -m backtest.run_factor_backtest --universe-scope midcap_only

Runs the 6-combination grid (2 rebalance frequencies x 3 overlay settings) on
the cached universe and prints CAGR/MaxDD/Sharpe/Calmar for each, plus
reference lines for comparison. Default (`full_nifty500`, 30 holdings)
reproduces Task 5's original run unchanged. `--universe-scope midcap_only`
is Task 7 / Path B (2026-09-30, see STRATEGY.md §15) — restricts to
cap_band=="Mid" tickers and 50 holdings, a closer mechanical match for
Nifty Midcap150 Momentum 50 specifically. Either way, see STRATEGY.md §8d/§15
for why this backtest's own CAGR number is not to be read as validated
performance — the printed reference lines carry that comparison forward,
not just the written doc.
"""

import argparse
import logging
import sys

from modules.sector_mapper import build_universe

from .data_cache import fetch_and_cache
from .factor_momentum import MIDCAP_NUM_HOLDINGS, NUM_HOLDINGS, FactorConfig, run_factor_backtest
from .metrics import _cagr, _max_drawdown, _sharpe

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("backtest.run_factor")

REBALANCE_FREQS = ["semiannual", "quarterly"]
OVERLAYS = ["none", "half_defensive", "full_defensive"]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the periodic-rebalance factor strategy backtest.")
    parser.add_argument("--universe-scope", choices=["full_nifty500", "midcap_only"], default="full_nifty500")
    parser.add_argument("--num-holdings", type=int, default=None)
    args = parser.parse_args()

    num_holdings = args.num_holdings or (
        MIDCAP_NUM_HOLDINGS if args.universe_scope == "midcap_only" else NUM_HOLDINGS
    )

    logger.info("Loading universe data (cached, or fetching if absent) ...")
    universe_data, benchmark = fetch_and_cache(period="10y")

    cap_band_by_ticker = None
    if args.universe_scope == "midcap_only":
        universe = build_universe()
        cap_band_by_ticker = {u.ticker: u.cap_band for u in universe}
        eligible = sum(1 for b in cap_band_by_ticker.values() if b == "Mid")
        logger.info("Universe scope=midcap_only: %d 'Mid' cap-band tickers tagged (sanity-check: expect ~150)", eligible)

    results = []
    for freq in REBALANCE_FREQS:
        for overlay in OVERLAYS:
            config = FactorConfig(
                rebalance_freq=freq, overlay=overlay, num_holdings=num_holdings,
                universe_scope=args.universe_scope,
            )
            logger.info("Running variant: rebalance=%s overlay=%s ...", freq, overlay)
            equity = run_factor_backtest(universe_data, benchmark, config, cap_band_by_ticker=cap_band_by_ticker)
            results.append((freq, overlay, equity))

    print("\n" + "=" * 80)
    print(f"Universe scope: {args.universe_scope} ({num_holdings} holdings)")
    print(f"{'REBALANCE':12s} {'OVERLAY':16s} {'CAGR':>8s} {'MaxDD':>8s} {'Sharpe':>8s} {'Calmar':>8s}")
    print("-" * 80)
    for freq, overlay, equity in results:
        cagr, dd, sharpe = _cagr(equity), _max_drawdown(equity), _sharpe(equity)
        calmar = cagr / abs(dd) if dd else 0.0
        print(f"{freq:12s} {overlay:16s} {cagr:+7.1f}% {dd:7.1f}% {sharpe:8.2f} {calmar:8.2f}")
    print("-" * 80)
    print("Reference lines (NOT this run's own output — see STRATEGY.md §8d/§15 for why "
          "this backtest's own CAGR isn't validated performance):")

    trading_days = benchmark.dropna().index
    sim_days = trading_days[262:] if len(trading_days) > 262 else trading_days
    bh = benchmark.reindex(sim_days).dropna()
    cagr, dd, sharpe = _cagr(bh), _max_drawdown(bh), _sharpe(bh)
    print(f"{'(reference)':12s} {'Nifty500 B&H':16s} {cagr:+7.1f}% {dd:7.1f}% {sharpe:8.2f} "
          f"{cagr/abs(dd):8.2f}  ({bh.index[0].date()} to {bh.index[-1].date()})")
    print(f"{'(reference)':12s} {'Task 5: full_nifty500/30h, semiannual/none':43s} "
          f"{'+34.5%':>8s} {'-33.4%':>8s} {'1.53':>8s} {'1.03':>8s}  (STRATEGY.md §8d — same bias, for continuity)")
    print(f"{'(reference)':12s} {'NSE real Nifty Midcap150 Momentum 50':43s} "
          f"{'+20.4%':>8s} {'-72.5%':>8s} {'  n/a':>8s} {' n/a':>8s}  "
          f"(real, bias-free, favorable 7y window — STRATEGY.md §8d)")
    print("=" * 80)

    return 0


if __name__ == "__main__":
    sys.exit(main())
