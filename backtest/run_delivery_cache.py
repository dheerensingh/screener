"""
Backtest: Delivery Percentage Cache Backfill CLI

    python -m backtest.run_delivery_cache [--force]

One-time (or occasional-resume) backfill of ~5 years of NSE delivery% into
backtest/cache/delivery_5y.parquet. Resumable — safe to interrupt and re-run.
"""

import argparse
import logging
import sys

from .delivery_cache import backfill_delivery_cache

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("backtest.run_delivery_cache")


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill the NSE delivery% cache.")
    parser.add_argument("--force", action="store_true", help="Re-fetch every day, ignoring the existing cache")
    args = parser.parse_args()

    result = backfill_delivery_cache(force=args.force)
    logger.info("Cache now holds %d rows across %d trading days.",
                len(result), result["date"].nunique() if len(result) else 0)
    return 0


if __name__ == "__main__":
    sys.exit(main())
