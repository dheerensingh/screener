"""
Backtest: inspect one sector's synthetic basket

    python -m backtest.inspect_sector Telecom Defense

For each member the live ranking uses (liquidity-filtered, top 40): first close
in the window, min/max close, its own 1M/3M/6M returns, the largest one-day
move, and its weight in the basket's latest value (close / first close). A
member with a huge weight or a one-day move no stock really made is the
reason a sector's score jumps around.
"""

import logging
import os
import sys

import pandas as pd

from modules.sector_mapper import build_universe, group_by_sector
from modules.sector_rotation import (
    LOOKBACKS, MAX_STOCKS_PER_SECTOR, MIN_AVG_DAILY_VALUE_INR, _avg_daily_value, _cumulative_return,
)
from modules.stock_fetcher import fetch_stock_data

logging.basicConfig(level=logging.WARNING, handlers=[logging.StreamHandler(sys.stdout)])


def main() -> int:
    sectors = sys.argv[1:] or ["Telecom"]
    grouped = group_by_sector(build_universe())
    lines = []
    for sector in sectors:
        tickers = [m.ticker for m in grouped.get(sector, [])]
        data = fetch_stock_data(tickers, period="1y")
        liquid = sorted((t for t in data if _avg_daily_value(data[t]) >= MIN_AVG_DAILY_VALUE_INR),
                        key=lambda t: _avg_daily_value(data[t]), reverse=True)[:MAX_STOCKS_PER_SECTOR]
        lines += [f"### {sector} — {len(liquid)} members", "",
                  "| Stock | Bars | First | Min | Max | Last | Weight (last/first) | "
                  + " | ".join(f"{k} %" for k in LOOKBACKS) + " | Biggest 1-day move | On |",
                  "|" + "---|" * (9 + len(LOOKBACKS))]
        for t in liquid:
            c = data[t]["Close"].astype(float).dropna()
            moves = c.pct_change().abs()
            big = moves.idxmax()
            rets = " | ".join(f"{_cumulative_return(c, d):+.1f}" for d in LOOKBACKS.values())
            lines.append(f"| {t} | {len(c)} | {c.iloc[0]:.2f} | {c.min():.2f} | {c.max():.2f} | {c.iloc[-1]:.2f} | "
                         f"{c.iloc[-1] / c.iloc[0]:.2f} | {rets} | {moves.max() * 100:+.1f}% | {big.date()} |")
        lines.append("")
    text = "\n".join(lines)
    print(text)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as f:
            f.write(text + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
