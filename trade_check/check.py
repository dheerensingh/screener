"""
Trade Check: daily check + paper account

    python -m trade_check.check --instrument BTC [--paper-start 2026-10-20]
                                [--account 500000] [--trend sma50] [--target-r 2]
                                [--long-only] [--sizing lots|fractional]

Prints:
  1. The paper account: every trade the rules would have taken since
     --paper-start, replayed bar by bar with the same engine as the backtest,
     plus equity and any open position.
  2. The trade check for the next session, in the doc's VERDICT format.
  3. The doc's trade log (Date, Entry, Stop, Target, Result, Rule broken?).

The paper account is a deterministic replay from --paper-start, not a stored
ledger, so it needs no state files and nothing is committed anywhere. The cost
of that: if Yahoo revises an old bar, a past paper trade can change.
Outputs also go to trade_check/output/ and $GITHUB_STEP_SUMMARY.
"""

import argparse
import logging
import os
import sys
from pathlib import Path

import pandas as pd

from . import config
from .data import fetch
from .engine import simulate, summarize
from .rules import Params, format_check

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("trade_check.check")

OUTPUT_DIR = Path(__file__).parent / "output"


def trade_log(trades, open_trade) -> pd.DataFrame:
    rows = []
    for t in trades + ([open_trade] if open_trade else []):
        result = "open" if t.exit_date is None else f"{t.exit_reason}, {t.r_multiple:+.2f}R (Rs {t.pnl:+,.0f})"
        rows.append({
            "Date": t.entry_date.date(), "Direction": t.direction, "Entry": round(t.entry, 2),
            "Stop": round(t.stop, 2), "Target": round(t.target, 2), "Units": f"{t.units:g}",
            "Exit date": t.exit_date.date() if t.exit_date is not None else "",
            "Result": result, "Rule broken? Y/N": "N",
        })
    return pd.DataFrame(rows)


def to_markdown(df: pd.DataFrame) -> str:
    """Plain markdown table (pandas' to_markdown needs the tabulate package)."""
    head = "| " + " | ".join(df.columns) + " |"
    sep = "|" + "---|" * len(df.columns)
    body = ["| " + " | ".join(str(v) for v in row) + " |" for row in df.itertuples(index=False)]
    return "\n".join([head, sep, *body])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--instrument", required=True, choices=sorted(config.INSTRUMENTS))
    ap.add_argument("--paper-start", default=None, help="YYYY-MM-DD; default: 90 days ago")
    ap.add_argument("--account", type=float, default=config.ACCOUNT_SIZE_INR)
    ap.add_argument("--trend", default=config.DEFAULT_TREND_FILTER,
                    choices=["sma20", "sma50", "sma200", "prior_candle"])
    ap.add_argument("--target-r", type=float, default=config.DEFAULT_TARGET_R)
    ap.add_argument("--long-only", action="store_true")
    ap.add_argument("--sizing", default="lots", choices=["lots", "fractional"])
    args = ap.parse_args()

    inst = config.INSTRUMENTS[args.instrument]
    params = Params(trend_filter=args.trend, target_r=args.target_r,
                    allow_short=not args.long_only, sizing=args.sizing)
    df = fetch(inst, "2y", use_cache=False)
    start = pd.Timestamp(args.paper_start) if args.paper_start else df.index[-1] - pd.Timedelta(days=90)
    res = simulate(df, inst, params, start_equity=args.account, start=start)

    stats = summarize(res.trades, res.equity, 365 if inst.trades_weekends else 252)
    last = df.index[-1].date()
    equity = res.equity.iloc[-1] if len(res.equity) else args.account
    log = trade_log(res.trades, res.open_trade)

    md = [
        f"# Trade Check: {inst.key} ({params.label()}, {params.sizing} sizing)",
        "",
        f"Last completed bar: **{last}**. Paper account since {start.date()}: "
        f"Rs {args.account:,.0f} → **Rs {equity:,.0f}** ({(equity / args.account - 1) * 100:+.2f}%), "
        f"{stats['trades']} closed trades, win {stats['win_pct']:.0f}%, avg {stats['avg_r']:+.2f}R, "
        f"max DD {stats['max_dd_pct']:.1f}%.",
        f"Losses in a row: {res.losses_in_row}. Sessions off left: {res.pause_left}.",
        "",
    ]
    if res.open_trade:
        t = res.open_trade
        md += [f"**Open:** {t.direction} {t.units:g} @ {t.entry:,.2f} since {t.entry_date.date()}, "
               f"stop {t.stop:,.2f}, target {t.target:,.2f}.", ""]
    md += ["## Next session", "", "```", format_check(res.next_check) if res.next_check else "No check.", "```",
           "", "## Trade log", ""]
    md.append(to_markdown(log) if not log.empty else "No trades yet.")
    text = "\n".join(md) + "\n"

    print(text)
    OUTPUT_DIR.mkdir(exist_ok=True)
    (OUTPUT_DIR / f"check_{inst.key}.md").write_text(text)
    log.to_csv(OUTPUT_DIR / f"paper_trades_{inst.key}.csv", index=False)
    res.equity.rename("equity").to_csv(OUTPUT_DIR / f"paper_equity_{inst.key}.csv", index_label="date")
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
            f.write(text)


if __name__ == "__main__":
    main()
