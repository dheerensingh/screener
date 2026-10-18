"""
Trade Check: backtest across candidate instruments

    python -m trade_check.run_backtest [--period 10y] [--only NIFTY,BTC] [--no-cache]

For each instrument in config.INSTRUMENTS:
  1. Runs every entry variant (trend filter × target × long-only/long+short)
     with fractional sizing, which ignores contract sizes and so measures the
     rule's edge.
  2. Picks the variant with the best Sharpe on the first 70% of the history
     (min 15 trades) and reports it on the last 30%, which it never saw.
  3. Re-runs that variant with real contract sizes on a ₹5L account, to show
     how many trades the account could actually take.

Writes a markdown summary (to stdout and $GITHUB_STEP_SUMMARY) and the full
variant grid to trade_check/output/backtest_variants.csv.
"""

import argparse
import itertools
import logging
import os
import sys
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd

from . import config
from .config import Instrument
from .data import fetch
from .engine import SimResult, simulate, summarize
from .rules import Params, add_indicators, direction, lot_size

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("trade_check.backtest")

OUTPUT_DIR = Path(__file__).parent / "output"
MIN_TRAIN_TRADES = 15

VARIANTS = [
    Params(trend_filter=tf, target_r=r, allow_short=short, sizing="fractional")
    for tf, r, short in itertools.product(("sma20", "sma50", "sma200", "prior_candle"), (1.5, 2.0, 3.0), (True, False))
]
DEFAULT = Params(sizing="fractional")


def _window(res: SimResult, lo: pd.Timestamp, hi: pd.Timestamp, ppy: int) -> dict:
    eq = res.equity[(res.equity.index >= lo) & (res.equity.index <= hi)]
    trades = [t for t in res.trades if lo <= t.entry_date <= hi]
    return summarize(trades, eq, ppy)


def _buy_hold(df: pd.DataFrame, lo: pd.Timestamp, hi: pd.Timestamp) -> float:
    c = df["Close"][(df.index >= lo) & (df.index <= hi)]
    years = (c.index[-1] - c.index[0]).days / 365.25
    return ((c.iloc[-1] / c.iloc[0]) ** (1 / years) - 1.0) * 100.0 if years > 0 else 0.0


def _min_account_for_one_lot(df: pd.DataFrame, inst: Instrument) -> float:
    """Account at which one lot's typical (median, last year) risk is 0.5% of it."""
    b = add_indicators(df).tail(252 if not inst.trades_weekends else 365)
    per_unit = (b["High"] - b["Low"] + 2 * config.STOP_BUFFER_ATR * b["ATR"]).median()
    price = b["Close"].iloc[-1]
    risk_rule = lot_size(inst, price) * per_unit / (config.RISK_PCT / 100.0)
    notional_rule = lot_size(inst, price) * price / (config.MAX_NOTIONAL_PCT / 100.0)
    return max(risk_rule, notional_rule)


def _now(df: pd.DataFrame, inst: Instrument, params: Params) -> str:
    b = add_indicators(df)
    d = direction(b, params.trend_filter, params.allow_short)
    atr_pct = b["ATR"].iloc[-1] / b["Close"].iloc[-1] * 100
    yr = (b["Close"].iloc[-1] / b["Close"].iloc[-min(len(b), 365 if inst.trades_weekends else 252)] - 1) * 100
    return f"{d or 'flat'}; ATR {atr_pct:.1f}%; 1y {yr:+.0f}%"


def run_instrument(inst: Instrument, period: str, use_cache: bool) -> tuple[dict, list[dict]]:
    df = fetch(inst, period, use_cache)
    ppy = 365 if inst.trades_weekends else 252
    dates = df.index[210:]
    split = dates[int(len(dates) * config.TRAIN_FRACTION)]
    lo, hi = dates[0], dates[-1]

    grid: list[dict] = []
    runs: dict[Params, SimResult] = {}
    for p in VARIANTS:
        res = simulate(df, inst, p)
        runs[p] = res
        tr = _window(res, lo, split - pd.Timedelta(days=1), ppy)
        te = _window(res, split, hi, ppy)
        grid.append({"instrument": inst.key, "variant": p.label(),
                     **{f"train_{k}": v for k, v in tr.items()},
                     **{f"test_{k}": v for k, v in te.items()}})

    eligible = [g for g, p in zip(grid, VARIANTS) if g["train_trades"] >= MIN_TRAIN_TRADES]
    best_row = max(eligible or grid, key=lambda g: g["train_sharpe"])
    best = VARIANTS[grid.index(best_row)]
    default_row = grid[VARIANTS.index(DEFAULT)]

    lots_params = replace(best, sizing="lots")
    lots = simulate(df, inst, lots_params)
    lots_full = summarize(lots.trades, lots.equity, ppy)
    sessions = len(lots.equity)
    too_big = lots.skips.get("lot above account", 0) + lots.skips.get("lot risk above 0.5%", 0)

    row = {
        "inst": inst.key, "kind": inst.kind, "first": lo.date(), "split": split.date(), "last": hi.date(),
        "best": best.label(), "best_row": best_row, "default_row": default_row,
        "bh_test": _buy_hold(df, split, hi), "bh_full": _buy_hold(df, lo, hi),
        "lots": lots_full, "too_big": too_big, "sessions": sessions,
        "min_account": _min_account_for_one_lot(df, inst), "now": _now(df, inst, best),
    }
    return row, grid


def _fmt(x: float, nd: int = 2) -> str:
    return "∞" if x == float("inf") else f"{x:.{nd}f}"


def render(rows: list[dict], period: str) -> str:
    rows = sorted(rows, key=lambda r: r["best_row"]["test_sharpe"], reverse=True)
    out = [
        f"# Trade Check backtest ({period}, daily bars)",
        "",
        "Rules: 0.5% risk per trade, stop 0.1×ATR past the prior day's high/low, one position, "
        "never add, 3 losses = 3 sessions off, position ≤ account. Entry: stop order past the prior "
        "day's high (long) / low (short), direction from the trend filter. Costs and carry included.",
        "",
        "## 1. Does the rule have an edge? (fractional sizing, test window = last 30%, unseen when choosing)",
        "",
        "| Instrument | Variant chosen on train | Train trades / avg R / Sharpe | Test trades | Win % | Avg R | "
        "Profit factor | CAGR % | Max DD % | Sharpe | Default variant test avg R / Sharpe | Buy & hold CAGR % (test) |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        b, d = r["best_row"], r["default_row"]
        out.append(
            f"| {r['inst']} | {r['best']} | {b['train_trades']} / {_fmt(b['train_avg_r'])} / {_fmt(b['train_sharpe'])} "
            f"| {b['test_trades']} | {b['test_win_pct']:.0f} | {_fmt(b['test_avg_r'])} | {_fmt(b['test_profit_factor'])} "
            f"| {b['test_cagr_pct']:.1f} | {b['test_max_dd_pct']:.1f} | {_fmt(b['test_sharpe'])} "
            f"| {_fmt(d['test_avg_r'])} / {_fmt(d['test_sharpe'])} | {r['bh_test']:.1f} |"
        )
    out += [
        "",
        f"## 2. Can a ₹{config.ACCOUNT_SIZE_INR / 1e5:g}L account take these trades? (real contract sizes, full period)",
        "",
        "| Instrument | Type | Trades taken | Sessions skipped: contract too big | Lots-mode CAGR % | Max DD % "
        "| Account needed for 1 lot (last year) | Now |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        lt = r["lots"]
        out.append(
            f"| {r['inst']} | {r['kind']} | {lt['trades']} | {r['too_big']} of {r['sessions']} "
            f"| {lt['cagr_pct']:.1f} | {lt['max_dd_pct']:.1f} | ₹{r['min_account'] / 1e5:,.1f}L | {r['now']} |"
        )
    out += [
        "",
        "## Read this before trusting a number",
        "",
        f"- {len(VARIANTS)} variants × {len(rows)} instruments were tried. The best of many backtests looks good "
        "partly by luck. Section 1's test columns are the honest ones; the train columns are not.",
        "- NSE prices are the spot index/stock from Yahoo, not the futures contract. Basis is modelled as a "
        "6%/year carry cost on longs (credit on shorts); expiry rolls are not charged separately.",
        "- BTC is Yahoo's BTC-INR spot; a Delta Exchange India perpetual's price and funding can differ. "
        "Funding is modelled as 10%/year paid by longs.",
        "- Daily bars can't show whether the stop or the target came first; the engine assumes the stop.",
        "- Stock lot sizes are estimated (smallest lot worth ≥ ₹15L); the real lot may differ by a few units.",
    ]
    return "\n".join(out) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--period", default=config.BACKTEST_PERIOD)
    ap.add_argument("--only", default="", help="comma-separated instrument keys")
    ap.add_argument("--no-cache", action="store_true")
    args = ap.parse_args()

    keys = [k.strip().upper() for k in args.only.split(",") if k.strip()] or list(config.INSTRUMENTS)
    rows, grid, errors = [], [], []
    for k in keys:
        try:
            row, g = run_instrument(config.INSTRUMENTS[k], args.period, not args.no_cache)
            rows.append(row)
            grid += g
        except Exception as e:  # one bad ticker shouldn't sink the comparison
            logger.exception("%s failed", k)
            errors.append(f"{k}: {e}")

    md = render(rows, args.period) if rows else "# Trade Check backtest\n\nNo instrument produced results.\n"
    if errors:
        md += "\n**Failed:** " + "; ".join(errors) + "\n"
    print(md)
    OUTPUT_DIR.mkdir(exist_ok=True)
    (OUTPUT_DIR / "backtest_summary.md").write_text(md)
    pd.DataFrame(grid).to_csv(OUTPUT_DIR / "backtest_variants.csv", index=False)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
            f.write(md)


if __name__ == "__main__":
    main()
