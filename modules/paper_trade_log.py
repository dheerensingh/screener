"""
Module: Paper Trade Log

Appends each day's newly-sized signals to a local CSV, so the "test this
forward for ~3 months with no real money" step (task 5) has a concrete, dated,
system-generated record of what the pipeline actually recommended each day —
independent of manually copying numbers out of the email/report. The user
replicates these into a paper-trading platform (see STRATEGY.md for the
TradingView setup guide); this module doesn't touch any external platform
itself, it just keeps the record.

This CSV is also the ONLY persistent position state this project has: main.py
is a stateless daily signal generator with no memory of what's currently
"held," so `check_exits()` below re-derives each open row's current trailing
stop from its entry date forward every run (no separate state to keep in
sync) and applies the same exit rules as backtest/simulator.py and STRATEGY.md
§5 — otherwise the paper-trading test would only ever tell the user when to
enter, never when to exit.

Idempotent per ticker+date: re-running main.py on the same day won't duplicate
an already-logged signal for the same ticker.
"""

import csv
import logging
import os
from datetime import datetime
from typing import Optional

import pandas as pd

from .report_generator import SectorBundle
from .stock_fetcher import fetch_stock_data
from .technical_analyzer import _rsi, atr as compute_atr

logger = logging.getLogger(__name__)

LOG_PATH = "paper_trades.csv"
FIELDNAMES = [
    "date", "ticker", "sector", "cap_band", "entry_price", "stop_price",
    "quantity", "allocation_rupees", "allocation_pct", "rsi", "strength_score",
    "exit_date", "exit_price", "exit_reason",
]

# Must match backtest/simulator.py's "trailing" exit paradigm and STRATEGY.md §5 —
# duplicated here (rather than imported) to keep modules/ from depending on backtest/.
CHANDELIER_MULTIPLIER = 2.5
RSI_FAILURE_FLOOR = 45.0
TIME_BACKSTOP_DAYS = 26


def _already_logged_today(path: str, today_str: str) -> set[str]:
    if not os.path.exists(path):
        return set()
    logged = set()
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("date") == today_str:
                logged.add(row.get("ticker", ""))
    return logged


def log_signals(bundles: list[SectorBundle], path: str = LOG_PATH) -> int:
    """
    Appends one row per newly-sized (accepted by the portfolio allocator)
    signal across all sector bundles. Returns the number of rows appended.
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    already = _already_logged_today(path, today_str)

    rows = []
    for bundle in bundles:
        for r in bundle.qualified:
            pos = bundle.position_sizes.get(r.ticker)
            if pos is None or r.ticker in already:
                continue
            rows.append({
                "date": today_str,
                "ticker": r.ticker,
                "sector": bundle.score.sector,
                "cap_band": r.cap_band,
                "entry_price": f"{r.current_price:.2f}",
                "stop_price": f"{pos.stop_price:.2f}",
                "quantity": pos.quantity,
                "allocation_rupees": f"{pos.allocation_rupees:.0f}",
                "allocation_pct": f"{pos.allocation_pct:.2f}",
                "rsi": f"{r.rsi:.1f}",
                "strength_score": f"{r.strength_score:.2f}",
                "exit_date": "",
                "exit_price": "",
                "exit_reason": "",
            })

    if not rows:
        logger.info("Paper trade log: no new signals to append today.")
        return 0

    file_exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)

    logger.info("Paper trade log: appended %d new signal(s) to %s", len(rows), path)
    return len(rows)


def _evaluate_exit(row: dict, df: pd.DataFrame) -> Optional[tuple[str, float, str]]:
    """Returns (exit_date_str, exit_price, exit_reason) if `row`'s open position should exit today, else None."""
    entry_date = pd.Timestamp(row["date"])
    since_entry = df[df.index >= entry_date]
    if since_entry.empty:
        return None

    initial_stop = float(row["stop_price"])
    highest_close = float(since_entry["Close"].astype(float).max())
    latest_atr_series = compute_atr(df)  # needs the full df's history, not just since-entry, to warm up
    if latest_atr_series.dropna().empty:
        return None
    latest_atr = float(latest_atr_series.dropna().iloc[-1])
    current_stop = max(initial_stop, highest_close - CHANDELIER_MULTIPLIER * latest_atr)

    latest_low = float(since_entry["Low"].astype(float).iloc[-1])
    latest_close = float(since_entry["Close"].astype(float).iloc[-1])
    latest_rsi = float(_rsi(df["Close"].astype(float)).dropna().iloc[-1])
    holding_days = len(since_entry) - 1
    today_str = since_entry.index[-1].strftime("%Y-%m-%d")

    if latest_low <= current_stop:
        return today_str, current_stop, "Stop hit (incl. trailing)"
    if latest_rsi < RSI_FAILURE_FLOOR:
        return today_str, latest_close, "RSI momentum failure"
    if holding_days >= TIME_BACKSTOP_DAYS:
        return today_str, latest_close, f"Time backstop ({TIME_BACKSTOP_DAYS}d)"
    return None


def check_exits(path: str = LOG_PATH) -> int:
    """
    For every still-open row (blank exit_date) in the paper trade log, fetches
    recent price data and checks the same exit rules used live/in the
    backtest — chandelier trailing stop, RSI momentum-failure, time backstop.
    Updates exit_date/exit_price/exit_reason in place when one triggers.
    Returns the number of positions closed by this call.
    """
    if not os.path.exists(path):
        return 0

    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    open_rows = [r for r in rows if not r.get("exit_date")]
    if not open_rows:
        return 0

    tickers = sorted({r["ticker"] for r in open_rows})
    data = fetch_stock_data(tickers, period="6mo")

    closed = 0
    for row in open_rows:
        df = data.get(row["ticker"])
        if df is None:
            continue
        result = _evaluate_exit(row, df)
        if result is None:
            continue
        exit_date, exit_price, exit_reason = result
        row["exit_date"], row["exit_price"], row["exit_reason"] = exit_date, f"{exit_price:.2f}", exit_reason
        closed += 1

    if closed:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()
            writer.writerows(rows)
        logger.info("Paper trade log: closed %d position(s)", closed)

    return closed
