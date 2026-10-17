"""
Module: Paper Trader

Runs the forward paper-trading test inside this project. TradingView has no API
that accepts orders from outside, so it could only ever be a manual mirror; this
module is the actual paper account. Each daily run (from main.py):

  1. Fill — orders booked on the previous run become positions at the NEXT
     session's open (the same T+1-open convention as backtest/simulator.py).
     An order is cancelled if that open is already at/below its stop, or if
     there isn't enough cash.
  2. Exit — every open position is walked bar by bar from its entry date, so a
     missed daily run never skips a stop. Rules match STRATEGY.md §5: initial /
     chandelier trailing stop (intraday), gap below stop (at the open), RSI(14)
     closing below 45 (exit at the next open), 26-session time backstop.
  3. Book — today's accepted signals become pending orders for the next open.
     A buy that adds to a stock already held is its own ledger row (a "lot")
     with its own entry, stop and exits; holdings() combines a stock's lots
     for the allocator.
  4. Snapshot — one row per market date in paper_equity.csv for the dashboard.

Gold (STRATEGY.md §19): portfolio_allocator.GOLD_WEIGHT_PCT of equity is held in
a gold ETF. plan_gold() books an order on the first run of each month when the
holding is more than GOLD_REBALANCE_BAND_PCT away from target (and on the very
first run); it fills at the next session's open like a stock order, after the
stock orders, limited to the cash left. Gold orders are their own ledger,
paper_gold.csv.

Idle cash is assumed to sit in a liquid fund (STRATEGY.md §19): each run credits
modules/cash_yield.py's rate on the previous snapshot's cash, for the calendar
days since it. The running total is the `cash_income` column of paper_equity.csv
(credited from 2026-10-19 on, not back-dated).

All state lives in two committed CSVs: paper_trades.csv (one row per order, its
whole lifecycle), paper_gold.csv (gold orders) and paper_equity.csv (daily
snapshots). Cash is re-derived from the two ledgers every run (plus the
liquid-fund income above) rather than stored, so the files can't drift apart.

Costs: COST_PCT_PER_SIDE on every buy and sell. Deliberately higher than the
backtest's 0.1% round trip — Indian delivery trades pay 0.1% STT on *each* side
plus stamp duty, exchange, GST and DP charges — since the point of paper trading
is to be realistic.
"""

import csv
import logging
import math
import os
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from .cash_yield import interest as cash_interest
from .portfolio_allocator import GOLD_REBALANCE_BAND_PCT, GOLD_TICKER, GOLD_WEIGHT_PCT, Holding
from .stock_fetcher import fetch_stock_data
from .technical_analyzer import _rsi, atr as compute_atr

logger = logging.getLogger(__name__)

LEDGER_PATH = "paper_trades.csv"
EQUITY_PATH = "paper_equity.csv"
GOLD_PATH = "paper_gold.csv"

COST_PCT_PER_SIDE = 0.15
GOLD_COST_PCT_PER_SIDE = 0.05  # gold ETFs pay no STT; brokerage and exchange charges only

# Must match backtest/simulator.py's "trailing" exit paradigm and STRATEGY.md §5 —
# duplicated rather than imported to keep modules/ from depending on backtest/.
CHANDELIER_MULTIPLIER = 2.5
RSI_FAILURE_FLOOR = 45.0
TIME_BACKSTOP_SESSIONS = 26

PENDING, OPEN, CLOSED, CANCELLED = "PENDING", "OPEN", "CLOSED", "CANCELLED"

LEDGER_FIELDS = [
    "trade_id", "status", "ticker", "sector", "cap_band",
    "signal_date", "signal_price", "stop_distance", "quantity", "risk_amount",
    "strength_score", "rsi_at_signal",
    "entry_date", "entry_price", "initial_stop",
    "exit_date", "exit_price", "exit_reason",
    "pnl", "pnl_pct", "r_multiple", "note",
]
EQUITY_FIELDS = [
    "date", "equity", "cash", "market_value", "realized_pnl", "unrealized_pnl",
    "open_positions", "pending_orders", "benchmark_close", "cash_income", "gold_value",
]
GOLD_FIELDS = ["order_id", "status", "signal_date", "signal_price", "units", "fill_date", "fill_price", "cost", "note"]


@dataclass
class PositionView:
    ticker: str
    sector: str
    cap_band: str
    entry_date: str
    entry_price: float
    quantity: int
    last_price: float
    current_stop: float
    unrealized_pnl: float
    unrealized_pct: float
    r_multiple: float
    sessions_held: int
    to_stop_pct: float  # how far price can fall before the stop triggers

    @property
    def market_value(self) -> float:
        return self.quantity * self.last_price


@dataclass
class PaperState:
    as_of: str
    start_capital: float
    cash: float
    positions: list[PositionView]
    ledger: list[dict]
    equity_history: list[dict]
    filled_today: list[dict] = field(default_factory=list)
    closed_today: list[dict] = field(default_factory=list)
    cancelled_today: list[dict] = field(default_factory=list)
    booked_today: list[dict] = field(default_factory=list)
    cash_income: float = 0.0  # liquid-fund income credited so far (included in cash)
    gold_units: int = 0
    gold_price: float = 0.0
    gold_cost_basis: float = 0.0  # rupees paid for the gold still held, costs included
    gold_orders: list[dict] = field(default_factory=list)

    @property
    def gold_value(self) -> float:
        return self.gold_units * self.gold_price

    @property
    def market_value(self) -> float:
        return sum(p.market_value for p in self.positions)

    @property
    def equity(self) -> float:
        return self.cash + self.market_value + self.gold_value

    @property
    def unrealized_pnl(self) -> float:
        return sum(p.unrealized_pnl for p in self.positions)

    @property
    def closed(self) -> list[dict]:
        return [r for r in self.ledger if r["status"] == CLOSED]

    @property
    def pending(self) -> list[dict]:
        return [r for r in self.ledger if r["status"] == PENDING]

    @property
    def realized_pnl(self) -> float:
        return sum(float(r["pnl"]) for r in self.closed)


def _read_csv(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _write_csv(path: str, fieldnames: list[str], rows: list[dict]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _cost(value: float) -> float:
    return value * COST_PCT_PER_SIDE / 100.0


def _derive_cash(ledger: list[dict], start_capital: float) -> float:
    cash = start_capital
    for r in ledger:
        if r["status"] in (OPEN, CLOSED):
            buy = int(r["quantity"]) * float(r["entry_price"])
            cash -= buy + _cost(buy)
        if r["status"] == CLOSED:
            sell = int(r["quantity"]) * float(r["exit_price"])
            cash += sell - _cost(sell)
    return cash


def _gold_spend(orders: list[dict]) -> float:
    """Net cash paid into gold by filled orders (a sale has negative units, so it returns cash)."""
    return sum(int(o["units"]) * float(o["fill_price"]) + float(o["cost"]) for o in orders if o["status"] == "FILLED")


def _bars_after(df: pd.DataFrame, date_str: str) -> pd.DataFrame:
    return df[df.index > pd.Timestamp(date_str)]


def _bars_from(df: pd.DataFrame, date_str: str) -> pd.DataFrame:
    return df[df.index >= pd.Timestamp(date_str)]


def _walk_exit(row: dict, df: pd.DataFrame) -> tuple[Optional[tuple[str, float, str]], float]:
    """
    Replays the position bar by bar from its entry. Returns (exit, current_stop):
    exit is (date, price, reason) or None if still open; current_stop is the
    trailing stop as of the latest bar.
    """
    bars = _bars_from(df, row["entry_date"])
    atr_series = compute_atr(df)  # full history so ATR is warmed up at entry
    rsi_series = _rsi(df["Close"].astype(float))

    entry = float(row["entry_price"])
    stop = float(row["initial_stop"])
    highest_close = entry
    pending_exit: Optional[str] = None

    for i, (day, bar) in enumerate(bars.iterrows()):
        o, h, l, c = (float(bar[k]) for k in ("Open", "High", "Low", "Close"))
        day_str = day.strftime("%Y-%m-%d")
        if i > 0:
            if pending_exit:
                return (day_str, o, pending_exit), stop
            if i >= TIME_BACKSTOP_SESSIONS:
                return (day_str, o, f"Time backstop ({TIME_BACKSTOP_SESSIONS} sessions)"), stop
            if o < stop:
                return (day_str, o, "Gapped below stop"), stop
        if l <= stop:
            kind = "Trailing stop hit" if stop > float(row["initial_stop"]) else "Initial stop hit"
            return (day_str, stop, kind), stop

        highest_close = max(highest_close, c)
        a = atr_series.get(day)
        if a == a and a is not None:
            stop = max(stop, highest_close - CHANDELIER_MULTIPLIER * float(a))
        r = rsi_series.get(day)
        if r == r and r is not None and float(r) < RSI_FAILURE_FLOOR:
            pending_exit = f"RSI closed below {RSI_FAILURE_FLOOR:.0f}"

    return None, stop


def _close_row(row: dict, exit_date: str, exit_price: float, reason: str) -> None:
    exit_price = round(exit_price, 2)  # P&L from the same price the ledger stores, so cash reconciles exactly
    qty = int(row["quantity"])
    entry = float(row["entry_price"])
    buy, sell = qty * entry, qty * exit_price
    pnl = sell - buy - _cost(buy) - _cost(sell)
    risk = qty * float(row["stop_distance"])
    row.update({
        "status": CLOSED, "exit_date": exit_date, "exit_price": f"{exit_price:.2f}",
        "exit_reason": reason, "pnl": f"{pnl:.2f}",
        "pnl_pct": f"{(pnl / buy * 100.0) if buy else 0.0:.2f}",
        "r_multiple": f"{(pnl / risk) if risk else 0.0:.2f}",
    })


class PaperTrader:
    def __init__(self, start_capital: float, ledger_path: str = LEDGER_PATH, equity_path: str = EQUITY_PATH,
                 gold_path: str = GOLD_PATH):
        self.start_capital = start_capital
        self.ledger_path = ledger_path
        self.equity_path = equity_path
        self.gold_path = gold_path
        self.ledger = _read_csv(ledger_path)
        self.gold_orders = _read_csv(gold_path) if os.path.exists(gold_path) else []
        self.gold_price = 0.0
        self.equity_history = _read_csv(equity_path)
        self.as_of: Optional[str] = None
        self.data: dict[str, pd.DataFrame] = {}
        self.positions: list[PositionView] = []
        self.filled_today: list[dict] = []
        self.closed_today: list[dict] = []
        self.cancelled_today: list[dict] = []
        self.booked_today: list[dict] = []
        self.cash_income = 0.0

    def _cash(self) -> float:
        return _derive_cash(self.ledger, self.start_capital) + self.cash_income - _gold_spend(self.gold_orders)

    @property
    def gold_units(self) -> int:
        return sum(int(o["units"]) for o in self.gold_orders if o["status"] == "FILLED")

    @property
    def gold_value(self) -> float:
        return self.gold_units * self.gold_price

    def _gold_cost_basis(self) -> float:
        """Rupees paid for the units still held (average cost; a sale removes its share)."""
        units, basis = 0, 0.0
        for o in self.gold_orders:
            if o["status"] != "FILLED":
                continue
            n = int(o["units"])
            if n > 0:
                units, basis = units + n, basis + n * float(o["fill_price"]) + float(o["cost"])
            elif units:
                basis *= (units + n) / units
                units += n
        return basis

    def _update_gold(self, cash: float) -> None:
        """Fills a pending gold order at the next open (within `cash`) and marks gold at the as_of close."""
        df = self.data.get(GOLD_TICKER)
        if df is None or df.empty:
            if self.gold_orders:
                logger.warning("Paper trader: no %s prices — gold left unchanged", GOLD_TICKER)
            return
        for o in self.gold_orders:
            if o["status"] != "PENDING":
                continue
            nxt = _bars_after(df, o["signal_date"])
            if nxt.empty:
                continue
            open_px = round(float(nxt.iloc[0]["Open"]), 2)
            units = int(o["units"])
            if units > 0:
                units = min(units, math.floor(cash / (open_px * (1 + GOLD_COST_PCT_PER_SIDE / 100.0))))
            else:
                units = -min(-units, self.gold_units)
            if units == 0 or not open_px > 0:
                o.update(status="CANCELLED", note="Not enough cash" if open_px > 0 else "No valid opening price")
                continue
            value = abs(units) * open_px
            o.update(status="FILLED", units=str(units), fill_date=nxt.index[0].strftime("%Y-%m-%d"),
                     fill_price=f"{open_px:.2f}", cost=f"{value * GOLD_COST_PCT_PER_SIDE / 100.0:.2f}")
            cash -= units * open_px + value * GOLD_COST_PCT_PER_SIDE / 100.0
        upto = df[df.index <= pd.Timestamp(self.as_of)] if self.as_of else df
        closes = upto["Close"].dropna()
        if len(closes) >= 2 and abs(closes.iloc[-1] / closes.iloc[-2] - 1) > 0.15:
            # Yahoo has served GOLDBEES bars at 1/100 of the price (STRATEGY.md §19) — never mark on one.
            logger.warning("Paper trader: %s moved %.0f%% on %s — bad bar, using the previous close",
                           GOLD_TICKER, (closes.iloc[-1] / closes.iloc[-2] - 1) * 100, closes.index[-1].date())
            closes = closes.iloc[:-1]
        if len(closes):
            self.gold_price = float(closes.iloc[-1])

    def plan_gold(self) -> Optional[dict]:
        """
        Books a gold order toward GOLD_WEIGHT_PCT of equity, for the next open: on the
        first run, and on the first run of each month if the holding has drifted more
        than GOLD_REBALANCE_BAND_PCT of equity. Call after update(), before sizing stocks.
        """
        if not self.as_of or not self.gold_price > 0 or any(o["status"] == "PENDING" for o in self.gold_orders):
            return None
        month = self.as_of[:7]
        if any(o["signal_date"][:7] == month for o in self.gold_orders):
            return None  # already looked at gold this month
        equity = self._cash() + sum(p.market_value for p in self.positions) + self.gold_value
        diff = equity * GOLD_WEIGHT_PCT / 100.0 - self.gold_value
        first = not self.gold_orders
        if not first and abs(diff) < equity * GOLD_REBALANCE_BAND_PCT / 100.0:
            return None
        units = int(diff / self.gold_price)
        if units == 0:
            return None
        order = {k: "" for k in GOLD_FIELDS}
        order.update(order_id=f"{self.as_of}-GOLD", status="PENDING", signal_date=self.as_of,
                     signal_price=f"{self.gold_price:.2f}", units=str(units), cost="0",
                     note=f"{'Buy' if units > 0 else 'Sell'} toward {GOLD_WEIGHT_PCT:.0f}% of equity")
        self.gold_orders.append(order)
        logger.info("Paper trader: gold order %+d units of %s (~₹%.0f)", units, GOLD_TICKER, units * self.gold_price)
        return order

    def _accrue_cash_income(self) -> None:
        """Liquid-fund income on the last snapshot before as_of — so re-running a session never double-credits."""
        prev = [h for h in self.equity_history if self.as_of and h["date"] < self.as_of]
        if not prev:
            return
        last = prev[-1]
        self.cash_income = float(last.get("cash_income") or 0.0) + cash_interest(
            float(last["cash"]), pd.Timestamp(last["date"]).date(), pd.Timestamp(self.as_of).date(),
        )

    def _ensure_data(self, price_data: dict[str, pd.DataFrame]) -> None:
        self.data = dict(price_data)
        needed = {r["ticker"] for r in self.ledger if r["status"] in (PENDING, OPEN)} | {GOLD_TICKER}
        missing = sorted(t for t in needed if t not in self.data)
        if missing:
            logger.info("Paper trader: fetching prices for %d held ticker(s) outside today's sectors", len(missing))
            self.data.update(fetch_stock_data(missing, period="1y"))

    def update(self, price_data: dict[str, pd.DataFrame]) -> None:
        """Fills yesterday's orders, applies exit rules, and marks open positions to market."""
        self._ensure_data(price_data)
        last_dates = pd.Series([df.index[-1] for df in self.data.values() if len(df)])
        # The most common last bar across all stocks — robust to one stale ticker.
        self.as_of = last_dates.mode().iloc[0].strftime("%Y-%m-%d") if len(last_dates) else None

        self._accrue_cash_income()
        cash = self._cash()
        for row in self.ledger:
            if row["status"] != PENDING:
                continue
            df = self.data.get(row["ticker"])
            if df is None:
                continue
            nxt = _bars_after(df, row["signal_date"])
            if nxt.empty:
                continue  # next session hasn't happened yet
            day = nxt.index[0]
            open_px = round(float(nxt.iloc[0]["Open"]), 2)
            signal_stop = float(row["signal_price"]) - float(row["stop_distance"])
            if not open_px > 0:
                row.update(status=CANCELLED, note="No valid opening price")
            elif open_px <= signal_stop:
                row.update(status=CANCELLED, note=f"Opened at ₹{open_px:.2f}, already below the stop")
            else:
                qty = int(row["quantity"])
                affordable = math.floor(cash / (open_px * (1 + COST_PCT_PER_SIDE / 100.0)))
                if affordable < qty:
                    qty = affordable
                    row["note"] = "; ".join(filter(None, [row.get("note"), "Quantity reduced to available cash"]))
                if qty <= 0:
                    row.update(status=CANCELLED, note="Not enough cash")
                else:
                    buy = qty * open_px
                    cash -= buy + _cost(buy)
                    row.update(
                        status=OPEN, quantity=str(qty), entry_date=day.strftime("%Y-%m-%d"),
                        entry_price=f"{open_px:.2f}",
                        initial_stop=f"{open_px - float(row['stop_distance']):.2f}",
                    )
                    self.filled_today.append(row)
            if row["status"] == CANCELLED:
                self.cancelled_today.append(row)
        self._update_gold(cash)

        self.positions = []
        for row in self.ledger:
            if row["status"] != OPEN:
                continue
            df = self.data.get(row["ticker"])
            if df is None:
                logger.warning("Paper trader: no price data for open position %s — left unchanged", row["ticker"])
                continue
            exit_info, current_stop = _walk_exit(row, df)
            if exit_info:
                _close_row(row, *exit_info)
                self.closed_today.append(row)
                continue
            held = _bars_from(df, row["entry_date"])
            last = float(held["Close"].iloc[-1])
            qty, entry = int(row["quantity"]), float(row["entry_price"])
            buy = qty * entry
            # Buy cost included, sell cost not yet incurred — so start + realized +
            # unrealized == cash + market value == equity, exactly.
            unrealized = qty * last - buy - _cost(buy)
            risk = qty * float(row["stop_distance"])
            self.positions.append(PositionView(
                ticker=row["ticker"], sector=row["sector"], cap_band=row["cap_band"],
                entry_date=row["entry_date"], entry_price=entry, quantity=qty,
                last_price=last, current_stop=current_stop, unrealized_pnl=unrealized,
                unrealized_pct=(unrealized / buy * 100.0) if buy else 0.0,
                r_multiple=(unrealized / risk) if risk else 0.0,
                sessions_held=len(held) - 1,
                to_stop_pct=(last - current_stop) / last * 100.0 if last else 0.0,
            ))

    def holdings(self) -> dict[str, Holding]:
        """Everything open or pending, one Holding per stock (its lots combined) — for the allocator."""
        out: dict[str, Holding] = {}
        mv = {}
        for p in self.positions:
            mv[p.ticker] = mv.get(p.ticker, 0.0) + p.market_value
        for r in self.ledger:
            if r["status"] not in (PENDING, OPEN):
                continue
            h = out.setdefault(r["ticker"], Holding(
                ticker=r["ticker"], sector=r["sector"], lots=0, pending=False,
                last_entry_price=0.0, sessions_since_last_entry=0, market_value=mv.get(r["ticker"], 0.0),
            ))
            if r["status"] == PENDING:
                h.pending = True
                continue
            h.lots += 1
            df = self.data.get(r["ticker"])
            since = len(_bars_after(df, r["entry_date"])) if df is not None else 0
            if h.lots == 1 or since < h.sessions_since_last_entry:
                h.last_entry_price, h.sessions_since_last_entry = float(r["entry_price"]), since
        return out

    def exposure_pct(self, account_size: float) -> tuple[float, float]:
        """(deployed %, heat %) of open + pending positions and gold — fed to the allocator."""
        pending = [r for r in self.ledger if r["status"] == PENDING]
        deployed = sum(p.market_value for p in self.positions) + sum(
            int(r["quantity"]) * float(r["signal_price"]) for r in pending
        ) + self.gold_value + sum(
            int(o["units"]) * float(o["signal_price"]) for o in self.gold_orders if o["status"] == "PENDING"
        )
        open_rows = [r for r in self.ledger if r["status"] == OPEN]
        heat = sum(float(r["risk_amount"]) for r in open_rows + pending)
        return deployed / account_size * 100.0, heat / account_size * 100.0

    def book(self, accepted: list, sector_of: dict[str, str], adds: Optional[dict[str, int]] = None) -> None:
        """
        accepted: [(ScreenResult, PositionSize)] from portfolio_allocator.allocate().
        adds: ticker -> add number, for buys that add to a stock already held.
        """
        adds = adds or {}
        pending = {r["ticker"] for r in self.ledger if r["status"] == PENDING}
        for r, pos in accepted:
            if r.ticker in pending:
                continue
            df = self.data.get(r.ticker)
            # The signal's own bar date, so the fill can only ever be the bar AFTER
            # the close the signal was computed from (no look-ahead).
            signal_date = df.index[-1].strftime("%Y-%m-%d") if df is not None and len(df) else self.as_of
            row = {k: "" for k in LEDGER_FIELDS}
            row.update(
                trade_id=f"{signal_date}-{r.ticker}", status=PENDING, ticker=r.ticker,
                sector=sector_of.get(r.ticker, ""), cap_band=r.cap_band, signal_date=signal_date,
                signal_price=f"{r.current_price:.2f}", stop_distance=f"{pos.stop_distance:.2f}",
                quantity=str(pos.quantity), risk_amount=f"{pos.risk_amount:.2f}",
                strength_score=f"{r.strength_score:.2f}", rsi_at_signal=f"{r.rsi:.1f}",
                note=f"Add #{adds[r.ticker]} to a winning position" if r.ticker in adds else "",
            )
            self.ledger.append(row)
            self.booked_today.append(row)

    def snapshot(self, benchmark_close: float) -> None:
        cash = self._cash()
        market_value = sum(p.market_value for p in self.positions)
        realized = sum(float(r["pnl"]) for r in self.ledger if r["status"] == CLOSED)
        row = {
            "date": self.as_of, "equity": f"{cash + market_value + self.gold_value:.2f}", "cash": f"{cash:.2f}",
            "market_value": f"{market_value:.2f}", "realized_pnl": f"{realized:.2f}",
            "unrealized_pnl": f"{sum(p.unrealized_pnl for p in self.positions):.2f}",
            "open_positions": str(len(self.positions)),
            "pending_orders": str(sum(1 for r in self.ledger if r["status"] == PENDING)),
            "benchmark_close": f"{benchmark_close:.2f}", "cash_income": f"{self.cash_income:.2f}",
            "gold_value": f"{self.gold_value:.2f}",
        }
        self.equity_history = [h for h in self.equity_history if h["date"] != self.as_of] + [row]
        self.equity_history.sort(key=lambda h: h["date"])

    def save(self) -> None:
        _write_csv(self.ledger_path, LEDGER_FIELDS, self.ledger)
        _write_csv(self.equity_path, EQUITY_FIELDS, self.equity_history)
        if self.gold_orders:
            _write_csv(self.gold_path, GOLD_FIELDS, self.gold_orders)

    def state(self) -> PaperState:
        return PaperState(
            as_of=self.as_of or "", start_capital=self.start_capital,
            cash=self._cash(), cash_income=self.cash_income, positions=self.positions,
            gold_units=self.gold_units, gold_price=self.gold_price, gold_cost_basis=self._gold_cost_basis(),
            gold_orders=self.gold_orders,
            ledger=self.ledger, equity_history=self.equity_history,
            filled_today=self.filled_today, closed_today=self.closed_today,
            cancelled_today=self.cancelled_today, booked_today=self.booked_today,
        )
