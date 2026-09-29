"""
Module 4: Position Sizing

Fixed-fractional risk sizing off a configurable trading account size. The
stop-loss is ATR-based (entry - N x ATR(14), reusing ScreenResult.atr from
technical_analyzer.py), capped at a max % loss from entry (Minervini's own
discipline is famously tight — 5-10% max loss — much tighter than 2xATR can
produce on a volatile smallcap; see CLAUDE.md). Every intermediate value is
exposed on the result, not just the final quantity/allocation, so the report
can show the formula the user is meant to sanity-check before trading (see
instruction.md).
"""

import logging
import math
import os
from dataclasses import dataclass
from typing import Optional

from .technical_analyzer import ScreenResult

logger = logging.getLogger(__name__)

DEFAULT_ACCOUNT_SIZE = 500_000.0
DEFAULT_RISK_PCT_PER_TRADE = 1.0       # % of account risked per trade
DEFAULT_ATR_STOP_MULTIPLIER = 2.0      # stop = entry - multiplier x ATR(14)
DEFAULT_MAX_STOP_PCT = 8.0             # hard cap on stop distance as % of entry (Minervini-informed)
DEFAULT_MAX_POSITION_PCT = 20.0        # hard cap on any single position's % of account


@dataclass
class PositionSize:
    ticker: str
    account_size: float
    risk_pct_per_trade: float
    entry_price: float
    atr: float
    stop_multiplier: float
    stop_price: float
    stop_distance: float
    risk_amount: float
    quantity: int
    allocation_rupees: float
    allocation_pct: float
    capped_by_max_position: bool
    capped_by_max_stop_pct: bool = False
    error: Optional[str] = None

    @property
    def formula(self) -> str:
        if self.error:
            return self.error
        cap_note = " (capped at max position %)" if self.capped_by_max_position else ""
        stop_note = " [stop capped at max %]" if self.capped_by_max_stop_pct else ""
        return (
            f"risk_amount = ₹{self.account_size:,.0f} × {self.risk_pct_per_trade:.1f}% = ₹{self.risk_amount:,.0f}  |  "
            f"stop = ₹{self.entry_price:.2f} − ₹{self.stop_distance:.2f}{stop_note} = ₹{self.stop_price:.2f}  |  "
            f"qty = floor(₹{self.risk_amount:,.0f} / ₹{self.stop_distance:.2f}) = {self.quantity}{cap_note}"
        )


def get_account_size() -> float:
    try:
        return float(os.environ.get("TRADING_ACCOUNT_SIZE", DEFAULT_ACCOUNT_SIZE))
    except ValueError:
        return DEFAULT_ACCOUNT_SIZE


def get_risk_pct() -> float:
    try:
        return float(os.environ.get("RISK_PCT_PER_TRADE", DEFAULT_RISK_PCT_PER_TRADE))
    except ValueError:
        return DEFAULT_RISK_PCT_PER_TRADE


def size_position(
    result: ScreenResult,
    account_size: Optional[float] = None,
    risk_pct_per_trade: Optional[float] = None,
    atr_stop_multiplier: float = DEFAULT_ATR_STOP_MULTIPLIER,
    max_stop_pct: float = DEFAULT_MAX_STOP_PCT,
    max_position_pct: float = DEFAULT_MAX_POSITION_PCT,
) -> PositionSize:
    """
    Sizes a single position via fixed-fractional risk: risk `risk_pct_per_trade`%
    of `account_size` on this trade. Stop distance is `atr_stop_multiplier` x
    ATR(14) below entry, capped at `max_stop_pct` of entry price (whichever is
    tighter) so an unusually volatile stock never gets an oversized-risk trade
    just because its ATR is wide. Quantity is then capped so the position never
    exceeds `max_position_pct` of the account, even if the stop is unusually
    tight.
    """
    account_size = account_size if account_size is not None else get_account_size()
    risk_pct_per_trade = risk_pct_per_trade if risk_pct_per_trade is not None else get_risk_pct()

    entry = result.current_price
    atr_val = result.atr

    if atr_val != atr_val or atr_val <= 0:  # NaN or non-positive
        return PositionSize(
            ticker=result.ticker, account_size=account_size, risk_pct_per_trade=risk_pct_per_trade,
            entry_price=entry, atr=float("nan"), stop_multiplier=atr_stop_multiplier,
            stop_price=float("nan"), stop_distance=float("nan"), risk_amount=float("nan"),
            quantity=0, allocation_rupees=0.0, allocation_pct=0.0, capped_by_max_position=False,
            error="ATR unavailable — cannot size position",
        )

    atr_stop_distance = atr_stop_multiplier * atr_val
    max_stop_distance = entry * max_stop_pct / 100.0
    stop_distance = min(atr_stop_distance, max_stop_distance) if max_stop_distance > 0 else atr_stop_distance
    stop_capped = stop_distance < atr_stop_distance

    stop_price = entry - stop_distance
    risk_amount = account_size * risk_pct_per_trade / 100.0
    quantity = math.floor(risk_amount / stop_distance) if stop_distance > 0 else 0

    allocation_rupees = quantity * entry
    max_allocation = account_size * max_position_pct / 100.0
    capped = False
    if allocation_rupees > max_allocation and entry > 0:
        quantity = math.floor(max_allocation / entry)
        allocation_rupees = quantity * entry
        capped = True

    allocation_pct = (allocation_rupees / account_size * 100.0) if account_size else 0.0

    return PositionSize(
        ticker=result.ticker,
        account_size=account_size,
        risk_pct_per_trade=risk_pct_per_trade,
        entry_price=entry,
        atr=atr_val,
        stop_multiplier=atr_stop_multiplier,
        stop_price=stop_price,
        stop_distance=stop_distance,
        risk_amount=risk_amount,
        quantity=quantity,
        allocation_rupees=allocation_rupees,
        allocation_pct=allocation_pct,
        capped_by_max_position=capped,
        capped_by_max_stop_pct=stop_capped,
    )
