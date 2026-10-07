"""
Module: Portfolio Allocator

Sits between technical_analyzer.screen_stocks() (per-sector qualifiers) and
position_sizer.size_position() (per-stock sizing math). Implements the
portfolio-construction rules neither of those modules knows about on its own:

  - Position limits — at most MAX_TOTAL_POSITIONS stocks, with each leading
    sector's share weighted by its relative-strength score (sector_slots()), so
    one sector's many qualifiers can't fill the whole book (CLAUDE.md "Gap 2":
    on a real run, 13 of 28 qualifiers were all in one sector).
  - Adding to winners — a held stock that qualifies again while in profit is
    bought again at a reduced size (add_blocker(), add_risk_fraction()).
  - Sequential, budget-aware sizing: candidates are walked in strength_score
    order (interleaved across sectors) and sized against REMAINING deployable
    capital, not fresh total capital each time, so the book can never be
    over-committed even if every candidate independently qualifies for the
    per-position cap.
  - A total portfolio "heat" ceiling (sum of every open position's risk_amount)
    independent of the position-count cap — protects against a correlated
    gap-down stopping out every position on the same day.
  - Cap-band-aware risk-per-trade scaling (Large/Mid/Small), since ATR alone
    doesn't fully price in a smallcap's extra gap/liquidity risk.

allocate() is the live entry point (main.py). backtest/simulator.py sizes
inline but uses the same constants and sector_slots()/add_blocker()/
add_risk_fraction(), so the rules can't drift apart.
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

from .position_sizer import DEFAULT_MAX_POSITION_PCT, PositionSize, size_position
from .technical_analyzer import ScreenResult

logger = logging.getLogger(__name__)

# ── Portfolio shape (2026-10-07, STRATEGY.md §17) ────────────────────────────
# Up to 15 different stocks at once. New buys only come from the leading
# sectors; each leading sector gets MIN_SLOTS_PER_SECTOR and the rest of the
# 15 is shared out in proportion to the sector's relative-strength score, so
# the strongest sector can hold more than the weakest. Holdings in sectors that
# have since dropped out of the lead still count toward the 15.
MAX_TOTAL_POSITIONS = 15
MIN_SLOTS_PER_SECTOR = 2
MAX_DEPLOYED_CAPITAL_PCT = 85.0
MAX_PORTFOLIO_HEAT_PCT = 8.0

# ── Adding to a winner (pyramiding) ──────────────────────────────────────────
# A stock already held can be bought again when it qualifies again AND is
# winning: today's close is above the price of its most recent buy (so every
# lot is in profit). Each add risks a smaller fraction of the stock's normal
# risk, at least MIN_SESSIONS_BETWEEN_LOTS after the previous buy, and the
# stock's total value stays under the 20% single-stock cap.
MAX_ADDS_PER_STOCK = 2
ADD_RISK_FRACTIONS = (0.5, 0.25)  # 1st add, 2nd add — of the stock's cap-band risk
MIN_SESSIONS_BETWEEN_LOTS = 5

CAP_BAND_RISK_PCT = {
    "Large": 1.0,
    "Mid": 0.75,
    "Small": 0.5,
}
DEFAULT_RISK_PCT = 0.75  # used when cap_band is unknown/blank


@dataclass
class Holding:
    """One stock the paper account (or backtest) already holds — all its lots together."""
    ticker: str
    sector: str
    lots: int                       # filled buys still open
    pending: bool                   # an order for it is waiting for the next open
    last_entry_price: float         # price of the most recent filled buy
    sessions_since_last_entry: int
    market_value: float


def sector_slots(
    sector_scores: dict[str, float],
    total: int = MAX_TOTAL_POSITIONS,
    min_per_sector: int = MIN_SLOTS_PER_SECTOR,
) -> dict[str, int]:
    """
    How many stocks each leading sector may hold. Every sector gets
    `min_per_sector`; the remaining slots are split in proportion to each
    sector's (positive) relative-strength score, largest remainder first.
    Example, 15 slots and scores 12/8/5/3 → 5/4/3/3. If no score is positive the
    split is equal.
    """
    sectors = list(sector_scores)
    if not sectors:
        return {}
    base = min(min_per_sector, total // len(sectors))
    extra = total - base * len(sectors)
    weights = {s: max(float(v), 0.0) for s, v in sector_scores.items()}
    wsum = sum(weights.values())
    if wsum <= 0:
        weights, wsum = {s: 1.0 for s in sectors}, float(len(sectors))
    raw = {s: extra * weights[s] / wsum for s in sectors}
    slots = {s: base + int(raw[s]) for s in sectors}
    leftover = total - sum(slots.values())
    by_remainder = sorted(sectors, key=lambda s: (raw[s] - int(raw[s]), weights[s]), reverse=True)
    for s in by_remainder[:leftover]:
        slots[s] += 1
    return slots


def add_blocker(holding: Holding, current_price: float) -> Optional[str]:
    """None if `holding` may be added to at `current_price`, else the reason it may not."""
    if holding.pending:
        return "Order already pending"
    if holding.lots > MAX_ADDS_PER_STOCK:
        return f"Already added to {MAX_ADDS_PER_STOCK} times"
    if current_price <= holding.last_entry_price:
        return f"Held, not winning (₹{current_price:.2f} ≤ last buy ₹{holding.last_entry_price:.2f})"
    if holding.sessions_since_last_entry < MIN_SESSIONS_BETWEEN_LOTS:
        return (f"Held, last bought {holding.sessions_since_last_entry} session(s) ago "
                f"(adds need {MIN_SESSIONS_BETWEEN_LOTS})")
    return None


def add_risk_fraction(holding: Holding) -> float:
    """Fraction of the normal per-trade risk used for the next add to `holding`."""
    return ADD_RISK_FRACTIONS[min(holding.lots, len(ADD_RISK_FRACTIONS)) - 1]


@dataclass
class AllocationResult:
    accepted: list[tuple[ScreenResult, PositionSize]] = field(default_factory=list)
    rejected: list[tuple[ScreenResult, str]] = field(default_factory=list)  # (result, reason)
    adds: dict[str, int] = field(default_factory=dict)  # ticker -> add number (1, 2) for accepted adds
    slots: dict[str, int] = field(default_factory=dict)  # sector -> slots it was given today
    total_deployed_pct: float = 0.0
    total_heat_pct: float = 0.0


def allocate(
    sector_qualifiers: dict[str, list[ScreenResult]],
    account_size: float,
    existing_heat_pct: float = 0.0,
    existing_deployed_pct: float = 0.0,
    sector_scores: Optional[dict[str, float]] = None,
    holdings: Optional[dict[str, Holding]] = None,
    max_total_positions: int = MAX_TOTAL_POSITIONS,
    max_deployed_capital_pct: float = MAX_DEPLOYED_CAPITAL_PCT,
    max_portfolio_heat_pct: float = MAX_PORTFOLIO_HEAT_PCT,
) -> AllocationResult:
    """
    sector_qualifiers: leading sector -> qualified ScreenResults, strongest
    sector first, each list sorted by strength_score descending (as
    screen_stocks() returns them).
    sector_scores: leading sector -> relative-strength score, for sector_slots().
    holdings: ticker -> Holding for everything already open or pending.
    existing_heat_pct / existing_deployed_pct: risk and capital already
    committed by those holdings.
    """
    holdings = holdings or {}
    scores = {s: (sector_scores or {}).get(s, 0.0) for s in sector_qualifiers}
    result = AllocationResult(total_deployed_pct=existing_deployed_pct, total_heat_pct=existing_heat_pct)
    result.slots = sector_slots(scores, total=max_total_positions)

    held_in_sector: dict[str, int] = {}
    for h in holdings.values():
        held_in_sector[h.sector] = held_in_sector.get(h.sector, 0) + 1

    # Per sector: adds to winners, plus new names up to the sector's free slots.
    per_sector: dict[str, list[tuple[ScreenResult, Optional[Holding]]]] = {}
    for sector, results in sector_qualifiers.items():
        free = max(0, result.slots.get(sector, 0) - held_in_sector.get(sector, 0))
        picks: list[tuple[ScreenResult, Optional[Holding]]] = []
        for r in results:
            h = holdings.get(r.ticker)
            if h is not None:
                blocker = add_blocker(h, r.current_price)
                if blocker:
                    result.rejected.append((r, blocker))
                else:
                    picks.append((r, h))
            elif free > 0:
                picks.append((r, None))
                free -= 1
            else:
                result.rejected.append((r, f"Sector at its {result.slots.get(sector, 0)}-stock limit"))
        per_sector[sector] = picks

    # Interleave sectors (each sector's #1 pick first, then each's #2, ...) so one
    # sector can't use up the capital/heat budget before the others get a turn.
    rounds = max((len(v) for v in per_sector.values()), default=0)
    ordered = [picks[i] for i in range(rounds) for picks in per_sector.values() if i < len(picks)]

    positions = len(holdings)
    for r, h in ordered:
        if h is None and positions >= max_total_positions:
            result.rejected.append((r, f"Portfolio at {max_total_positions} stocks"))
            continue
        risk_pct = CAP_BAND_RISK_PCT.get(r.cap_band, DEFAULT_RISK_PCT)
        max_position_pct = DEFAULT_MAX_POSITION_PCT
        if h is not None:
            risk_pct *= add_risk_fraction(h)
            max_position_pct -= (h.market_value / account_size * 100.0) if account_size else 0.0
            if max_position_pct <= 0:
                result.rejected.append((r, f"Held, already at the {DEFAULT_MAX_POSITION_PCT:.0f}% single-stock cap"))
                continue
        remaining_capital_pct = max_deployed_capital_pct - result.total_deployed_pct
        remaining_heat_pct = max_portfolio_heat_pct - result.total_heat_pct

        if remaining_capital_pct <= 0:
            result.rejected.append((r, "Max deployed-capital budget exhausted"))
            continue
        if remaining_heat_pct <= 0:
            result.rejected.append((r, "Max portfolio-heat budget exhausted"))
            continue
        if risk_pct > remaining_heat_pct:
            result.rejected.append((r, f"Would push portfolio heat over {max_portfolio_heat_pct:.1f}%"))
            continue

        pos = size_position(
            r, account_size=account_size, risk_pct_per_trade=risk_pct,
            max_position_pct=min(max_position_pct, remaining_capital_pct),
        )
        if pos.error or pos.quantity <= 0:
            result.rejected.append((r, pos.error or "Sized to zero quantity"))
            continue

        result.accepted.append((r, pos))
        if h is not None:
            result.adds[r.ticker] = h.lots
        else:
            positions += 1
        result.total_deployed_pct += pos.allocation_pct
        result.total_heat_pct += (pos.risk_amount / account_size * 100.0) if account_size else 0.0

    return result
