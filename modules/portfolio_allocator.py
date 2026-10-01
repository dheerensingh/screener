"""
Module: Portfolio Allocator

Sits between technical_analyzer.screen_stocks() (per-sector qualifiers) and
position_sizer.size_position() (per-stock sizing math). Implements the
portfolio-construction rules neither of those modules knows about on its own:

  - Per-sector position cap — diversification across the leading sectors, not
    just a raw position-count cap (see CLAUDE.md "Gap 2": on a real run, 13 of
    28 qualifiers were all in one sector — a count cap alone doesn't fix that).
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

Used by both the live main.py and the backtester, so portfolio construction is
identical in both — the same guarantee already applied to the indicator math.
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

from .position_sizer import PositionSize, size_position
from .technical_analyzer import ScreenResult

logger = logging.getLogger(__name__)

MAX_POSITIONS_PER_SECTOR = 2
MAX_DEPLOYED_CAPITAL_PCT = 85.0
MAX_PORTFOLIO_HEAT_PCT = 8.0

CAP_BAND_RISK_PCT = {
    "Large": 1.0,
    "Mid": 0.75,
    "Small": 0.5,
}
DEFAULT_RISK_PCT = 0.75  # used when cap_band is unknown/blank


@dataclass
class AllocationResult:
    accepted: list[tuple[ScreenResult, PositionSize]] = field(default_factory=list)
    rejected: list[tuple[ScreenResult, str]] = field(default_factory=list)  # (result, reason)
    total_deployed_pct: float = 0.0
    total_heat_pct: float = 0.0


def allocate(
    sector_qualifiers: dict[str, list[ScreenResult]],
    account_size: float,
    existing_heat_pct: float = 0.0,
    existing_deployed_pct: float = 0.0,
    max_positions_per_sector: int = MAX_POSITIONS_PER_SECTOR,
    max_deployed_capital_pct: float = MAX_DEPLOYED_CAPITAL_PCT,
    max_portfolio_heat_pct: float = MAX_PORTFOLIO_HEAT_PCT,
    held_tickers: frozenset[str] = frozenset(),
    held_per_sector: Optional[dict[str, int]] = None,
) -> AllocationResult:
    """
    sector_qualifiers: sector name -> qualified ScreenResults for that sector
    (already sorted by strength_score descending, per screen_stocks()).
    existing_heat_pct / existing_deployed_pct: lets a caller with positions
    already open (live day-to-day, or mid-backtest) account for capital/risk
    already committed before this call's candidates are considered.
    held_tickers / held_per_sector: positions already open or pending — never
    bought twice, and they use up their sector's slots.
    """
    result = AllocationResult(total_deployed_pct=existing_deployed_pct, total_heat_pct=existing_heat_pct)
    held_per_sector = held_per_sector or {}

    # Interleave sectors (each sector's #1 pick first, then each's #2, ...) rather
    # than exhausting one sector before the next — keeps the per-sector cap doing
    # real diversification work even when the capital/heat budget runs out early.
    per_sector_capped = {}
    for sector, results in sector_qualifiers.items():
        for r in results:
            if r.ticker in held_tickers:
                result.rejected.append((r, "Already held"))
        fresh = [r for r in results if r.ticker not in held_tickers]
        slots = max(0, max_positions_per_sector - held_per_sector.get(sector, 0))
        for r in fresh[slots:]:
            result.rejected.append((r, f"Sector already has {max_positions_per_sector} positions"))
        per_sector_capped[sector] = fresh[:slots]
    rounds = max((len(v) for v in per_sector_capped.values()), default=0)
    ordered: list[ScreenResult] = []
    for round_idx in range(rounds):
        for results in per_sector_capped.values():
            if round_idx < len(results):
                ordered.append(results[round_idx])

    for r in ordered:
        risk_pct = CAP_BAND_RISK_PCT.get(r.cap_band, DEFAULT_RISK_PCT)
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
            max_position_pct=min(20.0, remaining_capital_pct),
        )
        if pos.error or pos.quantity <= 0:
            result.rejected.append((r, pos.error or "Sized to zero quantity"))
            continue

        result.accepted.append((r, pos))
        result.total_deployed_pct += pos.allocation_pct
        result.total_heat_pct += (pos.risk_amount / account_size * 100.0) if account_size else 0.0

    return result
