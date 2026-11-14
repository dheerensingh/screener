"""
Module: Benchmark

The paper account and backtest are judged against the Nifty500 **Total Return
Index** (price + reinvested dividends), not the price index the data feeds
give: the price index understates the benchmark by its dividend yield, ~1.2-1.5%
a year (STRATEGY.md §21).

No free feed publishes the TRI daily, so it is rebuilt from the price index and
its dividend yield: each day the index's price change is applied and the yield
accrues for the calendar days held. NSE's daily index file (`ind_close_all`)
carries the yield; `main.py` records it in paper_equity.csv's
`benchmark_div_yield`. Days without it use DEFAULT_DIV_YIELD_PCT. The result
tracks the official TRI to within a few basis points a year.
"""

from datetime import date
from typing import Optional, Sequence

DEFAULT_DIV_YIELD_PCT = 1.3  # Nifty500's long-run average, used when a day's yield is unknown


def total_return_index(dates: Sequence[date], closes: Sequence[float],
                       div_yields: Optional[Sequence[Optional[float]]] = None,
                       default_yield_pct: float = DEFAULT_DIV_YIELD_PCT) -> list[float]:
    """TRI levels for each date, starting at closes[0]. `div_yields` are % a year, per date (None = default)."""
    if not closes:
        return []
    out = [float(closes[0])]
    for i in range(1, len(closes)):
        dy = div_yields[i - 1] if div_yields is not None and div_yields[i - 1] is not None else default_yield_pct
        days = (dates[i] - dates[i - 1]).days
        out.append(out[-1] * float(closes[i]) / float(closes[i - 1]) * (1 + dy / 100.0 * days / 365.0))
    return out


def history_tri(history: list[dict]) -> list[float]:
    """TRI series for paper_equity.csv rows (date, benchmark_close, optional benchmark_div_yield)."""
    def _dy(h: dict) -> Optional[float]:
        try:
            return float(h.get("benchmark_div_yield") or "")
        except ValueError:
            return None
    return total_return_index(
        [date.fromisoformat(h["date"]) for h in history],
        [float(h["benchmark_close"]) for h in history],
        [_dy(h) for h in history],
    )
