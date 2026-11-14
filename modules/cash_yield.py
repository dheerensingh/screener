"""
Module: Cash yield

Idle cash in the account is assumed to sit in a liquid mutual fund (or a
liquid ETF) and earn its yield, instead of nothing. Used by the paper account
and the backtest so both credit the same rate (STRATEGY.md §19).

The rates are approximate calendar-year returns of Indian liquid funds after
expenses, before tax — they track the RBI repo rate. They are estimates, not a
data feed: edit the table when a year's actual figure is known. Tax is not
modelled (since April 2023, debt-fund gains are taxed at the investor's slab
rate), so the after-tax benefit is smaller.
"""

from datetime import date

LIQUID_FUND_YIELD_PCT = {
    2015: 7.9, 2016: 7.2, 2017: 6.6, 2018: 7.2, 2019: 6.9, 2020: 4.6,
    2021: 3.3, 2022: 4.8, 2023: 6.9, 2024: 7.2, 2025: 6.6, 2026: 5.8,
}
DEFAULT_YIELD_PCT = 6.0  # years not in the table


def annual_yield_pct(year: int) -> float:
    return LIQUID_FUND_YIELD_PCT.get(year, DEFAULT_YIELD_PCT)


def interest(cash: float, start: date, end: date) -> float:
    """Liquid-fund income on `cash` held from `start` to `end` (calendar days, the year of `end`'s rate)."""
    days = (end - start).days
    if cash <= 0 or days <= 0:
        return 0.0
    return cash * annual_yield_pct(end.year) / 100.0 * days / 365.0
