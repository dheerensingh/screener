"""
Module: NSE trading calendar

NSE publishes its trading holidays for the year (the "holiday-master" list on
nseindia.com). They're cached in HOLIDAYS_PATH, which is committed with the
other docs/data files, so the workflow's skip-guard can read it with plain
python3 before any dependency is installed, and a run still works if NSE is
unreachable. refresh_holidays() re-fetches when the cache is missing, more than
REFRESH_DAYS old, or doesn't cover the current year.

The file can also be edited by hand from NSE's circular — the format is just
{"holidays": {"YYYY-MM-DD": "description", ...}}.
"""

import json
import logging
import os
from datetime import date, datetime, timedelta
from typing import Optional

import requests

logger = logging.getLogger(__name__)

HOLIDAYS_PATH = os.path.join("docs", "data", "nse_holidays.json")
HOLIDAY_API_URL = "https://www.nseindia.com/api/holiday-master?type=trading"
REFRESH_DAYS = 30
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept": "application/json",
}


def load_holidays(path: str = HOLIDAYS_PATH) -> dict[str, str]:
    """{"YYYY-MM-DD": description} from the cache; empty if there is none."""
    try:
        with open(path, encoding="utf-8") as f:
            return dict(json.load(f).get("holidays", {}))
    except (OSError, ValueError):
        return {}


def _fetch_from_nse() -> dict[str, str]:
    session = requests.Session()
    session.headers.update(HEADERS)
    resp = session.get(HOLIDAY_API_URL, timeout=15)
    if resp.status_code in (401, 403):
        # Some nseindia.com endpoints want the cookies the homepage sets first.
        session.get("https://www.nseindia.com/", timeout=15)
        resp = session.get(HOLIDAY_API_URL, timeout=15)
    resp.raise_for_status()
    out: dict[str, str] = {}
    for row in resp.json().get("CM", []):  # "CM" = capital market (equities) segment
        d = datetime.strptime(row["tradingDate"].strip(), "%d-%b-%Y").date()
        out[d.isoformat()] = (row.get("description") or "").strip()
    if not out:
        raise ValueError("NSE holiday-master returned no CM holidays")
    return out


def refresh_holidays(path: str = HOLIDAYS_PATH, today: Optional[date] = None) -> dict[str, str]:
    """Re-fetches NSE's list if the cache is stale; always returns the best list available."""
    today = today or date.today()
    cached: dict = {}
    try:
        with open(path, encoding="utf-8") as f:
            cached = json.load(f)
    except (OSError, ValueError):
        pass
    holidays = dict(cached.get("holidays", {}))
    fetched = cached.get("fetched")
    fresh = (
        fetched and (today - date.fromisoformat(fetched)).days < REFRESH_DAYS
        and any(d.startswith(str(today.year)) for d in holidays)
    )
    if fresh:
        return holidays
    try:
        latest = _fetch_from_nse()
    except Exception as exc:
        logger.warning("NSE holiday list refresh failed (using %d cached dates): %s", len(holidays), exc)
        return holidays
    holidays.update(latest)  # keep past years' dates; NSE only lists the current year
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"source": HOLIDAY_API_URL, "fetched": today.isoformat(),
                   "holidays": dict(sorted(holidays.items()))}, f, indent=1)
    logger.info("NSE holiday list refreshed: %d dates for %d", len(latest), today.year)
    return holidays


def is_trading_day(d: date, holidays: Optional[dict[str, str]] = None) -> bool:
    holidays = load_holidays() if holidays is None else holidays
    return d.weekday() < 5 and d.isoformat() not in holidays


def previous_trading_day(d: date, holidays: Optional[dict[str, str]] = None) -> date:
    holidays = load_holidays() if holidays is None else holidays
    d -= timedelta(days=1)
    while not is_trading_day(d, holidays):
        d -= timedelta(days=1)
    return d
