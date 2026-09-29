"""
Module 2b: Governance / Promoter-Pledge Filter

Scrapes the same public Screener.in company pages fundamental_analyzer.py already
uses (no login needed) for two things `sector_confirmation`'s YoY-profit check never
looks at: the Shareholding Pattern table (promoter holding % trend) and the site's
own auto-generated Cons list, which discloses a confirmed promoter share pledge in
plain English when one exists (spot-checked live, 2026-09-28: DBREALTY's Cons list
reads "Promoters have pledged 44.7% of their holding.").

Design (STRATEGY.md §12 has the full reasoning): a confirmed pledge is a HARD GATE —
it's the company's own unambiguous disclosure, not a heuristic, so false positives are
close to impossible. A declining promoter holding % is informational only — too many
benign causes (succession planning, funding another venture, buyback/open-offer
mechanics, promoter/public reclassification) to hard-gate a signal this noisy against
an already-small qualifier pool. This exists specifically because the strategy is
headed toward "family and close people" capital, and Indian small/midcap momentum
blow-ups are disproportionately governance failures, not macro reversals.

Only ever called on the small set of stocks that already passed technical screening
(main.py Step 3a), not the full sector basket — the same reasoning that keeps
fundamental_analyzer.sector_confirmation scoped to a sector's basket rather than the
whole universe.

Known limitation (stated, not hidden): Screener.in exposes only *current* shareholding/
pledge data, no historical snapshot API — this cannot be backtested against 5 years of
history. Verification here is a hand spot-check (see STRATEGY.md §12), the same bar
trend_template.py/market_regime.py were held to when first built.
"""

import logging
import re
import time
from dataclasses import dataclass
from typing import Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BASE_URL = "https://www.screener.in/company/{ticker}/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
}
REQUEST_SLEEP_SEC = 1  # polite pause between company pages, matching fundamental_analyzer.py
QUARTERS_BACK_FOR_TREND = 4  # compare latest promoter holding vs the same quarter a year ago
DECLINING_HOLDING_THRESHOLD_PP = 2.0  # informational flag: >2 percentage-point drop YoY
PLEDGE_PATTERN = re.compile(r"pledg\w*[^.]*?([\d.]+)\s*%", re.IGNORECASE)


@dataclass
class GovernanceResult:
    ticker: str
    promoter_holding_pct: Optional[float] = None
    promoter_holding_pct_4q_ago: Optional[float] = None
    promoter_holding_trend_pct_pts: Optional[float] = None
    declining_promoter_holding_flag: bool = False
    pledge_flag: bool = False
    pledge_text: Optional[str] = None
    error: Optional[str] = None


def _parse_pct(text: str) -> Optional[float]:
    text = text.strip().replace(",", "").replace("%", "")
    if not text or text in ("-", "NA"):
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _fetch_promoter_holding_trend(soup: BeautifulSoup) -> tuple[Optional[float], Optional[float]]:
    """Latest promoter holding % and the value QUARTERS_BACK_FOR_TREND quarters ago,
    from the Shareholding Pattern table's quarterly tab (id="quarterly-shp")."""
    section = soup.find("section", id="shareholding")
    if section is None:
        return None, None
    quarterly = section.find("div", id="quarterly-shp")
    if quarterly is None:
        return None, None
    table = quarterly.find("table")
    if table is None:
        return None, None

    rows = table.find_all("tr")
    if len(rows) < 2:
        return None, None

    for tr in rows[1:]:
        cells = [c.get_text(strip=True).replace("\xa0", " ") for c in tr.find_all(["th", "td"])]
        if not cells:
            continue
        label = cells[0].rstrip("+").strip()
        if label == "Promoters":
            values = [_parse_pct(c) for c in cells[1:]]
            if not values:
                return None, None
            latest = values[-1]
            year_ago = values[-1 - QUARTERS_BACK_FOR_TREND] if len(values) > QUARTERS_BACK_FOR_TREND else None
            return latest, year_ago
    return None, None


def _fetch_pledge_flag(soup: BeautifulSoup) -> tuple[bool, Optional[str]]:
    """Scans the auto-generated Cons list for a confirmed pledge disclosure."""
    section = soup.find("section", id="analysis")
    if section is None:
        return False, None
    cons = section.find("div", class_="cons")
    if cons is None:
        return False, None
    for li in cons.find_all("li"):
        text = li.get_text(strip=True)
        if PLEDGE_PATTERN.search(text):
            return True, text
    return False, None


def check_governance(ticker: str) -> GovernanceResult:
    """Fetches promoter-holding trend + pledge disclosure for one stock."""
    base = BASE_URL.format(ticker=ticker)
    for segment in ("consolidated/", ""):
        url = base + segment
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "lxml")

            latest, year_ago = _fetch_promoter_holding_trend(soup)
            pledge_flag, pledge_text = _fetch_pledge_flag(soup)

            trend = None
            declining = False
            if latest is not None and year_ago is not None:
                trend = latest - year_ago
                declining = trend <= -DECLINING_HOLDING_THRESHOLD_PP

            if latest is None and not pledge_flag:
                continue  # try the other segment before giving up

            return GovernanceResult(
                ticker=ticker,
                promoter_holding_pct=latest,
                promoter_holding_pct_4q_ago=year_ago,
                promoter_holding_trend_pct_pts=trend,
                declining_promoter_holding_flag=declining,
                pledge_flag=pledge_flag,
                pledge_text=pledge_text,
            )
        except Exception as exc:
            logger.debug("Governance fetch failed for %s (%s segment): %s", ticker, segment or "standalone", exc)

    return GovernanceResult(ticker=ticker, error="Could not fetch/parse Shareholding Pattern or Cons list")


def sector_governance_check(tickers: list[str]) -> tuple[list[GovernanceResult], str]:
    """
    Checks governance (pledge + promoter-holding trend) for a small set of already-
    qualified tickers. Returns (per-ticker results, summary string).
    """
    results: list[GovernanceResult] = []
    for i, ticker in enumerate(tickers):
        results.append(check_governance(ticker))
        if i < len(tickers) - 1:
            time.sleep(REQUEST_SLEEP_SEC)

    pledged = [r for r in results if r.pledge_flag]
    summary = (
        f"{len(pledged)}/{len(results)} flagged for promoter pledge"
        if results else "No stocks to check"
    )
    return results, summary
