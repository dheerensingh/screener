"""
Module 2: Fundamental Confirmation

Scrapes Screener.in's *public* company pages (no login, no paid plan needed —
confirmed in a Phase 0 spike, 2026-09-27: the Quarterly Results table renders
in plain HTML on e.g. screener.in/company/TCS/consolidated/) to check whether
a stock's latest-quarter profit grew year-on-year.

Used only for the leading sector(s) picked by `sector_rotation.py` (a few dozen
tickers — TOP_N_SECTORS baskets, not the full universe), as a confirmation/
tiebreaker signal — it does not override the technical rotation ranking.
"""

import logging
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
REQUEST_SLEEP_SEC = 1  # polite pause between company pages
QUARTERS_BACK_FOR_YOY = 4  # compare latest quarter vs the same quarter a year ago


@dataclass
class FundamentalResult:
    ticker: str
    latest_quarter_label: Optional[str]
    latest_net_profit: Optional[float]
    yoy_quarter_label: Optional[str]
    yoy_net_profit: Optional[float]
    yoy_growth_pct: Optional[float]
    error: Optional[str] = None

    @property
    def profit_grew_yoy(self) -> bool:
        return self.yoy_growth_pct is not None and self.yoy_growth_pct > 0


def _parse_num(text: str) -> Optional[float]:
    text = text.strip().replace(",", "")
    if not text or text in ("-", "NA"):
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _fetch_quarterly_row(ticker: str, row_label: str) -> Optional[tuple[list[str], list[Optional[float]]]]:
    """
    Returns (quarter_labels, values) for the given row label (e.g. "Net Profit"),
    trying the consolidated page first and falling back to standalone if that
    segment doesn't exist for this company (some companies only publish one).
    """
    base = BASE_URL.format(ticker=ticker)
    for segment in ("consolidated/", ""):
        url = base + segment
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "lxml")
            section = soup.find("section", id="quarters")
            if section is None:
                continue
            table = section.find("table")
            if table is None:
                continue

            rows = table.find_all("tr")
            header_cells = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])]
            quarter_labels = header_cells[1:]

            for tr in rows[1:]:
                cells = [c.get_text(strip=True) for c in tr.find_all(["th", "td"])]
                if not cells:
                    continue
                label = cells[0].rstrip("+").strip()
                if label == row_label:
                    values = [_parse_num(c) for c in cells[1:]]
                    return quarter_labels, values
        except Exception as exc:
            logger.debug("Fetch failed for %s (%s segment): %s", ticker, segment or "standalone", exc)
    return None


def check_profit_growth(ticker: str) -> FundamentalResult:
    """Fetches quarterly Net Profit and checks YoY growth for the latest quarter."""
    parsed = _fetch_quarterly_row(ticker, "Net Profit")
    if parsed is None:
        return FundamentalResult(
            ticker=ticker, latest_quarter_label=None, latest_net_profit=None,
            yoy_quarter_label=None, yoy_net_profit=None, yoy_growth_pct=None,
            error="Could not fetch/parse Quarterly Results table",
        )

    labels, values = parsed
    if len(values) < QUARTERS_BACK_FOR_YOY + 1:
        return FundamentalResult(
            ticker=ticker, latest_quarter_label=labels[-1] if labels else None,
            latest_net_profit=values[-1] if values else None,
            yoy_quarter_label=None, yoy_net_profit=None, yoy_growth_pct=None,
            error="Fewer than 5 quarters of history available",
        )

    latest = values[-1]
    year_ago = values[-1 - QUARTERS_BACK_FOR_YOY]
    growth = None
    if latest is not None and year_ago not in (None, 0):
        growth = (latest / year_ago - 1.0) * 100.0

    return FundamentalResult(
        ticker=ticker,
        latest_quarter_label=labels[-1],
        latest_net_profit=latest,
        yoy_quarter_label=labels[-1 - QUARTERS_BACK_FOR_YOY],
        yoy_net_profit=year_ago,
        yoy_growth_pct=growth,
    )


def sector_confirmation(tickers: list[str]) -> tuple[list[FundamentalResult], str]:
    """
    Checks YoY profit growth for every ticker in a sector's basket.
    Returns (per-ticker results, summary string like "9/12 stocks grew profit YoY").
    """
    results: list[FundamentalResult] = []
    for i, ticker in enumerate(tickers):
        results.append(check_profit_growth(ticker))
        if i < len(tickers) - 1:
            time.sleep(REQUEST_SLEEP_SEC)

    usable = [r for r in results if r.yoy_growth_pct is not None]
    grew = [r for r in usable if r.profit_grew_yoy]
    summary = (
        f"{len(grew)}/{len(usable)} stocks grew profit YoY"
        if usable else "Insufficient fundamental data"
    )
    return results, summary
