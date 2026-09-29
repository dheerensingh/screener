"""
Module: Sector Universe (NSE-sourced)

Builds the sector -> stock mapping from NSE's own free, static index-
constituent CSVs (archives.nseindia.com — a different, unaffected subdomain
from www.nseindia.com's blocked API; see CLAUDE.md) rather than a hand-picked
list. This, as of 2026-09-27:

  - Covers the full Nifty500 universe. NSE itself builds Nifty500 as
    Nifty100 + Midcap150 + Smallcap250 (confirmed: those two lists add zero
    tickers beyond what's already in Nifty500), so fetching Nifty500 alone
    is the full large+mid+small-cap universe. Each stock is tagged with its
    cap band (Large/Mid/Small) from which of those lists it appears in.
  - Uses NSE's own ~20 "Industry" categories as the sector taxonomy for most
    stocks — authoritative and self-maintaining, unlike a hand-picked list.
    A category with no direct equivalent in our older sector names (e.g.
    "Capital Goods", "Textiles") becomes its own sector rather than being
    silently dropped.
  - Overrides the generic Industry tag with NSE's own official thematic
    index lists where one exists and is more specific: Nifty India Defence,
    Nifty PSE (our "PSU" theme), Nifty Infra (a better-curated Infrastructure
    basket than the old manual list — and its ticker for the defence maker
    is `MTARTECH`, not `MTAR`, which is why that symbol kept failing on
    yfinance under the old hand-picked list).
  - Falls back to a small manually curated list only for "Railways" — no
    official NSE index covers this theme (checked several plausible CSV
    filenames; none exist).

No login, no paid plan, no scraping of a protected endpoint — just CSV
downloads from the same archives.nseindia.com subdomain already used
successfully elsewhere in this repo (the old Nifty500-constituent fetch).
"""

import io
import logging
from dataclasses import dataclass

import pandas as pd
import requests

logger = logging.getLogger(__name__)

ARCHIVE_BASE = "https://archives.nseindia.com/content/indices"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; StockScreener/1.0)"}
REQUEST_TIMEOUT = 20

UNIVERSE_FILE = "ind_nifty500list.csv"

CAP_BAND_FILES = {
    "Large": "ind_nifty100list.csv",
    "Mid": "ind_niftymidcap150list.csv",
    "Small": "ind_niftysmallcap250list.csv",
}

# Official NSE thematic index constituent lists, applied as overrides — take
# priority over the generic Industry tag below, since they're a more specific
# classification than "Capital Goods" / "Services" etc.
THEMATIC_FILES = {
    "Defense": "ind_niftyindiadefence_list.csv",
    "PSU": "ind_niftypselist.csv",
    "Infrastructure": "ind_niftyinfralist.csv",
}

# No official NSE index exists for this theme (confirmed 2026-09-27) — kept
# as a small manual override, applied with the same priority as the official
# thematic files above. (The old list's "JUNIPERHOTEL" entry was a data-entry
# error — dropped.)
RAILWAYS_TICKERS = [
    "IRCTC", "RVNL", "RAILTEL", "IRCON", "RITES", "TITAGARH", "IRFC", "TEXRAIL", "KERNEX",
]

# NSE's raw "Industry" value -> our sector label. A category left out of this
# map keeps its NSE name as-is (e.g. "Capital Goods", "Textiles", "Services"),
# so every Nifty500 stock lands in *some* sector — nothing is silently dropped.
INDUSTRY_TO_SECTOR = {
    "Automobile and Auto Components": "Auto",
    "Healthcare": "Pharma",
    "Financial Services": "Banking",
    "Fast Moving Consumer Goods": "FMCG",
    "Metals & Mining": "Metals",
    "Oil Gas & Consumable Fuels": "Energy",
    "Power": "Energy",
    "Telecommunication": "Telecom",
    "Realty": "Real Estate",
    "Information Technology": "IT",
}

MAX_STOCKS_PER_SECTOR = 40  # keeps per-run yfinance/Screener.in volume predictable


@dataclass
class UniverseStock:
    ticker: str
    sector: str
    cap_band: str  # "Large" / "Mid" / "Small"


def _fetch_csv(filename: str) -> pd.DataFrame:
    url = f"{ARCHIVE_BASE}/{filename}"
    resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return pd.read_csv(io.StringIO(resp.text))


def _cap_bands() -> dict[str, str]:
    """ticker -> 'Large' / 'Mid' / 'Small', from NSE's own index membership."""
    bands: dict[str, str] = {}
    for band, filename in CAP_BAND_FILES.items():
        try:
            df = _fetch_csv(filename)
            for t in df["Symbol"].dropna().str.strip():
                bands[t] = band
        except Exception as exc:
            logger.warning("Could not fetch %s cap-band list (%s): %s", band, filename, exc)
    return bands


def _thematic_overrides() -> dict[str, str]:
    """ticker -> sector, for the Railways manual list plus official NSE thematic indices."""
    overrides: dict[str, str] = {t: "Railways" for t in RAILWAYS_TICKERS}
    for sector, filename in THEMATIC_FILES.items():
        try:
            df = _fetch_csv(filename)
            for t in df["Symbol"].dropna().str.strip():
                overrides.setdefault(t, sector)
        except Exception as exc:
            logger.warning("Could not fetch %s thematic list (%s): %s", sector, filename, exc)
    return overrides


def build_universe() -> list[UniverseStock]:
    """
    Full Nifty500 universe, each stock tagged with its sector (thematic
    override, or NSE Industry) and cap band. Uncapped — callers apply
    MAX_STOCKS_PER_SECTOR / liquidity filtering downstream, where the actual
    traded-volume data is available (sector_rotation.py).
    """
    nifty500 = _fetch_csv(UNIVERSE_FILE)
    bands = _cap_bands()
    overrides = _thematic_overrides()

    universe: list[UniverseStock] = []
    for _, row in nifty500.iterrows():
        ticker = str(row["Symbol"]).strip()
        if not ticker or ticker == "nan":
            continue
        industry = str(row.get("Industry", "")).strip()
        sector = overrides.get(ticker) or INDUSTRY_TO_SECTOR.get(industry) or industry or "Other"
        cap_band = bands.get(ticker, "Mid")
        universe.append(UniverseStock(ticker=ticker, sector=sector, cap_band=cap_band))

    logger.info(
        "Universe built: %d stocks across %d sectors", len(universe), len({u.sector for u in universe}),
    )
    return universe


def group_by_sector(universe: list[UniverseStock]) -> dict[str, list[UniverseStock]]:
    grouped: dict[str, list[UniverseStock]] = {}
    for u in universe:
        grouped.setdefault(u.sector, []).append(u)
    return grouped
