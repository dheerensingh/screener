"""
Module 2a: Sector-to-Stock Mapping

Maps each detected sector to a curated list of NSE tickers (without the .NS suffix;
that is appended by stock_fetcher.py for Yahoo Finance compatibility).

Curation rationale:
  - Top 10–15 liquid, widely-tracked names per sector.
  - Prioritises large- and mid-caps where option liquidity and analyst coverage
    make technical signals more reliable.
  - Avoids illiquid micro-caps where RSI can spike on thin volume.
  - Updated for NSE as of 2025; verify tickers via finance.yahoo.com/<TICKER>.NS
    if yfinance returns empty DataFrames for any symbol.
"""

from typing import Optional

# fmt: off
SECTOR_STOCKS: dict[str, list[str]] = {
    "Defense": [
        "HAL", "BEL", "BEML", "COCHINSHIP", "GRSE", "MAZDOCK",
        "BHARATFORG", "SOLARINDS", "PARAS", "DATAPATTNS",
        "BHEL", "MTAR",
    ],
    "Railways": [
        "IRCTC", "RVNL", "RAILTEL", "IRCON", "RITES",
        "TITAGARH", "JUNIPERHOTEL",  # Jupiter Wagons is JUBLPHARMA — see note
        "IRFC", "TEXRAIL", "KERNEX",
    ],
    "IT": [
        "TCS", "INFY", "WIPRO", "HCLTECH", "TECHM",
        "LTIM", "MPHASIS", "PERSISTENT", "COFORGE", "OFSS",
        "KPITTECH", "TATAELXSI",
    ],
    "Pharma": [
        "SUNPHARMA", "DRREDDY", "CIPLA", "DIVISLAB", "LUPIN",
        "AUROPHARMA", "BIOCON", "ALKEM", "TORNTPHARM", "IPCALAB",
        "GLENMARK", "ABBOTINDIA",
    ],
    "Banking": [
        "HDFCBANK", "ICICIBANK", "SBIN", "KOTAKBANK", "AXISBANK",
        "INDUSINDBK", "BANDHANBNK", "FEDERALBNK", "IDFCFIRSTB",
        "AUBANK", "RBLBANK", "CANBK",
    ],
    "FMCG": [
        "HINDUNILVR", "NESTLEIND", "BRITANNIA", "DABUR", "MARICO",
        "ITC", "GODREJCP", "EMAMILTD", "TATACONSUM", "COLPAL",
        "PGHH", "BIKAJI",
    ],
    "Auto": [
        "MARUTI", "TATAMOTORS", "M&M", "HEROMOTOCO", "BAJAJ-AUTO",
        "EICHERMOT", "MOTHERSON", "BOSCHLTD", "BHARATFORG",
        "SUNDRMFAST", "TIINDIA", "BALKRISIND",
    ],
    "Metals": [
        "TATASTEEL", "JSWSTEEL", "HINDALCO", "VEDL", "SAIL",
        "NATIONALUM", "NMDC", "HINDZINC", "APLAPOLLO", "RATNAMANI",
        "JSWINFRA",
    ],
    "Energy": [
        "NTPC", "POWERGRID", "ADANIGREEN", "TORNTPOWER", "CESC",
        "ONGC", "IOC", "BPCL", "PETRONET", "GAIL",
        "TATAPOWER", "SJVN",
    ],
    "Infrastructure": [
        "LT", "NCC", "KNRCON", "PNCINFRA", "KEC",
        "KALPATPOWR", "IRB", "ASHOKA", "HGINFRA", "GPPL",
        "AHLUCONT", "JKCEMENT",
    ],
    "Chemicals": [
        "PIDILITIND", "ASIANPAINT", "AARTIIND", "VINATIORGA", "DEEPAKNTR",
        "NAVINFLUOR", "FINEORG", "GALAXYSURF", "BALAMINES", "TATACHEM",
        "GHCL", "GUJALKALI",
    ],
    "Telecom": [
        "BHARTIARTL", "INDUSTOWER", "TATACOMM", "RAILTEL",
        "VINDHYATEL", "ITI",
    ],
    "PSU": [
        "COALINDIA", "BHEL", "HAL", "NTPC", "POWERGRID",
        "ONGC", "IOC", "BPCL", "GAIL", "SAIL",
        "NATIONALUM", "NMDC",
    ],
    "Real Estate": [
        "DLF", "GODREJPROP", "OBEROIRLTY", "PRESTIGE", "PHOENIXLTD",
        "SOBHA", "MAHLIFE", "BRIGADE", "SUNTECK", "LODHA",
    ],
}
# fmt: on

# Aliases so the extractor's sector names map cleanly
SECTOR_ALIASES: dict[str, str] = {
    "Information Technology": "IT",
    "Technology": "IT",
    "Pharmaceutical": "Pharma",
    "Healthcare": "Pharma",
    "Bank": "Banking",
    "Finance": "Banking",
    "Consumer": "FMCG",
    "Automobile": "Auto",
    "Metal": "Metals",
    "Steel": "Metals",
    "Power": "Energy",
    "Oil": "Energy",
    "Infra": "Infrastructure",
    "Chemical": "Chemicals",
    "Realty": "Real Estate",
}


def get_stocks_for_sector(sector: Optional[str]) -> tuple[list[str], str]:
    """
    Returns (tickers_without_suffix, canonical_sector_name).

    Falls back to a broad large-cap basket when the sector is unknown,
    so the pipeline always has something to screen.
    """
    if sector is None:
        return _broad_market_basket(), "Broad Market (sector undetected)"

    # Resolve aliases
    canonical = SECTOR_ALIASES.get(sector, sector)

    tickers = SECTOR_STOCKS.get(canonical)
    if tickers:
        return list(tickers), canonical

    # Partial match: accept if the identified sector is a substring of a known key
    for key in SECTOR_STOCKS:
        if sector.lower() in key.lower() or key.lower() in sector.lower():
            return list(SECTOR_STOCKS[key]), key

    return _broad_market_basket(), f"Broad Market (unknown sector: {sector})"


def _broad_market_basket() -> list[str]:
    """Top 20 Nifty50 heavyweights — used as fallback universe."""
    return [
        "RELIANCE", "TCS", "HDFCBANK", "ICICIBANK", "INFY",
        "HINDUNILVR", "ITC", "SBIN", "BHARTIARTL", "KOTAKBANK",
        "LT", "AXISBANK", "ASIANPAINT", "MARUTI", "BAJFINANCE",
        "SUNPHARMA", "NTPC", "TITAN", "WIPRO", "ULTRACEMCO",
    ]
