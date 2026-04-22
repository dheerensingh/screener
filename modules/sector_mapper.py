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

    Falls back to the full Nifty500 universe when the sector is unknown,
    so the pipeline always has something meaningful to screen.
    """
    if sector is None:
        tickers = fetch_nifty500_tickers()
        return tickers, f"Broad Market — Nifty500 ({len(tickers)} stocks)"

    # Resolve aliases
    canonical = SECTOR_ALIASES.get(sector, sector)

    tickers = SECTOR_STOCKS.get(canonical)
    if tickers:
        return list(tickers), canonical

    # Partial match: accept if the identified sector is a substring of a known key
    for key in SECTOR_STOCKS:
        if sector.lower() in key.lower() or key.lower() in sector.lower():
            return list(SECTOR_STOCKS[key]), key

    # Unknown sector — scan full Nifty500
    tickers = fetch_nifty500_tickers()
    return tickers, f"Broad Market — Nifty500 ({len(tickers)} stocks, unknown sector: {sector})"


def fetch_nifty500_tickers() -> list[str]:
    """
    Downloads the live Nifty500 constituent list from NSE's public archive CSV.
    No authentication required. Falls back to a hardcoded Nifty50 basket if
    the download fails (network issue in CI, etc.).

    NSE CSV columns: Company Name, Industry, Symbol, Series, ISIN Code
    We only need the 'Symbol' column.
    """
    import io
    import logging
    import requests

    logger = logging.getLogger(__name__)
    url = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"
    headers = {"User-Agent": "Mozilla/5.0 (compatible; StockScreener/1.0)"}

    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        import pandas as pd
        df = pd.read_csv(io.StringIO(resp.text))
        # Column is named 'Symbol' in NSE CSV
        symbols = df["Symbol"].dropna().str.strip().tolist()
        # Filter to equity series only (remove ETFs, etc. that sneak in)
        symbols = [s for s in symbols if s.isalpha() or "&" in s or "-" in s]
        logger.info("Nifty500 list fetched from NSE: %d symbols", len(symbols))
        return symbols
    except Exception as exc:
        logger.warning("Failed to fetch Nifty500 from NSE (%s) — using hardcoded Nifty50 fallback", exc)
        return _nifty50_fallback()


def _nifty50_fallback() -> list[str]:
    """~500 hardcoded liquid NSE stocks — used only when NSE CSV download fails."""
    from .stock_universe import _NIFTY50, _NIFTY_NEXT_50, _NIFTY_MIDCAP100, _NIFTY_SMALLCAP150
    return _NIFTY50 + _NIFTY_NEXT_50 + _NIFTY_MIDCAP100 + _NIFTY_SMALLCAP150
