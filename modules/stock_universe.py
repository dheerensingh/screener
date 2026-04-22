"""
NSE Full Equity Universe Fetcher

Tries to pull the complete NSE equity list (~1800+ symbols) from the NSE
archives CSV. Falls back to a hardcoded Nifty 500 basket if the NSE request
fails (common in CI environments due to NSE's anti-bot headers).

Set env var SCAN_UNIVERSE=NIFTY50 | NIFTY200 | NIFTY500 | NSE_ALL (default: NIFTY500)
"""

import io
import logging
import os

import pandas as pd
import requests

logger = logging.getLogger(__name__)

NSE_EQUITY_CSV = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"

_NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
}


def fetch_nse_all_equities() -> list[str]:
    """
    Downloads the NSE equity master CSV and returns all EQ-series symbols.
    Returns empty list on failure so the caller can fall back gracefully.
    """
    try:
        session = requests.Session()
        # Warm up session cookies (NSE rejects cold requests)
        session.get("https://www.nseindia.com", headers=_NSE_HEADERS, timeout=10)
        resp = session.get(NSE_EQUITY_CSV, headers=_NSE_HEADERS, timeout=20)
        resp.raise_for_status()

        df = pd.read_csv(io.StringIO(resp.text))
        df.columns = df.columns.str.strip()

        if "SERIES" in df.columns:
            df = df[df["SERIES"].str.strip() == "EQ"]

        symbols = df["SYMBOL"].dropna().str.strip().tolist()
        logger.info("NSE equity list fetched: %d symbols", len(symbols))
        return symbols

    except Exception as exc:
        logger.warning("NSE equity list fetch failed (%s) — using hardcoded fallback", exc)
        return []


def get_universe(mode: str | None = None) -> tuple[list[str], str]:
    """
    Returns (ticker_list, description) based on the SCAN_UNIVERSE env var or
    the `mode` argument.

    Modes
    -----
    NIFTY50   →  50 stocks  (fastest, ~10s)
    NIFTY200  →  200 stocks (~30s)
    NIFTY500  →  500 stocks (~90s)   ← default
    NSE_ALL   →  all NSE EQ stocks   (~8–15 min on GitHub Actions)
    """
    mode = (mode or os.environ.get("SCAN_UNIVERSE", "NIFTY500")).upper()

    if mode == "NSE_ALL":
        symbols = fetch_nse_all_equities()
        if symbols:
            return symbols, f"NSE All Equities ({len(symbols)} stocks)"
        # Fall through to NIFTY500 on failure
        logger.warning("NSE_ALL fetch failed — falling back to NIFTY500")
        mode = "NIFTY500"

    if mode == "NIFTY50":
        return _NIFTY50, "Nifty 50 (50 stocks)"
    if mode == "NIFTY200":
        return _NIFTY50 + _NIFTY_NEXT_50 + _NIFTY_MIDCAP100[:100], "Nifty 200 (200 stocks)"

    # Default: NIFTY500
    return _NIFTY50 + _NIFTY_NEXT_50 + _NIFTY_MIDCAP100 + _NIFTY_SMALLCAP150, "Nifty 500 (~500 stocks)"


# ── Hardcoded index constituents (as of Apr 2025) ─────────────────────────────

_NIFTY50 = [
    "RELIANCE", "TCS", "HDFCBANK", "ICICIBANK", "INFY",
    "HINDUNILVR", "ITC", "SBIN", "BHARTIARTL", "KOTAKBANK",
    "LT", "AXISBANK", "ASIANPAINT", "MARUTI", "BAJFINANCE",
    "SUNPHARMA", "NTPC", "TITAN", "WIPRO", "ULTRACEMCO",
    "BAJAJFINSV", "HCLTECH", "TECHM", "TATAMOTORS", "ONGC",
    "POWERGRID", "TATASTEEL", "NESTLEIND", "JSWSTEEL", "ADANIPORTS",
    "COALINDIA", "M&M", "DRREDDY", "HINDALCO", "CIPLA",
    "DIVISLAB", "APOLLOHOSP", "BPCL", "TATACONSUM", "GRASIM",
    "INDUSINDBK", "BRITANNIA", "EICHERMOT", "HEROMOTOCO", "ADANIENT",
    "UPL", "BAJAJ-AUTO", "LTIM", "SBILIFE", "HDFCLIFE",
]

_NIFTY_NEXT_50 = [
    "SIEMENS", "ABB", "HAVELLS", "PIDILITIND", "GODREJCP",
    "MUTHOOTFIN", "MARICO", "DABUR", "LUPIN", "BANDHANBNK",
    "AUROPHARMA", "TORNTPHARM", "DLF", "AMBUJACEM", "ACC",
    "SHREECEM", "VEDL", "SAIL", "NMDC", "GAIL",
    "TATAPOWER", "ADANIGREEN", "COLPAL", "EMAMILTD", "MOTHERSON",
    "BALKRISIND", "BOSCHLTD", "MPHASIS", "PERSISTENT", "COFORGE",
    "OFSS", "HDFCAMC", "PIIND", "ASTRAL", "POLYCAB",
    "TRENT", "NAUKRI", "ZOMATO", "DMART", "IRCTC",
    "PAGEIND", "BERGEPAINT", "KANSAINER", "VOLTAS", "CROMPTON",
    "TORNTPOWER", "CANBK", "BANKBARODA", "PNB", "IDBI",
]

_NIFTY_MIDCAP100 = [
    # IT / Tech
    "LTTS", "KPITTECH", "TATAELXSI", "MASTEK", "NIITTECH",
    # Banking / Finance
    "FEDERALBNK", "IDFCFIRSTB", "AUBANK", "RBLBANK", "UJJIVANSFB",
    "MANAPPURAM", "CHOLAFIN", "SUNDARMFIN", "LIChousing", "PNBHOUSING",
    # Pharma / Healthcare
    "ALKEM", "IPCALAB", "GLENMARK", "ABBOTINDIA", "PFIZER",
    "NATCOPHARM", "GRANULES", "LAURUSLABS", "SOLARA", "AJANTPHARM",
    "FORTIS", "METROPOLIS", "DRLAL", "MAXHEALTH",
    # Auto / Ancillary
    "TIINDIA", "SUNDRMFAST", "EXIDEIND", "AMARAJABAT", "SUPRAJIT",
    "GABRIEL", "ENDURANCE", "MINDA", "SUBROS",
    # Infrastructure / Construction
    "KEC", "KALPATPOWR", "IRB", "ASHOKA", "HGINFRA",
    "AHLUCONT", "NCC", "KNRCON", "PNCINFRA", "JKCEMENT",
    "RAMCOCEM", "HEIDELBERG", "NUVOCO",
    # Chemicals
    "AARTIIND", "VINATIORGA", "DEEPAKNTR", "NAVINFLUOR", "FINEORG",
    "GALAXYSURF", "BALAMINES", "TATACHEM", "GHCL", "GUJALKALI",
    "ATUL", "NOCIL", "THIRUMALCHM",
    # Defense
    "HAL", "BEL", "BEML", "COCHINSHIP", "GRSE",
    "MAZDOCK", "SOLARINDS", "DATAPATTNS", "PARAS", "MTAR",
    # Railways
    "RVNL", "RAILTEL", "IRCON", "RITES", "IRFC",
    "TITAGARH", "TEXRAIL", "KERNEX",
    # Metals
    "APLAPOLLO", "RATNAMANI", "JSWINFRA", "NATIONALUM", "HINDZINC",
    # Consumer / FMCG
    "TATACONSUM", "PGHH", "BIKAJI", "VBL", "VARUNBEV",
    "RADICO", "UNITDSPR",
    # Energy / Power
    "SJVN", "NHPC", "RECLTD", "PFC", "IREDA",
    "ADANIENSOL", "GREENPWR",
    # Real Estate
    "GODREJPROP", "OBEROIRLTY", "PRESTIGE", "PHOENIXLTD",
    "SOBHA", "BRIGADE", "SUNTECK", "LODHA",
    # Telecom
    "INDUSTOWER", "TATACOMM",
    # Miscellaneous
    "ZYDUSLIFE", "ALEMBICPHARM", "IGL", "MGL", "GUJGASLTD",
    "INDIAMART", "JUSTDIAL", "CARTRADE",
]

_NIFTY_SMALLCAP150 = [
    # IT Smallcap
    "TANLA", "NUCLEUS", "KELLTON", "SAKSOFT", "INTELLECT",
    "NEWGEN", "ZENSAR", "BIRLASOFT", "RATEGAIN",
    # Pharma Smallcap
    "CAPLIPOINT", "IOLCP", "SEQUENT", "SUVEN", "NEULANDLAB",
    "LAURUS", "SHILPAMED", "STRIDES", "MARKSANS",
    # Finance Smallcap
    "CREDITACC", "ARMANFIN", "SPANDANA", "AAVAS", "HOMEFIRST",
    "REPCO", "APTUS",
    # Chemicals Smallcap
    "CLEAN", "SUDARSCHEM", "JAYAGROGN", "DHARAMSI", "ULTRAMARINE",
    "IGPL", "SPORTKING",
    # Defense Smallcap
    "DCAL", "IDEAFORGE", "AEROSPACEMI",
    # Infrastructure Smallcap
    "IRBINVIT", "POWERMECH", "WELSPUNIND", "GARFIBRES",
    "CAPACITE", "HFCL", "STLTECH",
    # Auto Smallcap
    "VARROC", "JBM", "CRAFTSMAN", "SHRIRAMCIT",
    # Consumer Smallcap
    "ZYDUSWELL", "HONASA", "CAMPUS", "BATA", "RELAXO",
    "VSTIND", "GODFRYPHLP",
    # Energy Smallcap
    "INOXWIND", "SUZLON", "RPOWER", "MPSLTD", "WAAREEENER",
    # Metals Smallcap
    "MOIL", "TINPLATE", "IMFA", "NSLNISP",
    # Real Estate Smallcap
    "MAHLIFE", "KOLTEPATIL", "ANANTRAJ", "ELDEHSG",
    # Miscellaneous
    "TEJASNET", "RAILSYS", "GMMPFAUDLR", "SMLISUZU",
    "VSTILLERS", "KIRIINDUS", "ESTER", "HINDCOPPER",
    "ORIENTELEC", "CERA", "GREENLAM",
]
