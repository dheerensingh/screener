"""
Module 3b: NSE Delivery Percentage

NSE's daily bhavcopy (same trusted archives.nseindia.com subdomain sector_mapper.py
already depends on for the Nifty500 constituent lists) publishes a delivery
percentage (DELIV_PER) per stock per day — the share of traded volume that settled
as an actual delivery rather than an intraday/F&O-driven round-trip. In the Indian
market specifically, a lot of daily volume is intraday/derivatives noise, so a
breakout backed by high delivery% is a stronger "real accumulation" signal than raw
volume surge alone (confirmed live, 2026-09-28: sec_bhavdata_full_DDMMYYYY.csv
returns one row per NSE stock with an explicit DELIV_PER column).

Kept purely informational for v1 (see technical_analyzer.ScreenResult.delivery_pct —
contributes to strength_score, never gates `passes`), mirroring volume_surge's exact
treatment. Whether a delivery%-above-median filter is actually worth *gating* on is
a separate, tested hypothesis — see backtest/run_delivery_backtest.py and
STRATEGY.md §13, not assumed here.
"""

import io
import logging
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import requests

from .nse_eod import equity_rows

logger = logging.getLogger(__name__)

NSE_BHAVCOPY_URL = "https://archives.nseindia.com/products/content/sec_bhavdata_full_{date}.csv"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
}
MAX_LOOKBACK_DAYS = 5  # how far back to search for the most recently published bhavcopy


def fetch_delivery_data(date: datetime) -> Optional[pd.DataFrame]:
    """
    One day, ALL NSE EQ-series stocks in a single request. Returns a DataFrame with
    columns [ticker, deliv_per], or None on a non-trading day (confirmed 404) or any
    other fetch/parse failure.
    """
    url = NSE_BHAVCOPY_URL.format(date=date.strftime("%d%m%Y"))
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
    except requests.RequestException as exc:
        logger.debug("Delivery data request failed for %s: %s", date.date(), exc)
        return None
    if resp.status_code == 404:
        return None
    if resp.status_code != 200:
        logger.debug("Delivery data unexpected status %s for %s", resp.status_code, date.date())
        return None

    try:
        df = pd.read_csv(io.StringIO(resp.text))
        df.columns = [c.strip() for c in df.columns]
        df = equity_rows(df)
        out = df[["SYMBOL", "DELIV_PER"]].rename(
            columns={"SYMBOL": "ticker", "DELIV_PER": "deliv_per"}
        )
        out["ticker"] = out["ticker"].str.strip()
        out["deliv_per"] = pd.to_numeric(out["deliv_per"], errors="coerce")
        return out.dropna(subset=["deliv_per"])
    except Exception as exc:
        logger.debug("Delivery data parse failed for %s: %s", date.date(), exc)
        return None


def fetch_latest_delivery_pct(tickers: Optional[list[str]] = None) -> dict[str, float]:
    """
    Live-pipeline use: walks back from today (up to MAX_LOOKBACK_DAYS calendar days)
    to find the most recently published trading day's bhavcopy — handles running
    before NSE has published today's file, or on a weekend/holiday.
    """
    today = datetime.now()
    for offset in range(MAX_LOOKBACK_DAYS + 1):
        day = today - timedelta(days=offset)
        df = fetch_delivery_data(day)
        if df is not None and len(df):
            if tickers is not None:
                wanted = set(tickers)
                df = df[df["ticker"].isin(wanted)]
            return dict(zip(df["ticker"], df["deliv_per"]))
    logger.warning("No delivery data found in the last %d days.", MAX_LOOKBACK_DAYS)
    return {}
