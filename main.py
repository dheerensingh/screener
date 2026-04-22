"""
Daily Sentiment-Driven Stock Screener
Entry point — orchestrates all five modules.

Pipeline:
  1. Extract sector from @BasantBaheti3 tweets
  2. Map sector → NSE tickers, fetch 6-month OHLCV data
  3. Screen stocks: RSI(14) ≥ 60 AND MACD bullish
  4. Send HTML email with results (always sent — even on partial failure)

Run locally:
  pip install -r requirements.txt
  cp .env.example .env && fill in your credentials
  python main.py

Run via GitHub Actions:
  Push to main → Actions → "Daily Stock Screener" workflow fires at 08:00 IST.
"""

import logging
import os
import sys
from datetime import datetime

# Load .env file when running locally (GitHub Actions injects secrets as env vars directly)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed — rely on environment variables already set

from modules.twitter_extractor import get_sector
from modules.sector_mapper import get_stocks_for_sector
from modules.stock_fetcher import fetch_stock_data
from modules.technical_analyzer import screen_stocks
from modules.email_alerter import build_html_email, send_email, send_error_email

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("screener.main")

TWITTER_HANDLE = "BasantBaheti3"
TWEET_FETCH_COUNT = 20


def main() -> int:
    """
    Returns 0 on success (email sent), 1 on critical failure (email also attempted).
    """
    start = datetime.now()
    logger.info("=== Daily Stock Screener starting at %s ===", start.strftime("%Y-%m-%d %H:%M:%S"))

    errors: list[str] = []
    sector: str | None = None
    sector_source: str = "error"
    qualified = []
    all_results = []

    # ── Step 1: Detect sector from tweets ────────────────────────────────────
    try:
        logger.info("Step 1: Fetching tweets from @%s ...", TWITTER_HANDLE)
        sector, sector_source = get_sector(TWITTER_HANDLE, TWEET_FETCH_COUNT)

        if sector is None:
            msg = (
                "Sector detection returned None — tweets may not discuss any "
                "mapped sector, or all fetch methods failed. Falling back to broad market."
            )
            logger.warning(msg)
            errors.append(msg)
        else:
            logger.info("Sector detected: %s (via %s)", sector, sector_source)

    except Exception as exc:
        msg = f"Step 1 (tweet extraction) crashed unexpectedly: {exc}"
        logger.exception(msg)
        errors.append(msg)
        # send a minimal error email and exit
        send_error_email(errors)
        return 1

    # ── Step 2: Resolve tickers, fetch data ──────────────────────────────────
    try:
        scan_mode = os.environ.get("SCAN_UNIVERSE", "NIFTY500").upper()
        logger.info("Step 2: Mapping sector to tickers ... [SCAN_UNIVERSE=%s]", scan_mode)
        tickers, canonical_sector = get_stocks_for_sector(sector)
        logger.info("Universe: %s", canonical_sector)
        logger.info("Total tickers to screen: %d", len(tickers))

        logger.info("Step 2: Fetching 6-month OHLCV data via yfinance ...")
        data = fetch_stock_data(tickers, period="6mo")

        if not data:
            msg = "yfinance returned no usable data for any ticker in this sector."
            logger.error(msg)
            errors.append(msg)
            send_error_email(errors)
            return 1

        missing_count = len(tickers) - len(data)
        if missing_count:
            errors.append(
                f"{missing_count} ticker(s) had insufficient data and were skipped: "
                + ", ".join(t for t in tickers if t not in data)
            )

    except Exception as exc:
        msg = f"Step 2 (data fetching) crashed: {exc}"
        logger.exception(msg)
        errors.append(msg)
        send_error_email(errors)
        return 1

    # ── Step 3: Technical screening ───────────────────────────────────────────
    try:
        logger.info("Step 3: Running RSI + MACD screening on %d stocks ...", len(data))
        qualified, all_results = screen_stocks(data)

        # ── Full screening report (every stock) ───────────────────────────────
        logger.info("=" * 90)
        logger.info(
            "  %-14s | %-10s | %-6s | %-8s | %-8s | %s",
            "TICKER", "PRICE (₹)", "RSI", "RSI≥60", "MACD↑", "DETAIL"
        )
        logger.info("-" * 90)
        for r in sorted(all_results, key=lambda x: x.rsi, reverse=True):
            rsi_pass  = "✅" if "below" not in r.rsi_status  else "❌"
            macd_pass = "✅" if "bullish" in r.macd_status   else "❌"
            overall   = "✅ PASS" if r.passes else "❌ FAIL"
            logger.info(
                "  %-14s | %10.2f | %6.1f | %-8s | %-8s | [%s] %s",
                r.ticker, r.current_price, r.rsi,
                rsi_pass, macd_pass, overall, r.rsi_status,
            )
        logger.info("=" * 90)
        logger.info(
            "SCREENED: %d  |  PASSED: %d  |  FAILED: %d  |  Filters: RSI(14)≥60 AND MACD line > Signal",
            len(all_results), len(qualified), len(all_results) - len(qualified),
        )
        logger.info("=" * 90)

        # ── Clean final summary of passed stocks only ─────────────────────────
        logger.info("")
        logger.info("★  QUALIFIED STOCKS SUMMARY  ★")
        logger.info("-" * 50)
        if qualified:
            for i, r in enumerate(qualified, 1):
                logger.info(
                    "  %2d. %-14s | ₹%9.2f | RSI: %.1f | %s",
                    i, r.ticker, r.current_price, r.rsi, r.macd_status,
                )
        else:
            logger.info("  No stocks passed both filters today.")
        logger.info("-" * 50)

    except Exception as exc:
        msg = f"Step 3 (technical analysis) crashed: {exc}"
        logger.exception(msg)
        errors.append(msg)
        # Don't abort — still send email with whatever we have

    # ── Step 4: Build and send email ──────────────────────────────────────────
    try:
        logger.info("Step 4: Building HTML email ...")
        html = build_html_email(
            sector=canonical_sector if sector else None,
            sector_source=sector_source,
            qualified=qualified,
            all_results=all_results,
            errors=errors,
        )

        today_str = datetime.now().strftime("%d %b %Y")
        subject = (
            f"📊 Screener [{canonical_sector}]: {len(qualified)} stock(s) qualify — {today_str}"
            if qualified
            else f"📊 Screener [{canonical_sector}]: No qualifiers today — {today_str}"
        )

        sent = send_email(html, subject)
        if sent:
            logger.info("Email delivered successfully.")
        else:
            logger.error("Email delivery failed — check SMTP credentials in environment.")
            return 1

    except Exception as exc:
        logger.exception("Step 4 (email) crashed: %s", exc)
        return 1

    elapsed = (datetime.now() - start).total_seconds()
    logger.info("=== Screener finished in %.1fs ===", elapsed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
