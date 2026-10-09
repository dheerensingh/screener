"""
Sector-Rotation + Momentum Stock Screener, with a built-in paper-trading account.
Entry point — orchestrates all pipeline modules.

Pipeline:
  1. Rank all NSE sectors (full Nifty500 universe) by relative strength vs the
     benchmark (sector_rotation.py); also get universe-wide RS Ratings + the
     raw benchmark series.
  1b. Market regime (market_regime.py) and FII/DII flow — informational only.
      The 200-day market filter stopped gating entries on 2026-10-01 (STRATEGY.md §16).
  2. Confirm the leading sector(s) with YoY profit-growth data (fundamental_analyzer.py)
  3. Screen the leading sector(s)' baskets for momentum stocks — RSI/MACD +
     Minervini's Trend Template/RS Rating gate (technical_analyzer.py, trend_template.py)
  3a. Governance gate on the qualified stocks — reject any with a confirmed
      promoter share pledge (governance_analyzer.py).
  3b. Paper account (paper_trader.py): fill yesterday's orders at today's open,
      apply exit rules to open positions, mark everything to market.
  3c. Allocate new orders around what's already held — up to 15 stocks with
      sector shares weighted by relative strength, smaller adds to held winners,
      budget-aware sizing, portfolio-heat ceiling, cap-band risk
      (portfolio_allocator.py) — and book them as pending orders for the next open.
  4. Publish the screening report and the paper-trading dashboard to docs/
     (report_generator.py, dashboard.py) for GitHub Pages.
  5. Send a summary email linking to both (email_alerter.py).

Run locally:
  pip install -r requirements.txt
  cp .env.example .env && fill in your credentials
  python main.py

Run via GitHub Actions: every weekday after the NSE close (18:17 IST, with overnight
backups) — see
.github/workflows/schedule.yml. See CLAUDE.md for the architecture.
"""

import csv
import logging
import os
import sys
from datetime import datetime

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed — rely on environment variables already set

from modules.sector_rotation import rank_sectors, leading_sectors
from modules.fundamental_analyzer import sector_confirmation
from modules.governance_analyzer import sector_governance_check
from modules.delivery_analyzer import fetch_latest_delivery_pct
from modules.market_calendar import refresh_holidays
from modules import nse_eod
from modules.technical_analyzer import screen_stocks
from modules.market_regime import evaluate as evaluate_market_regime
from modules.fii_dii_flow import fetch_latest_flow
from modules.portfolio_allocator import MAX_TOTAL_POSITIONS, allocate as allocate_portfolio
from modules.position_sizer import get_account_size
from modules.report_generator import SectorBundle, build_report_html, publish_report
from modules.paper_trader import PaperTrader
from modules.dashboard import build_dashboard_html, publish_dashboard
from modules.email_alerter import build_summary_email, send_email, send_error_email
from modules.stock_fetcher import latest_completed_session

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("screener.main")

LAST_SESSION_PATH = os.path.join("docs", "data", "last_session.txt")
FEED_LOG_PATH = os.path.join("docs", "data", "feed_log.csv")


def _log_feed_status(price_data: dict, benchmark: pd.Series, expected: str) -> None:
    """
    One row per run in FEED_LOG_PATH: did Yahoo (stocks, benchmark) and NSE's own
    bhavcopy have the expected session yet? Builds the evidence for how often, and
    how late, Yahoo lags — which can't be reconstructed after the fact.
    """
    last_bars = pd.Series([df.index[-1].strftime("%Y-%m-%d") for df in price_data.values() if len(df)])
    nse_filled = len(nse_eod.FILLED.get(expected, set()) & set(price_data))
    at_expected = int((last_bars >= expected).sum())
    yahoo_pct = (at_expected - nse_filled) / len(last_bars) * 100.0 if len(last_bars) else 0.0
    bench_last = benchmark.index[-1].strftime("%Y-%m-%d") if len(benchmark) else ""
    try:
        bhav = nse_eod.bhavcopy(datetime.strptime(expected, "%Y-%m-%d").date())
        nse_has_it = "yes" if bhav is not None and len(bhav) else "no"
    except Exception:
        nse_has_it = "error"
    row = {
        "run_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M"), "expected_session": expected,
        "stocks_last_bar": last_bars.mode().iloc[0] if len(last_bars) else "",
        "yahoo_stocks_at_expected_pct": f"{yahoo_pct:.1f}",
        "nse_filled_stocks": str(nse_filled),
        "yahoo_benchmark_last_bar": bench_last, "nse_bhavcopy_available": nse_has_it,
    }
    logger.info("  Feed status: %s", row)
    try:
        new_file = not os.path.exists(FEED_LOG_PATH)
        os.makedirs(os.path.dirname(FEED_LOG_PATH), exist_ok=True)
        with open(FEED_LOG_PATH, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(row))
            if new_file:
                writer.writeheader()
            writer.writerow(row)
    except OSError as exc:
        logger.warning("Could not write feed log: %s", exc)


def main() -> int:
    start = datetime.now()
    logger.info("=== Sector Screener starting at %s ===", start.strftime("%Y-%m-%d %H:%M:%S"))

    errors: list[str] = []
    try:
        refresh_holidays()  # NSE's holiday list for the skip-guard and latest_completed_session()
    except Exception as exc:
        logger.warning("Holiday list refresh crashed (non-fatal): %s", exc)

    # ── Step 1: Rank sectors by relative strength ────────────────────────────
    try:
        logger.info("Step 1: Ranking sectors by relative strength vs Nifty500 ...")
        rotation = rank_sectors()
        if not rotation.scores:
            msg = "Sector rotation produced no scored sectors — aborting."
            logger.error(msg)
            errors.append(msg)
            send_error_email(errors)
            return 1
        all_scores = rotation.scores
        leading = leading_sectors(all_scores)
        logger.info("Leading sector(s): %s", [s.sector for s in leading])
    except Exception as exc:
        msg = f"Step 1 (sector rotation) crashed: {exc}"
        logger.exception(msg)
        errors.append(msg)
        send_error_email(errors)
        return 1

    # ── Step 1b: Market context (informational only) ─────────────────────────
    regime = evaluate_market_regime(rotation.benchmark, breadth_pct=rotation.breadth_pct)
    logger.info("Market regime (informational): %s", regime.reason)
    try:
        flow = fetch_latest_flow()
        if flow.error:
            logger.warning("FII/DII flow unavailable (non-fatal): %s", flow.error)
        else:
            logger.info(
                "FII/DII flow (%s, informational, not backtested): FII ₹%+.0f cr, DII ₹%+.0f cr",
                flow.date, flow.fii_net_cr or 0.0, flow.dii_net_cr or 0.0,
            )
    except Exception as exc:
        logger.warning("FII/DII flow check crashed (non-fatal): %s", exc)
        flow = None

    # ── Step 2-3a: Per leading sector — fundamentals, screening, governance ──
    account_size = get_account_size()
    bundles: list[SectorBundle] = []
    sector_qualifiers: dict[str, list] = {}
    governance_rejects: list[tuple] = []  # (ScreenResult, sector, reason)

    for score in leading:
        sector = score.sector
        try:
            logger.info("Step 2 [%s]: Fundamental confirmation ...", sector)
            fundamental_results, fundamental_summary = sector_confirmation(list(score.data.keys()))
        except Exception as exc:
            msg = f"Fundamental confirmation failed for {sector}: {exc}"
            logger.warning(msg)
            errors.append(msg)
            fundamental_results, fundamental_summary = [], "Fundamental check failed"

        try:
            logger.info("Step 3 [%s]: Momentum screening on %d stocks ...", sector, len(score.data))
            try:
                delivery_pct = fetch_latest_delivery_pct(list(score.data.keys()))
            except Exception as exc:
                logger.warning("Delivery %% fetch failed for %s (non-fatal, informational only): %s", sector, exc)
                delivery_pct = {}
            qualified, all_results = screen_stocks(
                score.data, sector_index=score.synthetic_index, cap_bands=score.cap_bands,
                rs_ratings=rotation.rs_ratings, delivery_pct=delivery_pct,
            )
            logger.info("  [%s] %d / %d stocks qualify", sector, len(qualified), len(all_results))
        except Exception as exc:
            msg = f"Momentum screening failed for {sector}: {exc}"
            logger.warning(msg)
            errors.append(msg)
            qualified, all_results = [], []

        try:
            if qualified:
                logger.info("Step 3a [%s]: Governance check on %d qualifier(s) ...", sector, len(qualified))
                governance_results, governance_summary = sector_governance_check([r.ticker for r in qualified])
                pledged = {gr.ticker: gr.pledge_text for gr in governance_results if gr.pledge_flag}
                if pledged:
                    logger.info("  [%s] Governance gate rejected (pledge): %s", sector, sorted(pledged))
                governance_rejects += [(r, sector, f"Rejected: {pledged[r.ticker]}") for r in qualified if r.ticker in pledged]
                qualified = [r for r in qualified if r.ticker not in pledged]
            else:
                governance_results, governance_summary = [], "No qualifiers to check"
        except Exception as exc:
            msg = f"Governance check failed for {sector}: {exc}"
            logger.warning(msg)
            errors.append(msg)
            governance_results, governance_summary = [], "Governance check failed"

        sector_qualifiers[sector] = qualified
        bundles.append(SectorBundle(
            score=score,
            fundamental_summary=fundamental_summary,
            fundamental_results=fundamental_results,
            qualified=qualified,
            all_results=all_results,
            position_sizes={},  # filled in below by the portfolio allocator
            governance_summary=governance_summary,
            governance_results=governance_results,
        ))

    # ── Step 3b-3c: Paper account — fills, exits, then new orders ────────────
    trader = PaperTrader(start_capital=account_size)
    candidates: list[dict] = []
    try:
        logger.info("Step 3b: Updating paper account (fills, exits, mark-to-market) ...")
        price_data = {t: df for s in rotation.scores for t, df in s.data.items()}
        trader.update(price_data)
        expected = latest_completed_session()
        _log_feed_status(price_data, rotation.benchmark, expected)
        if trader.as_of and trader.as_of < expected:
            msg = (f"Price data ends {trader.as_of}, but the {expected} session should be complete — "
                   f"Yahoo hasn't published it yet. The next scheduled run will retry.")
            logger.warning(msg)
            errors.append(msg)
        logger.info(
            "  As of %s: %d filled, %d closed, %d cancelled, %d open",
            trader.as_of, len(trader.filled_today), len(trader.closed_today),
            len(trader.cancelled_today), len(trader.positions),
        )

        # Never order a stock whose data stops before the market date: its signal
        # would come from an older session (GLAND, 2026-10-09).
        stale = {t: df.index[-1].strftime("%Y-%m-%d") for t, df in price_data.items()
                 if trader.as_of and len(df) and df.index[-1].strftime("%Y-%m-%d") < trader.as_of}
        stale_rejects = [(r, sector) for sector, rs in sector_qualifiers.items() for r in rs if r.ticker in stale]
        if stale_rejects:
            logger.warning("  Skipping %d qualifier(s) with stale prices: %s",
                           len(stale_rejects), [r.ticker for r, _ in stale_rejects])
        sector_qualifiers = {sector: [r for r in rs if r.ticker not in stale]
                             for sector, rs in sector_qualifiers.items()}

        logger.info("Step 3c: Allocating new orders around current holdings ...")
        deployed_pct, heat_pct = trader.exposure_pct(account_size)
        allocation = allocate_portfolio(
            sector_qualifiers, account_size=account_size,
            existing_deployed_pct=deployed_pct, existing_heat_pct=heat_pct,
            sector_scores={s.sector: s.score for s in leading}, holdings=trader.holdings(),
        )
        logger.info("  Sector slots (of %d stocks): %s", MAX_TOTAL_POSITIONS, allocation.slots)
        sector_of = {r.ticker: sector for sector, rs in sector_qualifiers.items() for r in rs}
        trader.book(allocation.accepted, sector_of, adds=allocation.adds)
        logger.info(
            "  Booked %d order(s), rejected %d — %.1f%% capital, %.1f%% heat committed",
            len(trader.booked_today), len(allocation.rejected),
            allocation.total_deployed_pct, allocation.total_heat_pct,
        )

        accepted_by_ticker = {r.ticker: pos for r, pos in allocation.accepted}
        for bundle in bundles:
            bundle.position_sizes = {r.ticker: accepted_by_ticker[r.ticker]
                                     for r in bundle.qualified if r.ticker in accepted_by_ticker}

        decisions = {
            r.ticker: (f"Add #{allocation.adds[r.ticker]} (winner): {pos.quantity} shares for next open"
                       if r.ticker in allocation.adds else f"Order placed: {pos.quantity} shares for next open")
            for r, pos in allocation.accepted
        }
        decisions.update({r.ticker: reason for r, reason in allocation.rejected})
        for sector, results in sector_qualifiers.items():
            for r in results:
                candidates.append(dict(ticker=r.ticker, sector=sector, cap_band=r.cap_band, price=r.current_price,
                                       rsi=r.rsi, strength=r.strength_score, decision=decisions.get(r.ticker, "—")))
        for r, sector in stale_rejects:
            candidates.append(dict(ticker=r.ticker, sector=sector, cap_band=r.cap_band, price=r.current_price,
                                   rsi=r.rsi, strength=r.strength_score,
                                   decision=f"Skipped: prices end {stale[r.ticker]}, behind {trader.as_of}"))
        for r, sector, reason in governance_rejects:
            candidates.append(dict(ticker=r.ticker, sector=sector, cap_band=r.cap_band, price=r.current_price,
                                   rsi=r.rsi, strength=r.strength_score, decision=reason))

        bench = rotation.benchmark.dropna()
        bench_close = float(bench.loc[:trader.as_of].iloc[-1]) if trader.as_of else float(bench.iloc[-1])
        trader.snapshot(bench_close)
        trader.save()
        # The workflow's skip-guard compares this to the latest completed session,
        # so a run whose data came back stale is retried by the next backup slot.
        if trader.as_of:
            os.makedirs(os.path.dirname(LAST_SESSION_PATH), exist_ok=True)
            with open(LAST_SESSION_PATH, "w", encoding="utf-8") as f:
                f.write(trader.as_of + "\n")
    except Exception as exc:
        msg = f"Step 3b/3c (paper account) crashed: {exc}"
        logger.exception(msg)
        errors.append(msg)
    paper = trader.state()

    # ── Step 4: Publish the screening report + dashboard ─────────────────────
    pages_base = os.environ.get("PAGES_BASE_URL", "").rstrip("/")
    report_url = dashboard_url = None
    try:
        logger.info("Step 4: Building + publishing HTML report and dashboard ...")
        # Named by the market session the data is from, not the calendar date of the
        # run — a late-evening and an early-morning run of the same session then land
        # on the same file, which is what the workflow's skip-guard checks for.
        session = datetime.strptime(trader.as_of, "%Y-%m-%d") if trader.as_of else None
        html = build_report_html(all_scores, bundles, account_size, errors, generated_at=session,
                                 regime=regime, flow=flow)
        dated_path, _ = publish_report(html, date=session)
        publish_dashboard(build_dashboard_html(
            paper, all_scores, {s.sector for s in leading}, candidates, regime, flow,
        ))
        if pages_base:
            report_url = f"{pages_base}/{os.path.relpath(dated_path, 'docs')}"
            dashboard_url = f"{pages_base}/"
    except Exception as exc:
        msg = f"Step 4 (report/dashboard generation) crashed: {exc}"
        logger.exception(msg)
        errors.append(msg)

    # ── Step 5: Send summary email ─────────────────────────────────────────────
    try:
        logger.info("Step 5: Sending summary email ...")
        html = build_summary_email(
            leading_sectors=[b.score.sector for b in bundles], paper=paper,
            dashboard_url=dashboard_url, report_url=report_url, errors=errors,
        )
        ret = (paper.equity / paper.start_capital - 1.0) * 100.0 if paper.start_capital else 0.0
        subject = (f"📊 Paper portfolio ₹{paper.equity:,.0f} ({ret:+.2f}%) · "
                   f"{len(paper.booked_today)} new order(s) — {datetime.now():%d %b %Y}")
        if send_email(html, subject):
            logger.info("Email delivered successfully.")
        else:
            logger.error("Email delivery failed — check SMTP credentials in environment.")
            return 1
    except Exception as exc:
        logger.exception("Step 5 (email) crashed: %s", exc)
        return 1

    elapsed = (datetime.now() - start).total_seconds()
    logger.info("=== Screener finished in %.1fs ===", elapsed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
