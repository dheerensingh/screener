"""
Sector-Rotation + Momentum Stock Screener
Entry point — orchestrates all pipeline modules.

Pipeline:
  1. Rank all NSE sectors (full Nifty500 universe) by relative strength vs the
     benchmark (sector_rotation.py); also get universe-wide RS Ratings + the
     raw benchmark series for the market-regime check.
  1b. Market regime filter (market_regime.py): pause new entries entirely if
      the Nifty500 benchmark isn't in its own uptrend.
  2. Confirm the leading sector(s) with YoY profit-growth data (fundamental_analyzer.py)
  3. Screen the leading sector(s)' baskets for momentum stocks — RSI/MACD +
     Minervini's Trend Template/RS Rating gate (technical_analyzer.py, trend_template.py)
  3a. Governance gate on the qualified stocks — reject any with a confirmed
      promoter share pledge (governance_analyzer.py); a declining promoter
      holding trend is surfaced but does not gate (see STRATEGY.md §12).
  3b. Allocate the portfolio across sectors — per-sector cap, budget-aware
      sizing, portfolio-heat ceiling, cap-band risk scaling (portfolio_allocator.py,
      position_sizer.py)
  3c. Log today's accepted signals for the paper-trading forward test (paper_trade_log.py)
  4. Publish an HTML report to docs/ for GitHub Pages (report_generator.py)
  5. Send a short summary email with a link to the report (email_alerter.py)

Run locally:
  pip install -r requirements.txt
  cp .env.example .env && fill in your credentials
  python main.py

Run via GitHub Actions:
  Push to main → Actions → "Daily Stock Screener" workflow fires at 08:00 IST.

See CLAUDE.md for the architecture/data-source decisions behind this pipeline.
"""

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
from modules.technical_analyzer import screen_stocks
from modules.market_regime import evaluate as evaluate_market_regime
from modules.fii_dii_flow import fetch_latest_flow
from modules.portfolio_allocator import allocate as allocate_portfolio
from modules.position_sizer import get_account_size
from modules.report_generator import SectorBundle, build_report_html, publish_report
from modules.email_alerter import build_summary_email, send_email, send_error_email
from modules.paper_trade_log import check_exits, log_signals

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("screener.main")


def main() -> int:
    start = datetime.now()
    logger.info("=== Sector Screener starting at %s ===", start.strftime("%Y-%m-%d %H:%M:%S"))

    errors: list[str] = []

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

    # ── Step 1b: Market regime — pause NEW entries if the broad market isn't
    #             in an uptrend (existing positions aren't tracked by this repo
    #             yet, so this only affects whether today generates new signals) ──
    regime = evaluate_market_regime(rotation.benchmark, breadth_pct=rotation.breadth_pct)
    logger.info("Market regime: %s", regime.reason)
    if not regime.in_uptrend:
        errors.append(f"Market regime filter: {regime.reason} — no new entries today.")

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

    # ── Step 2: Per leading sector — fundamentals + screening ────────────────
    account_size = get_account_size()
    bundles: list[SectorBundle] = []
    sector_qualifiers: dict[str, list] = {}

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
            logger.info(
                "  [%s] %d / %d stocks qualify", sector, len(qualified), len(all_results),
            )
        except Exception as exc:
            msg = f"Momentum screening failed for {sector}: {exc}"
            logger.warning(msg)
            errors.append(msg)
            qualified, all_results = [], []

        try:
            if qualified:
                logger.info("Step 3a [%s]: Governance check on %d qualifier(s) ...", sector, len(qualified))
                governance_results, governance_summary = sector_governance_check(
                    [r.ticker for r in qualified]
                )
                pledge_flagged = {gr.ticker for gr in governance_results if gr.pledge_flag}
                if pledge_flagged:
                    logger.info("  [%s] Governance gate rejected (pledge): %s", sector, pledge_flagged)
                qualified = [r for r in qualified if r.ticker not in pledge_flagged]
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

    # ── Step 3b: Portfolio-level allocation (Gap 2 — per-sector cap, budget-aware
    #             sizing, portfolio-heat ceiling, cap-band risk scaling) ─────────
    if regime.in_uptrend:
        try:
            logger.info("Step 3b: Allocating portfolio across %d sectors' qualifiers ...", len(sector_qualifiers))
            allocation = allocate_portfolio(sector_qualifiers, account_size=account_size)
            logger.info(
                "  Accepted %d, rejected %d — deployed %.1f%% capital, %.1f%% heat",
                len(allocation.accepted), len(allocation.rejected),
                allocation.total_deployed_pct, allocation.total_heat_pct,
            )
            accepted_by_ticker = {r.ticker: pos for r, pos in allocation.accepted}
            for bundle in bundles:
                bundle.position_sizes = {
                    t: pos for t, pos in accepted_by_ticker.items()
                    if t in {r.ticker for r in bundle.qualified}
                }
        except Exception as exc:
            msg = f"Step 3b (portfolio allocation) crashed: {exc}"
            logger.exception(msg)
            errors.append(msg)
    else:
        logger.info("Step 3b: Skipped (market regime not in uptrend) — no positions sized today.")

    # ── Step 3c: Paper-trade log — close positions that hit an exit rule, then
    #             log today's newly-accepted entries ───────────────────────────
    try:
        closed = check_exits()
        if closed:
            logger.info("Paper trade log: %d position(s) closed today.", closed)
    except Exception as exc:
        logger.warning("Paper-trade exit check failed (non-fatal): %s", exc)
    try:
        log_signals(bundles)
    except Exception as exc:
        logger.warning("Paper-trade logging failed (non-fatal): %s", exc)

    # ── Step 4: Build + publish the HTML report ───────────────────────────────
    report_url = None
    try:
        logger.info("Step 4: Building + publishing HTML report ...")
        html = build_report_html(all_scores, bundles, account_size, errors, regime=regime, flow=flow)
        dated_path, _ = publish_report(html)
        pages_base = os.environ.get("PAGES_BASE_URL", "").rstrip("/")
        if pages_base:
            report_url = f"{pages_base}/{os.path.relpath(dated_path, 'docs')}"
    except Exception as exc:
        msg = f"Step 4 (report generation) crashed: {exc}"
        logger.exception(msg)
        errors.append(msg)

    # ── Step 5: Send summary email ─────────────────────────────────────────────
    try:
        logger.info("Step 5: Sending summary email ...")
        qualified_rows = [
            (r.ticker, r.current_price,
             bundle.position_sizes[r.ticker].quantity if r.ticker in bundle.position_sizes else 0,
             bundle.position_sizes[r.ticker].allocation_pct if r.ticker in bundle.position_sizes else 0.0)
            for bundle in bundles for r in bundle.qualified
        ]
        html = build_summary_email(
            leading_sectors=[b.score.sector for b in bundles],
            qualified_rows=qualified_rows,
            report_url=report_url,
            errors=errors,
        )
        today_str = datetime.now().strftime("%d %b %Y")
        total_qualified = sum(len(b.qualified) for b in bundles)
        subject = f"📊 Sector Screener: {total_qualified} stock(s) qualify — {today_str}"

        sent = send_email(html, subject)
        if sent:
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
