# task.md — Build backlog

Living backlog for evolving the current sentiment→RSI/MACD screener into the full
sector-rotation + momentum + position-sizing pipeline described in `CLAUDE.md`. Update
the Status column as work proceeds; check here before starting new work.

Phases 0–6 are the pipeline build; Phase 7 is the web app. Architecture for Phase 7 is
now locked in (see `CLAUDE.md` → Task 2 architecture decisions): static GitHub Pages
site under `docs/`, dated report archive, and a short summary+link email instead of
full HTML inline.

| Phase | Description | Key files | Status |
|---|---|---|---|
| 0 | ~~Verify data sources.~~ **Done 2026-09-27.** Official yfinance sector-index tickers are mostly dead (only `^CNXIT`/`^CNXPHARMA`/`^NSEBANK` live) → pivoted to synthetic sector baskets. `www.nseindia.com` API is blocked (403/503) → don't depend on it. Screener.in public pages expose Quarterly Results with no login/paid plan → scrape those directly. | — (spike/research) | Done |
| 1 | **Sector rotation module.** New `modules/sector_rotation.py`: build a synthetic sector index per sector by equal-weight-aggregating the OHLCV of each sector's basket (reusing `stock_fetcher.py`), compute relative strength vs Nifty500 over multiple lookbacks (1M/3M/6M), rank sectors. Replaces `twitter_extractor.py` as the sector-selection input to `main.py`. | `modules/sector_rotation.py` (new), `main.py` | **Done 2026-09-27** — verified locally, ranks all 14 sectors correctly |
| 1b | **Universe expansion to full Nifty500** (at the user's request, 2026-09-27): replaced the hand-picked 157-ticker `SECTOR_STOCKS` with `sector_mapper.build_universe()` — sourced from NSE's own Nifty500 + Industry-classification + thematic-index CSVs (~23 sectors, 501 stocks), tagged with cap band (Large/Mid/Small), liquidity-filtered (min ₹2cr/day avg traded value) and capped at 40/sector. `sector_rotation.py` now does one bulk 501-ticker fetch instead of per-sector fetches. See `CLAUDE.md` → "Universe & sector taxonomy". | `modules/sector_mapper.py` (rewritten), `modules/sector_rotation.py`, `modules/technical_analyzer.py` (`cap_band` field), `modules/report_generator.py` (cap-band badge + `# Stocks` column) | **Done 2026-09-27** — verified: 501 stocks → 23 sectors, capping/liquidity logic confirmed correct on a full local run |
| 2 | **Fundamental confirmation module.** New `modules/fundamental_analyzer.py`: scrape quarterly results / earnings-growth per stock in the leading sector(s) from Screener.in's public company pages (no login needed). Used as a secondary filter/tiebreaker, not the primary signal. | `modules/fundamental_analyzer.py` (new) | **Done 2026-09-27** — verified against real tickers (TCS, HDFCBANK) |
| 3 | **Extend momentum screening.** Add volume-surge and 52-week-high-proximity / relative-strength-vs-sector checks to the existing RSI+MACD screen. Stay pure pandas (no `pandas_ta`). | `modules/technical_analyzer.py` | **Done 2026-09-27** — added `atr()`, volume surge, 52w-high proximity, sector relative strength, `strength_score` ranking |
| 4 | **Position sizing module.** New `modules/position_sizer.py`: risk-based sizing off a configurable ₹5,00,000 account — fixed-fractional risk % per trade, ATR-based stop distance → share quantity → ₹ allocation and % of capital. Must expose the formula/assumptions, not just the final number. | `modules/position_sizer.py` (new) | **Done 2026-09-27** — hand-verified capped and uncapped cases |
| 5 | **HTML report generation.** Persist a styled, dated HTML report per run (sector rationale, qualifying stocks, entry/stop levels, position size + % of capital) into `docs/reports/YYYY-MM-DD.html`, plus update `docs/index.html` to point at it. | new `modules/report_generator.py` | **Done 2026-09-27** — verified in-browser via a local preview server |
| 6 | **Wire into `main.py` + GitHub Actions.** Replace the sentiment step with sector rotation + fundamentals; commit the generated `docs/` files back to the repo so GitHub Pages picks them up; keep the existing cron schedule; update `.env.example` for any new secrets. | `main.py`, `.github/workflows/schedule.yml`, `.env.example` | **Done 2026-09-27** — `main.py` rewritten, workflow updated (untested in CI itself, see notes) |
| 7 | **GitHub Pages + email summary.** Enable GitHub Pages on the repo (serve from `docs/`); change `modules/email_alerter.py` to send a short summary (qualifying stocks + position sizes) with a link to the day's `docs/reports/*.html` page, instead of the full HTML inline. | `modules/email_alerter.py`, repo Pages settings | **Code done 2026-09-27** (`build_summary_email`) — **enabling Pages itself is a manual one-time step, still pending** (Settings → Pages → Deploy from branch → `main` / `/docs`) |

## Task 3: Strategy, backtest, tuning, docs, paper trading (2026-09-27)

| Phase | Description | Key files | Status |
|---|---|---|---|
| 3.1 | **Gap 1 — Exit rules.** Chandelier trailing stop (2.5×ATR) + RSI(45) momentum-failure exit + 26-day time backstop, as the primary exit paradigm; fixed-target and scale-out built as comparison variants. Stop distance also capped at 8% of entry (Minervini-informed), reinforcing the 2×ATR stop. | `modules/position_sizer.py`, `backtest/simulator.py` | **Done** |
| 3.2 | **Gap 2 — Portfolio construction.** Per-sector position cap (2), sequential budget-aware sizing (85% max deployed), portfolio-heat ceiling (8%), cap-band risk scaling (Large/Mid/Small = 1.0/0.75/0.5%). | `modules/portfolio_allocator.py` (new) | **Done** — synthetic-scenario stress tests passed |
| 3.3 | **Minervini research → Trend Template + RS Rating + market regime filter.** Reviewed `marco-hui-95/vcp_screener`; adopted the 8-point Trend Template and universe-wide RS Rating as a required gate alongside RSI/MACD, plus a market-regime filter (benchmark vs its own 200-day SMA) pausing all new entries when not in an uptrend. VCP pattern-timing deliberately deferred (see CLAUDE.md). | `modules/trend_template.py`, `modules/market_regime.py` (new) | **Done** — hand-verified against real tickers/dates |
| 3.4 | **5-year backtest engine.** Custom pandas simulator (not vectorbt/backtesting.py — see CLAUDE.md) reusing live indicator/sizing code; no look-ahead (T-close signal → T+1-open execution); 6 entry×exit paradigm variants compared. | `backtest/` (new package: `data_cache.py`, `simulator.py`, `metrics.py`, `run_backtest.py`, `report.py`) | **Done** — see `STRATEGY.md` §8 for results |
| 3.5 | **Strategy document.** | `STRATEGY.md` (new) | **Done** |
| 3.6 | **Paper trading.** Researched free NSE-capable platforms; recommended TradingView (free, delayed data irrelevant for swing holds). Account creation is the user's own action. | `modules/paper_trade_log.py` (new), `STRATEGY.md` §10 | **Code done** — account setup itself is a user action, not yet done |
| 3.7 | **Regime filter v2 — RoC threshold** (2026-09-28, user-suggested, evaluated against 3 external suggestions). Investigated the Nov2022-Apr2023 drawdown with cached data before changing anything: SMA200's 21d rate-of-change averaged 0.5% in the chop vs never below 1.0% in a healthy trending period — clean separation, added as a second required regime condition. Re-ran the full 6-variant backtest: max DD improved 4-6pp everywhere, but CAGR dropped meaningfully too — a real tradeoff, not a pure win, documented as such. Market breadth (also user-suggested) tested on the same episode, found noisier, kept informational-only. | `modules/market_regime.py`, `backtest/simulator.py`, `modules/sector_rotation.py` (`breadth_pct`) | **Done** — see `STRATEGY.md` §3, §8 for v1-vs-v2 numbers |
| 3.8 | **Benchmark comparison + 2 more improvement attempts** (2026-09-28). Extended the data cache to 10y (the original "5y" test only *simulated* ~3.9y after SMA200 warmup) and directly compared the strategy to Nifty500 buy-and-hold: **the strategy loses to passive on every metric** (CAGR, MaxDD, Sharpe, Calmar). Tested a portfolio-level drawdown circuit breaker (found and fixed a self-locking bug — equity-recovery resume can never trigger once entries are frozen — then still rejected it, worse on every metric even fixed) and wider diversification (3/sector, 6 sectors — also rejected, worse). A risk-per-trade reduction test was invalidated by a hardcoded dict in `simulator.py` — flagged as not yet properly tested, not silently passed. | `backtest/simulator.py` (circuit breaker, config), `backtest/data_cache.py` (10y) | **Done** — see `STRATEGY.md` §8b; recommendation is now a core-satellite allocation, not full deployment, pending real paper-trading evidence |

## Task 4: VCP entry-timing (2026-09-28)

| Phase | Description | Key files | Status |
|---|---|---|---|
| 4.1 | **VCP pattern detection.** Built as a stated simplification of true VCP (fixed 3×15-day sub-window contraction/volume-decline check, not genuine swing-point detection — see module docstring). Hand-verified against real data: found 16 genuine tightening bases in the universe with sensible numbers, correctly rejected a random sample of 5 non-trending stocks. | `modules/vcp_pattern.py` (new) | **Done** — hand-verified |
| 4.2 | **Backtest as a third entry paradigm** (`vcp_breakout`), Trend-Template-gated, VCP breakout replacing RSI/MACD as the trigger, base-low as a tighter stop. Full 9-combination backtest (3 entry × 3 exit) on the 10y cache. | `backtest/simulator.py` (`EntryParadigm`, vectorized VCP columns), `backtest/run_backtest.py` | **Done** |
| 4.3 | **Result: rejected.** Win rate (38.0%) statistically indistinguishable from the existing paradigms (37.6%/38.8%) — the core hypothesis didn't hold in this implementation. CAGR/Sharpe clearly worse, mainly from a much smaller trade sample (~163-180 trades over 8.8y). Confirmed this wasn't just a strict-parameter artifact by re-testing with looser tolerances and the RS-Rating co-gate removed — conclusion held. **Not wired into the live pipeline** — `vcp_pattern.py` stays available for a future, more rigorous swing-point-based attempt, but `main.py`/`technical_analyzer.py` are unchanged. | — | **Done** — see `STRATEGY.md` §8c |

## Task 5: Chasing 20-25% CAGR — periodic-rebalance factor strategy (2026-09-28)

| Phase | Description | Key files | Status |
|---|---|---|---|
| 5.1 | **Research real reference points first.** NSE's Nifty200 Momentum 30: ~14% CAGR over a full 18y cycle after costs, -70.5% max DD. Nifty Midcap150 Momentum 50: 20.4% CAGR only over a favorable recent 7y window, -72.5% max DD. Dual Momentum: 12.3% CAGR / -33.7% DD (absolute-momentum cash filter). Tested dialing up risk on our own swing-trading system directly — CAGR barely moved (stayed 8-11%), confirming the ceiling there is the signal, not the risk controls. | — (research + empirical test on existing `backtest/simulator.py`) | **Done** |
| 5.2 | **Built a genuinely separate strategy**: periodic-rebalance (semiannual/quarterly), no-active-stop-loss factor portfolio replicating Nifty200 Momentum 30's own disclosed formula (6M+12M risk-adjusted, cross-sectionally z-scored, top-30, equal-weighted — market-cap weighting not available, no cap data cached). Optional absolute-momentum cash overlay reusing `modules/market_regime.py`. Hand-verified the formula against a manual calculation (exact match) and confirmed selection favors real higher-momentum stocks. | `backtest/factor_momentum.py`, `backtest/run_factor_backtest.py` (new) | **Done** — hand-verified |
| 5.3 | **Result: backtest numbers (34-35% CAGR, Sharpe 1.53) are implausibly good and confirmed NOT trustworthy** — checked by comparing the momentum-selected picks' actual 2018-2026 forward return (median +230%) against a random 30-stock sample from the same universe (median +241%, statistically indistinguishable). The universe itself (today's Nifty500, applied retroactively) is severely survivorship-biased for a returns-selecting strategy — this project's data cannot honestly backtest this strategy style. Real reference (NSE's own bias-free calculation) stands: ~14% CAGR / ~-70% DD is the honest ceiling for this strategy type; if that risk/return profile is wanted, the real NSE momentum index funds (UTI/Kotak/HDFC/Edelweiss/Tata) are the trustworthy way to get it, not a custom backtest. | — | **Done** — see `STRATEGY.md` §8d |

## Task 6: Alpha/beta diagnostic, governance filter, delivery% + FII/DII flow (2026-09-28)

Asked, as a 20-year Indian-markets analyst, what's missing for ≥5% alpha over Nifty500
on top of the existing sector-rotation design — six gaps were flagged, the user picked
three to build, in priority order.

| Phase | Description | Key files | Status |
|---|---|---|---|
| 6.1 | **Alpha/beta regression diagnostic.** Hand-rolled numpy OLS (no scipy/statsmodels — none in `requirements.txt`), sanity-checked against a benchmark-vs-itself identity regression (alpha≈0, beta≈1.0000, R²=1.0, floating-point exact). Run across all 9 entry×exit backtest variants on the existing cached data. **Finding**: every variant shows a positive alpha point estimate (10-38% annualized) but none are statistically significant (p>0.2 throughout) — R² is only 0.00-0.03, meaning the regression has almost no power because the portfolio-heat cap keeps the book frequently in cash. Doesn't overturn the §8b benchmark-comparison loss; closes an open question rather than adding a new negative finding. | `backtest/alpha_analysis.py`, `backtest/run_alpha_analysis.py` (new) | **Done** — see `STRATEGY.md` §11 |
| 6.2 | **Governance/promoter-pledge filter.** Reuses `fundamental_analyzer.py`'s exact Screener.in scraping pattern for the Shareholding Pattern table + auto-generated Cons text. A confirmed pledge disclosure hard-gates (company's own unambiguous fact); a declining promoter-holding trend is informational only (too many benign causes to hard-gate a thin qualifier pool). Spot-checked live: TCS (clean, passes), ZEEL (low holding but flat, correctly not gated), DBREALTY (correctly rejected — "Promoters have pledged 44.7% of their holding."). Wired into `main.py` as Step 3a, between screening and portfolio allocation. No historical Screener.in API found, so **cannot be backtested** — the spot-check above is the verification bar, matching how `trend_template.py`/`market_regime.py` were first verified. | `modules/governance_analyzer.py` (new), `main.py`, `modules/report_generator.py` | **Done** — see `STRATEGY.md` §12 |
| 6.3 | **Delivery% (backtestable) + FII/DII flow (live-only).** Delivery%: confirmed free via NSE's `sec_bhavdata_full` bhavcopy (same trusted archive subdomain), but **only available from 2019-09-30 onward** — earlier dates 404 regardless of trading day (confirmed via binary search), a real limitation on backtest depth. Backfilled the ~6-year available window (1,720/1,721 trading days) into a resumable parquet cache. Wired as an informational `ScreenResult.delivery_pct` field live (mirrors `volume_surge`'s treatment exactly — never gates). Tested as a hard entry-filter hypothesis (`require_delivery_above_median`, requiring delivery% above the stock's own trailing-20-day median) on the current best paradigm: **rejected** — win rate/avg-R improved marginally (38.5%→39.0%, 0.35→0.40) but trade count fell 29% (494→349), and CAGR/MaxDD/Sharpe all got worse (14.4%→11.3%, -32.8%→-40.3%, 0.55→0.51) — same rejection shape as VCP (§8c). Not wired as a live/default gate; stays informational only. FII/DII: confirmed live via `www.nseindia.com/api/fiidiiTradeReact` (a narrower, corrected finding vs. the existing "www.nseindia.com is blocked" note — a different, lighter endpoint), but confirmed **no historical query support** — permanently live-only, cannot be backtested. Built anyway, deliberately minimal, with an explicit "not backtested" caveat printed in the report itself next to the number. | `modules/delivery_analyzer.py`, `backtest/delivery_cache.py`, `backtest/run_delivery_cache.py`, `backtest/run_delivery_backtest.py`, `modules/fii_dii_flow.py` (all new); `backtest/simulator.py`, `modules/technical_analyzer.py`, `main.py`, `modules/report_generator.py` | **Done** — see `STRATEGY.md` §13-§14 |

## Task 7: Tighten the factor replica toward Midcap150 Momentum 50 — Path B (2026-09-30)

After seeing every trustworthy backtest number top out at 16.4% CAGR (Task 6.1's
diagnostic), the user stated explicitly: accepts a ~70% drawdown, wants CAGR ≥18%.
Two paths were offered — (A) chase point-in-time data to fix Task 5's backtest
properly, or (B) tighten the replica to mirror NSE's real Nifty Midcap150 Momentum 50
index (20.4% CAGR / -72.5% DD, a real bias-free track record) and borrow *its*
confidence instead of trying to validate our own number. **User picked Path B.**

| Phase | Description | Key files | Status |
|---|---|---|---|
| 7.1 | **Restrict Task 5's factor replica to the midcap-only pool, 50 holdings.** Added `FactorConfig.universe_scope="midcap_only"` (filters to `cap_band=="Mid"`, reusing `sector_mapper.build_universe()`'s existing tagging — no new data source) and `MIDCAP_NUM_HOLDINGS=50` (Midcap150 Momentum 50's real constituent count), additive to `backtest/factor_momentum.py`/`run_factor_backtest.py`. Confirmed the midcap filter tagged **exactly 150** tickers (matches NSE's real Midcap150 count precisely) and that the default (`full_nifty500`) CLI path reproduces Task 5's original +34.5%/+35.4% CAGR numbers exactly — a clean regression check. | `backtest/factor_momentum.py`, `backtest/run_factor_backtest.py` | **Done** |
| 7.2 | **Result, read carefully: >18% CAGR shown (18.3-26.9% across variants), but this is not validation.** Same survivorship-biased universe as Task 5, arguably worse for a midcap-only pool (smaller companies have a higher real-world delisting rate that "today's survivors" silently excludes). **New confirming evidence, not just a repeated caveat**: our backtest's Max DD (-33.0%) is roughly *half* the real index's own live -72.5% — the bias inflates both return AND understates risk, in the same direction, which is exactly what a survivor-only universe would do. The honest deliverable: a strategy whose mechanics now closely mirror a real index with a genuine 20.4%/-72.5% track record — the honest forward expectation is *that index's own number*, not this backtest's higher one. Path A (point-in-time data) remains the only route to a number from our own tooling worth trusting. | — | **Done** — see `STRATEGY.md` §15 |

## Task 8: Market filter off, built-in paper account, dashboard, schedule fix (2026-10-01)

The user asked to remove the 200-day market filter ("some sector always has a trade"),
to paper trade daily inside the project with a detailed dashboard, and reported that
no 8 AM email arrived.

| Phase | Description | Key files | Status |
|---|---|---|---|
| 8.1 | **Market filter → informational.** Measured first on the 8.8y backtest: Trend Template/trailing CAGR +7.3% → +13.9%, max DD -48.0% → -48.9%, Calmar 0.15 → 0.28 (index 0.29). RSI+MACD-only DD worsens sharply without it (-54.6% → -63.3%), showing the Trend Template is what protects the live config. Backtest default switched to match live. | `main.py`, `modules/market_regime.py`, `backtest/simulator.py` | **Done** — STRATEGY.md §16 |
| 8.2 | **Paper account.** Next-open fills, bar-by-bar exit replay, cash derived from the ledger, 0.15%/side costs. Verified on cached history (fills at T+1 open; RELIANCE and TCS hand-replayed bar by bar and matched; P&L matched to the rupee) and a 45-session daily replay (no duplicate holdings, ≤2 per sector, cash never negative, accounting reconciles exactly). | `modules/paper_trader.py` (new, replaces `paper_trade_log.py`), `modules/portfolio_allocator.py` | **Done** |
| 8.3 | **Dashboard** at the site root: KPIs, equity vs Nifty500, drawdown, per-trade P&L, sector exposure and ranking, tables for positions/orders/closed trades/candidates, CSV downloads. Checked in the browser at desktop and phone width; fixed category-axis labels, column alignment and a CSS class clash found there. | `modules/dashboard.py` (new), `modules/report_generator.py` (report → `latest.html`) | **Done** |
| 8.4 | **Schedule.** GitHub had started the 08:00 IST job 4-6h late every recorded day. Moved to 17:00 IST (after close) + 21:00 IST self-skipping backup; concurrency guard; commit step rebases before pushing. Email now leads with the paper portfolio. | `.github/workflows/schedule.yml`, `modules/email_alerter.py` | **Done** — watch the first evening run |

## Notes for future sessions

- Each phase should be sanity-checked against a few known historical examples before
  being wired into the live cron job (see `instruction.md`).
- Don't reorder phases without reason — Phase 1/2 (sector selection) must land before
  Phase 3 (momentum screening) can meaningfully narrow to "momentum stocks *in the
  winning sector*" rather than the whole universe.
- If a phase's data-source assumption from Phase 0 turns out to be wrong (e.g. a
  sectoral index ticker isn't available on yfinance), update `skills.md` and this table
  before proceeding, and flag the change to the user.

## Known issues found while building (Phases 1–7, 2026-09-27)

- A local end-to-end run surfaced a handful of `SECTOR_STOCKS` tickers that yfinance
  returned no data for: `MTAR`, `JUNIPERHOTEL`, `LTIM`, `TATAMOTORS`, `KALPATPOWR`.
  These are pre-existing curation entries (not introduced by this build); the pipeline
  already tolerates missing tickers gracefully (skips them, logs a warning), so this
  isn't blocking, but `sector_mapper.SECTOR_STOCKS` could use a refresh pass — several
  of these look like stale/renamed symbols rather than genuinely delisted companies.
- GitHub Pages must still be enabled manually (Settings → Pages → Deploy from branch
  → `main` / `/docs`) before the published report is reachable at a public URL — the
  workflow can commit to `docs/` but can't flip that repo setting itself.
- The new GitHub Actions commit-and-push step (`schedule.yml`) couldn't be dry-run
  locally — reviewed carefully, but watch the first real Actions run for issues.
