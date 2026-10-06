# CLAUDE.md — Sector-Rotation Swing-Trading Screener

## Project mission

A signal-generation system for **swing trading** (not intraday) a ₹5,00,000 NSE trading
account. Pipeline: identify which **sector** is likely to lead over the next quarter →
screen that sector for **momentum stocks** (technical) → confirm with **fundamentals**
(company results) → produce a report with buy signals and **position size** (% of the
₹5L account) per stock.

This repo currently implements an earlier, narrower version of that pipeline (sentiment
→ single sector → RSI/MACD screen → email). See `task.md` for the phased plan to evolve
it into the full pipeline above, `instruction.md` for how to work on this repo, and
`skills.md` for the tools/data-source inventory.

**Two-task roadmap:**
- **Task 1:** `CLAUDE.md` / `task.md` / `instruction.md` / `skills.md`. Done.
- **Task 2 (code built 2026-09-27, see `task.md`):** the actual pipeline — sector
  rotation, fundamental confirmation, momentum screening, position sizing, an HTML
  report generator, a static web app (GitHub Pages) to view reports, and a summary
  email linking to it. All code is written and passed a local end-to-end run; the one
  remaining step is enabling GitHub Pages itself in the repo's Settings (a manual,
  one-time toggle — see `task.md` Phase 7).

## Task 2 architecture decisions (locked in)

- **Web app hosting:** static **GitHub Pages** site. The GitHub Actions cron job
  generates the HTML report and commits it into a `docs/` folder (or `gh-pages`
  branch); Pages serves it for free with no server to run or maintain — this fits the
  existing serverless, cron-based architecture exactly. No Flask/FastAPI backend.
- **Report archive:** keep a **dated archive**, not just a "latest" overwrite — each run
  saves a new dated HTML file (e.g. `docs/reports/2026-09-27.html`) plus updates an
  `index.html`/`latest.html` pointer, so past signals stay reviewable.
- **Email content:** the daily email becomes a **short summary + link** to the hosted
  report (qualifying stocks and position sizes briefly, link for full rationale/
  formulas) — not the full HTML inline as today. Reuses the existing
  `modules/email_alerter.py` SMTP mechanism, just changes what it sends.

## Current architecture (as of this doc)

`main.py` orchestrates six steps, always emailing a result even on partial failure:

1. `modules/sector_rotation.py` (`rank_sectors`) — builds the sector universe from
   `sector_mapper.build_universe()` (full Nifty500, ~23 sectors as of 2026-09-27 — see
   "Universe & sector taxonomy" below), fetches 1y OHLCV for all ~500 tickers in **one**
   bulk call, ranks each sector's members by liquidity (avg daily traded value) and
   caps at `sector_mapper.MAX_STOCKS_PER_SECTOR` (40), then scores relative strength vs
   the Nifty500 benchmark (`^CRSLDX`) using synthetic sector baskets. `leading_sectors()`
   takes the top 4 (`TOP_N_SECTORS` in `sector_rotation.py`).
2. `modules/fundamental_analyzer.py` (`sector_confirmation`) — scrapes Screener.in
   public pages for each leading sector's basket, checks YoY Net Profit growth per
   stock, rolls up into a "N/M stocks grew profit YoY" summary.
3. `modules/technical_analyzer.py` (`screen_stocks`, extended) — pure-pandas Wilder's
   RSI(14) and standard MACD(12/26/9) as the required qualifying filter (RSI ≥
   `RSI_THRESHOLD`, or crossed above it in the last 2 sessions, **and** MACD line >
   signal line). `RSI_THRESHOLD` was lowered from 60 → **55** on 2026-09-27, at the
   user's request, to surface earlier-stage momentum — MACD confirmation is still
   required, so the two-indicator bar is only modestly loosened. Also adds
   informational signals (volume surge, 52-week-high proximity, relative strength vs
   the sector's synthetic index) that rank — not gate — qualifiers via `strength_score`.
   Also exposes `atr()` (Wilder's ATR-14), reused for position sizing.
4. `modules/position_sizer.py` (`size_position`) — fixed-fractional risk sizing off
   `TRADING_ACCOUNT_SIZE` (default ₹5,00,000): stop = entry − 2×ATR(14), quantity =
   risk_amount / stop_distance, capped at `MAX_POSITION_PCT` (default 20%) of the
   account. Every intermediate value is exposed for the report's formula display.
5. `modules/report_generator.py` (`build_report_html`, `publish_report`) — renders the
   sector-rotation table + per-sector qualifying-stock tables as a styled HTML page
   (dark theme shared with `email_alerter.PALETTE`), writes it to
   `docs/reports/YYYY-MM-DD.html` (permanent) and `docs/latest.html`, and regenerates
   `docs/reports/index.html` as a dated archive list. **`docs/index.html` is the paper
   trading dashboard** (`modules/dashboard.py`, Task 8), not the report.
6. `modules/email_alerter.py` (`build_summary_email`) — short summary (leading
   sector(s), qualifying stocks with quantity/% capital) + a link to the hosted report,
   sent via Gmail SMTP (`send_email`, unchanged).

`modules/stock_fetcher.py` (`fetch_stock_data`, line ~37) is the shared data-fetch used
by both sector rotation and per-stock screening — pulls OHLCV from `yfinance` in chunks
of 100 tickers, handling its MultiIndex column layout, requiring ≥60 rows of history.

Deployment: `.github/workflows/schedule.yml` runs every weekday after the NSE close —
18:17 IST primary, with backups at 21:47, 01:17 and 05:47 IST, because GitHub's
scheduler started jobs 5-8 hours late (2026-10). Whichever slot runs first processes
the latest completed session and writes it to `docs/data/last_session.txt`; the rest
see it and exit in seconds. Manual `workflow_dispatch` always runs (untick `force` to
get the same skip). The checkout is always the latest `main`, never the run's original
commit — "Re-run" on an old run used to check out a stale paper account and then fail
to push (2026-10-06). It runs `python main.py`, then commits `docs/**`,
`paper_trades.csv` and `paper_equity.csv` back to `main` even if email failed, so
GitHub Pages picks up the new report and the paper account persists. Reports are named
by market session date (`trader.as_of`), not the run's calendar date. Runs during
market hours are safe: `stock_fetcher.drop_unsettled_bar` drops today's live bar
before 16:00 IST, so signals and the paper account only ever see completed sessions.

**Retired:** `modules/twitter_extractor.py` (sentiment-based sector detection) and
`modules/stock_universe.py` (broad-market ~500-ticker fallback) were deleted — the new
pipeline always screens within the sectors it ranks, so there's no "unknown sector"
broad-market fallback case left to serve. `sector_mapper.py`'s original hand-picked
14-sector/157-ticker `SECTOR_STOCKS` dict was itself later replaced (2026-09-27, at the
user's request) — see "Universe & sector taxonomy" below.

## Universe & sector taxonomy (2026-09-27 — replaces the original hand-picked list)

The original `SECTOR_STOCKS` (14 hand-picked sectors, ~157 tickers total) was replaced
with `sector_mapper.build_universe()`, sourced entirely from NSE's own free, static
index-constituent CSVs at `archives.nseindia.com` (a different, unaffected subdomain
from `www.nseindia.com`'s blocked API — see below):

- **Universe = full Nifty500** (`ind_nifty500list.csv`, 501 stocks). NSE itself builds
  Nifty500 as Nifty100 + Midcap150 + Smallcap250 — confirmed those two lists add zero
  tickers beyond what's already in Nifty500, so fetching Nifty500 alone already covers
  large + mid + small cap. Each stock is tagged with its cap band (Large/Mid/Small,
  from which of those three lists it appears in) — surfaced in the report next to every
  qualifying stock, since a signal on a smallcap carries materially more liquidity/
  volatility risk than one on a largecap.
- **Sector taxonomy = NSE's own `Industry` column** (~20 categories in the Nifty500
  CSV, e.g. "Financial Services", "Healthcare", "Automobile and Auto Components") —
  authoritative and self-maintaining, unlike a hand-picked list. A category with no
  equivalent in the old 14 names (Capital Goods, Construction, Consumer Durables,
  Consumer Services, Textiles, Media Entertainment & Publication, Diversified,
  Services, Construction Materials) becomes its own sector rather than being silently
  dropped — the universe is now ~23 sectors, not 14.
- **Thematic overrides, applied before the generic Industry tag**, for cases where NSE
  publishes a more specific official index than the broad Industry category:
  **Nifty India Defence** (19 stocks — replaces the old Defense list; also fixes a bug,
  since the official ticker is `MTARTECH`, not the old list's `MTAR`, which is exactly
  why that symbol kept failing on yfinance), **Nifty PSE** (our "PSU" theme, 20 stocks),
  **Nifty Infra** (30 stocks, a better-curated Infrastructure basket than the old manual
  one). **Railways has no official NSE index** (checked several plausible CSV
  filenames — none exist) — kept as a small manually curated override list
  (`RAILWAYS_TICKERS` in `sector_mapper.py`), applied with the same priority as the
  official thematic lists, ahead of the generic Industry tag.
- **Liquidity filter**: a stock below `sector_rotation.MIN_AVG_DAILY_VALUE_INR`
  (₹2 crore/day average traded value, computed from the already-fetched OHLCV — no new
  data source) is excluded entirely — a "momentum signal" on an illiquid stock isn't
  realistically tradeable on a ₹5L account.
- **Per-sector cap**: after the liquidity filter, each sector keeps only its top
  `MAX_STOCKS_PER_SECTOR` (40) most-liquid members, to keep yfinance/Screener.in call
  volume predictable (some NSE industries, e.g. Financial Services, have 90+ Nifty500
  constituents). This is a deliberate tradeoff — a big sector's less-liquid tail is
  never screened — flagged here rather than silently applied.
- **⚠️ Survivorship bias warning for any future *returns-based backtest* on this
  universe (found 2026-09-28, see STRATEGY.md §8d):** `build_universe()` is *today's*
  Nifty500 applied retroactively — a stock delisted or dropped from the index between
  then and now is simply absent from the data. This is mostly harmless for the live
  pipeline and the swing-trading backtest (§9's documented point-in-time bias is
  minor there). It is **not** harmless for a strategy whose backtest *selects on
  historical returns* (e.g. a momentum factor portfolio) — confirmed empirically: a
  momentum-selected 30-stock portfolio's forward return was statistically identical
  to a *random* 30-stock sample from the same universe, because the whole universe is
  pre-filtered to "stocks that turned out fine." Don't trust a backtested CAGR/Sharpe
  from any new strategy built on this universe without checking selected-vs-random
  returns the same way, if the strategy selects on trailing performance.

## Data source decisions (locked in — don't re-litigate without a new user request)

- **Technicals:** stay on `yfinance` end-of-day data. This is swing trading, not
  intraday, so EOD OHLCV is sufficient — no need for a paid live-data feed.
- **Fundamentals:** **confirmed by the same spike** — Screener.in's public company
  pages (e.g. `screener.in/company/TCS/consolidated/`) expose the Quarterly Results
  table in plain HTML with **no login and no paid plan required**. Use free scraping of
  these public pages for v1 — Premium (~₹4,999/yr) is only worth revisiting later for
  bulk CSV export convenience or if scraping gets rate-limited, not a v1 blocker.
  Free NSE/BSE scraping libraries (e.g. `Bharat-sm-data`) remain a fallback if
  Screener.in's HTML structure changes.
- **Sector rotation:** replace the single-Twitter-handle sentiment hack with an
  objective, free technical measure — relative strength vs Nifty500 — as the primary
  signal, confirmed by fundamental earnings-growth trend as a secondary filter.
  **Implementation note (confirmed by a Phase 0 spike, 2026-09-27):** official NSE
  sectoral index tickers on `yfinance` are mostly unreliable — of ~13 tried
  (`^CNXAUTO`, `^CNXFMCG`, `^CNXMETAL`, `^CNXENERGY`, `^CNXREALTY`, `^CNXPSE`,
  `^CNXMEDIA`, `^CNXINFRA`, `^CNXFIN`, `^CNXCONSUM`, `^CNXPSUBANK`), only `^CNXIT`,
  `^CNXPHARMA`, and `^NSEBANK` returned live multi-day data — the rest returned a
  single stale row. **Pivot:** instead of official sector indices, compute a
  **synthetic sector index** per sector by aggregating (equal-weight) the OHLCV of each
  sector's member stocks (`sector_mapper.build_universe()` — see "Universe & sector
  taxonomy" above), using the already-proven `stock_fetcher.py` pipeline. This needs no
  new external data source at all for the rotation signal itself.
- **Not using Zerodha Kite Connect for v1:** it's a market-data/execution API (₹2000/mo)
  with **no fundamentals data**, and EOD `yfinance` already covers the technical need
  for swing trading. Revisit only if the user later wants live intraday data or
  automated order execution.
- **NSE's main site/API (`www.nseindia.com/api/...`) is not usable from this
  environment** — the spike got a 403 on the homepage and 503 (WAF) on the historical
  indices API. The existing `archives.nseindia.com` static-CSV pattern in
  `sector_mapper.py` (used for the Nifty500 constituent list) is a **different,
  unaffected subdomain** — keep using that, just don't add new dependencies on
  `www.nseindia.com`'s protected API.

## Coding conventions already established in this repo

- Pure `pandas`/`numpy` for indicators — **do not reintroduce `pandas_ta`** (removed in
  commit `f5d8270` for Python 3.11/Linux incompatibility).
- yfinance calls are chunked (100 tickers/batch) to avoid rate limits.
- Secrets via `.env` locally (`python-dotenv`, optional import) / GitHub Actions secrets
  in CI — see `.env.example`.
- Logging via the standard `logging` module to stdout (captured by Actions), not print.
- The pipeline always attempts to send an email/report, even on partial failure, with
  errors collected and surfaced rather than raising.

## Safety / scope boundary

This system produces **signals and reports only**. Never add real order-execution or
brokerage integration (e.g. Kite Connect order placement) without the user explicitly
asking for it in a future request. Any position-sizing output must show its formula and
assumptions alongside the final number, not just the number, so the user can sanity-check
it before trading.

## Task 3 (2026-09-27): formalized strategy, 5-year backtest, tuning, docs, paper trading

**`STRATEGY.md` is the canonical strategy reference** — full entry/exit/portfolio rules,
backtest results, and limitations. Don't duplicate that content here; this section is
only the architecture/module map for future coding sessions.

- **New required entry gate:** `modules/trend_template.py` — Minervini's 8-point Trend
  Template (SMA50/150/200 stack + slope, 52-week high/low distance) plus a universe-wide
  **Relative Strength Rating** (`compute_rs_ratings`, percentile vs the full ~500-stock
  universe — distinct from `sector_rotation.py`'s sector-vs-benchmark relative strength).
  Research note: adopted after reviewing `marco-hui-95/vcp_screener` (a published
  implementation of a two-time U.S. Investing Championship winner's rules). Wired into
  `technical_analyzer.screen_stocks()` via `require_trend_template` (default True) and
  `rs_ratings` params — pass `require_trend_template=False` to get the "RSI+MACD only"
  variant for comparison. **VCP (Volatility Contraction Pattern) entry-timing — built
  2026-09-28 (`modules/vcp_pattern.py`), backtested as a third entry paradigm
  (`backtest/simulator.py`'s `vcp_breakout`), and rejected**: win rate came out
  statistically identical to the existing paradigms (~38% either way) and CAGR/Sharpe
  were clearly worse, mainly from a much smaller trade sample — confirmed this wasn't
  just strict parameters by re-testing looser tolerances (see STRATEGY.md §8c for the
  full comparison). **Not wired into `main.py`/`technical_analyzer.py`** — the module
  stays available for a future, more rigorous swing-point-based attempt (this
  implementation is a stated simplification: fixed sub-window contractions, not
  genuine peak/trough detection), but nothing live changed.
- **Market regime check:** `modules/market_regime.py` — the Nifty500 benchmark above its
  own 200-day SMA **and** that SMA rising ≥1.0% over 21 sessions. **Informational only
  since 2026-10-01 (Task 8)** — it used to block all new entries; see STRATEGY.md §16 for
  why it was turned off. `sector_rotation.rank_sectors()` now returns a `RotationResult` (scores +
  universe-wide RS ratings + the raw benchmark series + `breadth_pct`), not a bare list —
  needed so `main.py` can pass the benchmark/breadth to this check. **v2, 2026-09-28**:
  the RoC condition was added after the original price-vs-SMA-only check missed a real
  Nov2022-Apr2023 chop in the backtest; re-measured on the full 5-year backtest, it
  reduces max drawdown 4-6pp across every variant but also reduces CAGR meaningfully —
  a real tradeoff, not a pure win (see STRATEGY.md §3/§8 for both v1 and v2 numbers).
  Market breadth (`RotationResult.breadth_pct`) was tested as an alternative gate on the
  same episode, found noisier, kept informational-only rather than a second hard gate.
- **New portfolio-construction layer:** `modules/portfolio_allocator.py`, sitting between
  `screen_stocks()` and `position_sizer.size_position()`. Fixes two gaps the original
  per-stock sizing had: (1) max 2 positions per sector (so 4 leading sectors don't
  collapse into 6-7 correlated picks from one sector dressed up as "diversified"), and
  (2) sequential budget-aware sizing against **remaining** deployable capital (capped at
  85% total) plus an 8%-of-capital total portfolio-heat ceiling — independent guardrails
  a per-position-only cap can't provide. Cap-band risk scaling (Large/Mid/Small =
  1.0/0.75/0.5%) also lives here. Used by both `main.py` and `backtest/simulator.py`.
- **`position_sizer.py` reinforced**: stop distance is now `min(2×ATR, 8% of entry)`
  (Minervini-informed — his own discipline is tight, 5-10% max loss, tighter than 2×ATR
  alone can produce on a volatile smallcap).
- **`backtest/` (new package)** — a custom pandas-based simulator, not a new library
  (`vectorbt`/`backtesting.py` don't fit this "rank sectors → screen leaders →
  portfolio-constrained sizing" workflow cleanly, and reusing this repo's own
  indicator/sizing functions guarantees the backtest measures the same logic that runs
  live). `data_cache.py` fetches+caches 5y OHLCV locally (parquet, git-ignored) so tuning
  iterations don't re-hit yfinance; `simulator.py` runs the day-by-day trade state
  machine (no look-ahead: signal from day T's close → entered/exited at T+1's open;
  resting stop/target levels checked against T's own intraday high/low); `metrics.py`
  computes CAGR/drawdown/Sharpe/win-rate/avg-R; `run_backtest.py` is the CLI, running all
  6 entry×exit paradigm combinations and picking the best by CAGR/max-drawdown; `report.py`
  renders an HTML report to `docs/backtest_report.html`. **Methodology simplification**
  (stated in STRATEGY.md, not hidden): the backtest applies today's Nifty500/sector
  classification retroactively across all 5 years (no point-in-time reconstruction — NSE
  doesn't expose free historical index-constituent archives), and builds sector synthetic
  indices from all sufficiently-historied members rather than the live pipeline's
  liquidity-capped 40 (a stability simplification; liquidity/cap filtering still gates
  which stocks can become actual positions).
- **Paper trading** — originally `modules/paper_trade_log.py`; replaced in Task 8 by
  `modules/paper_trader.py` (see below). `paper_trades.csv` and `paper_equity.csv` are
  **committed to the repo, not git-ignored**: they are this project's only persistent
  state, and every GitHub Actions run starts from a fresh checkout.

## Task 6 (2026-09-28): alpha/beta diagnostic, governance filter, delivery% + FII/DII flow

**`STRATEGY.md` §11-§14 are the canonical reference** — full methodology, spot-check/
backtest evidence, and stated limitations. This section is only the module map.

- **`backtest/alpha_analysis.py`** — CAPM-style OLS (`compute_alpha_beta`, pure numpy,
  no new dependency) decomposing a backtest equity curve's daily returns into alpha vs
  beta against the Nifty500 benchmark. `backtest/run_alpha_analysis.py` runs it across
  all 9 entry×exit variants on the existing cached data (no new fetch). **Finding**: all
  9 variants show a positive alpha point estimate but none are statistically significant
  (p>0.2 throughout) — R² is only 0.00–0.03, meaning the benchmark's daily moves barely
  explain this strategy's return variance at all (expected: the book is frequently
  partly/fully in cash under the portfolio-heat cap). Diagnostic, not a fix — see
  STRATEGY.md §11 for the full table and honest reading.
- **New governance gate:** `modules/governance_analyzer.py` — scrapes the same public
  Screener.in pages `fundamental_analyzer.py` already uses (Shareholding Pattern table +
  auto-generated Cons text) for promoter-holding trend and a confirmed pledge
  disclosure. A confirmed pledge is a **hard gate** (company's own unambiguous fact);
  a declining promoter holding % is informational only (too many benign causes to
  hard-gate against an already-thin qualifier pool). Wired into `main.py` as **Step
  3a**, between `screen_stocks()` and `portfolio_allocator.allocate()`, checking only
  each sector's already-qualified stocks. Spot-checked live against TCS (clean),
  ZEEL (low holding, no pledge), DBREALTY (correctly flagged — confirmed 44.7%
  pledge). **No historical Screener.in API was found, so this cannot be backtested**
  — verification is the spot-check only, the same bar `trend_template.py`/
  `market_regime.py` were held to when first built.
- **New informational signal:** `modules/delivery_analyzer.py` — NSE delivery%
  (`sec_bhavdata_full_DDMMYYYY.csv`, same `archives.nseindia.com` subdomain already
  used for the Nifty500 constituent lists). **Only available from 2019-09-30
  onward** — everything before that 404s regardless of trading day (confirmed via
  direct binary search) — a real, stated limitation on how far back this can be
  backtested, not a bug. `ScreenResult.delivery_pct` is informational only (mirrors
  `volume_surge`'s exact treatment — contributes to `strength_score`, never gates
  `passes`) via a new `screen_stocks(delivery_pct=...)` param, sourced in `main.py`
  from `fetch_latest_delivery_pct()`. `backtest/delivery_cache.py` backfilled the
  ~6-year available window (1,720/1,721 trading days) into `backtest/cache/
  delivery_5y.parquet` (long-format parquet, resumable; one bhavcopy request = all
  NSE stocks for that day, not one request per ticker). `backtest/simulator.py`'s
  `run_backtest()` takes an additive optional `delivery_data` param and
  `BacktestConfig.require_delivery_above_median` tested a delivery%-above-own-median
  entry filter on the current best paradigm — **rejected**: win rate/avg-R improved
  marginally but trade count fell 29%, and CAGR/MaxDD/Sharpe all got worse (same
  rejection shape as VCP, §8c). Stays informational-only, not wired as a gate — see
  STRATEGY.md §13 for the full comparison table.
- **New live-only signal:** `modules/fii_dii_flow.py` — NSE's `fiidiiTradeReact`
  JSON endpoint, confirmed reachable with a plain `requests` call (a narrower,
  corrected finding vs. this doc's existing note that `www.nseindia.com`'s API is
  blocked — that finding was about different, heavier endpoints on the same domain).
  **No historical query support was found (`?date=` is silently ignored) — this can
  never be backtested with this project's tooling**, unlike delivery% above. Kept
  deliberately minimal: informational only, surfaced in the report's new "Market
  Context" block with an explicit "not backtested" caveat printed next to the number
  itself, not just in this doc — see STRATEGY.md §14 for the argued for/against.

## Task 7 (2026-09-30): factor replica tightened toward Midcap150 Momentum 50

User stated an explicit target after Task 6.1's diagnostic (accepts ~70% drawdown,
wants CAGR ≥18%). `backtest/factor_momentum.py`'s `FactorConfig.universe_scope=
"midcap_only"` (+ `MIDCAP_NUM_HOLDINGS=50`) restricts Task 5's replica to the
`cap_band=="Mid"` pool, matching Nifty Midcap150 Momentum 50's real methodology more
closely — additive, `full_nifty500` (default) reproduces Task 5's numbers unchanged.
Result shows >18% CAGR (18.3-26.9%), but **this is not validation** — same
survivorship-biased universe as Task 5, and the backtest's Max DD (-33%) being
roughly half the real index's own -72.5% is further evidence of the same bias, not a
new finding. See STRATEGY.md §15 for the full "don't trust this number, trust the
real index" framing — don't let a future session read this CAGR at face value.

## Task 8 (2026-10-01): market filter off, built-in paper account, dashboard

**STRATEGY.md §16 is the canonical reference.** Module map only:

- **Market filter off.** `main.py` no longer gates allocation on
  `market_regime.evaluate()`; it is logged and shown on the dashboard. Measured
  first: for the Trend Template configuration, CAGR +7.3% → +13.9% with max DD
  -48.0% → -48.9% over 8.8 years. `BacktestConfig.apply_market_regime_filter`
  defaults to `False` to keep backtest and live identical.
- **`modules/paper_trader.py`** (`PaperTrader`) — the paper account. Per run:
  fill pending orders at the next session's open (cancel if the open is at or
  below the stop, cut quantity to cash), replay exits bar by bar from entry
  (initial/trailing stop intraday, gap below stop at the open, RSI<45 and the
  26-session backstop at the next open), mark to market, book new orders, write a
  snapshot. State: `paper_trades.csv` (ledger, statuses PENDING/OPEN/CLOSED/
  CANCELLED) and `paper_equity.csv` (one row per market date). Cash is derived
  from the ledger, never stored. Costs: 0.15% per side. The market date is the most
  common last bar across held/screened stocks; each order's `signal_date` is that
  stock's own last bar, so a fill can never use a price the signal already saw.
- **`portfolio_allocator.allocate()`** gained `held_tickers` / `held_per_sector`:
  already-held stocks are skipped and use up their sector's 2 slots. Rejections are
  recorded with reasons (shown on the dashboard).
- **`modules/dashboard.py`** — builds `docs/index.html` (Chart.js from jsdelivr,
  data embedded as JSON, light/dark from the data-viz reference palette, every chart
  with a table twin) and copies both CSVs to `docs/data/`.
- **Email** (`email_alerter.build_summary_email`) now takes the `PaperState`:
  portfolio value and return vs Nifty500, today's buys/sells, orders for the next
  open, links to the dashboard and report.
