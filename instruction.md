# instruction.md — How to work on this project

Operating instructions for implementation sessions on this repo (Task 2 onward). Read
alongside `CLAUDE.md` (architecture/decisions) and `task.md` (the backlog).

## Before starting work

- Re-check `task.md`'s Status column. Don't re-plan a phase that's already Done; don't
  jump ahead of a phase that's still Not started unless there's a specific reason
  (explain it before proceeding).
- Update `task.md`'s Status column after finishing a phase (Not started → In progress →
  Done), so it stays a living backlog rather than going stale.

## Reuse-first rule

Before adding any new module, check whether an existing one already covers the need:

- Data fetching → `modules/stock_fetcher.py` (yfinance, chunked)
- Universe/sector classification → `modules/sector_mapper.py` (NSE CSVs, cap bands)
- Indicator math (RSI/MACD/ATR) → `modules/technical_analyzer.py`
- Structural trend confirmation / RS Rating → `modules/trend_template.py`
- Broad-market regime → `modules/market_regime.py`
- Portfolio-level allocation (sector caps, budget, heat) → `modules/portfolio_allocator.py`
- Email delivery → `modules/email_alerter.py`
- Backtesting → `backtest/` (reuses the `modules/` functions above — never
  reimplement indicator/sizing math inside `backtest/`; see CLAUDE.md)

Extend these in place where the fit is close; only create a new `modules/*.py` file
when the responsibility is genuinely new (as `task.md` Phases 1, 2, 4, 5, and Task 3 do).

## Data source hierarchy

Try sources in this order before reaching for anything new; if you need to go further
down the list than this, or want to add a source not listed in `skills.md`, flag it to
the user before wiring it in:

1. Free official data — NSE/BSE/niftyindices.com direct.
2. `yfinance` (already in use, free, EOD).
3. Screener.in export (paid, ~₹417/mo, cheap) — for fundamentals only.
4. Anything else (a new paid API, a new scraper target) — ask the user first.

Never add Zerodha Kite Connect or any brokerage/order-execution integration without the
user explicitly asking for it in a future request — this project stays signal/report-
only otherwise (see `CLAUDE.md` safety boundary).

## Testing expectation

This is a financial signal system — a silent logic bug produces a wrong trade signal,
not just a wrong test. Before wiring any new screening/sizing logic into the live cron
job:

- Run it against a few known historical examples (a stock/sector you can reason about
  by hand) and confirm the output matches expectation.
- For position sizing specifically, show the formula and intermediate values (account
  size, risk %, stop distance, resulting quantity and ₹ allocation) — never just the
  final percentage — so the numbers are auditable.
- Prefer a small script/notebook-style manual check over a full test suite for now,
  given the project's size; add `pytest` only if the logic complexity later justifies it.

## Report content expectations (once Phase 5 lands)

Every report must state, per qualifying stock: sector and why it was selected (rotation
rank + fundamental confirmation), the technical signal(s) that qualified it, entry
price/level, stop-loss level, position size in ₹ and as % of the ₹5,00,000 account, and
the sizing formula used. Don't collapse this into a bare buy/sell list.

## Strategy-rule changes (Task 3 onward)

Entry/exit/portfolio-construction rules now have a single source of truth:
**`STRATEGY.md`**. Before changing `RSI_THRESHOLD`, any exit-rule constant, or any
`portfolio_allocator.py`/`position_sizer.py` cap, re-run the relevant `backtest/`
variant(s) and update `STRATEGY.md`'s §8 comparison table — don't change a live
parameter without a backtest number to back it up. `trend_template.py`,
`market_regime.py`, and `portfolio_allocator.py` all ship with hand-verification
examples in this project's history (real tickers/dates, synthetic stress-test
scenarios) — follow that pattern for any new rule, not just a unit test in the abstract.

## Phase 7 architecture (locked in — don't re-litigate)

The web app is a **static GitHub Pages site**, not a running server: the cron job
commits generated HTML into `docs/`, Pages serves it for free. Don't introduce a
Flask/FastAPI backend or paid hosting for this. Keep a dated report archive (don't
overwrite a single `report.html`). The email becomes a short summary with a link to the
hosted report, not the full HTML inline.
