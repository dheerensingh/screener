# Sector Rotation + Momentum Swing Strategy

A systematic, rules-based swing-trading strategy for a ₹5,00,000 NSE trading
account: rank sectors by relative strength, screen the leading sectors for
momentum stocks, size positions by risk, and manage a diversified portfolio —
all reused, unmodified, between the live daily pipeline (`main.py`) and the
5-year backtest (`backtest/`), so what's tested is exactly what runs live.

## 1. Universe & Sector Classification

- **Universe:** the full Nifty500 (501 stocks) — NSE itself constructs Nifty500
  as Nifty100 + Midcap150 + Smallcap250, confirmed to add zero tickers beyond
  Nifty500 itself. Every stock is tagged with its cap band (Large/Mid/Small)
  from which of those three NSE lists it belongs to.
- **Sector taxonomy:** NSE's own `Industry` classification (~20 categories,
  free from `archives.nseindia.com`'s Nifty500 CSV), overridden by three
  official NSE thematic indices where they're more specific than the broad
  industry tag: **Nifty India Defence**, **Nifty PSE** (our "PSU" theme), and
  **Nifty Infra**. **Railways** has no official NSE index (checked and
  confirmed absent) and is kept as a small, manually curated 9-ticker list.
  Any NSE industry with no equivalent in the original hand-picked list
  (Capital Goods, Construction, Consumer Durables, Consumer Services,
  Textiles, Media, Diversified, Services) becomes its own sector — nothing
  is silently dropped. ~23 sectors total.
- **Liquidity filter:** a stock must average **≥ ₹2 crore/day** in traded
  value (Close × Volume, trailing 20 sessions) to be eligible at all — a
  "signal" on an illiquid stock isn't realistically tradeable at any size.
- **Per-sector cap:** each sector's screening pool is capped at its **40**
  most-liquid members (by the same traded-value measure), keeping per-run
  data-fetch and Screener.in scraping volume predictable.

## 2. Sector Rotation

For each sector, a **synthetic index** is built by equal-weight-averaging the
normalized close prices of its member stocks (no official NSE sector-index
tickers survive on yfinance — see §7 Data Sources). Relative strength vs the
Nifty500 benchmark (`^CRSLDX`) is computed over 1-month / 3-month / 6-month
lookbacks, weighted **50 / 30 / 20** (favouring recent momentum, appropriate
for swing trading). The **top 4 sectors** by this composite score become the
day's "leading sectors" — only these are screened for momentum stocks.

## 3. Market Regime Filter

> **Since 2026-10-01 this filter is informational only — it no longer gates entries.** See §16 for the backtest evidence.

New entries are paused entirely — system-wide, regardless of sector or stock
signal — unless **both**:
- the Nifty500 benchmark's close is above its own 200-day SMA, **and**
- that 200-day SMA is itself rising at least **1.0%** over the trailing 21
  sessions.

Existing open positions are unaffected — they're still managed by their own
exit rules regardless of the regime.

**v2 refinement (2026-09-27), added after investigating §8's original
drawdown finding:** the first backtest showed a plain price-vs-200-SMA check
missed a real 5-month chop (Nov 2022-Apr 2023) that drove the worst
drawdown — price stayed above a barely-rising 200-SMA the whole time.
Investigated with the cached 5-year data before changing anything: during
that chop, the SMA's own 21-day rate of change averaged just 0.5% (max
1.2%); during a genuinely healthy trending period (Nov 2023-Feb 2024) it
never dropped below 1.0% — a clean separation, so a minimum SMA rate-of-rise
became a second required condition. **Market breadth** (% of the universe
above their own 50-day SMA) was tested on the same chop window too — a real
signal, but noisier (swung from ~24% to ~81% within the same episode) — kept
as an informational/logged value only (`RotationResult.breadth_pct`), not a
hard gate, rather than risk overfitting a filter to one historical episode
on a signal that didn't cleanly separate the two windows.

**The honest tradeoff, re-measured on the full 5-year backtest (§8):** max
drawdown improved 4-6 percentage points across every variant, and avg-R/win-
rate improved too (fewer, better-quality trades) — but CAGR dropped
meaningfully everywhere, since the stricter gate also blocks some periods
that would have traded profitably. This is a real risk/return tradeoff, not
a pure win — kept as the new default because, for capital other people are
trusting you with, the lower-drawdown profile is judged worth the lower raw
return (see §6's same reasoning re: diversification over Minervini's own
concentrated style).

## 4. Entry Rules

A stock in a leading sector must pass **all** of the following the day before
entry (entry executes at the next trading day's open — no look-ahead):

**Timing trigger:**
- RSI(14) ≥ 55, either sustained or just crossed up within the last 2 sessions
- MACD(12,26,9) line > signal line (bullish histogram)

**Structural trend confirmation — Minervini's Trend Template** (adopted after
reviewing `marco-hui-95/vcp_screener`, a published implementation of a
two-time U.S. Investing Championship winner's rules — see CLAUDE.md for the
full research note):
- Price above both the 150-day and 200-day SMA
- 50-day SMA above both the 150-day and 200-day SMA
- 200-day SMA itself rising over the last month
- Price at least 30% above its 52-week low
- Price within 25% of its 52-week high
- **Relative Strength Rating ≥ 70** — percentile rank of a blended 3M/6M/~11M
  return **against the entire ~500-stock universe** (not just the stock's own
  sector) — a distinct, universe-wide signal from the sector-level relative
  strength in §2.

RSI/MACD are the short-term timing trigger; the Trend Template confirms the
longer-term structural trend is real before that trigger is trusted — the
two are complementary timeframes, not redundant. **Deliberately deferred:**
Minervini's VCP (Volatility Contraction Pattern) precise entry-timing
technique — genuinely valuable, but real pattern-recognition engineering
(detecting swing highs/lows, measuring contraction ranges) is a materially
bigger lift than the Trend Template's plain moving-average math; revisit as
a v2 entry-timing refinement once this baseline is validated.

## 5. Exit Rules

Checked daily, in priority order, on every open position:

1. **Initial hard stop:** entry − 2×ATR(14), capped at a maximum 8% of entry
   price (whichever is tighter) — a smallcap's ATR-based stop can be far
   wider than a large/midcap's; this cap prevents an oversized-risk trade
   just because a stock is unusually volatile (Minervini's own discipline is
   famously tight — typically 5-10% max loss).
2. **Trailing "chandelier" stop:** ratchets up daily to
   `max(current stop, highest close since entry − 2.5×ATR(14))` — never
   loosens. This is deliberately **not** a fixed profit target: this is a
   momentum system, and momentum's edge comes from a fat right tail of
   trades that run far further than any fixed R-multiple would capture.
   Capping winners at a fixed target throws away the reason the strategy has
   an edge. "Cut losses short, let winners run."
3. **Momentum-failure exit:** RSI closing back below 45 — a real reversal
   signal — exits regardless of where price/stop currently sit.
4. **Time backstop:** 26 trading days (tied to MACD's own slow-EMA period —
   the system's signals are built on that timescale) — a backstop against
   dead capital, not the primary exit mechanism.

**Two alternative exit paradigms were also built and backtested for
comparison** (§8): a simpler fixed-target design (stop / 2R target / 15-day
time exit) and a partial scale-out (sell half at 1.5R, trail the remainder).

**Considered and rejected:** exiting a position when its sector drops out of
the leading-sector list. Sector rankings can flip week to week on noise;
exiting on that basis adds turnover and whipsaw without a real edge — a stock
already vetted as individually strong should be judged on its own technicals
once it's in a position.

## 6. Position Sizing & Portfolio Construction

**Per-position sizing** (fixed-fractional risk): `risk_amount = account_size ×
risk_pct`; `quantity = floor(risk_amount / stop_distance)`; capped so no
single position exceeds 20% of the account. `risk_pct` is **scaled by cap
band** — Large 1.0%, Mid 0.75%, Small 0.5% — since ATR alone doesn't fully
price in a smallcap's extra gap/liquidity risk. Since 2026-10-07 these are
multiplied by 0.75 (0.75% / 0.56% / 0.375%) so more of the 15 slots fit
under the 85% capital cap (§17).

**Portfolio-level rules** (not just per-position sizing in isolation):

- **Up to 15 stocks, sector shares weighted by relative strength** (since
  2026-10-07, §17; before that, a flat max of 2 per sector, ≤8 in total).
  Each of the 4 leading sectors gets 2 slots; the other 7 are split by each
  sector's relative-strength score, so the strongest sector can hold more.
  Without per-sector limits the top-scoring qualifiers concentrate in
  whichever single sector is strongest that day — concentration dressed up as
  diversification.
- **Adding to winners** (§17). A held stock that qualifies again is bought
  again only while winning (above its last buy price), 5+ sessions after the
  last buy, at 50% then 25% of its normal risk, at most twice, within the 20%
  single-stock cap.
- **Sequential, budget-aware capital allocation.** Candidates are walked in
  `strength_score` order (interleaved across sectors) and sized against
  *remaining* deployable capital, capped at **85% total** — not fresh total
  capital each time, so the book can never be over-committed even if every
  candidate independently qualifies for the per-position cap. The 15% left
  undeployed absorbs share-quantity rounding slack, keeps dry powder for a
  fresh signal, and gives a fully-invested momentum book some shock absorber.
- **Total portfolio "heat" ceiling: 8% of capital.** Per-trade risk is
  1%-equivalent (scaled by cap band), but several simultaneous positions
  could all get stopped out on the same day by a correlated, market-wide
  gap-down — a risk a per-trade-only limit doesn't address. A new signal
  that would push total open risk over 8% is skipped even if a sector/count
  slot is free.
- **Explicitly out of scope for v1:** full pairwise return-correlation
  checks between candidate positions. The per-sector cap captures most of
  the same risk far more cheaply.

**One deliberate deviation from Minervini's own practice, stated
explicitly:** Minervini trades concentrated — a handful of high-conviction
positions. Given this project's own stated intent (a "smallcase" for the
user, then family and close people), this strategy stays on the conservative
side of that tradeoff — diversification discipline matters more when
managing capital other people are trusting you with than when risking only
your own.

## 7. Data Sources

| Need | Source | Cost |
|---|---|---|
| Universe, cap bands, sector classification | `archives.nseindia.com` static CSVs (Nifty500, Nifty100, Midcap150, Smallcap250, Nifty India Defence, Nifty PSE, Nifty Infra) | Free |
| Technical price/volume data | `yfinance` (EOD only — sufficient for swing trading) | Free |
| Fundamental confirmation (YoY profit growth) | Screener.in public company pages (no login/paid plan needed) | Free |
| Benchmark | `^CRSLDX` (Nifty 500) via yfinance | Free |

**Not used, with reasoning:** official NSE sector-index tickers on yfinance
(mostly dead — only `^CNXIT`/`^CNXPHARMA`/`^NSEBANK` returned live data of 13
tried); Zerodha Kite Connect (₹2000/mo, no fundamentals data, unnecessary for
EOD swing trading); `www.nseindia.com`'s protected API (403/503 from this
environment — `archives.nseindia.com` is a different, working subdomain).

## 8. Backtest Results (5 years, full Nifty500 universe, ₹5,00,000 starting capital)

### v1 — original regime filter (price vs 200-SMA only)

| Entry | Exit | Trades | Win % | Avg R | CAGR | Max DD | Sharpe | Avg hold |
|---|---|---|---|---|---|---|---|---|
| Trend Template | Trailing | 488 | 36.1% | +0.12 | **11.6%** | -47.7% | 0.59 | 18.6d |
| Trend Template | Fixed target | 678 | 45.1% | +0.10 | 11.1% | -48.2% | **0.67** | 12.6d |
| Trend Template | Scale-out | 509 | 36.5% | +0.19 | 11.9% | **-45.8%** | 0.60 | 18.7d |
| RSI+MACD only | Trailing | 569 | 36.2% | +0.18 | **16.4%** | -46.0% | 0.62 | 18.7d |
| RSI+MACD only | Fixed target | 791 | 44.0% | +0.09 | 9.4% | -49.8% | 0.66 | 9.7d |
| RSI+MACD only | Scale-out | 587 | 35.3% | +0.18 | 10.5% | -47.0% | 0.60 | 18.8d |

**Investigated the drawdown rather than accepting it at face value:** the
worst single-trade loss across the whole run was **-1.63R** — not a
catastrophic gap-risk blowup, the per-trade stop discipline held. The
drawdown instead came from a **sustained chop, Nov 2022 - Apr 2023**, where
the regime filter correctly saw a long-term uptrend (price above a *rising*
200-day SMA throughout) while price still whipsawed sideways for ~5 months,
repeatedly stopping out entries at small losses in sequence — a real, disclosed
characteristic of the strategy, not a bug (see §3 for the fix this led to).

### v2 — RoC-enhanced regime filter (§3), same 6 variants, same data

| Entry | Exit | Trades | Win % | Avg R | CAGR | Max DD | Sharpe | Avg hold |
|---|---|---|---|---|---|---|---|---|
| Trend Template | Trailing | 299 | 35.8% | +0.20 | 9.1% | -43.6% | 0.52 | 17.4d |
| Trend Template | Fixed target | 423 | 45.2% | +0.11 | 6.9% | -44.2% | 0.55 | 11.4d |
| Trend Template | Scale-out | 306 | 38.2% | +0.29 | 9.0% | **-42.3%** | 0.53 | 18.2d |
| RSI+MACD only | Trailing | 312 | 40.1% | +0.26 | **11.1%** | -42.3% | 0.53 | 18.4d |
| RSI+MACD only | Fixed target | 453 | 45.5% | +0.13 | 7.8% | -44.7% | 0.52 | 11.6d |
| RSI+MACD only | Scale-out | 330 | 39.7% | +0.29 | 9.3% | -42.5% | **0.53** | 18.5d |

**The honest tradeoff — not a pure win:** max drawdown improved 4-6 points
across every variant (e.g. RSI+MACD/Trailing: -46.0% → -42.3%), and avg-R/win-
rate improved too (e.g. same row: +0.18 → +0.26, 36.2% → 40.1%) — fewer,
better trades. But **CAGR dropped meaningfully everywhere** (same row:
16.4% → 11.1%) since the stricter gate also blocks periods that would have
traded profitably, and Sharpe landed flat-to-slightly-lower across the board.
This is a real risk/return tradeoff. Kept as the new default anyway — for
capital other people are trusting you with, a materially lower drawdown is
judged worth the lower raw return, consistent with the diversification
reasoning already applied in §6.

**Recommendation (v2 data): Trend Template entry + trailing-chandelier
exit.** Best Sharpe among the Trend Template rows here goes to fixed-target
(0.55), but that comes with a much shorter average hold (11.4d vs 17.4d) and
more trades — more transaction-cost drag and more manual paper-trading effort
per month for a marginal Sharpe gain. RSI+MACD-only still edges out on raw
CAGR (11.1% vs 9.1%), but Trend Template's individual-trade discipline is
worth more than 2 points of CAGR here, especially since this one 5-year
window is not the last word — worth revisiting once the paper-trading period
(§10) adds real forward data.

**Market breadth** (§3) was tested as an alternative/additional gate on the
same chop episode and found noisier (swung ~24%-81% within the episode) —
kept informational-only rather than built into a second hard gate, to avoid
tuning two filters to one historical episode.

## 8b. Benchmark comparison — the honest headline number (2026-09-28)

Every number in §8 is meaningless without comparing it to simply buying and
holding the index — a zero-effort alternative. Re-ran everything on a
genuinely robust **8.8-year** window (2016-2026; the original "5-year" test
actually only *simulated* ~3.9 years, since the first 252 days of any fetch
are consumed warming up the 200-day SMA — a data-window correction made here,
not a strategy change) and compared directly:

| | CAGR | Max DD | Sharpe | Calmar |
|---|---|---|---|---|
| **Nifty500 buy & hold** | **+11.3%** | **-38.3%** | **0.74** | **0.29** |
| Trend Template + Trailing (this strategy) | +9.4% | -46.8% | 0.47 | 0.20 |
| RSI+MACD + Trailing | +8.1% | -51.5% | 0.47 | 0.16 |

**This strategy currently loses to passive index buy-and-hold on every single
risk-adjusted metric** — lower CAGR, materially worse drawdown, worse Sharpe,
worse Calmar. A Calmar ratio of ~0.2 (annual return is only ~20% of peak
drawdown) is weak by professional standards regardless of the benchmark —
institutional systematic strategies target Calmar ≥1.0; this is roughly 5x
short of that bar. This is the single most important number in this document
and should not be softened: as currently configured, the active complexity
(daily screening, manual paper-trading effort, concentrated single-stock
risk) is not yet earning its keep versus a Nifty500 index fund.

**Two further improvement attempts, tested and rejected — reported honestly
rather than omitted:**

- **Portfolio-level drawdown circuit breaker** (halt new entries once
  strategy equity falls X% below its own peak): first attempt used an
  equity-recovery resume condition and found a genuine self-locking bug —
  halting entries freezes equity at flat cash once open positions finish
  closing, so equity can never recover on its own to satisfy a "resume once
  recovered" condition. Fixed with a time-based cooldown instead. **Still
  made every metric worse after the fix** (e.g. CAGR +9.4%→+1.9%, Calmar
  0.20→0.04, across several halt/cooldown combinations) — halting right after
  a drawdown also means missing the sharp recovery trades that typically
  follow one in a trend-following system. Rejected; not adopted.
- **Wider diversification** (3 positions/sector, or 6 leading sectors instead
  of 4): tested, made CAGR and drawdown both slightly *worse* (e.g. CAGR
  +9.4%→+7.4% at 6 sectors) — more positions drawn from the same handful of
  currently-leading, correlated sectors isn't real diversification, it's more
  exposure to the same regime. Rejected; not adopted.

**Not yet properly tested** (flagged rather than silently claimed): reducing
risk-per-trade below the current cap-band scaling. An initial quick test was
invalid — the Large/Mid/Small risk percentages are hardcoded inline in
`backtest/simulator.py` rather than read from a patchable constant, so the
test silently changed nothing. Worth a proper follow-up.

## 8c. VCP entry-timing — built, tested, rejected (2026-09-28)

Built `modules/vcp_pattern.py`: a stock already passing Trend Template is
checked for a tightening base (trailing 45 sessions split into 3 consecutive
15-day windows, each with a smaller high-low range % and lower average volume
than the last — a stated simplification of true VCP, which uses genuine
swing-point detection; see that module's docstring) and, if valid, a
breakout above the base's high on above-average volume becomes the entry
trigger — replacing RSI/MACD for this variant, with the base's own low as a
tighter stop than the generic ATR/8% caps. Hand-verified against real data
first: scanning the universe found 16 genuine tightening bases with sensible
depth/volume numbers, and a random sample of 5 non-trending stocks were all
correctly rejected (expanding ranges, non-monotonic volume).

**Backtested as a third entry paradigm (`vcp_breakout`) across all 3 exit
paradigms — did not improve win rate, the thing it was built for:**

| Entry | Exit | Trades | Win % | CAGR | Max DD | Sharpe |
|---|---|---|---|---|---|---|
| Trend Template (RSI/MACD-triggered) | Trailing | 588 | 37.6% | +9.4% | -46.8% | 0.47 |
| RSI+MACD only | Trailing | 641 | 38.8% | +8.1% | -51.5% | 0.47 |
| **VCP breakout** | Trailing | 163 | **38.0%** | +2.3% | -47.4% | 0.31 |

Win rate (38.0%) is statistically indistinguishable from the other two
paradigms (37.6%/38.8%) — the "precisely-timed entry" hypothesis didn't
materialize as a hit-rate improvement in this implementation. CAGR and
Sharpe are clearly *worse*, largely because the combined Trend Template + VCP
gate cuts trade count to ~163-180 over 8.8 years (~18-20/year across the
*entire* universe) — checked this wasn't just an overly strict parameter
choice by re-running with 2.5x looser depth/volume tolerances (214 trades,
win rate still flat at 38.3%) and with the RS-Rating co-gate removed (189
trades, win rate 41.3% but CAGR/Sharpe still worse) — the conclusion holds
across parameterizations, not just one strict setting.

**Honest assessment of why:** the fixed-3-window contraction proxy is a
coarse stand-in for genuine VCP — real swing-point-based contraction
detection (finding actual peaks/troughs and confirming a true multi-wave
tightening structure, rather than checking 3 arbitrary consecutive 15-day
blocks) might behave differently, but that's the materially bigger
engineering lift already flagged when VCP was first deferred in Task 3, and
this result doesn't provide evidence it would justify that investment.

**Not adopted.** `vcp_pattern.py` and the `vcp_breakout` backtest paradigm are
left in the codebase (available for a future, more rigorous swing-point
attempt), but `main.py`/`technical_analyzer.py` are unchanged — no speculative
wiring into the live pipeline ahead of evidence, per this project's own rule.

## 8d. Chasing 20-25% CAGR: a periodic-rebalance factor strategy, and why its
backtest number can't be trusted (2026-09-28)

Researched real reference points first, not guesses: NSE's own **Nifty200
Momentum 30** index nets **~14% CAGR over a full 18-year cycle** after costs
(max drawdown **-70.5%**, 65-month recovery); **Nifty Midcap150 Momentum 50**
shows 20.4% CAGR only over a favorable recent 7-year window (max drawdown
**-72.5%**). Dual Momentum (Antonacci) trades CAGR for a much better drawdown
(12.3% CAGR, -33.7% DD) via an absolute-momentum cash filter. Directly testing
on our own swing-trading system: removing every risk control (heat cap,
diversification, regime filter) barely moved its CAGR (stayed 8-11%) — the
ceiling there is the entry/exit logic, not the risk controls.

Built a genuinely different, second strategy to test the real question:
`backtest/factor_momentum.py` — a periodic-rebalance (semi-annual/quarterly),
no-active-stop-loss factor portfolio replicating Nifty200 Momentum 30's own
disclosed formula (6M + 12M price return, each risk-adjusted by trailing
annualized volatility, cross-sectionally z-scored, top 30 selected — equal-
weighted here, since this project has no market-cap data for the official
weighting scheme). Hand-verified the formula against a manual calculation on
real data before trusting it (exact match), and confirmed the selection
logic correctly favors genuinely higher-momentum stocks over random ones.

**The backtest result: implausibly good, and confirmed not real.**

| Rebalance | Overlay | CAGR | Max DD | Sharpe | Calmar |
|---|---|---|---|---|---|
| Quarterly | None | **+35.4%** | -33.7% | **1.53** | 1.05 |
| Semiannual | None | +34.5% | -33.4% | 1.53 | 1.03 |
| Semiannual | Half-defensive overlay | +24.7% | -22.2% | 1.48 | 1.11 |

A Sharpe of 1.53 would make this one of the best long-only equity strategies
ever published — better than NSE's own real momentum index, better than
Dual Momentum's 40-year live-ish track record. That's a red flag, not a
result, and it was checked rather than reported at face value: took the
30 stocks this strategy would have picked at a 2018 rebalance, and looked at
their **actual total return from 2018 to today**. Median: **+230%**. Then
did the same for a **random** 30-stock sample from the identical universe:
median **+241%** — statistically indistinguishable, if anything higher.

**The momentum selection contributes nothing measurable here — the entire
backtest's outperformance is coming from the universe itself.** The universe
(`sector_mapper.build_universe()`, today's Nifty500) is, by construction, the
set of stocks large/successful enough to *still be in the index today*. Any
stock that underperformed badly enough to be delisted or drop out of the
index entirely between 2016 and now is **simply absent from the data** — so
every backtest run on this universe is secretly informed by which companies
turned out to be winners, regardless of what the strategy's selection logic
actually does. This is a textbook, severe form of survivorship bias, and it
poisons this kind of backtest far worse than the swing-trading system's own
already-documented point-in-time bias (§9), because here it directly
contaminates the exact metric (returns) the strategy is selecting on.

**Conclusion: this project's tooling cannot honestly answer "can we get
20-25%" for this style of strategy**, because the data source itself already
knows the answer is "yes" before any strategy logic runs. A trustworthy
backtest would need genuine point-in-time historical index constituents
(which stocks by name during each year, including the ones that later
failed) — NSE doesn't expose that for free in a clean form, the same gap
already flagged for the swing-trading system's sector classification.

**The realistic reference stays NSE's own numbers, because their index
calculation is free of this bias** (it uses the real historical membership
at each rebalance date, including stocks that later dropped out): **~14%
CAGR over a full cycle, with a ~70% drawdown** is what an honest, unlevered
momentum factor strategy in this market actually looks like — not 20-25%
with a tame drawdown. If that risk/return profile is genuinely wanted, the
simplest, most reliable way to get it is the real thing: NSE's Nifty200
Momentum 30 / Nifty Midcap150 Momentum 50 index funds are directly investable
(UTI, Kotak, HDFC, Edelweiss, and Tata all run them) — not a custom backtest
this project's data can't validate honestly.

**Where this leaves the recommendation:** given the swing-trading strategy
doesn't yet clearly beat passive (§8b), and a genuine 20-25%-CAGR factor
strategy can't be honestly backtested with this project's data (§8d) without
accepting a real ~70% drawdown even when it *can* be trusted (NSE's own
numbers), the responsible recommendation — especially for the stated "family
and close people" use case — is a **core-satellite structure**: keep the
large majority of capital in a low-cost Nifty500 index fund (the benchmark's
own 11.3% CAGR / -38.3% max DD / 0.74 Sharpe is a genuinely solid, zero-effort
outcome), optionally a smaller sleeve in a real NSE momentum index fund for
those who explicitly want the higher-CAGR/higher-drawdown trade-off with a
trustworthy track record behind it, and treat *this project's own* active
strategy as a small satellite allocation (e.g. 10-20% of capital) until the
paper-trading period (§10) provides real forward evidence of an edge neither
backtest has yet shown.

## 9. Limitations (stated explicitly, not buried)

- **Point-in-time universe bias:** the backtest applies *today's* Nifty500
  membership, NSE Industry tags, and thematic-index membership retroactively
  across all 5 years — not a true point-in-time reconstruction. A stock that
  only joined Nifty500 in 2025 is treated as eligible for the full period; a
  stock that IPO'd in 2024 is naturally excluded before it has price history.
  A fully rigorous point-in-time rebuild would need historical index-
  reconstitution archives NSE doesn't expose for free in a clean form.
- **Sector synthetic indices in the backtest use all sufficiently-historied
  sector members, not the liquidity-capped 40** used for live stock
  selection — a stated simplification for computational stability, not an
  oversight. The liquidity/cap filter still gates which stocks are eligible
  to actually become positions.
- **Transaction costs modeled at a flat 0.1% round-trip** (blended estimate
  for discount-broker brokerage + STT + slippage) — a simplification, not a
  broker-specific costing model.
- **No look-ahead in execution:** entries/signal-based exits (RSI failure,
  time backstop) always execute at the next trading day's open; resting stop/
  target levels are checked against the same day's intraday high/low, as a
  real resting order would.

## 10. Forward Paper-Trading Plan

> **Superseded on 2026-10-01 by the built-in paper account (§16).** TradingView has no API to accept orders, so the steps below can only ever be a manual mirror — optional, not needed for the test. The automatic account and its dashboard are the record now.

**Platform: TradingView** (free plan). Researched India-specific alternatives
(StockGro, PaperTradingApp, MegaBull — all free but more gamified, less
suited to systematically logging a rules-based strategy). TradingView's free
plan gives ~15-minute delayed NSE data, which is irrelevant for a strategy
holding positions for days-to-weeks, and its charting/indicators match what
this system already uses.

**Setup (the user does this — account creation isn't something this session
does on someone else's behalf):**
1. Create a free account at tradingview.com.
2. Open any NSE symbol's chart (search e.g. "RELIANCE" and select the NSE listing).
3. Bottom panel → **Trading Panel** → **Paper Trading** — TradingView provisions
   a virtual account (configurable starting balance; set it to ₹5,00,000 to
   match this strategy).
4. Each time `main.py` runs (weekdays, 08:00 IST via GitHub Actions):
   - It first checks every still-open row in `paper_trades.csv` against the
     exit rules in §5 (`modules/paper_trade_log.check_exits` — re-derives the
     current trailing stop from each position's entry date forward using
     fresh price data, since this project has no other persistent state) and
     fills in `exit_date`/`exit_price`/`exit_reason` for anything that
     triggered. **Manually close the matching position in TradingView when
     this happens.**
   - It then logs any newly-accepted entries. **Manually place the equivalent
     buy order** in the TradingView paper account at the logged entry price/
     quantity, with the logged stop as a stop order.
5. After ~3 months, compare TradingView's own paper P&L against
   `paper_trades.csv`'s recorded entries/exits — the two should closely track
   (any large divergence signals a logging or execution-assumption bug worth
   investigating before risking real capital).

## 11. Alpha/Beta Regression Diagnostic (2026-09-28)

Every comparison against Nifty500 so far (§8b) was a head-to-head CAGR/Sharpe/
Calmar/MaxDD table — never a real decomposition of the strategy's own daily
returns into alpha (stock-picking skill) vs beta (market exposure). This
section runs that regression: `strategy_daily_return = alpha + beta *
benchmark_daily_return + residual`, OLS via `backtest/alpha_analysis.py`
(pure numpy — this project has no scipy/statsmodels, so a hand-rolled OLS with
a proper standard-error/t-stat formula was used rather than adding a new
dependency for a ~30-line calculation). Verified correct first: regressing the
benchmark against itself returns alpha≈0, beta≈1.0000, R²=1.0 to
floating-point precision.

Run via `python -m backtest.run_alpha_analysis` against the existing cached
5-year data (no new fetch) across all 9 entry×exit variants:

| Entry | Exit | N (days) | Alpha (ann. %) | Beta | R² | t-stat | p-value | Sig. at 5%? |
|---|---|---|---|---|---|---|---|---|
| trend_template | trailing | 2209 | +24.8% | 0.79 | 0.03 | 1.01 | 0.313 | No |
| trend_template | fixed_target | 2209 | +32.2% | 0.56 | 0.01 | 1.14 | 0.253 | No |
| trend_template | scale_out | 2209 | +25.3% | 0.77 | 0.03 | 0.97 | 0.330 | No |
| rsi_macd_only | trailing | 2209 | +27.9% | 0.88 | 0.03 | 1.01 | 0.315 | No |
| rsi_macd_only | fixed_target | 2209 | +38.0% | 0.59 | 0.01 | 1.22 | 0.223 | No |
| rsi_macd_only | scale_out | 2209 | +28.5% | 0.84 | 0.03 | 1.01 | 0.315 | No |
| vcp_breakout | trailing | 2209 | +10.8% | 0.40 | 0.02 | 0.62 | 0.534 | No |
| vcp_breakout | fixed_target | 2209 | +12.9% | 0.18 | 0.00 | 0.74 | 0.461 | No |
| vcp_breakout | scale_out | 2209 | +10.4% | 0.34 | 0.01 | 0.61 | 0.545 | No |

**Honest reading — neither the "hidden alpha" story nor a clean "alpha is
zero" verdict, and that itself is the finding:**

- Every variant's **point estimate** for annualized alpha is positive
  (10–38%), which sounds like it contradicts §8b's benchmark-comparison loss.
  It doesn't, once R² is read alongside it: **R² is 0.00–0.03 across every
  variant** — the benchmark's daily moves explain essentially none of this
  strategy's daily return variance. That's mechanically expected: the
  portfolio-heat cap and per-sector limits (§6) mean the book is frequently
  partly or fully in cash, so most days look nothing like a beta-1 market bet
  — the regression has very little to work with day to day, and what
  variance there is comes overwhelmingly from idiosyncratic, stock-specific
  moves in whichever handful of positions happen to be open.
- With R² that low, the alpha estimate is extremely noisy — **not one variant
  clears p<0.05** (best case p=0.223, most around 0.25–0.55). We cannot
  statistically distinguish any of these point estimates from zero. This is
  the same honest caveat as the factor-strategy investigation in §8d, just
  arrived at through a different method: a plausible-looking number that
  doesn't survive a real significance check.
- **What this changes vs. before:** §8b's benchmark-comparison loss is not
  contradicted by "secret alpha hiding under beta" — there's no statistically
  reliable evidence of stock-picking skill in this backtest, under any of the
  9 variants. But it's also not a clean "alpha is proven zero" — the
  regression simply doesn't have the statistical power to say either way,
  because the strategy's sparse, low-beta position-holding pattern is a bad
  fit for a daily-return CAPM regression in the first place. A **monthly or
  trade-level regression** (aggregating away the many flat-cash days) would
  have more power to actually answer this question, and is a natural next
  step if this is worth re-investigating — not done here, flagged as a
  concrete follow-up rather than silently assumed away.
- **Net effect on the recommendation in §8d:** this diagnostic doesn't
  overturn the core-satellite recommendation — if anything it removes one
  potential objection to it (there's no statistically-backed "hidden edge"
  being left on the table by not hedging beta), while also not adding new
  evidence against pursuing the system further. Treat this section as closing
  an open question, not as a new negative finding on top of §8b.

## 12. Governance / Promoter-Pledge Filter (2026-09-28)

Fundamental confirmation (§7, `fundamental_analyzer.py`) only ever checked YoY
profit growth — nothing about *who controls the company* or whether they're
under financial stress. That's a real gap given this system is explicitly
headed toward money that isn't just the user's own ("we will develop this
into a small case for myself and then for family and closed people," stated
earlier in this project): Indian small/midcap momentum blow-ups are
disproportionately governance failures (a promoter pledging shares into a
rally, then a forced sale on a margin call triggers the crash), not macro
reversals. `modules/governance_analyzer.py` closes that gap using the same
public Screener.in pages `fundamental_analyzer.py` already scrapes — no new
data source.

**Design: gate on a confirmed pledge, inform on promoter-holding trend —
argued, not defaulted to "gate everything because it sounds safer."** A
disclosed pledge is the company's own unambiguous fact (Screener.in's
auto-generated Cons list states it directly, e.g. *"Promoters have pledged
44.7% of their holding"* — spot-checked live below), so false positives are
close to impossible; this is a hard gate, the same treatment as
`trend_template_passes`. A *declining* promoter holding % has too many benign
explanations (succession planning, funding another venture, a buyback/
open-offer mechanic, promoter/public reclassification) to hard-gate against
an already-thin qualifier pool (RSI/MACD + Trend Template + RS≥70 already
stack hard) — kept informational only, surfaced in the report, never gating
`passes`, the same treatment already given to `volume_surge`/`near_52w_high`/
`rel_strength_vs_sector`.

**Verification — hand spot-check against 2-3 real stocks (2026-09-28), the
same bar `trend_template.py`/`market_regime.py` were held to when first
built** (no historical Screener.in API was found for past shareholding/pledge
snapshots, so a full backtest isn't possible here — see limitation below):

| Ticker | Promoter holding | 4Q trend | Pledge flag | Result |
|---|---|---|---|---|
| TCS | 71.8% | flat (0.0pp) | No | Clean — correctly passes |
| ZEEL | 4.0% | flat (0.0pp) | No | Low holding, but no pledge/decline — correctly not gated |
| DBREALTY | 47.2% | -0.3pp | **Yes** | Correctly rejected — "Promoters have pledged 44.7% of their holding." |

Wired into `main.py` as a new **Step 3a**, between `screen_stocks()` and
`portfolio_allocator.allocate()`, run only on each sector's already-qualified
stocks (not the full basket) — the same scoping reason `sector_confirmation`
only checks the leading sector's basket rather than the whole universe.
Pledge-flagged tickers are dropped from that sector's `qualified` list before
allocation; a scrape failure is non-fatal (logged to `errors`, doesn't block
the run). Rendered in the report via a new governance line per sector,
showing every checked ticker's promoter-holding trend, and — in red,
explicitly labeled REJECTED — the pledge disclosure text for anything
filtered out, so a pledge-driven rejection is visible, not silent.

**Explicit, stated limitation:** Screener.in exposes only *current*
shareholding/pledge data, no historical snapshot API — unlike §13's delivery%
filter below, **this cannot be backtested against 5 years of history** with
this project's tooling. Confidence in it rests on the spot-check above plus
the fact that the pledge signal itself is a direct company disclosure, not a
derived heuristic — not on a historical performance comparison. If Screener.in
or another free source ever exposes historical shareholding snapshots, this
is the natural thing to revisit for real backtesting.

## 13. NSE Delivery % — tested as an entry filter, rejected (2026-09-28)

A lot of NSE daily volume is intraday/F&O-driven noise rather than real
accumulation, so the hypothesis: requiring delivery% above a stock's own
trailing median at entry should filter out weaker breakouts and improve win
rate/CAGR. Unlike §12's governance check, this one *is* backtestable — NSE's
combined bhavcopy (`sec_bhavdata_full_DDMMYYYY.csv`, same trusted
`archives.nseindia.com` subdomain `sector_mapper.py` already depends on)
publishes an explicit `DELIV_PER` column per stock per day, cross-checked
byte-for-byte against the raw CSV (TCS, 25-Sep-2026: `DELIV_PER=47.33`,
matched exactly by `modules/delivery_analyzer.fetch_delivery_data()`).

**A real, stated constraint found while building the cache**: this bhavcopy
format only exists from **30-Sep-2019 onward** — every date before that 404s
regardless of whether it was a trading day (confirmed via direct binary
search against the live archive: 2017/2018/most-of-2019 all 404, 2020 onward
all 200 on genuine trading days). So the delivery% backtest can only ever
cover ~6 years, not the full ~10-year benchmark window — `backtest/
delivery_cache.py`'s `DELIVERY_DATA_AVAILABLE_FROM` constant encodes this.
Backfilled 1,720 of 1,721 trading days in that window (99.9%; the one gap
wasn't chased further — immaterial to the comparison below).

**Test: current best paradigm (Trend Template entry + trailing-chandelier
exit, per §8b) with vs without requiring `delivery% > the stock's own
trailing-20-day median` at entry**, both runs restricted to the same
2019-09-30-onward window so the comparison isn't confounded by the years the
filter has no data for:

| Variant | Trades | Win % | Avg R | CAGR % | MaxDD % | Sharpe | Avg days |
|---|---|---|---|---|---|---|---|
| Baseline (no delivery filter) | 494 | 38.5 | 0.35 | **14.4** | **-32.8** | **0.55** | 17.9 |
| Delivery% > own 20d median | 349 | 39.0 | 0.40 | 11.3 | -40.3 | 0.51 | 19.1 |

**Verdict: rejected**, on the same "does it actually beat the baseline, not
just look plausible" bar VCP was held to in §8c. Per-trade quality did
improve slightly (win rate +0.5pp, avg R +0.05) — the core mechanism isn't
wrong — but cutting trade count by 29% (494→349) cost more than that
per-trade edge bought back: CAGR fell 3.1pp, max drawdown *worsened* by
7.5pp, and Sharpe fell too. This is the same shape of result as VCP's
rejection: a filter that plausibly improves signal quality but shrinks the
opportunity set enough to hurt the portfolio-level outcome. **Not promoted**
to a live gate or a default backtest config — `require_delivery_above_median`
stays available in `BacktestConfig` for a future re-test (e.g. a looser
threshold than the median, or combining it with a different exit paradigm)
but defaults to `False` everywhere. The live pipeline's `ScreenResult.
delivery_pct` field stays exactly what it already was: informational only,
surfaced in the report, contributing lightly to `strength_score` — unchanged
by this result, since that was never gated on delivery% to begin with.

## 14. FII/DII Net Flow — live-only, built deliberately minimal (2026-09-28)

`modules/fii_dii_flow.py` surfaces NSE's daily FII/DII cash-market net flow
(₹ crore) via `https://www.nseindia.com/api/fiidiiTradeReact` — confirmed
reachable live with a plain `requests` call and a User-Agent header, no
cookie/session handshake needed (verified live, 2026-09-28: returned real
same-day FII/DII figures). **This is a narrower, corrected finding, not a
reversal, of `CLAUDE.md`'s existing note that `www.nseindia.com`'s API is
blocked** — that finding was about the homepage and historical-indices
endpoints (403/503 in the original Phase 0 spike); this is a different,
lighter endpoint on the same domain that happens to be reachable.

**Confirmed hard limitation**: this endpoint has no historical query support
— a `?date=` parameter is silently ignored (tested live: identical response
with and without it) — it always returns only the latest day. No free
historical FII/DII archive was found in the time available (NSDL's FPI
report site failed to connect from this environment). **This data source
cannot be backtested with this project's tooling, full stop** — unlike §13's
delivery%, there is no future path to validating it here short of finding a
different historical source.

**The build-anyway decision, argued rather than defaulted:** cost is
near-zero (one HTTP call, no gating logic, ~70 lines) and there's a direct
precedent in this codebase for informational-only signals that were tested
and found not gate-worthy (`RotationResult.breadth_pct`, §3). Against it: a
number that can *never* earn backtested evidence sits awkwardly next to a
project whose whole discipline is "test before trusting a signal." Resolved
by keeping it deliberately minimal and explicitly labeled: `main.py` logs it
non-fatally, and the report's new "Market Context" block prints
*"FII/DII net flow (informational, not backtested): ..."* — the caveat lives
in the report itself, next to the number, not just in this document, so
anyone reading the report later sees the limitation where it matters.
Wired as a plain informational display, never a gate — if it's ever found to
correlate with anything measurable via a future historical source, that's
the trigger to reconsider its role, not this build.

## 15. Tightening the factor replica toward Midcap150 Momentum 50 — Path B (2026-09-30)

The user reviewed the honest CAGR table across every backtest this project has
ever run (§11 diagnostic included) and stated explicitly: **accepts a ~70%
drawdown, wants CAGR ≥18%.** Given that, two paths forward were offered — (A)
chase free point-in-time NSE constituent data to properly fix §8d's
survivorship bias, or (B) stop trying to validate our own backtest number and
instead tighten the factor replica to mechanically mirror **Nifty Midcap150
Momentum 50** specifically — the real, bias-free index that showed 20.4% CAGR
over a favorable recent 7-year window (-72.5% max DD). **The user chose Path
B.**

**What changed in `backtest/factor_momentum.py`**: §8d's replica had two
stated deviations from this specific index — full Nifty500 universe (not the
midcap-only pool) and top-30 selection (not Midcap150 Momentum 50's actual
50). Both are now closable: `FactorConfig.universe_scope="midcap_only"`
restricts the eligible pool to `cap_band=="Mid"` tickers (reusing
`sector_mapper.build_universe()`'s existing tagging, no new data source) and
`MIDCAP_NUM_HOLDINGS=50` matches the real index's constituent count.
Weighting stays equal, not market-cap-weighted — historical market-cap data
isn't freely available, and using *current* market cap to weight a
*historical* backtest would itself be a new look-ahead bias, so this
deviation is not worth closing. Confirmed the `midcap_only` filter is doing
the right thing before trusting any result: it tagged **exactly 150** "Mid"
cap-band tickers — landing precisely on NSE's real Midcap150 count, not some
off number that would indicate a lookup bug. Also confirmed
`--universe-scope full_nifty500` (the default, unchanged) reproduces §8d's
original numbers exactly (+34.5%/+35.4% CAGR for semiannual/quarterly,
Sharpe 1.53 both) — a clean regression check that this was purely additive.

**Result — `python -m backtest.run_factor_backtest --universe-scope midcap_only`:**

| Rebalance | Overlay | CAGR | Max DD | Sharpe | Calmar |
|---|---|---|---|---|---|
| Semiannual | None | +26.9% | -33.0% | 1.45 | 0.81 |
| Semiannual | Half-defensive | +19.3% | -19.3% | 1.43 | 1.00 |
| Quarterly | None | +24.7% | -32.9% | 1.34 | 0.75 |
| Quarterly | Half-defensive | +18.3% | -19.6% | 1.36 | 0.94 |
| *(reference)* Nifty500 B&H | | +10.8% | -38.3% | 0.72 | 0.28 |
| *(reference)* §8d full-Nifty500/30-holdings, semiannual/none | | +34.5% | -33.4% | 1.53 | 1.03 |
| *(reference)* **NSE real Nifty Midcap150 Momentum 50** | | **+20.4%** | **-72.5%** | n/a | n/a |

**Read this table carefully — twice, not once, in the same direction:**

1. **The CAGR is still >18% across every non-defensive variant** (18.3%-26.9%)
   — on its face, "meets the target." **Do not read that as validation.** This
   is the exact same `sector_mapper.build_universe()` universe §8d already
   proved is severely survivorship-biased for any returns-selecting strategy
   — restricting to midcap-only doesn't fix that, and arguably makes it
   *worse*: smaller companies have a structurally higher real-world rate of
   delisting/failing, and "today's surviving Midcap150" silently excludes
   exactly those failures from the data entirely.
2. **New evidence for that same conclusion, not just a repeated caveat**:
   look at **Max DD**. Our backtest shows -33.0%/-32.9% — the real index's own
   live history shows **-72.5%**. Our number isn't just optimistic on
   *return*, it's equally optimistic on *risk* — both diverge from the real
   index in the same bias-consistent direction. That's exactly what you'd
   expect if the survivor-only universe never contains the genuine crash
   episodes (near-delistings, multi-year collapses before ultimate failure)
   that drag a real historical index's drawdown down to -72.5%. If this
   backtest were trustworthy, its drawdown should look *like* the real
   index's, not roughly half of it.

**The actual deliverable of Path B, stated plainly**: this build does not
give you a validated 18%+ strategy. What it gives you is a strategy whose
*mechanics* — universe, holding count, rebalance cadence, scoring formula —
now closely mirror a real index with a genuine, bias-free, live 20.4%/-72.5%
track record. **The honest forward expectation for running this live is the
real index's own number, not this backtest's number** — because survivorship
bias only corrupts a look-*backward* test; it says nothing about how a
mechanically-faithful replica performs starting today, decided in real time,
with no foreknowledge of which stocks will still exist in 2033. Concretely:
if you want the 18%+/~70%-drawdown risk-return profile with a trustworthy
basis for believing in it, the two honest paths are (a) invest directly in
the real NSE Nifty Midcap150 Momentum 50 index fund, or (b) run *this*
project's now-tightened replica going forward and expect something in the
real index's neighborhood — not the 24-27% this backtest displays, and
prepared for the real index's actual -72.5% drawdown depth, not this
backtest's -33%. Path A (genuine point-in-time constituent data) remains the
only route to a number from *this project's own tooling* that would actually
deserve to be trusted, and is still open as a future investigation.

## 16. Market filter removed; built-in paper account and dashboard (2026-10-01)

**The 200-day market filter (§3) no longer gates entries.** The user's reasoning:
some sector is always leading, so a market-wide pause throws away good trades.
Measured before changing anything, on the 8.8-year backtest, same run and same
universe for each pair:

| Entry | Exit | Market filter | Trades | Win % | CAGR | Max DD | Sharpe | Calmar |
|---|---|---|---|---|---|---|---|---|
| Trend Template | Trailing | ON | 592 | 37.3 | +7.3% | -48.0% | 0.46 | 0.15 |
| Trend Template | Trailing | **OFF** | 1,196 | 36.1 | **+13.9%** | -48.9% | **0.60** | **0.28** |
| Trend Template | Scale-out | ON | 619 | 38.1 | +6.9% | -45.8% | 0.45 | 0.15 |
| Trend Template | Scale-out | OFF | 1,248 | 36.6 | +13.7% | -47.6% | 0.60 | 0.29 |
| RSI+MACD only | Trailing | ON | 648 | 36.1 | +8.1% | -54.6% | 0.47 | 0.15 |
| RSI+MACD only | Trailing | OFF | 1,476 | 35.8 | +14.3% | -63.3% | 0.66 | 0.23 |
| *Nifty500 buy & hold* | | | | | +11.3% | -38.3% | 0.74 | 0.29 |

For the live configuration (Trend Template entry) removing the filter nearly
doubles CAGR while max drawdown barely moves, and Calmar rises to match the
index's. The reason shows in the RSI+MACD rows: without a stock-level trend
check, removing the market filter deepens the drawdown sharply (-54.6% →
-63.3%). The Trend Template already makes *each stock* prove its own uptrend,
which does most of what the market filter was for. So: off for the Trend
Template configuration only. `market_regime.py` still runs and is shown on the
dashboard as context; `BacktestConfig.apply_market_regime_filter` now
defaults to `False` so the backtest keeps matching live, and stays switchable.

Honest reading: +13.9% CAGR beats the index's +11.3% but with a deeper
drawdown (-48.9% vs -38.3%) and a lower Sharpe (0.60 vs 0.74) — roughly
index-like risk-adjusted, not clearly better. The forward paper test is what
decides it. (Absolute numbers here differ slightly from §8b's because
`build_universe()` reads NSE's *current* constituent lists, which changed at
the end-September reconstitution; each ON/OFF pair above is like-for-like.)

**Built-in paper account (`modules/paper_trader.py`).** TradingView has no API
that accepts orders, so it could never be automated. The project now runs its
own paper account, starting at ₹5,00,000:

- Orders are placed after the close and **fill at the next session's open**,
  the same convention as the backtest. An order is cancelled if the stock
  opens at or below its stop, and cut to the cash available.
- Exits follow §5 exactly, replayed **bar by bar** from the entry date, so a
  missed daily run never skips a stop. RSI-failure exits happen at the *next*
  open after RSI closes below 45, never the same day.
- Costs of 0.15% per side (STT alone is 0.1% each way on delivery), more
  conservative than the backtest's 0.1% round trip, deliberately.
- New orders skip anything already held and count held positions toward the
  2-per-sector cap, the 85% capital cap and the 8% heat cap. Before this, the
  live allocator re-sized from scratch daily and could buy the same stock
  every day it qualified. That never surfaced only because the market filter
  had blocked every entry.
- State is two committed CSVs: `paper_trades.csv` (every order's lifecycle)
  and `paper_equity.csv` (one row per session). Cash is re-derived from the
  ledger each run; start + realized + unrealized reconciles to equity exactly.

Verified before going live: test orders on cached history filled at the
correct next-session open; RELIANCE and TCS were replayed by hand bar by bar
(stop levels, trailing ratchet, exit day and price matched the engine); and a
45-session day-by-day replay confirmed no stock was ever held twice, no
sector exceeded 2 positions, cash never went negative, and the accounting
reconciled to the paisa.

**Dashboard (`docs/index.html`, `modules/dashboard.py`).** The site's front
page is now the paper account: equity vs Nifty500, drawdown, P&L per closed
trade, sector exposure, today's sector ranking, and tables for open
positions (with trailing stop and room-to-stop), pending orders, closed
trades, and every screening candidate with the portfolio's decision. The
daily screening report moved to `docs/latest.html`; both CSVs are
downloadable from `docs/data/`.

**Schedule.** The 08:00 IST run never arrived on time: GitHub's scheduler
had started it 4–6 hours late on every recorded day, and on 2026-10-01
it still hadn't started by 14:35 IST. It now runs at 17:00 IST, after the close and on the
day's final prices, with a 21:00 IST backup that skips itself if the first
already published. Even with GitHub's usual delay, the email lands before
the next morning's open.
