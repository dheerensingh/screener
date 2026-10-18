# Trade Check: one instrument, futures, the 5 rules

A separate tool from the sector-rotation screener. It doesn't import or change
anything the daily run uses (`main.py`, `modules/`, `docs/`, `paper_*.csv`,
`schedule.yml`). It applies the 5 rules from "The AI Trade Check: Prompt + 5
Rules" as plain code, so the same input always gives the same answer and the
rules can be backtested. There is no LLM call.

It produces signals, backtests and reports only. There is no broker connection
and no order placement.

## The rules

| # | Rule | In code |
|---|---|---|
| 1 | Risk 0.5% of the account per trade | `config.RISK_PCT`, applied to current equity |
| 2 | Stop just past the prior day's high/low | `config.STOP_BUFFER_ATR`: 0.1×ATR(14) past it |
| 3 | Never add to a losing trade | Never more than one position, and it is never added to |
| 4 | 3 losses in a row = 3 days off | No orders for the next 3 sessions |
| 5 | Position never above the account | Size cut to the account; in lots mode, SKIP if one lot is bigger than the account |
| — | One market, one daily chart, max one new trade a day | One instrument; one stop order per session |

The doc leaves the trade idea to the trader. Here it is a defined rule:

- **Direction:** comes from a trend filter: price above a rising SMA means long, below a falling SMA means short. The filter is SMA20, SMA50, SMA200 or the prior candle's colour.
- **Long:** a buy-stop at the prior day's high plus the buffer, with the stop at its low minus the buffer.
- **Short:** the mirror of the long.
- **Target:** 1.5R, 2R or 3R.
- **Time limit:** exit at the close after 10 sessions.

## Files

| File | What it does |
|---|---|
| `config.py` | Rules, defaults and candidate instruments (lot sizes, costs, carry) |
| `rules.py` | `plan_trade()`, the 5 rules turned into a TAKE/SKIP plus levels; `format_check()` prints the doc's output format |
| `engine.py` | Day-by-day simulator shared by the backtest and the paper account |
| `data.py` | yfinance daily bars, cached in `cache/` (git-ignored) |
| `run_backtest.py` | Compares every variant on every instrument and picks one on the first 70% of the history, then reports it on the last 30% |
| `check.py` | The daily check for the next session, plus the paper account replayed from a start date, plus the trade log |
| `tests/` | Hand-built bar sequences with known answers |

## Running it

Cloud sessions can't reach Yahoo, so the tool runs on GitHub Actions
(`.github/workflows/trade-check.yml`):

- **Push to a non-main branch** that touches `trade_check/**`: runs the tests and the backtest.
- **Actions → Trade Check → Run workflow**:
  - `job=backtest`: the instrument comparison.
  - `job=check`: the daily check and paper account for one instrument.

The workflow never commits anything. Results are on the run's summary page and
in its `trade-check-output` artifact. It has no schedule. A daily run can be
added later if you ask.

To run locally (needs Yahoo access):

```
python -m trade_check.run_backtest --only NIFTY,BANKNIFTY,BTC
python -m trade_check.check --instrument BTC --paper-start 2026-10-20 --trend sma50 --target-r 2
python -m trade_check.tests.test_engine
```

## Known limits

- **NSE contract sizes vs a ₹5L account.** One Nifty lot is 65 units, about
  ₹16L of exposure, and stock futures have a ₹15L minimum contract value.
  Rule 5 (no leverage above the account) therefore rejects every NSE futures
  trade below roughly ₹16L of capital. Rule 1 usually needs more: a normal
  day's range on one Nifty lot risks far more than 0.5% of ₹5L. The
  backtest's section 2 prints the account size each instrument actually
  needs. BTC perpetuals (0.001 BTC contracts) can be sized at any account
  size.
- **NSE prices are spot, not futures.** The futures basis is charged as a
  6%/year carry on longs and credited to shorts. Expiry rolls are not charged
  separately.
- **BTC.**
  - The price is Yahoo's BTC-INR spot, not a Delta Exchange India perpetual.
  - Funding is modelled as 10%/year paid by longs.
  - Indian crypto derivatives sit outside SEBI's framework (FIU registration
    only), and their tax treatment is unsettled. Check with a CA.
- **Same-day stop and target.** When one daily bar reaches both, the engine
  assumes the stop was hit first.
- **Selection bias.** Choosing the best of many variants and instruments
  flatters the winner. Trust the test-window columns, not the train ones.
- **The paper account is a replay.** It is re-simulated from the start date on
  every run rather than stored, so a revised Yahoo bar can change a past paper
  trade.

## Connecting to the main strategy

Not done. When you ask, it will come as a separate PR for you to review.
