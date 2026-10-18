"""
Trade Check: configuration

The 5 rules (from the "AI Trade Check" doc) plus the defined entry rule and the
candidate instruments the backtest compares. Nothing here is read by the live
screener (main.py / modules/).
"""

from dataclasses import dataclass

# --- The 5 rules -------------------------------------------------------------
ACCOUNT_SIZE_INR = 500_000        # the user's account; the doc's test used ₹10L
RISK_PCT = 0.5                    # Rule 1: risk 0.5% of the account per trade
STOP_BUFFER_ATR = 0.1             # Rule 2: stop sits 0.1×ATR(14) past the prior day's high/low
# Rule 3 (never add to a loser): the engine holds at most one position, never adds.
LOSS_STREAK_PAUSE = 3             # Rule 4: 3 losses in a row ...
PAUSE_SESSIONS = 3                # ... = 3 sessions off
MAX_NOTIONAL_PCT = 100.0          # Rule 5: position value never above the account
# Setup: one market, one daily chart, max one new trade a day.

# --- The defined entry rule (the doc leaves the idea to the trader) ---------
# Long: buy-stop just above the prior day's high, stop just below its low.
# Short: sell-stop just below the prior day's low, stop just above its high.
# Direction comes from a trend filter; target is a multiple of the risk.
DEFAULT_TREND_FILTER = "sma50"    # "sma20" | "sma50" | "sma200" | "prior_candle"
DEFAULT_TARGET_R = 2.0            # target = entry ± 2 × (entry − stop)
DEFAULT_MAX_HOLD = 10             # sessions; exit at the next open after this many
SMA_SLOPE_LOOKBACK = 5

# --- Backtest -----------------------------------------------------------------
BACKTEST_PERIOD = "10y"
TRAIN_FRACTION = 0.7              # pick the variant on the first 70%, report it on the last 30%
# Equity used to compute per-trade risk. The "fractional" sizing mode ignores
# lot sizes (measures the rule's edge); "lots" mode enforces them (measures
# whether a ₹5L account can actually take the trades).
SIZING_MODES = ("fractional", "lots")

# NSE futures: SEBI's minimum contract value since Nov 2024 is ₹15 lakh. Stock
# lot sizes change every few months, so the backtest estimates each stock's lot
# as ceil(₹15L / price) on the signal day, rounded up to the next 25 units.
NSE_MIN_CONTRACT_INR = 1_500_000


@dataclass(frozen=True)
class Instrument:
    key: str
    yahoo: str
    kind: str                 # "index_future" | "stock_future" | "crypto_perp"
    lot: float | None         # fixed units per contract; None = estimate from NSE_MIN_CONTRACT_INR
    cost_per_side_pct: float  # brokerage + taxes + slippage, % of notional, each side
    carry_annual_pct: float   # what a long pays per year to hold (futures basis / perp funding); shorts receive it
    trades_weekends: bool = False


# Costs, per side, as a % of notional:
#   NSE futures: STT 0.05% on the sell side since Budget 2026 (≈0.025%/side
#   averaged), exchange + SEBI + stamp + GST ≈ 0.005%, ₹20 brokerage
#   (negligible on a ₹15L+ contract), plus slippage: 0.01% index, 0.03% stock.
#   Carry: a long future is priced ~ cost of carry over spot, so its return
#   trails the spot index by ≈ (interest − dividend yield) ≈ 6%/year.
#   BTC perpetual (Delta Exchange India): 0.05% taker + 18% GST ≈ 0.059%, plus
#   0.03% slippage. Funding ≈ 10%/year paid by longs in a normal market.
# FINNIFTY (lot 60) was dropped: Yahoo's NIFTY_FIN_SERVICE.NS returns no bars.
INSTRUMENTS: dict[str, Instrument] = {i.key: i for i in [
    Instrument("NIFTY", "^NSEI", "index_future", 65, 0.04, 6.0),
    Instrument("BANKNIFTY", "^NSEBANK", "index_future", 30, 0.04, 6.0),
    Instrument("RELIANCE", "RELIANCE.NS", "stock_future", None, 0.06, 6.0),
    Instrument("HDFCBANK", "HDFCBANK.NS", "stock_future", None, 0.06, 6.0),
    Instrument("ICICIBANK", "ICICIBANK.NS", "stock_future", None, 0.06, 6.0),
    Instrument("SBIN", "SBIN.NS", "stock_future", None, 0.06, 6.0),
    Instrument("INFY", "INFY.NS", "stock_future", None, 0.06, 6.0),
    Instrument("TCS", "TCS.NS", "stock_future", None, 0.06, 6.0),
    Instrument("BHARTIARTL", "BHARTIARTL.NS", "stock_future", None, 0.06, 6.0),
    Instrument("LT", "LT.NS", "stock_future", None, 0.06, 6.0),
    # Priced in INR via Yahoo's BTC-INR so sizing is in rupees; contract 0.001 BTC.
    Instrument("BTC", "BTC-INR", "crypto_perp", 0.001, 0.09, 10.0, trades_weekends=True),
]}
