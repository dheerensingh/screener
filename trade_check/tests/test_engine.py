"""
Hand-built bar sequences with known answers. Run with either:
    python -m pytest trade_check/tests
    python -m trade_check.tests.test_engine
"""

import pandas as pd

from trade_check import config
from trade_check.config import Instrument
from trade_check.engine import simulate
from trade_check.rules import Params, add_indicators, plan_trade

FREE = Instrument("TEST", "TEST", "index_future", 1, 0.0, 0.0)   # no costs, no carry, lot of 1
P = Params(trend_filter="prior_candle", target_r=2.0, sizing="fractional")
WARM = 30


def bars(*rows: tuple[float, float, float, float]) -> pd.DataFrame:
    """30 flat bars (O=C=100, H=101, L=99, so ATR = 2 and no setup), then `rows` as (O, H, L, C)."""
    data = [(100.0, 101.0, 99.0, 100.0)] * WARM + list(rows)
    idx = pd.bdate_range("2024-01-01", periods=len(data))
    return pd.DataFrame(data, index=idx, columns=["Open", "High", "Low", "Close"])


def run(df, **kw):
    return simulate(df, FREE, kw.pop("params", P), start_equity=1_000_000, start=df.index[WARM - 1], **kw)


BULL = (100.0, 102.0, 99.0, 101.5)       # prior day: long setup, entry 102.2, stop 98.8 (ATR ≈ 2, buffer ≈ 0.2)


def test_plan_long_levels_and_size():
    df = add_indicators(bars(BULL))
    c = plan_trade(df, FREE, 1_000_000, P)
    buf = config.STOP_BUFFER_ATR * df["ATR"].iloc[-1]
    assert c.verdict == "TAKE" and c.direction == "LONG"
    assert abs(c.entry - (102 + buf)) < 1e-9 and abs(c.stop - (99 - buf)) < 1e-9
    assert abs(c.target - (c.entry + 2 * (c.entry - c.stop))) < 1e-9
    assert abs(c.risk_amount - 5_000) < 1e-6          # 0.5% of ₹10L


def test_nifty_lot_too_big_for_5L():
    df = add_indicators(bars(BULL) * 250)              # scale prices to ~25,000
    c = plan_trade(df, config.INSTRUMENTS["NIFTY"], 500_000, Params(trend_filter="prior_candle"))
    assert c.verdict == "SKIP" and c.rules[5] is False   # 65 × ~25,500 ≈ ₹16.6L > ₹5L


def test_target_hit_on_entry_day():
    res = run(bars(BULL, (101.5, 110.0, 101.0, 109.0)))
    assert len(res.trades) == 1
    t = res.trades[0]
    assert t.exit_reason == "target (entry day)" and abs(t.r_multiple - 2.0) < 1e-6


def test_stop_and_target_same_bar_assumes_stop():
    res = run(bars(BULL, (101.5, 110.0, 98.0, 105.0)))
    assert res.trades[0].exit_reason == "stop (entry day)"
    assert abs(res.trades[0].r_multiple + 1.0) < 1e-6


def test_order_expires_if_not_triggered():
    res = run(bars(BULL, (101.0, 101.9, 100.5, 100.5), (100.5, 100.6, 100.4, 100.5)))
    assert res.trades == []


def test_gap_through_stop_exits_at_open():
    # Day 2 fills and closes inside the range; day 3 opens below the stop.
    res = run(bars(BULL, (101.5, 103.0, 101.0, 102.5), (95.0, 96.0, 94.0, 95.5)))
    t = res.trades[0]
    assert t.exit_reason == "gap through stop" and t.exit == 95.0 and t.r_multiple < -1.0


def test_three_losses_pause_three_sessions():
    lose = [BULL, (101.5, 103.0, 98.0, 99.0)]        # setup, then fill and stop the same day
    # Three losing trades, then three more bullish setups that would trigger.
    rows = lose * 3 + [BULL, (101.5, 103.0, 101.0, 102.9)] * 3
    res = run(bars(*rows), keep_plans=True)
    assert len(res.trades) == 3                         # nothing after the third loss ...
    skipped = [c for _, c in res.plans if not c.rules.get(4, True)]
    assert len(skipped) == 3                            # ... for exactly 3 sessions
    # The 4th post-pause session can trade again.
    res2 = run(bars(*rows, BULL, (101.5, 103.0, 101.0, 102.9)))
    assert res2.open_trade is not None


def test_never_adds_while_in_a_trade():
    res = run(bars(BULL, (101.5, 103.0, 101.0, 102.9), BULL, (101.5, 103.5, 101.0, 103.0)), keep_plans=True)
    assert len(res.trades) == 0 and res.open_trade is not None
    assert any(not c.rules.get(3, True) for _, c in res.plans)


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
