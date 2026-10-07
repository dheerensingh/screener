"""
Module: Paper-Trading Dashboard

Renders docs/index.html — the site's landing page — from the paper trader's
state: headline stats, equity vs Nifty500, drawdown, per-trade P&L, sector
exposure, today's sector ranking, and tables for every open position, pending
order, closed trade and today's screening candidates. Static HTML with the data
embedded as JSON and drawn by Chart.js, so it works on GitHub Pages and when
opened locally from a `git pull`.

Colours follow the data-viz reference palette (both light and dark validated):
the paper portfolio is the one emphasised series (blue) with the Nifty500 in a
neutral grey; gains/losses use the blue/red diverging pair. Every chart has a
table twin, and P&L text carries ▲/▼ so colour is never the only signal.
"""

import html
import json
import logging
import os
import shutil
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

from .fii_dii_flow import FlowResult
from .market_regime import MarketRegime
from .paper_trader import COST_PCT_PER_SIDE, EQUITY_PATH, LEDGER_PATH, PaperState
from .sector_rotation import SectorScore

logger = logging.getLogger(__name__)

DOCS_DIR = "docs"
CHART_JS = "https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"

# Reference numbers from STRATEGY.md §16 (8.8-year backtest, live configuration).
BACKTEST_REFERENCE = {
    "strategy": {"cagr": 13.9, "max_dd": -48.9, "sharpe": 0.60, "win_rate": 36.1},
    "nifty500": {"cagr": 11.3, "max_dd": -38.3, "sharpe": 0.74},
}


def inr(value: float, decimals: int = 0) -> str:
    """₹ with Indian digit grouping (₹5,00,000)."""
    if value != value:
        return "—"
    sign = "−" if value < 0 else ""
    value = abs(value)
    whole, _, frac = f"{value:.{decimals}f}".partition(".")
    if len(whole) > 3:
        head, tail = whole[:-3], whole[-3:]
        groups = []
        while len(head) > 2:
            groups.insert(0, head[-2:])
            head = head[:-2]
        if head:
            groups.insert(0, head)
        whole = ",".join(groups + [tail])
    return f"{sign}₹{whole}" + (f".{frac}" if frac else "")


def _signed(value: float, fmt: str, suffix: str = "") -> str:
    """Signed value with a direction glyph and status class, so colour isn't the only cue."""
    if value != value:
        return '<span class="muted">—</span>'
    if round(value, 2) == 0:
        return f'<span class="flat">{format(0.0, fmt.lstrip("+"))}{suffix}</span>'
    cls = "up" if value > 0 else "down"
    glyph = "▲" if value > 0 else "▼"
    return f'<span class="{cls}">{glyph} {format(value, fmt)}{suffix}</span>'


def _signed_inr(value: float) -> str:
    if value != value:
        return '<span class="muted">—</span>'
    if round(value) == 0:
        return f'<span class="flat">{inr(0)}</span>'
    cls = "up" if value > 0 else "down"
    glyph = "▲" if value > 0 else "▼"
    return f'<span class="{cls}">{glyph} {inr(value)}</span>'


def _e(text) -> str:
    return html.escape(str(text))


def _max_drawdown_pct(values: list[float]) -> float:
    peak, worst = float("-inf"), 0.0
    for v in values:
        peak = max(peak, v)
        if peak > 0:
            worst = min(worst, (v / peak - 1.0) * 100.0)
    return worst


def _table(headers: list[str], rows: list[list[str]], empty: str, num: set[int] = frozenset(),
           wrap: set[int] = frozenset(), visible: int = 0) -> str:
    """num: right-aligned numeric columns; wrap: free-text columns allowed to wrap.
    visible: if set, rows beyond this go into a collapsed "Show all" block."""
    if not rows:
        return f'<p class="empty">{_e(empty)}</p>'

    def cls(i: int) -> str:
        return "num" if i in num else "txt" if i in wrap else ""

    head = "".join(f'<th class="{cls(i)}">{_e(h)}</th>' for i, h in enumerate(headers))

    def render(rs: list[list[str]]) -> str:
        body = "".join(
            "<tr>" + "".join(f'<td class="{cls(i)}">{cell}</td>' for i, cell in enumerate(row)) + "</tr>" for row in rs
        )
        return f'<div class="table-wrap"><table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>'

    if visible and len(rows) > visible:
        return render(rows[:visible]) + f'<details><summary>Show all {len(rows)}</summary>{render(rows)}</details>'
    return render(rows)


def _tile(label: str, value: str, sub: str = "") -> str:
    return (f'<div class="tile"><div class="tile-label">{_e(label)}</div>'
            f'<div class="tile-value">{value}</div>'
            f'<div class="tile-sub">{sub}</div></div>')


def build_dashboard_html(
    state: PaperState,
    sector_scores: list[SectorScore],
    leading: set[str],
    candidates: list[dict],
    regime: Optional[MarketRegime],
    flow: Optional[FlowResult],
    generated_at: Optional[datetime] = None,
) -> str:
    """
    candidates: one dict per stock that passed screening today —
    {ticker, sector, cap_band, price, rsi, strength, decision}.
    """
    now_ist = (generated_at or datetime.now(ZoneInfo("UTC"))).astimezone(ZoneInfo("Asia/Kolkata"))
    history = state.equity_history
    start = state.start_capital
    equity = state.equity
    total_return = (equity / start - 1.0) * 100.0 if start else 0.0

    bench_return = float("nan")
    if history:
        b0, b1 = float(history[0]["benchmark_close"]), float(history[-1]["benchmark_close"])
        if b0:
            bench_return = (b1 / b0 - 1.0) * 100.0
    equity_series = [float(h["equity"]) for h in history]
    max_dd = _max_drawdown_pct(equity_series) if equity_series else 0.0

    closed = sorted(state.closed, key=lambda r: r["exit_date"])
    wins = [r for r in closed if float(r["pnl"]) > 0]
    win_rate = len(wins) / len(closed) * 100.0 if closed else float("nan")
    avg_r = sum(float(r["r_multiple"]) for r in closed) / len(closed) if closed else float("nan")
    cash_pct = state.cash / equity * 100.0 if equity else 0.0

    # ── Headline tiles ───────────────────────────────────────────────────────
    alpha = total_return - bench_return if bench_return == bench_return else float("nan")
    tiles = "".join([
        _tile("Total return", _signed(total_return, "+.2f", "%"), f"since {_e(history[0]['date']) if history else 'start'}"),
        _tile("Nifty500, same period", _signed(bench_return, "+.2f", "%"),
              f"difference {_signed(alpha, '+.2f', ' pp')}" if alpha == alpha else "first snapshot today"),
        _tile("Realized P&L", _signed_inr(state.realized_pnl), f"{len(closed)} closed trade{'s' if len(closed) != 1 else ''}"),
        _tile("Unrealized P&L", _signed_inr(state.unrealized_pnl), f"{len(state.positions)} open position{'s' if len(state.positions) != 1 else ''}"),
        _tile("Cash", inr(state.cash), f"{cash_pct:.0f}% of equity · {len(state.pending)} order{'s' if len(state.pending) != 1 else ''} for next open"),
        _tile("Win rate", f"{win_rate:.0f}%" if win_rate == win_rate else "—",
              f"avg {avg_r:+.2f}R per trade" if avg_r == avg_r else "no closed trades yet"),
        _tile("Max drawdown", f"{max_dd:.1f}%" if equity_series else "—", f"{len(history)} session{'s' if len(history) != 1 else ''} tracked"),
    ])

    # ── Today's activity ─────────────────────────────────────────────────────
    activity = []
    for r in state.filled_today:
        activity.append(["Bought", _e(r["ticker"]), _e(r["sector"]), f'{r["quantity"]} @ {inr(float(r["entry_price"]), 2)}',
                         f'stop {inr(float(r["initial_stop"]), 2)}'])
    for r in state.closed_today:
        activity.append(["Sold", _e(r["ticker"]), _e(r["sector"]), f'{r["quantity"]} @ {inr(float(r["exit_price"]), 2)}',
                         f'{_signed_inr(float(r["pnl"]))} · {_e(r["exit_reason"])}'])
    for r in state.booked_today:
        est = int(r["quantity"]) * float(r["signal_price"])
        action = "Add to winner" if r.get("note", "").startswith("Add #") else "Order placed"
        activity.append([action, _e(r["ticker"]), _e(r["sector"]), f'{r["quantity"]} shares (~{inr(est)})',
                         "buys at next session's open"])
    for r in state.cancelled_today:
        activity.append(["Cancelled", _e(r["ticker"]), _e(r["sector"]), "—", _e(r.get("note", ""))])
    activity_table = _table(["Action", "Ticker", "Sector", "Quantity", "Detail"], activity,
                            "No trades today — nothing filled, sold or ordered.", wrap={4})

    # ── Positions / pending / closed / candidates ───────────────────────────
    positions = sorted(state.positions, key=lambda p: p.unrealized_pnl, reverse=True)
    pos_rows = [[
        f'<strong>{_e(p.ticker)}</strong><br><span class="muted">{_e(p.cap_band)} cap</span>', _e(p.sector),
        _e(p.entry_date), inr(p.entry_price, 2), str(p.quantity), inr(p.last_price, 2),
        inr(p.current_stop, 2), f"{p.to_stop_pct:.1f}%", _signed_inr(p.unrealized_pnl),
        _signed(p.unrealized_pct, "+.1f", "%"), f"{p.r_multiple:+.2f}R", str(p.sessions_held),
    ] for p in positions]
    positions_table = _table(
        ["Ticker", "Sector", "Entry date", "Entry", "Qty", "Last", "Stop", "Room to stop", "P&L", "P&L %", "R", "Sessions"],
        pos_rows, "No open positions.", num=set(range(3, 12)))

    pending_rows = [[
        f'<strong>{_e(r["ticker"])}</strong>', _e(r["sector"]), _e(r["signal_date"]),
        inr(float(r["signal_price"]), 2), r["quantity"],
        inr(float(r["signal_price"]) - float(r["stop_distance"]), 2), inr(float(r["risk_amount"])),
        inr(int(r["quantity"]) * float(r["signal_price"])),
    ] for r in state.pending]
    pending_table = _table(
        ["Ticker", "Sector", "Signal date", "Signal close", "Qty", "Est. stop", "Risk", "Est. cost"],
        pending_rows, "No orders waiting for the next open.", num=set(range(3, 8)))

    closed_rows = [[
        f'<strong>{_e(r["ticker"])}</strong>', _e(r["sector"]), _e(r["entry_date"]), _e(r["exit_date"]),
        inr(float(r["entry_price"]), 2), inr(float(r["exit_price"]), 2), r["quantity"],
        _signed_inr(float(r["pnl"])), _signed(float(r["pnl_pct"]), "+.1f", "%"), f'{float(r["r_multiple"]):+.2f}R',
        _e(r["exit_reason"]),
    ] for r in reversed(closed)]
    closed_table = _table(
        ["Ticker", "Sector", "Entry", "Exit", "Entry ₹", "Exit ₹", "Qty", "P&L", "P&L %", "R", "Exit reason"],
        closed_rows, "No closed trades yet.", num=set(range(4, 10)), wrap={10}, visible=15)

    cand_rows = [[
        f'<strong>{_e(c["ticker"])}</strong>', _e(c["sector"]), _e(c["cap_band"]), inr(c["price"], 2),
        f'{c["rsi"]:.1f}', f'{c["strength"]:.2f}', _e(c["decision"]),
    ] for c in candidates]
    candidates_table = _table(["Ticker", "Sector", "Cap", "Close", "RSI", "Strength", "Decision"],
                              cand_rows, "No stock passed today's screen.", num={3, 4, 5}, wrap={6})

    sector_rows = [[
        f'{i}{" ★" if s.sector in leading else ""}', _e(s.sector), str(s.num_stocks),
        _signed(s.score, "+.1f"), f'{s.relative_strength.get("1M", float("nan")):+.1f}%',
        f'{s.relative_strength.get("3M", float("nan")):+.1f}%', f'{s.relative_strength.get("6M", float("nan")):+.1f}%',
    ] for i, s in enumerate(sector_scores, 1)]
    sector_table = _table(["Rank", "Sector", "Stocks", "Score", "1M RS", "3M RS", "6M RS"],
                          sector_rows, "Sector ranking unavailable.", num=set(range(2, 7)))

    equity_rows = [[
        _e(h["date"]), inr(float(h["equity"])), inr(float(h["cash"])), inr(float(h["market_value"])),
        _signed_inr(float(h["realized_pnl"])), _signed_inr(float(h["unrealized_pnl"])),
        h["open_positions"], f'{float(h["benchmark_close"]):,.2f}',
    ] for h in reversed(history)]
    equity_table = _table(["Date", "Equity", "Cash", "Invested", "Realized", "Unrealized", "Open", "Nifty500"],
                          equity_rows, "No snapshots yet.", num=set(range(1, 8)))

    # ── Market context ───────────────────────────────────────────────────────
    context = []
    if regime:
        context.append(f'<li><strong>Broad market:</strong> {_e(regime.reason)}. '
                       f'<span class="muted">Shown for context only — it no longer blocks trades.</span></li>')
    if flow and not flow.error:
        context.append(f'<li><strong>FII/DII net flow ({_e(flow.date)}):</strong> FII {_signed_inr(flow.fii_net_cr or 0)} cr, '
                       f'DII {_signed_inr(flow.dii_net_cr or 0)} cr <span class="muted">(informational, not backtested)</span></li>')
    context_html = f'<ul class="context">{"".join(context)}</ul>' if context else ""

    # ── Chart data ───────────────────────────────────────────────────────────
    b0 = float(history[0]["benchmark_close"]) if history else 0.0
    chart_data = {
        "dates": [h["date"] for h in history],
        "equity": [round(float(h["equity"]), 2) for h in history],
        "benchmark": [round(start * float(h["benchmark_close"]) / b0, 2) if b0 else None for h in history],
        "trades": [{"label": f'{r["ticker"]} ({r["exit_date"]})', "pnl": round(float(r["pnl"]), 2)} for r in closed],
        "exposure": sorted(
            ({"sector": s, "value": round(sum(p.market_value for p in positions if p.sector == s), 2)}
             for s in {p.sector for p in positions}),
            key=lambda x: -x["value"],
        ),
        "sectors": [{"sector": s.sector + (" ★" if s.sector in leading else ""), "score": round(s.score, 2)}
                    for s in sector_scores],
    }
    chart_json = json.dumps(chart_data).replace("</", "<\\/")

    hero_sub = f"started at {inr(start)}"
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Paper Trading Dashboard</title>
<style>
:root {{
  color-scheme: dark;
  --page: #0d0d0d; --surface: #1a1a19; --ink: #ffffff; --ink-2: #c3c2b7; --muted: #898781;
  --grid: #2c2c2a; --baseline: #383835; --border: rgba(255,255,255,0.10);
  --series-1: #3987e5; --context: #898781; --gain: #3987e5; --loss: #e66767;
  --good: #0ca30c; --critical: #e66767; --accent: #3987e5;
}}
@media (prefers-color-scheme: light) {{
  :root {{
    color-scheme: light;
    --page: #f9f9f7; --surface: #fcfcfb; --ink: #0b0b0b; --ink-2: #52514e; --muted: #898781;
    --grid: #e1e0d9; --baseline: #c3c2b7; --border: rgba(11,11,11,0.10);
    --series-1: #2a78d6; --context: #898781; --gain: #2a78d6; --loss: #e34948;
    --good: #006300; --critical: #d03b3b; --accent: #2a78d6;
  }}
}}
* {{ box-sizing: border-box; }}
body {{ margin: 0; background: var(--page); color: var(--ink);
  font-family: system-ui, -apple-system, "Segoe UI", sans-serif; font-size: 14px; line-height: 1.5; }}
.wrap {{ max-width: 1200px; margin: 0 auto; padding: 24px 16px 48px; }}
header {{ display: flex; flex-wrap: wrap; justify-content: space-between; align-items: flex-end; gap: 12px; margin-bottom: 20px; }}
h1 {{ font-size: 22px; margin: 0; }}
h2 {{ font-size: 15px; margin: 0 0 4px; }}
.sub, .muted {{ color: var(--muted); }}
nav {{ display: flex; flex-wrap: wrap; gap: 16px; }}
nav a {{ color: var(--accent); text-decoration: none; font-weight: 600; }}
.card {{ background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 18px; margin-bottom: 16px; }}
.card-desc {{ color: var(--ink-2); margin: 0 0 12px; font-size: 13px; }}
.hero {{ display: flex; flex-wrap: wrap; gap: 24px; align-items: center; }}
.hero-value {{ font-size: 48px; font-weight: 650; letter-spacing: -0.5px; }}
.tiles {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); gap: 12px; margin-bottom: 16px; }}
.tile {{ background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 14px; }}
.tile-label {{ color: var(--ink-2); font-size: 12px; }}
.tile-value {{ font-size: 22px; font-weight: 600; margin: 2px 0; }}
.tile-sub {{ color: var(--muted); font-size: 12px; }}
.grid-2 {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(340px, 1fr)); gap: 16px; }}
.grid-2 .card {{ margin-bottom: 0; }}
.chart-box {{ position: relative; height: 300px; }}
.chart-box.tall {{ height: 520px; }}
.up {{ color: var(--good); }} .down {{ color: var(--critical); }} .flat {{ color: var(--ink-2); }}
.table-wrap {{ overflow-x: auto; }}
table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
th {{ text-align: left; font-weight: 600; color: var(--ink-2); font-size: 12px; padding: 8px 10px; border-bottom: 1px solid var(--baseline); white-space: nowrap; }}
td {{ padding: 8px 10px; border-bottom: 1px solid var(--grid); vertical-align: top; white-space: nowrap; }}
td.txt {{ white-space: normal; min-width: 180px; }}
th.num, td.num {{ text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }}
.empty {{ color: var(--muted); margin: 8px 0; }}
details {{ margin-top: 10px; }} summary {{ cursor: pointer; color: var(--accent); font-size: 13px; }}
.context {{ margin: 0; padding-left: 18px; color: var(--ink-2); }}
.legend {{ display: flex; gap: 16px; font-size: 12px; color: var(--ink-2); margin-bottom: 8px; }}
.key {{ display: inline-block; width: 14px; height: 2px; vertical-align: middle; margin-right: 6px; }}
.swatch {{ display: inline-block; width: 10px; height: 10px; border-radius: 2px; vertical-align: middle; margin-right: 6px; }}
.note {{ color: var(--ink-2); font-size: 13px; }}
.note li {{ margin-bottom: 4px; }}
footer {{ color: var(--muted); font-size: 12px; margin-top: 24px; }}
</style>
</head>
<body>
<div class="wrap">
<header>
  <div>
    <h1>Paper Trading Dashboard</h1>
    <div class="sub">NSE sector-rotation swing strategy · no real money · prices as of {_e(state.as_of) or "—"} · updated {now_ist:%d %b %Y, %H:%M} IST</div>
  </div>
  <nav><a href="latest.html">Today's screening report</a><a href="reports/index.html">Archive</a><a href="data/paper_trades.csv">Trades CSV</a><a href="data/paper_equity.csv">Equity CSV</a></nav>
</header>

<div class="card hero">
  <div><div class="tile-label">Paper portfolio equity</div><div class="hero-value">{inr(equity)}</div><div class="muted">{hero_sub}</div></div>
  <div class="context-wrap">{context_html}</div>
</div>

<div class="tiles">{tiles}</div>

<div class="card">
  <h2>Equity vs Nifty500</h2>
  <p class="card-desc">The paper portfolio's value each session, against what the same starting capital would be worth in the Nifty500.</p>
  <div class="legend"><span><span class="key" style="background:var(--series-1)"></span>Paper portfolio</span><span><span class="key" style="background:var(--context)"></span>Nifty500 (same start)</span></div>
  <div class="chart-box"><canvas id="equityChart" aria-label="Equity versus Nifty500 line chart" role="img"></canvas><p class="empty" id="equityEmpty" hidden>The curve appears after the second daily run.</p></div>
  <details><summary>Show data table</summary>{equity_table}</details>
</div>

<div class="grid-2">
  <div class="card">
    <h2>Drawdown</h2>
    <p class="card-desc">How far each line is below its own previous high.</p>
    <div class="legend"><span><span class="key" style="background:var(--series-1)"></span>Paper portfolio</span><span><span class="key" style="background:var(--context)"></span>Nifty500</span></div>
    <div class="chart-box"><canvas id="drawdownChart" aria-label="Drawdown line chart" role="img"></canvas><p class="empty" id="drawdownEmpty" hidden>Needs at least two sessions.</p></div>
  </div>
  <div class="card">
    <h2>P&amp;L per closed trade</h2>
    <p class="card-desc">Each bar is one finished trade, after costs ({COST_PCT_PER_SIDE:.2f}% per side).</p>
    <div class="legend"><span><span class="swatch" style="background:var(--gain)"></span>Gain</span><span><span class="swatch" style="background:var(--loss)"></span>Loss</span></div>
    <div class="chart-box"><canvas id="tradesChart" aria-label="Profit and loss per closed trade bar chart" role="img"></canvas><p class="empty" id="tradesEmpty" hidden>No closed trades yet.</p></div>
  </div>
</div>
<div style="height:16px"></div>

<div class="card">
  <h2>Today's activity</h2>
  <p class="card-desc">Orders are placed after the close and fill at the next session's open.</p>
  {activity_table}
</div>

<div class="card">
  <h2>Open positions</h2>
  <p class="card-desc">Stops trail at 2.5×ATR below the highest close. "Room to stop" is how far price can fall before the position is sold.</p>
  {positions_table}
</div>

<div class="grid-2">
  <div class="card">
    <h2>Exposure by sector</h2>
    <p class="card-desc">Market value of open positions.</p>
    <div class="chart-box"><canvas id="exposureChart" aria-label="Exposure by sector bar chart" role="img"></canvas><p class="empty" id="exposureEmpty" hidden>No open positions.</p></div>
  </div>
  <div class="card">
    <h2>Orders for the next open</h2>
    <p class="card-desc">Placed today; cancelled automatically if the stock opens below its stop.</p>
    {pending_table}
  </div>
</div>
<div style="height:16px"></div>

<div class="card">
  <h2>Today's screening candidates</h2>
  <p class="card-desc">Every stock that passed RSI/MACD, the Trend Template and the governance check in the leading sectors, and what the portfolio rules decided.</p>
  {candidates_table}
</div>

<div class="grid-2">
  <div class="card">
    <h2>Sector ranking today</h2>
    <p class="card-desc">Relative strength vs Nifty500 (weighted 1M/3M/6M). ★ = the 4 leading sectors screened today.</p>
    <div class="legend"><span><span class="swatch" style="background:var(--gain)"></span>Beating Nifty500</span><span><span class="swatch" style="background:var(--loss)"></span>Lagging</span></div>
    <div class="chart-box tall"><canvas id="sectorChart" aria-label="Sector relative strength bar chart" role="img"></canvas></div>
  </div>
  <div class="card">
    <h2>Sector ranking table</h2>
    <p class="card-desc">&nbsp;</p>
    {sector_table}
  </div>
</div>
<div style="height:16px"></div>

<div class="card">
  <h2>Closed trades</h2>
  {closed_table}
</div>

<div class="card note">
  <h2>How this paper account works</h2>
  <ul>
    <li>Starts with {inr(start)}. Each weekday evening the screener ranks sectors, screens the 4 leaders, and places orders for qualifying stocks. Orders fill at the next session's opening price.</li>
    <li>Sizing: risk 0.75% / 0.56% / 0.375% of capital per trade for large / mid / small caps; at most 85% of capital invested, 8% total risk and 20% in any one stock.</li>
    <li>Up to 15 stocks. Each of the 4 leading sectors gets 2 slots; the other 7 go to sectors in proportion to their relative-strength score. A stock already held that qualifies again is bought again if it is above its last buy price and 5+ sessions have passed — at half, then a quarter, of the normal risk, at most twice.</li>
    <li>Exits: initial stop at min(2×ATR, 8%) below entry, trailing at 2.5×ATR below the highest close; sell at the next open if RSI closes below 45 or after 26 sessions.</li>
    <li>Costs of {COST_PCT_PER_SIDE:.2f}% on every buy and sell (STT, stamp duty, exchange and DP charges).</li>
    <li>The broad-market (200-day) filter was turned off on 1 Oct 2026; it is still shown above for context.</li>
    <li>What to expect, from the 8.8-year backtest of this exact setup: about {BACKTEST_REFERENCE['strategy']['cagr']:.1f}% a year with drawdowns up to {BACKTEST_REFERENCE['strategy']['max_dd']:.0f}% and a win rate near {BACKTEST_REFERENCE['strategy']['win_rate']:.0f}%, against {BACKTEST_REFERENCE['nifty500']['cagr']:.1f}% a year and {BACKTEST_REFERENCE['nifty500']['max_dd']:.0f}% for simply holding the Nifty500. Most trades lose a little; a few large winners carry the result. Three months is a short sample — judge it against the index, not in isolation.</li>
  </ul>
</div>

<footer>Paper trading only — not financial advice. Data: Yahoo Finance end-of-day prices, NSE, Screener.in.</footer>
</div>

<script type="application/json" id="chartData">{chart_json}</script>
<script src="{CHART_JS}"></script>
<script>
(function () {{
  const data = JSON.parse(document.getElementById('chartData').textContent);
  if (typeof Chart === 'undefined') return;
  const css = getComputedStyle(document.documentElement);
  const v = n => css.getPropertyValue(n).trim();
  const C = {{ ink: v('--ink'), ink2: v('--ink-2'), muted: v('--muted'), grid: v('--grid'), base: v('--baseline'),
               s1: v('--series-1'), ctx: v('--context'), gain: v('--gain'), loss: v('--loss'), surface: v('--surface') }};
  Chart.defaults.color = C.muted;
  Chart.defaults.font.family = 'system-ui, -apple-system, "Segoe UI", sans-serif';
  Chart.defaults.font.size = 12;
  const inr = x => (x < 0 ? '−₹' : '₹') + Math.abs(Math.round(x)).toLocaleString('en-IN');
  const tooltip = {{ backgroundColor: C.surface, borderColor: C.base, borderWidth: 1, titleColor: C.ink2,
                     bodyColor: C.ink, padding: 10, boxPadding: 4, usePointStyle: true }};
  const axis = {{ grid: {{ color: C.grid }}, border: {{ color: C.base }}, ticks: {{ color: C.muted }} }};
  const show = (id, ok) => {{ if (!ok) document.getElementById(id).hidden = false; return ok; }};

  const crosshair = {{ id: 'crosshair', afterDatasetsDraw(chart) {{
    const a = chart.tooltip && chart.tooltip.getActiveElements();
    if (!a || !a.length) return;
    const x = a[0].element.x, {{ top, bottom }} = chart.chartArea, ctx = chart.ctx;
    ctx.save(); ctx.strokeStyle = C.base; ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(x, top); ctx.lineTo(x, bottom); ctx.stroke(); ctx.restore();
  }} }};
  const endLabels = fmt => ({{ id: 'endLabels', afterDatasetsDraw(chart) {{
    const ctx = chart.ctx; ctx.save(); ctx.font = '12px system-ui, sans-serif'; ctx.fillStyle = C.ink2; ctx.textAlign = 'left';
    chart.data.datasets.forEach((ds, i) => {{
      const pts = chart.getDatasetMeta(i).data; const last = pts[pts.length - 1];
      const val = ds.data[ds.data.length - 1]; if (!last || val == null) return;
      ctx.fillText(ds.label + ' ' + fmt(val), last.x + 8, last.y + 4);
    }}); ctx.restore();
  }} }});
  const lineDs = (label, values, color, fill) => ({{ label, data: values, borderColor: color, backgroundColor: color + '1a',
    borderWidth: 2, pointRadius: 0, pointHoverRadius: 4, pointHoverBorderWidth: 2, pointHoverBorderColor: C.surface,
    pointHoverBackgroundColor: color, tension: 0, fill: fill ? 'origin' : false, borderJoinStyle: 'round', borderCapStyle: 'round' }});
  const lineOpts = (fmt, rightPad) => ({{ responsive: true, maintainAspectRatio: false, animation: false,
    interaction: {{ mode: 'index', intersect: false }}, layout: {{ padding: {{ right: rightPad }} }},
    plugins: {{ legend: {{ display: false }}, tooltip: {{ ...tooltip, callbacks: {{ label: c => ' ' + c.dataset.label + ': ' + fmt(c.parsed.y) }} }} }},
    scales: {{ x: {{ ...axis, grid: {{ display: false }}, ticks: {{ color: C.muted, maxTicksLimit: 6, maxRotation: 0,
               callback(v) {{ const d = new Date(this.getLabelForValue(v)); return d.toLocaleDateString('en-IN', {{ day: 'numeric', month: 'short' }}); }} }} }},
               y: {{ ...axis, ticks: {{ color: C.muted, callback: fmt }} }} }} }});

  if (show('equityEmpty', data.dates.length >= 2)) {{
    new Chart(document.getElementById('equityChart'), {{ type: 'line',
      data: {{ labels: data.dates, datasets: [lineDs('Paper', data.equity, C.s1, false), lineDs('Nifty500', data.benchmark, C.ctx, false)] }},
      options: lineOpts(inr, 120), plugins: [crosshair, endLabels(inr)] }});
  }}

  const dd = arr => {{ let peak = -Infinity; return arr.map(x => {{ if (x == null) return null; peak = Math.max(peak, x); return +((x / peak - 1) * 100).toFixed(2); }}); }};
  const pct = x => x.toFixed(1) + '%';
  if (show('drawdownEmpty', data.dates.length >= 2)) {{
    new Chart(document.getElementById('drawdownChart'), {{ type: 'line',
      data: {{ labels: data.dates, datasets: [lineDs('Paper', dd(data.equity), C.s1, true), lineDs('Nifty500', dd(data.benchmark), C.ctx, false)] }},
      options: lineOpts(pct, 10), plugins: [crosshair] }});
  }}

  const barOpts = (horizontal, fmt) => ({{ responsive: true, maintainAspectRatio: false, animation: false,
    indexAxis: horizontal ? 'y' : 'x',
    plugins: {{ legend: {{ display: false }}, tooltip: {{ ...tooltip, callbacks: {{ label: c => ' ' + fmt(horizontal ? c.parsed.x : c.parsed.y) }} }} }},
    scales: {{ x: {{ ...axis, grid: {{ display: horizontal, color: C.grid }}, ticks: {{ color: C.muted, maxRotation: 0, ...(horizontal ? {{ callback: fmt }} : {{}}) }} }},
               y: {{ ...axis, grid: {{ display: !horizontal, color: C.grid }}, ticks: {{ color: C.muted, autoSkip: false, ...(horizontal ? {{}} : {{ callback: fmt }}) }} }} }} }});
  const barDs = (values, colors) => ({{ data: values, backgroundColor: colors, maxBarThickness: 24,
    borderRadius: {{ topLeft: 4, topRight: 4, bottomLeft: 4, bottomRight: 4 }}, borderSkipped: 'start', borderWidth: 0,
    hoverBackgroundColor: colors }});

  if (show('tradesEmpty', data.trades.length > 0)) {{
    const vals = data.trades.map(t => t.pnl);
    const opts = barOpts(false, inr); opts.scales.x.ticks.display = data.trades.length <= 20;
    new Chart(document.getElementById('tradesChart'), {{ type: 'bar',
      data: {{ labels: data.trades.map(t => t.label), datasets: [barDs(vals, vals.map(x => x >= 0 ? C.gain : C.loss))] }},
      options: opts }});
  }}

  if (show('exposureEmpty', data.exposure.length > 0)) {{
    new Chart(document.getElementById('exposureChart'), {{ type: 'bar',
      data: {{ labels: data.exposure.map(e => e.sector), datasets: [barDs(data.exposure.map(e => e.value), C.s1)] }},
      options: barOpts(true, inr) }});
  }}

  const sv = data.sectors.map(s => s.score);
  new Chart(document.getElementById('sectorChart'), {{ type: 'bar',
    data: {{ labels: data.sectors.map(s => s.sector), datasets: [barDs(sv, sv.map(x => x >= 0 ? C.gain : C.loss))] }},
    options: barOpts(true, x => (x > 0 ? '+' : '') + Number(x).toFixed(1)) }});
}})();
</script>
</body>
</html>"""


def publish_dashboard(html_text: str, docs_dir: str = DOCS_DIR) -> str:
    """Writes docs/index.html and copies the paper-trading CSVs to docs/data/ for download."""
    os.makedirs(docs_dir, exist_ok=True)
    path = os.path.join(docs_dir, "index.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(html_text)
    data_dir = os.path.join(docs_dir, "data")
    os.makedirs(data_dir, exist_ok=True)
    for src in (LEDGER_PATH, EQUITY_PATH):
        if os.path.exists(src):
            shutil.copyfile(src, os.path.join(data_dir, os.path.basename(src)))
    logger.info("Dashboard published: %s", path)
    return path
