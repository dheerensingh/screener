"""
Backtest: HTML Report

Renders equity curve (inline SVG — no new plotting dependency), trade log, and
the cross-variant comparison table, reusing email_alerter.PALETTE for visual
consistency with the live daily report.
"""

import os
from datetime import datetime

import pandas as pd

from modules.email_alerter import PALETTE

from .metrics import BacktestMetrics
from .simulator import BacktestConfig, Trade

DOCS_DIR = "docs"
REPORT_PATH = os.path.join(DOCS_DIR, "backtest_report.html")


def _equity_svg(equity: pd.Series, width: int = 880, height: int = 240) -> str:
    if len(equity) < 2:
        return "<p>Insufficient data for an equity curve.</p>"
    values = equity.values
    vmin, vmax = float(values.min()), float(values.max())
    vrange = vmax - vmin or 1.0
    n = len(values)
    points = " ".join(
        f"{(i / (n - 1)) * width:.1f},{height - ((v - vmin) / vrange) * height:.1f}"
        for i, v in enumerate(values)
    )
    baseline_y = height - ((equity.iloc[0] - vmin) / vrange) * height
    return f"""
    <svg viewBox="0 0 {width} {height}" style="width:100%;height:auto;background:{PALETTE['bg']};border-radius:8px">
      <line x1="0" y1="{baseline_y:.1f}" x2="{width}" y2="{baseline_y:.1f}"
            stroke="{PALETTE['border']}" stroke-dasharray="4,4" stroke-width="1"/>
      <polyline points="{points}" fill="none" stroke="{PALETTE['accent']}" stroke-width="2"/>
    </svg>"""


def _trade_rows(trades: list[Trade], limit: int = 60) -> str:
    if not trades:
        return f'<tr><td colspan="8" style="text-align:center;padding:16px;color:{PALETTE["muted"]}">No trades.</td></tr>'
    rows = []
    for t in sorted(trades, key=lambda x: x.entry_date, reverse=True)[:limit]:
        r_colour = PALETTE["green"] if t.r_multiple > 0 else PALETTE["red"]
        rows.append(f"""
        <tr style="border-bottom:1px solid {PALETTE['border']}">
          <td style="padding:6px 10px">{t.ticker}</td>
          <td style="padding:6px 10px">{t.sector}</td>
          <td style="padding:6px 10px">{t.entry_date.date()}</td>
          <td style="padding:6px 10px">{t.exit_date.date() if t.exit_date else '-'}</td>
          <td style="padding:6px 10px;text-align:right">₹{t.entry_price:,.2f}</td>
          <td style="padding:6px 10px;text-align:right">₹{t.exit_price:,.2f}</td>
          <td style="padding:6px 10px;text-align:right;color:{r_colour};font-weight:700">{t.r_multiple:+.2f}R</td>
          <td style="padding:6px 10px;font-size:11px;color:{PALETTE['muted']}">{t.exit_reason}</td>
        </tr>""")
    note = f'<p style="font-size:11px;color:{PALETTE["muted"]}">Showing {limit} most recent of {len(trades)} trades.</p>' \
        if len(trades) > limit else ""
    return "\n".join(rows) + note


def _comparison_table(results: list[tuple]) -> str:
    rows = []
    for config, _, _, m in results:
        rows.append(f"""
        <tr style="border-bottom:1px solid {PALETTE['border']}">
          <td style="padding:6px 10px">{config.entry_paradigm}</td>
          <td style="padding:6px 10px">{config.exit_paradigm}</td>
          <td style="padding:6px 10px;text-align:right">{m.total_trades}</td>
          <td style="padding:6px 10px;text-align:right">{m.win_rate_pct:.1f}%</td>
          <td style="padding:6px 10px;text-align:right">{m.avg_r_multiple:+.2f}</td>
          <td style="padding:6px 10px;text-align:right">{m.cagr_pct:+.1f}%</td>
          <td style="padding:6px 10px;text-align:right">{m.max_drawdown_pct:.1f}%</td>
          <td style="padding:6px 10px;text-align:right">{m.sharpe_ratio:.2f}</td>
        </tr>""")
    return "\n".join(rows)


def build_backtest_report(
    best_config: BacktestConfig,
    best_trades: list[Trade],
    best_equity: pd.Series,
    best_metrics: BacktestMetrics,
    all_results: list[tuple],
) -> str:
    today_str = datetime.now().strftime("%d %b %Y")
    sector_rows = "\n".join(
        f'<tr><td style="padding:6px 10px">{sector}</td>'
        f'<td style="padding:6px 10px;text-align:right">{s["trades"]}</td>'
        f'<td style="padding:6px 10px;text-align:right">{s["win_rate_pct"]:.1f}%</td>'
        f'<td style="padding:6px 10px;text-align:right">{s["avg_r"]:+.2f}</td></tr>'
        for sector, s in sorted(best_metrics.per_sector.items(), key=lambda x: -x[1]["avg_r"])
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Backtest Report — {today_str}</title></head>
<body style="margin:0;padding:0;background:{PALETTE['bg']};font-family:'Segoe UI',Arial,sans-serif;color:{PALETTE['text']}">
<div style="max-width:960px;margin:32px auto;padding:0 16px">

  <div style="background:linear-gradient(135deg,#1e40af,#0369a1);border-radius:12px 12px 0 0;padding:24px 28px">
    <h1 style="margin:0;font-size:22px">📈 5-Year Backtest — Sector Rotation + Momentum Strategy</h1>
    <p style="margin:6px 0 0;color:#bae6fd;font-size:14px">Generated {today_str} · Top CAGR/MaxDD variant:
      <strong>{best_config.entry_paradigm} entry / {best_config.exit_paradigm} exit</strong>
      (a single mechanical ranking — see STRATEGY.md §8 for the full comparison and reasoning)</p>
  </div>

  <div style="background:{PALETTE['card']};padding:20px 28px">
    <h2 style="margin:0 0 12px;font-size:14px;text-transform:uppercase;letter-spacing:1px;color:{PALETTE['muted']}">
      Equity Curve (best variant)</h2>
    {_equity_svg(best_equity)}
    <p style="margin:12px 0 0;font-size:13px">
      Trades: <strong>{best_metrics.total_trades}</strong> ·
      Win rate: <strong>{best_metrics.win_rate_pct:.1f}%</strong> ·
      Avg R: <strong>{best_metrics.avg_r_multiple:+.2f}</strong> ·
      CAGR: <strong>{best_metrics.cagr_pct:+.1f}%</strong> ·
      Max drawdown: <strong style="color:{PALETTE['red']}">{best_metrics.max_drawdown_pct:.1f}%</strong> ·
      Sharpe: <strong>{best_metrics.sharpe_ratio:.2f}</strong> ·
      Avg holding: <strong>{best_metrics.avg_holding_days:.0f}d</strong>
    </p>
  </div>

  <div style="background:{PALETTE['card']};margin-top:16px;padding:20px 28px;border-radius:8px">
    <h2 style="margin:0 0 12px;font-size:14px;text-transform:uppercase;letter-spacing:1px;color:{PALETTE['muted']}">
      Variant Comparison (all 6 entry × exit combinations)</h2>
    <table style="width:100%;border-collapse:collapse;font-size:13px">
      <thead><tr style="text-transform:uppercase;font-size:10px;color:{PALETTE['muted']}">
        <th style="padding:6px 10px;text-align:left">Entry</th>
        <th style="padding:6px 10px;text-align:left">Exit</th>
        <th style="padding:6px 10px;text-align:right">Trades</th>
        <th style="padding:6px 10px;text-align:right">Win %</th>
        <th style="padding:6px 10px;text-align:right">Avg R</th>
        <th style="padding:6px 10px;text-align:right">CAGR</th>
        <th style="padding:6px 10px;text-align:right">Max DD</th>
        <th style="padding:6px 10px;text-align:right">Sharpe</th>
      </tr></thead>
      <tbody>{_comparison_table(all_results)}</tbody>
    </table>
  </div>

  <div style="background:{PALETTE['card']};margin-top:16px;padding:20px 28px;border-radius:8px">
    <h2 style="margin:0 0 12px;font-size:14px;text-transform:uppercase;letter-spacing:1px;color:{PALETTE['muted']}">
      Per-Sector Contribution (best variant)</h2>
    <table style="width:100%;border-collapse:collapse;font-size:13px">
      <thead><tr style="text-transform:uppercase;font-size:10px;color:{PALETTE['muted']}">
        <th style="padding:6px 10px;text-align:left">Sector</th>
        <th style="padding:6px 10px;text-align:right">Trades</th>
        <th style="padding:6px 10px;text-align:right">Win %</th>
        <th style="padding:6px 10px;text-align:right">Avg R</th>
      </tr></thead>
      <tbody>{sector_rows}</tbody>
    </table>
  </div>

  <div style="background:{PALETTE['card']};margin-top:16px;padding:20px 28px;border-radius:8px">
    <h2 style="margin:0 0 12px;font-size:14px;text-transform:uppercase;letter-spacing:1px;color:{PALETTE['muted']}">
      Trade Log (best variant)</h2>
    <table style="width:100%;border-collapse:collapse;font-size:12px">
      <thead><tr style="text-transform:uppercase;font-size:10px;color:{PALETTE['muted']}">
        <th style="padding:6px 10px;text-align:left">Ticker</th><th style="padding:6px 10px;text-align:left">Sector</th>
        <th style="padding:6px 10px;text-align:left">Entry</th><th style="padding:6px 10px;text-align:left">Exit</th>
        <th style="padding:6px 10px;text-align:right">Entry ₹</th><th style="padding:6px 10px;text-align:right">Exit ₹</th>
        <th style="padding:6px 10px;text-align:right">R</th><th style="padding:6px 10px;text-align:left">Reason</th>
      </tr></thead>
      <tbody>{_trade_rows(best_trades)}</tbody>
    </table>
  </div>

  <div style="margin-top:16px;padding:16px 20px;background:{PALETTE['card']};border-radius:8px;
              border-left:4px solid {PALETTE['border']}">
    <p style="margin:0;font-size:11px;color:{PALETTE['muted']};line-height:1.6">
      <strong>Limitations:</strong> uses today's Nifty500/sector classification applied retroactively
      (not a true point-in-time reconstruction); transaction costs modeled at 0.1% round-trip;
      sector synthetic indices built from all sufficiently-historied members, not the liquidity-capped
      40 used for live stock selection. See STRATEGY.md for full methodology.
    </p>
  </div>

</div>
</body></html>"""


def publish_backtest_report(html: str, path: str = REPORT_PATH) -> str:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    return path
