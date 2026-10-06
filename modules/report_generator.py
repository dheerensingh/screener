"""
Module 5: HTML Report Generation

Renders the full daily report (sector rotation table, fundamental confirmation,
qualifying momentum stocks with position sizing) as a styled HTML page, reusing
email_alerter.PALETTE for a consistent look. Publishes it to `docs/` for GitHub
Pages: a permanent dated copy under `docs/reports/`, an overwritten
`docs/latest.html` so the newest report has a stable URL, and a
regenerated `docs/reports/index.html` archive list.
"""

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from .email_alerter import PALETTE
from .fii_dii_flow import FlowResult
from .fundamental_analyzer import FundamentalResult
from .governance_analyzer import GovernanceResult
from .market_regime import MarketRegime
from .position_sizer import PositionSize
from .sector_rotation import SectorScore
from .technical_analyzer import ScreenResult

logger = logging.getLogger(__name__)

DOCS_DIR = "docs"
REPORTS_SUBDIR = "reports"


@dataclass
class SectorBundle:
    """Everything the report needs for one leading sector."""
    score: SectorScore
    fundamental_summary: str
    fundamental_results: list[FundamentalResult] = field(default_factory=list)
    qualified: list[ScreenResult] = field(default_factory=list)
    all_results: list[ScreenResult] = field(default_factory=list)
    position_sizes: dict[str, PositionSize] = field(default_factory=dict)
    governance_summary: str = ""
    governance_results: list[GovernanceResult] = field(default_factory=list)


def _sector_rotation_table(all_scores: list[SectorScore], leading: set[str]) -> str:
    rows = []
    for rank, s in enumerate(all_scores, 1):
        is_leading = s.sector in leading
        row_style = (
            f"background:{PALETTE['card']};border-left:3px solid {PALETTE['accent']}"
            if is_leading else "border-left:3px solid transparent"
        )
        score_colour = PALETTE["green"] if s.score > 0 else PALETTE["red"]
        rows.append(f"""
        <tr style="{row_style}">
          <td style="padding:8px 14px">{rank}{"  ★" if is_leading else ""}</td>
          <td style="padding:8px 14px;font-weight:600">{s.sector}</td>
          <td style="padding:8px 14px;text-align:right;font-size:12px;color:{PALETTE['muted']}">{s.num_stocks}</td>
          <td style="padding:8px 14px;text-align:right;color:{score_colour};font-weight:700">{s.score:+.1f}</td>
          <td style="padding:8px 14px;text-align:right;font-size:12px;color:{PALETTE['muted']}">
              {s.relative_strength.get('1M', float('nan')):+.1f}%</td>
          <td style="padding:8px 14px;text-align:right;font-size:12px;color:{PALETTE['muted']}">
              {s.relative_strength.get('3M', float('nan')):+.1f}%</td>
          <td style="padding:8px 14px;text-align:right;font-size:12px;color:{PALETTE['muted']}">
              {s.relative_strength.get('6M', float('nan')):+.1f}%</td>
        </tr>""")
    return "\n".join(rows)


def _cap_band_colour(cap_band: str) -> str:
    return {"Large": PALETTE["green"], "Mid": PALETTE["amber"], "Small": PALETTE["red"]}.get(
        cap_band, PALETTE["muted"]
    )


def _stock_rows(bundle: SectorBundle) -> str:
    if not bundle.qualified:
        return (
            f'<tr><td colspan="9" style="text-align:center;padding:20px;'
            f'color:{PALETTE["muted"]}">No stocks qualified in this sector today.</td></tr>'
        )
    rows = []
    for r in bundle.qualified:
        pos = bundle.position_sizes.get(r.ticker)
        qty = pos.quantity if pos else 0
        alloc_pct = pos.allocation_pct if pos else 0.0
        stop_price = pos.stop_price if pos else float("nan")
        formula = pos.formula if pos else "n/a"
        tags = []
        if r.volume_surge:
            tags.append("Vol↑")
        if r.near_52w_high:
            tags.append("Near 52w High")
        if (r.rel_strength_vs_sector or 0) > 0:
            tags.append("RS > Sector")
        if (r.delivery_pct or 0) > 50.0:
            tags.append(f"Delivery {r.delivery_pct:.0f}%")
        tags_html = " ".join(
            f'<span style="background:{PALETTE["border"]};border-radius:4px;padding:2px 6px;'
            f'font-size:10px;margin-right:4px">{t}</span>' for t in tags
        ) or f'<span style="color:{PALETTE["muted"]};font-size:11px">—</span>'

        cap_colour = _cap_band_colour(r.cap_band)
        rows.append(f"""
        <tr style="border-bottom:1px solid {PALETTE['border']}">
          <td style="padding:10px 14px;font-weight:600;color:{PALETTE['accent']}">{r.ticker}<br>
              <span style="font-weight:400;font-size:10px;color:{cap_colour}">{r.cap_band or '—'} Cap</span></td>
          <td style="padding:10px 14px;text-align:right">₹{r.current_price:,.2f}</td>
          <td style="padding:10px 14px;text-align:right">{r.rsi:.1f}</td>
          <td style="padding:10px 14px">{tags_html}</td>
          <td style="padding:10px 14px;text-align:right">₹{stop_price:,.2f}</td>
          <td style="padding:10px 14px;text-align:right">{qty}</td>
          <td style="padding:10px 14px;text-align:right">₹{(qty * r.current_price):,.0f}</td>
          <td style="padding:10px 14px;text-align:right;font-weight:700">{alloc_pct:.1f}%</td>
          <td style="padding:10px 14px;font-size:10px;color:{PALETTE['muted']};max-width:260px">{formula}</td>
        </tr>""")
    return "\n".join(rows)


def _fundamental_badge(fr: FundamentalResult) -> str:
    if fr.yoy_growth_pct is None:
        return f"{fr.ticker}: n/a"
    sign = "+" if fr.yoy_growth_pct > 0 else ""
    return f"{fr.ticker}: {sign}{fr.yoy_growth_pct:.0f}% YoY"


def _governance_badge(gr: GovernanceResult) -> str:
    if gr.pledge_flag:
        return (f'<span style="color:{PALETTE["red"]};font-weight:700">'
                f'{gr.ticker}: REJECTED — {gr.pledge_text}</span>')
    if gr.error:
        return f'{gr.ticker}: n/a'
    parts = [f"promoter {gr.promoter_holding_pct:.1f}%"] if gr.promoter_holding_pct is not None else []
    if gr.declining_promoter_holding_flag:
        parts.append(
            f'<span style="color:{PALETTE["amber"]}">declining '
            f'({gr.promoter_holding_trend_pct_pts:+.1f}pp YoY)</span>'
        )
    return f"{gr.ticker}: " + (", ".join(parts) if parts else "n/a")


def _sector_section(bundle: SectorBundle) -> str:
    fund_rows = "".join(
        f'<span style="margin-right:10px">{_fundamental_badge(fr)}</span>'
        for fr in bundle.fundamental_results
    )
    gov_rows = "".join(
        f'<span style="margin-right:10px">{_governance_badge(gr)}</span>'
        for gr in bundle.governance_results
    )
    return f"""
    <div style="background:{PALETTE['card']};margin-top:20px;border-radius:8px;overflow:hidden;
                border:1px solid {PALETTE['border']}">
      <div style="padding:16px 20px;border-bottom:1px solid {PALETTE['border']}">
        <h2 style="margin:0;font-size:18px;color:{PALETTE['accent']}">{bundle.score.sector}</h2>
        <p style="margin:6px 0 0;font-size:12px;color:{PALETTE['muted']}">
          Fundamental confirmation: <strong style="color:{PALETTE['text']}">{bundle.fundamental_summary}</strong>
        </p>
        <p style="margin:6px 0 0;font-size:11px;color:{PALETTE['muted']}">{fund_rows}</p>
        <p style="margin:6px 0 0;font-size:12px;color:{PALETTE['muted']}">
          Governance check: <strong style="color:{PALETTE['text']}">{bundle.governance_summary}</strong>
        </p>
        <p style="margin:6px 0 0;font-size:11px;color:{PALETTE['muted']}">{gov_rows}</p>
      </div>
      <table style="width:100%;border-collapse:collapse;font-size:13px">
        <thead>
          <tr style="background:{PALETTE['bg']};text-transform:uppercase;font-size:10px;
                     letter-spacing:0.6px;color:{PALETTE['muted']}">
            <th style="padding:8px 14px;text-align:left">Ticker</th>
            <th style="padding:8px 14px;text-align:right">Entry</th>
            <th style="padding:8px 14px;text-align:right">RSI</th>
            <th style="padding:8px 14px;text-align:left">Signals</th>
            <th style="padding:8px 14px;text-align:right">Stop</th>
            <th style="padding:8px 14px;text-align:right">Qty</th>
            <th style="padding:8px 14px;text-align:right">₹ Alloc</th>
            <th style="padding:8px 14px;text-align:right">% Capital</th>
            <th style="padding:8px 14px;text-align:left">Sizing formula</th>
          </tr>
        </thead>
        <tbody>{_stock_rows(bundle)}</tbody>
      </table>
    </div>"""


def _market_context_block(regime: Optional[MarketRegime], flow: Optional[FlowResult]) -> str:
    regime_line = f"Market regime: <strong style=\"color:{PALETTE['text']}\">{regime.reason}</strong>" if regime else ""

    flow_line = ""
    if flow is not None and not flow.error:
        fii_colour = PALETTE["green"] if (flow.fii_net_cr or 0) >= 0 else PALETTE["red"]
        dii_colour = PALETTE["green"] if (flow.dii_net_cr or 0) >= 0 else PALETTE["red"]
        flow_line = (
            f'FII/DII net flow ({flow.date}, <em>informational, not backtested</em>): '
            f'FII <span style="color:{fii_colour};font-weight:700">₹{flow.fii_net_cr or 0:+,.0f} cr</span>, '
            f'DII <span style="color:{dii_colour};font-weight:700">₹{flow.dii_net_cr or 0:+,.0f} cr</span>'
        )

    if not regime_line and not flow_line:
        return ""

    return f"""
    <div style="background:{PALETTE['card']};padding:16px 28px;border-radius:8px;margin-bottom:20px;
                font-size:13px;color:{PALETTE['muted']}">
      <h2 style="margin:0 0 8px;font-size:14px;text-transform:uppercase;letter-spacing:1px;
                 color:{PALETTE['muted']}">Market Context</h2>
      {f'<p style="margin:4px 0">{regime_line}</p>' if regime_line else ''}
      {f'<p style="margin:4px 0">{flow_line}</p>' if flow_line else ''}
    </div>"""


def build_report_html(
    all_scores: list[SectorScore],
    bundles: list[SectorBundle],
    account_size: float,
    errors: list[str],
    generated_at: Optional[datetime] = None,
    regime: Optional[MarketRegime] = None,
    flow: Optional[FlowResult] = None,
) -> str:
    generated_at = generated_at or datetime.now()
    today_str = generated_at.strftime("%d %b %Y")
    leading_names = {b.score.sector for b in bundles}

    rotation_table = _sector_rotation_table(all_scores, leading_names)
    market_context = _market_context_block(regime, flow)
    sector_sections = "\n".join(_sector_section(b) for b in bundles)

    error_block = ""
    if errors:
        error_items = "".join(f"<li>{e}</li>" for e in errors)
        error_block = f"""
        <div style="margin-top:24px;padding:16px;background:#7f1d1d;border-radius:8px;
                    border-left:4px solid {PALETTE['red']}">
          <p style="margin:0 0 8px;font-weight:700;color:{PALETTE['red']}">⚠ Errors / Warnings</p>
          <ul style="margin:0;padding-left:18px;color:#fca5a5;font-size:13px">{error_items}</ul>
        </div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Sector Screener Report — {today_str}</title>
</head>
<body style="margin:0;padding:0;background:{PALETTE['bg']};font-family:'Segoe UI',Arial,sans-serif;
             color:{PALETTE['text']}">
<div style="max-width:960px;margin:32px auto;padding:0 16px">

  <div style="background:linear-gradient(135deg,#1e40af,#0369a1);border-radius:12px 12px 0 0;padding:24px 28px">
    <h1 style="margin:0;font-size:22px">📊 Sector Rotation + Momentum Screener</h1>
    <p style="margin:6px 0 0;color:#bae6fd;font-size:14px">
      {today_str} · NSE India · Account size ₹{account_size:,.0f}</p>
    <p style="margin:8px 0 0;font-size:13px"><a href="../index.html" style="color:#e0f2fe;font-weight:600">← Paper trading dashboard</a></p>
  </div>

  <div style="background:{PALETTE['card']};padding:20px 28px;border-radius:0 0 8px 8px;
              margin-bottom:20px">
    <h2 style="margin:0 0 12px;font-size:14px;text-transform:uppercase;letter-spacing:1px;
               color:{PALETTE['muted']}">Sector Rotation Ranking</h2>
    <table style="width:100%;border-collapse:collapse;font-size:13px">
      <thead>
        <tr style="text-transform:uppercase;font-size:10px;letter-spacing:0.6px;color:{PALETTE['muted']}">
          <th style="padding:8px 14px;text-align:left">Rank</th>
          <th style="padding:8px 14px;text-align:left">Sector</th>
          <th style="padding:8px 14px;text-align:right"># Stocks</th>
          <th style="padding:8px 14px;text-align:right">Composite RS</th>
          <th style="padding:8px 14px;text-align:right">1M RS</th>
          <th style="padding:8px 14px;text-align:right">3M RS</th>
          <th style="padding:8px 14px;text-align:right">6M RS</th>
        </tr>
      </thead>
      <tbody>{rotation_table}</tbody>
    </table>
    <p style="margin:12px 0 0;font-size:11px;color:{PALETTE['muted']}">
      ★ = leading sector(s), screened for momentum stocks below. RS = relative strength
      vs the Nifty500 benchmark (synthetic sector index — see CLAUDE.md for methodology).
    </p>
  </div>

  {market_context}

  {sector_sections}

  {error_block}

  <div style="margin-top:24px;padding:16px 20px;background:{PALETTE['card']};border-radius:8px;
              border-left:4px solid {PALETTE['border']}">
    <p style="margin:0;font-size:11px;color:{PALETTE['muted']};line-height:1.6">
      <strong>Disclaimer:</strong> This report is generated automatically for informational
      purposes only and does not constitute financial advice. Position sizing formulas are
      shown so you can verify the math yourself. Always perform your own due diligence
      before making any investment decisions. Past technical/fundamental signals do not
      guarantee future performance.
    </p>
  </div>

  <p style="text-align:center;font-size:11px;color:{PALETTE['muted']};margin:20px 0 32px">
    Generated by Sector Screener Bot · Runs weekdays after the NSE close via GitHub Actions
  </p>

</div>
</body>
</html>"""


def _archive_index_html(report_dates: list[str]) -> str:
    items = "\n".join(
        f'<li style="padding:8px 0;border-bottom:1px solid {PALETTE["border"]}">'
        f'<a href="{d}.html" style="color:{PALETTE["accent"]}">{d}</a></li>'
        for d in sorted(report_dates, reverse=True)
    )
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><title>Report Archive</title></head>
<body style="margin:0;padding:32px;background:{PALETTE['bg']};font-family:'Segoe UI',Arial,sans-serif;
             color:{PALETTE['text']}">
<div style="max-width:480px;margin:0 auto">
  <h1 style="font-size:18px">Report Archive</h1>
  <ul style="list-style:none;padding:0;margin:0">{items}</ul>
  <p style="margin-top:20px"><a href="../index.html" style="color:{PALETTE['accent']}">← Dashboard</a></p>
</div>
</body></html>"""


def publish_report(html: str, docs_dir: str = DOCS_DIR, date: Optional[datetime] = None) -> tuple[str, str]:
    """
    Writes the report to docs/reports/YYYY-MM-DD.html (permanent), overwrites
    docs/latest.html with the same content, and regenerates
    docs/reports/index.html as an archive list. (docs/index.html is the paper
    trading dashboard — see dashboard.py.)

    Returns (dated_report_path, latest_path).
    """
    date = date or datetime.now()
    date_str = date.strftime("%Y-%m-%d")

    reports_dir = os.path.join(docs_dir, REPORTS_SUBDIR)
    os.makedirs(reports_dir, exist_ok=True)

    dated_path = os.path.join(reports_dir, f"{date_str}.html")
    with open(dated_path, "w", encoding="utf-8") as f:
        f.write(html)

    latest_path = os.path.join(docs_dir, "latest.html")
    with open(latest_path, "w", encoding="utf-8") as f:
        f.write(html.replace('href="../index.html"', 'href="index.html"'))

    existing_dates = [
        fn[:-5] for fn in os.listdir(reports_dir)
        if fn.endswith(".html") and fn != "index.html"
    ]
    archive_path = os.path.join(reports_dir, "index.html")
    with open(archive_path, "w", encoding="utf-8") as f:
        f.write(_archive_index_html(existing_dates))

    logger.info("Report published: %s (+ latest.html, + archive)", dated_path)
    return dated_path, latest_path
