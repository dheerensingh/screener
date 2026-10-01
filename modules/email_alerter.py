"""
Module 4: HTML Email Alerting

Builds a clean, readable HTML email and delivers it via Gmail SMTP (port 465 / SSL).

Required environment variables:
  SENDER_EMAIL        — Gmail address used to send (e.g. mybot@gmail.com)
  SENDER_APP_PASSWORD — 16-char Google App Password (not your login password)
  RECEIVER_EMAIL      — Destination address (can be the same as sender)

Gmail setup:
  1. Enable 2-Step Verification on the sender account.
  2. Go to myaccount.google.com → Security → App Passwords.
  3. Generate a password for "Mail" / "Other device" → copy the 16-char code.
  4. Store that code as SENDER_APP_PASSWORD.
"""

import logging
import os
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html import escape
from typing import Optional

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Colour palette — shared with report_generator.py for a consistent look
# ─────────────────────────────────────────────────────────────────────────────
PALETTE = {
    "bg": "#0f172a",
    "card": "#1e293b",
    "accent": "#38bdf8",
    "green": "#4ade80",
    "amber": "#fbbf24",
    "red": "#f87171",
    "text": "#e2e8f0",
    "muted": "#94a3b8",
    "border": "#334155",
}


def _email_table(headers: list[str], rows: list[list[str]], empty: str) -> str:
    if not rows:
        return f'<p style="margin:8px 0 0;color:{PALETTE["muted"]};font-size:13px">{escape(empty)}</p>'
    head = "".join(
        f'<th style="padding:6px 10px;text-align:{"left" if i == 0 else "right"}">{escape(h)}</th>'
        for i, h in enumerate(headers)
    )
    body = "".join(
        f'<tr style="border-bottom:1px solid {PALETTE["border"]}">' + "".join(
            f'<td style="padding:6px 10px;text-align:{"left" if i == 0 else "right"}">{c}</td>'
            for i, c in enumerate(row)
        ) + "</tr>"
        for row in rows
    )
    return (f'<table style="width:100%;border-collapse:collapse;font-size:13px;margin-top:6px">'
            f'<thead><tr style="color:{PALETTE["muted"]};font-size:11px">{head}</tr></thead><tbody>{body}</tbody></table>')


def _section(title: str, content: str) -> str:
    return (f'<div style="background:{PALETTE["card"]};padding:14px 20px;margin-top:2px">'
            f'<p style="margin:0;font-size:11px;text-transform:uppercase;letter-spacing:1px;'
            f'color:{PALETTE["muted"]}">{escape(title)}</p>{content}</div>')


def _pnl(value: float) -> str:
    colour = PALETTE["green"] if value >= 0 else PALETTE["red"]
    glyph = "▲" if value >= 0 else "▼"
    return f'<span style="color:{colour}">{glyph} ₹{abs(value):,.0f}</span>'


def build_summary_email(
    leading_sectors: list[str],
    paper,  # modules.paper_trader.PaperState
    dashboard_url: Optional[str],
    report_url: Optional[str],
    errors: list[str],
) -> str:
    """
    Daily email: the paper portfolio's value and return vs Nifty500, what was
    bought and sold today, and the orders placed for the next open — with links
    to the dashboard and the full screening report on GitHub Pages.
    """
    today = datetime.now().strftime("%d %b %Y")
    sector_line = " + ".join(leading_sectors) if leading_sectors else "None detected"

    history = paper.equity_history
    ret = (paper.equity / paper.start_capital - 1.0) * 100.0 if paper.start_capital else 0.0
    bench = ""
    if len(history) >= 2:
        b0, b1 = float(history[0]["benchmark_close"]), float(history[-1]["benchmark_close"])
        bench = f" · Nifty500 {(b1 / b0 - 1.0) * 100.0:+.2f}% over the same period" if b0 else ""
    summary = (
        f'<p style="margin:4px 0 0;font-size:24px;font-weight:700">₹{paper.equity:,.0f} '
        f'<span style="font-size:15px;color:{PALETTE["green"] if ret >= 0 else PALETTE["red"]}">{ret:+.2f}%</span></p>'
        f'<p style="margin:2px 0 0;font-size:12px;color:{PALETTE["muted"]}">started ₹{paper.start_capital:,.0f}{bench} · '
        f'{len(paper.positions)} open · cash ₹{paper.cash:,.0f}</p>'
    )

    orders = _email_table(
        ["Ticker", "Sector", "Qty", "Signal close", "Stop"],
        [[escape(r["ticker"]), escape(r["sector"]), r["quantity"], f'₹{float(r["signal_price"]):,.2f}',
          f'₹{float(r["signal_price"]) - float(r["stop_distance"]):,.2f}'] for r in paper.booked_today],
        "No new orders today.",
    )
    sold = _email_table(
        ["Ticker", "Qty", "Exit", "P&L", "Reason"],
        [[escape(r["ticker"]), r["quantity"], f'₹{float(r["exit_price"]):,.2f}', _pnl(float(r["pnl"])),
          escape(r["exit_reason"])] for r in paper.closed_today],
        "Nothing sold today.",
    )
    bought = _email_table(
        ["Ticker", "Qty", "Entry", "Stop"],
        [[escape(r["ticker"]), r["quantity"], f'₹{float(r["entry_price"]):,.2f}', f'₹{float(r["initial_stop"]):,.2f}']
         for r in paper.filled_today],
        "Nothing bought today.",
    )

    links = " · ".join(
        f'<a href="{escape(url)}" style="color:{PALETTE["accent"]};font-weight:600">{label}</a>'
        for label, url in (("Open dashboard →", dashboard_url), ("Full screening report", report_url)) if url
    )
    link_html = f'<p style="margin:16px 0 0">{links}</p>' if links else ""

    error_block = ""
    if errors:
        error_items = "".join(f"<li>{e}</li>" for e in errors)
        error_block = (
            f'<div style="margin-top:20px;padding:14px;background:#7f1d1d;border-radius:8px;'
            f'border-left:4px solid {PALETTE["red"]}"><p style="margin:0 0 6px;font-weight:700;'
            f'color:{PALETTE["red"]}">⚠ Errors / Warnings</p>'
            f'<ul style="margin:0;padding-left:18px;color:#fca5a5;font-size:13px">{error_items}</ul></div>'
        )

    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><title>Screener Summary</title></head>
<body style="margin:0;padding:0;background:{PALETTE['bg']};font-family:'Segoe UI',Arial,sans-serif;
             color:{PALETTE['text']}">
<div style="max-width:560px;margin:32px auto;padding:0 16px">
  <div style="background:linear-gradient(135deg,#1e40af,#0369a1);border-radius:12px 12px 0 0;padding:20px 24px">
    <h1 style="margin:0;font-size:20px">📊 Sector Screener Summary</h1>
    <p style="margin:6px 0 0;color:#bae6fd;font-size:13px">{today} · NSE India</p>
  </div>
  {_section("Paper portfolio", summary)}
  {_section("Orders for the next open", orders)}
  {_section("Bought today", bought)}
  {_section("Sold today", sold)}
  {_section("Leading sectors", f'<p style="margin:4px 0 0;font-size:15px;font-weight:600;color:{PALETTE["accent"]}">{escape(sector_line)}</p>')}
  {link_html}
  {error_block}
  <p style="margin-top:20px;font-size:11px;color:{PALETTE['muted']};line-height:1.6">
    Paper trading only — no real money, not financial advice. Orders fill at the next
    session's opening price; every position, trade and chart is on the dashboard.
  </p>
</div>
</body>
</html>"""


def send_email(
    html_body: str,
    subject: str = "📊 Daily Stock Screener Results",
) -> bool:
    """
    Sends the HTML email via Gmail SMTP over SSL (port 465).
    Returns True on success, False on failure (never raises).
    """
    sender = os.environ.get("SENDER_EMAIL", "").strip()
    password = os.environ.get("SENDER_APP_PASSWORD", "").strip()
    receiver = os.environ.get("RECEIVER_EMAIL", "").strip()

    if not all([sender, password, receiver]):
        missing = [k for k, v in {
            "SENDER_EMAIL": sender,
            "SENDER_APP_PASSWORD": password,
            "RECEIVER_EMAIL": receiver,
        }.items() if not v]
        logger.error("Missing email env vars: %s — email not sent", missing)
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"Stock Screener <{sender}>"
    msg["To"] = receiver
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender, password)
            server.sendmail(sender, receiver, msg.as_string())
        logger.info("Email sent successfully to %s", receiver)
        return True
    except smtplib.SMTPAuthenticationError:
        logger.error(
            "SMTP authentication failed — verify SENDER_APP_PASSWORD is a valid "
            "16-char Google App Password (not your account password)"
        )
    except smtplib.SMTPException as exc:
        logger.error("SMTP error while sending email: %s", exc)
    except Exception as exc:
        logger.error("Unexpected error sending email: %s", exc)

    return False


def send_error_email(errors: list[str]) -> bool:
    """Convenience wrapper to send a minimal error-only email when pipeline fails early."""
    today = datetime.now().strftime("%d %b %Y")
    html = f"""<!DOCTYPE html>
<html><body style="font-family:Arial,sans-serif;background:#0f172a;color:#e2e8f0;padding:32px">
<h2 style="color:#f87171">⚠ Screener Error Report — {today}</h2>
<p>The daily stock screener encountered errors and could not complete:</p>
<ul>{"".join(f"<li>{e}</li>" for e in errors)}</ul>
<p style="color:#94a3b8;font-size:12px">Check GitHub Actions logs for full traceback.</p>
</body></html>"""
    return send_email(html, subject=f"⚠ Screener Error — {today}")
