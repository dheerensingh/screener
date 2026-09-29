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


def build_summary_email(
    leading_sectors: list[str],
    qualified_rows: list[tuple[str, float, int, float]],
    report_url: Optional[str],
    errors: list[str],
) -> str:
    """
    Short summary email: leading sector(s), qualifying stocks with quantity/%
    of capital, and a link to the full HTML report (hosted on GitHub Pages) —
    replaces the old full-HTML-inline email now that reports live on the web.

    qualified_rows: list of (ticker, entry_price, quantity, allocation_pct).
    """
    today = datetime.now().strftime("%d %b %Y")
    sector_line = " + ".join(leading_sectors) if leading_sectors else "None detected"

    if qualified_rows:
        row_html = "\n".join(
            f'<tr style="border-bottom:1px solid {PALETTE["border"]}">'
            f'<td style="padding:8px 12px;font-weight:600;color:{PALETTE["accent"]}">{ticker}</td>'
            f'<td style="padding:8px 12px;text-align:right">₹{price:,.2f}</td>'
            f'<td style="padding:8px 12px;text-align:right">{qty}</td>'
            f'<td style="padding:8px 12px;text-align:right">{pct:.1f}%</td></tr>'
            for ticker, price, qty, pct in qualified_rows
        )
    else:
        row_html = (
            f'<tr><td colspan="4" style="text-align:center;padding:20px;'
            f'color:{PALETTE["muted"]}">No stocks qualified today.</td></tr>'
        )

    link_html = (
        f'<p style="margin:16px 0 0"><a href="{report_url}" '
        f'style="color:{PALETTE["accent"]};font-weight:600">View full report →</a></p>'
        if report_url else ""
    )

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
  <div style="background:{PALETTE['card']};padding:16px 24px;border-left:4px solid {PALETTE['accent']}">
    <p style="margin:0;font-size:11px;text-transform:uppercase;letter-spacing:1px;color:{PALETTE['muted']}">
      Leading Sector(s)</p>
    <p style="margin:4px 0 0;font-size:20px;font-weight:700;color:{PALETTE['accent']}">{sector_line}</p>
  </div>
  <div style="background:{PALETTE['card']}">
    <table style="width:100%;border-collapse:collapse;font-size:13px">
      <thead><tr style="background:{PALETTE['bg']};text-transform:uppercase;font-size:10px;
                  letter-spacing:0.6px;color:{PALETTE['muted']}">
        <th style="padding:8px 12px;text-align:left">Ticker</th>
        <th style="padding:8px 12px;text-align:right">Price</th>
        <th style="padding:8px 12px;text-align:right">Qty</th>
        <th style="padding:8px 12px;text-align:right">% Capital</th>
      </tr></thead>
      <tbody>{row_html}</tbody>
    </table>
  </div>
  {link_html}
  {error_block}
  <p style="margin-top:20px;font-size:11px;color:{PALETTE['muted']};line-height:1.6">
    Informational only, not financial advice. Full rationale, formulas, and the sector
    rotation table are in the linked report.
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
