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

from .technical_analyzer import ScreenResult

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Colour palette
# ─────────────────────────────────────────────────────────────────────────────
_PALETTE = {
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


def _rsi_colour(rsi: float) -> str:
    if rsi >= 70:
        return _PALETTE["amber"]   # overbought — amber warning
    if rsi >= 60:
        return _PALETTE["green"]   # momentum zone
    return _PALETTE["red"]


def _build_stock_rows(results: list[ScreenResult]) -> str:
    if not results:
        return (
            f'<tr><td colspan="5" style="text-align:center;padding:24px;'
            f'color:{_PALETTE["muted"]}">No stocks passed both filters today.</td></tr>'
        )

    rows: list[str] = []
    for r in results:
        rsi_col = _rsi_colour(r.rsi)
        macd_label = "✅ Bullish" if "bullish" in r.macd_status.lower() else "❌ Bearish"
        row = f"""
        <tr style="border-bottom:1px solid {_PALETTE['border']}">
          <td style="padding:10px 14px;font-weight:600;color:{_PALETTE['accent']}">{r.ticker}</td>
          <td style="padding:10px 14px;text-align:right">₹{r.current_price:,.2f}</td>
          <td style="padding:10px 14px;text-align:right;color:{rsi_col};font-weight:700">{r.rsi:.1f}</td>
          <td style="padding:10px 14px;font-size:12px;color:{_PALETTE['muted']}">{r.rsi_status}</td>
          <td style="padding:10px 14px;font-size:12px">{macd_label}<br>
              <span style="color:{_PALETTE['muted']};font-size:11px">{r.macd_status}</span>
          </td>
        </tr>"""
        rows.append(row)
    return "\n".join(rows)


def build_html_email(
    sector: Optional[str],
    sector_source: str,
    qualified: list[ScreenResult],
    all_results: list[ScreenResult],
    errors: list[str],
) -> str:
    """Constructs the full HTML email body."""

    today = datetime.now().strftime("%d %b %Y")
    sector_display = sector or "Unknown / Not detected"
    source_badge = {
        "tweepy": "Twitter API v2",
        "nitter": "Nitter RSS",
        "manual": "Manual Override",
        "error": "Detection Failed",
    }.get(sector_source, sector_source)

    stock_rows = _build_stock_rows(qualified)

    error_block = ""
    if errors:
        error_items = "".join(f"<li>{e}</li>" for e in errors)
        error_block = f"""
        <div style="margin-top:28px;padding:16px;background:#7f1d1d;border-radius:8px;
                    border-left:4px solid {_PALETTE['red']}">
          <p style="margin:0 0 8px;font-weight:700;color:{_PALETTE['red']}">⚠ Errors / Warnings</p>
          <ul style="margin:0;padding-left:18px;color:#fca5a5;font-size:13px">{error_items}</ul>
        </div>"""

    # Summary stats
    total_screened = len(all_results)
    total_qualified = len(qualified)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Daily Stock Screener</title>
</head>
<body style="margin:0;padding:0;background:{_PALETTE['bg']};font-family:'Segoe UI',Arial,sans-serif;
             color:{_PALETTE['text']}">

<div style="max-width:680px;margin:32px auto;padding:0 16px">

  <!-- Header -->
  <div style="background:linear-gradient(135deg,#1e40af,#0369a1);border-radius:12px 12px 0 0;
              padding:24px 28px">
    <h1 style="margin:0;font-size:22px;letter-spacing:0.5px">
      📊 Daily Sentiment Screener
    </h1>
    <p style="margin:6px 0 0;color:#bae6fd;font-size:14px">{today} · NSE India</p>
  </div>

  <!-- Sector card -->
  <div style="background:{_PALETTE['card']};padding:20px 28px;
              border-left:4px solid {_PALETTE['accent']}">
    <p style="margin:0;font-size:12px;text-transform:uppercase;letter-spacing:1px;
              color:{_PALETTE['muted']}">Detected Sector · Source: {source_badge}</p>
    <p style="margin:6px 0 0;font-size:26px;font-weight:700;color:{_PALETTE['accent']}">
      {sector_display}
    </p>
    <p style="margin:8px 0 0;font-size:13px;color:{_PALETTE['muted']}">
      Screened <strong style="color:{_PALETTE['text']}">{total_screened}</strong> stocks ·
      <strong style="color:{_PALETTE['green']}">{total_qualified}</strong> passed both filters
    </p>
  </div>

  <!-- Filter legend -->
  <div style="background:{_PALETTE['card']};margin-top:2px;padding:14px 28px;
              border-bottom:1px solid {_PALETTE['border']}">
    <p style="margin:0;font-size:12px;color:{_PALETTE['muted']}">
      <strong style="color:{_PALETTE['text']}">Active Filters:</strong>
      &nbsp;①&nbsp;RSI(14) ≥ 60 or crossed above 60 in last 2 sessions
      &nbsp;&nbsp;②&nbsp;MACD line > Signal line (bullish histogram)
    </p>
  </div>

  <!-- Results table -->
  <div style="background:{_PALETTE['card']};border-radius:0 0 0 0;overflow:hidden">
    <table style="width:100%;border-collapse:collapse;font-size:14px">
      <thead>
        <tr style="background:{_PALETTE['bg']};text-transform:uppercase;font-size:11px;
                   letter-spacing:0.8px;color:{_PALETTE['muted']}">
          <th style="padding:10px 14px;text-align:left">Ticker</th>
          <th style="padding:10px 14px;text-align:right">Price (₹)</th>
          <th style="padding:10px 14px;text-align:right">RSI</th>
          <th style="padding:10px 14px;text-align:left">RSI Status</th>
          <th style="padding:10px 14px;text-align:left">MACD Momentum</th>
        </tr>
      </thead>
      <tbody>
        {stock_rows}
      </tbody>
    </table>
  </div>

  {error_block}

  <!-- Disclaimer -->
  <div style="margin-top:24px;padding:16px 20px;background:{_PALETTE['card']};
              border-radius:8px;border-left:4px solid {_PALETTE['border']}">
    <p style="margin:0;font-size:11px;color:{_PALETTE['muted']};line-height:1.6">
      <strong>Disclaimer:</strong> This report is generated automatically for
      informational purposes only and does not constitute financial advice.
      Always perform your own due diligence before making any investment decisions.
      Past technical signals do not guarantee future performance.
    </p>
  </div>

  <!-- Footer -->
  <p style="text-align:center;font-size:11px;color:{_PALETTE['muted']};margin:20px 0 32px">
    Generated by Daily Screener Bot · Runs weekdays at 08:00 IST via GitHub Actions
  </p>

</div>
</body>
</html>"""
    return html


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
