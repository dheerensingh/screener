"""
Module 1c: FII/DII Net Flow (live-only, informational)

Surfaces the latest day's FII/DII net cash-market flow (Rs crore) via NSE's
`fiidiiTradeReact` endpoint — confirmed reachable live, 2026-09-28, with a plain
`requests` call + User-Agent header, no cookie/session handshake needed. This is a
narrower, corrected finding vs CLAUDE.md's existing note that www.nseindia.com's API
is blocked — that finding was about the homepage/historical-indices endpoints; this
is a different, lighter endpoint on the same domain. Don't generalize this fix to
"all of www.nseindia.com now works."

IMPORTANT LIMITATION: this endpoint has no historical query support (a `?date=` param
is silently ignored — confirmed live) — it always returns only the latest day. This
data source CANNOT be backtested with this project's tooling, full stop. Treated the
same way sector_rotation.RotationResult.breadth_pct is: informational only, never a
hard gate, surfaced with an explicit "not backtested" caveat wherever it's shown
(see STRATEGY.md §14 for the full reasoning on why this is still worth building).
"""

import logging
from dataclasses import dataclass
from typing import Optional

import requests

logger = logging.getLogger(__name__)

FII_DII_URL = "https://www.nseindia.com/api/fiidiiTradeReact"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept": "application/json",
}


@dataclass
class FlowResult:
    date: Optional[str] = None
    fii_net_cr: Optional[float] = None
    dii_net_cr: Optional[float] = None
    error: Optional[str] = None


def fetch_latest_flow() -> FlowResult:
    """Latest single day's FII/DII net flow (Rs crore). Live-only — see module
    docstring: cannot be backtested with this project's tooling."""
    try:
        resp = requests.get(FII_DII_URL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        rows = resp.json()
    except Exception as exc:
        logger.warning("FII/DII flow fetch failed (non-fatal, informational only): %s", exc)
        return FlowResult(error=str(exc))

    fii_net, dii_net, date = None, None, None
    for row in rows:
        category = row.get("category", "")
        net = row.get("netValue")
        try:
            net = float(net) if net is not None else None
        except (TypeError, ValueError):
            net = None
        date = row.get("date", date)
        if category.upper().startswith("FII"):
            fii_net = net
        elif category.upper() == "DII":
            dii_net = net

    if fii_net is None and dii_net is None:
        return FlowResult(error="Unexpected response shape from NSE fiidiiTradeReact")

    return FlowResult(date=date, fii_net_cr=fii_net, dii_net_cr=dii_net)
