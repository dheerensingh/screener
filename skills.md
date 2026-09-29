# skills.md — Tools & data-source inventory

Reference inventory of libraries, techniques, and data sources relevant to this project.
Update this file whenever a source in `task.md` Phase 0 gets verified, replaced, or
deprecated.

## Already in use

| Tool | Purpose | Notes |
|---|---|---|
| `yfinance` | EOD OHLCV for individual stocks | Chunked 100/batch in `modules/stock_fetcher.py`; sufficient for swing trading (no intraday need) |
| `pandas` / `numpy` | RSI(14, Wilder's) / MACD(12/26/9) computed manually | No `pandas_ta` — removed for Python 3.11/Linux incompatibility (commit `f5d8270`), don't reintroduce |
| `requests` + `lxml` / `beautifulsoup4` | NSE CSV downloads (universe/sector data) + Screener.in HTML scraping (fundamentals) | `archives.nseindia.com` CSVs are reliable (static, no anti-bot); Screener.in scraping is more fragile — breaks if their page structure changes |
| GitHub Actions cron | Deployment (`schedule.yml`, 08:00 IST weekdays + manual dispatch) | Keep this schedule when wiring in new phases |
| Gmail SMTP (`modules/email_alerter.py`) | Report delivery | Short summary email (`build_summary_email`) with a link to the full hosted report |

## Universe & sector data sources (2026-09-27 — expansion to full Nifty500)

| Need | Source | Cost | Status |
|---|---|---|---|
| Full stock universe (large+mid+small cap) | `archives.nseindia.com/content/indices/ind_nifty500list.csv` — 501 stocks, includes `Industry` column | Free | Verified — in use (`sector_mapper.build_universe`) |
| Cap-band tagging (Large/Mid/Small) | `ind_nifty100list.csv`, `ind_niftymidcap150list.csv`, `ind_niftysmallcap250list.csv` — confirmed these two add zero tickers beyond Nifty500 (NSE builds Nifty500 = Nifty100+Midcap150+Smallcap250) | Free | Verified |
| Defense theme (overrides generic "Capital Goods" industry) | `ind_niftyindiadefence_list.csv` — official Nifty India Defence, 19 stocks | Free | Verified — also fixed a stale-ticker bug (`MTARTECH` not `MTAR`) |
| PSU theme | `ind_niftypselist.csv` — official Nifty PSE, 20 stocks | Free | Verified |
| Infrastructure theme | `ind_niftyinfralist.csv` — official Nifty Infra, 30 stocks | Free | Verified |
| Railways theme | No official NSE index found (tried several plausible filenames) — kept as a manual 9-ticker override list | N/A | Confirmed no official source exists |

## Verified in the Phase 0 spike (2026-09-27)

| Need | Finding | Cost | Status |
|---|---|---|---|
| Sector-level price history for relative-strength ranking | Official yfinance sector tickers mostly dead (tested `^CNXAUTO`, `^CNXFMCG`, `^CNXMETAL`, `^CNXENERGY`, `^CNXREALTY`, `^CNXPSE`, `^CNXMEDIA`, `^CNXINFRA`, `^CNXFIN`, `^CNXCONSUM`, `^CNXPSUBANK` — all returned only 1 stale row). Only `^CNXIT`, `^CNXPHARMA`, `^NSEBANK`, `^NSEI`, `^CRSLDX`, `^CNX100` are live. **Use synthetic sector baskets instead** (aggregate `SECTOR_STOCKS` tickers via existing `stock_fetcher.py`) — no new dependency. | Free | Verified — pivot adopted |
| `www.nseindia.com` API (historical indices, etc.) | 403 on homepage, 503 (WAF) on the API from this environment — don't build new features on this domain. `archives.nseindia.com` (static CSVs, already used for the Nifty500 list) is unaffected. | N/A | Verified — avoid |
| Fundamentals: quarterly results | Screener.in **public** company pages (e.g. `/company/TCS/consolidated/`) expose the Quarterly Results table with **no login/paid plan** required. | Free | Verified — use directly |
| Position sizing formulas | Fixed-fractional risk % + ATR-based stop distance (standard swing-trading position sizing) | N/A — pure calculation | To implement in Phase 4, no external dependency |

## Explicitly deprioritized (with reasoning)

| Tool | Why not for v1 |
|---|---|
| **Zerodha Kite Connect** | ₹2000/mo; provides live/intraday market data + order execution, but **no fundamentals data at all**. Swing trading only needs EOD data (already covered by `yfinance`), so this cost buys nothing v1 needs. Revisit only if the user wants live intraday data or automated order placement later. |
| **Trendlyne API** | Has strong DVM scoring and 1,200+ ratios, but programmatic/API pricing for retail use wasn't confirmed as affordable or available during research — treat as a "nice to have, verify pricing" item, not a v1 dependency. |
| **`pandas_ta`** | Previously removed for Python 3.11/Linux incompatibility (commit `f5d8270`). Don't reintroduce — stick to manual pandas indicator math. |
| **Official NSE sector-index tickers (yfinance `^CNX*`)** | Confirmed mostly dead in the Phase 0 spike — superseded by the synthetic-sector-basket approach. Don't resurrect this path unless the synthetic approach proves inadequate. |
| **`www.nseindia.com`'s protected API** | Confirmed blocked (403/503 WAF) from this environment in the Phase 0 spike. Don't build new features depending on it; `archives.nseindia.com` static CSVs are a different, working subdomain. |
