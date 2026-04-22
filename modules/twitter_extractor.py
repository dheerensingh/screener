"""
Module 1: Twitter/X Sector Extraction

Strategy (three-tier fallback, all free):
  Tier 1 — Tweepy v2 Bearer Token (needs a free developer account at developer.twitter.com).
            The free "Essential" level grants read access to up to 500k tweets/month. If you
            already have a Bearer Token, this is the most reliable path.
  Tier 2 — Nitter RSS (completely free, no key needed). Nitter is an open-source X front-end
            that exposes an RSS feed at <instance>/user/rss. We try a rotating list of public
            instances. Fragile if all instances are down, hence the tier-3 fallback.
  Tier 3 — MANUAL_SECTOR environment variable. Set this to force a sector when both API
            methods fail (useful for weekends / when nitter is down).
"""

import os
import re
import logging
import time
from typing import Optional

import feedparser
import requests

logger = logging.getLogger(__name__)

# ── Nitter public instances to try in order ──────────────────────────────────
NITTER_INSTANCES = [
    "https://nitter.poast.org",
    "https://nitter.privacydev.net",
    "https://nitter.net",
    "https://nitter.1d4.us",
    "https://nitter.kavin.rocks",
]

# ── Keyword → Sector mapping (case-insensitive substring match) ───────────────
# Each list contains words/phrases that strongly signal the sector.
SECTOR_KEYWORDS: dict[str, list[str]] = {
    "Defense": [
        "defence", "defense", "hal", "bel", "beml", "ordnance", "military",
        "drdo", "bharat electronics", "cochin shipyard", "mazagon", "garden reach",
        "army", "navy", "air force", "missile", "weapon", "armament", "fighter",
        "manohar parrikar", "gdp defense", "aerospace",
    ],
    "Railways": [
        "railways", "railway", "rail", "irctc", "rvnl", "railtel", "ircon",
        "rites", "train", "metro", "locomotive", "wagon", "vande bharat",
        "bullet train", "dedicated freight", "jupiter wagon", "titagarh",
    ],
    "IT": [
        " it ", "tech", "software", "tcs", "infosys", "wipro", "hcl",
        "technology", "digital", "ai ", "artificial intelligence", "cloud",
        "saas", "coding", "generative ai", "data center", "ltimindtree",
        "mphasis", "persistent", "coforge", "offshore",
    ],
    "Pharma": [
        "pharma", "pharmaceutical", "drug", "medicine", "healthcare",
        "hospital", "biotech", "api ", "generics", "sun pharma",
        "dr reddy", "cipla", "lupin", "aurobindo", "biocon", "divi",
        "bulk drug", "formulation", "fda", "usfda",
    ],
    "Banking": [
        "bank", "banking", "nbfc", "financial", "credit", "loan",
        "hdfc", "icici", "sbi", "kotak", "axis bank", "rbi",
        "interest rate", "npa", "repo rate", "credit growth", "deposit",
        "net interest margin", "nim", "psb", "psu bank",
    ],
    "FMCG": [
        "fmcg", "consumer staple", "hul", "nestle", "britannia", "dabur",
        "marico", "itc", "godrej consumer", "food", "beverage",
        "personal care", "volume growth", "rural demand", "gsk consumer",
    ],
    "Auto": [
        "auto", "automobile", "car", "vehicle", "ev", "electric vehicle",
        "maruti", "tata motors", "mahindra", "hero motocorp", "bajaj auto",
        "eicher", "two wheeler", "four wheeler", "suv", "emission",
        "component", "ancillary", "motherson",
    ],
    "Metals": [
        "metal", "steel", "aluminium", "copper", "zinc", "iron ore",
        "tata steel", "jsw", "hindalco", "vedanta", "sail", "nalco",
        "commodity", "base metal", "non-ferrous", "coking coal",
    ],
    "Energy": [
        "energy", "power", "electricity", "ntpc", "power grid", "renewable",
        "solar", "wind", "oil", "gas", "reliance", "ongc", "petronet",
        "coal", "lng", "crude", "adani green", "torrent power",
    ],
    "Infrastructure": [
        "infra", "infrastructure", "construction", "real estate", "realty",
        "dlf", "ncc", "larsen", "l&t", "roads", "highways", "bridge",
        "knr", "pnc", "psp projects", "irb", "gdp growth", "capex",
    ],
    "Chemicals": [
        "chemical", "specialty chemical", "pidilite", "asian paints",
        "aarti", "vinati", "deepak nitrite", "agro", "pesticide",
        "fertilizer", "chlor-alkali", "fluorine", "polymer", "pigment",
    ],
    "Telecom": [
        "telecom", "telecommunications", "airtel", "jio", "vi",
        "vodafone", "broadband", "5g", "mobile", "wireless", "spectrum",
        "arpu", "tower", "indus tower",
    ],
    "PSU": [
        "psu", "public sector", "government", "divestment", "navratna",
        "cpse", "bharat heavy", "bhel", "nil", "hindustan copper",
        "nmdc", "moil", "coal india",
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# Tier 1: Tweepy v2
# ─────────────────────────────────────────────────────────────────────────────

def _get_tweets_tweepy(handle: str, count: int = 20) -> list[str]:
    """Fetch up to `count` recent tweets via Tweepy v2 Bearer Token auth."""
    bearer_token = os.environ.get("TWITTER_BEARER_TOKEN", "").strip()
    if not bearer_token:
        raise EnvironmentError("TWITTER_BEARER_TOKEN not set — skipping Tweepy")

    try:
        import tweepy  # lazy import so missing package doesn't crash on tier-2 path
    except ImportError:
        raise ImportError("tweepy not installed")

    client = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)
    # Resolve handle → user id first
    user_resp = client.get_user(username=handle, user_auth=False)
    if not user_resp.data:
        raise ValueError(f"Twitter user @{handle} not found")

    user_id = user_resp.data.id
    tweets_resp = client.get_users_tweets(
        id=user_id,
        max_results=min(count, 100),
        tweet_fields=["text"],
        exclude=["retweets", "replies"],
    )
    if not tweets_resp.data:
        return []

    return [t.text for t in tweets_resp.data[:count]]


# ─────────────────────────────────────────────────────────────────────────────
# Tier 2: Nitter RSS
# ─────────────────────────────────────────────────────────────────────────────

def _get_tweets_nitter(handle: str, count: int = 20) -> list[str]:
    """Fetch tweets via a public Nitter instance RSS feed (no API key needed)."""
    headers = {"User-Agent": "Mozilla/5.0 (compatible; StockScreener/1.0)"}
    last_error: Optional[Exception] = None

    for instance in NITTER_INSTANCES:
        url = f"{instance}/{handle}/rss"
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            feed = feedparser.parse(resp.text)
            entries = feed.get("entries", [])
            if not entries:
                continue

            texts: list[str] = []
            for entry in entries[:count]:
                # Strip HTML tags from summary
                raw = entry.get("summary", entry.get("title", ""))
                clean = re.sub(r"<[^>]+>", " ", raw)
                clean = re.sub(r"\s+", " ", clean).strip()
                texts.append(clean)

            if texts:
                logger.info("Nitter RSS succeeded via %s (%d tweets)", instance, len(texts))
                return texts

        except Exception as exc:
            last_error = exc
            logger.debug("Nitter instance %s failed: %s", instance, exc)
            time.sleep(1)

    raise RuntimeError(f"All Nitter instances failed. Last error: {last_error}")


# ─────────────────────────────────────────────────────────────────────────────
# Sector extraction from tweet text
# ─────────────────────────────────────────────────────────────────────────────

def extract_sector_from_tweets(tweets: list[str]) -> Optional[str]:
    """
    Score each sector by counting keyword hits across all tweet text.
    Returns the highest-scoring sector, or None if nothing clears the
    minimum confidence threshold (≥2 hits).
    """
    if not tweets:
        return None

    combined = " ".join(tweets).lower()
    scores: dict[str, int] = {}

    for sector, keywords in SECTOR_KEYWORDS.items():
        hit_count = sum(1 for kw in keywords if kw in combined)
        if hit_count:
            scores[sector] = hit_count
            logger.debug("Sector %s scored %d", sector, hit_count)

    if not scores:
        return None

    best_sector = max(scores, key=lambda s: scores[s])
    best_score = scores[best_sector]

    # Require at least 2 keyword hits to avoid noise
    if best_score < 2:
        logger.warning(
            "Top sector '%s' has only %d hit(s) — below confidence threshold",
            best_sector, best_score,
        )
        return None

    logger.info("Detected sector: %s (score=%d)", best_sector, best_score)
    return best_sector


# ─────────────────────────────────────────────────────────────────────────────
# Public entry-point
# ─────────────────────────────────────────────────────────────────────────────

def get_sector(handle: str = "BasantBaheti3", tweet_count: int = 20) -> tuple[Optional[str], str]:
    """
    Orchestrates sector detection with full fallback chain.

    Returns
    -------
    (sector, source) where source is one of:
        "tweepy", "nitter", "manual", "error"
    """
    # Tier 3: manual override always wins if set
    manual = os.environ.get("MANUAL_SECTOR", "").strip()
    if manual:
        logger.info("Using MANUAL_SECTOR override: %s", manual)
        return manual, "manual"

    tweets: list[str] = []

    # Tier 1: Tweepy v2
    try:
        tweets = _get_tweets_tweepy(handle, tweet_count)
        source = "tweepy"
        logger.info("Retrieved %d tweets via Tweepy", len(tweets))
    except Exception as e1:
        logger.warning("Tweepy failed (%s) — trying Nitter RSS", e1)
        # Tier 2: Nitter RSS
        try:
            tweets = _get_tweets_nitter(handle, tweet_count)
            source = "nitter"
        except Exception as e2:
            logger.error("Nitter also failed: %s", e2)
            return None, "error"

    sector = extract_sector_from_tweets(tweets)
    return sector, source
