"""
RSS fetcher — Bloque 5.

Reads active sources from pulse_sources_config, fetches each RSS feed,
parses entries, dedups against pulse_candidates.dedup_key, and inserts
new candidates with status="pending" and expires_at = NOW + 6h.

Design:
- A failing source does NOT abort the cycle — logged + last_error updated.
- One SELECT per source to get existing dedup_keys (cheap), then one batch
  INSERT for new candidates. Avoids per-entry duplicate-violation roundtrips.
- HTTP fetch via requests (gives us timeout + custom UA), then pass body
  to feedparser.parse for XML/Atom parsing.
- Entries older than MAX_ENTRY_AGE_HOURS are skipped (stale news).
"""
import hashlib
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

import feedparser
import requests

from .supabase_client import get_client

log = logging.getLogger("rss-fetcher")

# How many entries from each feed to process per cycle.
MAX_ENTRIES_PER_SOURCE = 30
# Skip entries older than this — already-stale news isn't actionable.
MAX_ENTRY_AGE_HOURS = 12
# HTTP fetch timeout per source (seconds).
HTTP_TIMEOUT_SEC = 15
# Be a polite citizen — identify ourselves.
USER_AGENT = "WaCapital-PulseEngine/1.0 (+https://wastake.vercel.app)"

# Parasitic headline patterns. SEC-required filings, repetitive analyst calls,
# and other content that no human will click. Filtered BEFORE insertion so we
# don't even pay the news-angle Groq cost.
_PARASITIC_PATTERNS = [
    # SEC filings: Form 13F, Form 144, Form 6K, Form 8-K, Form S-1, Form DEF14A, etc.
    re.compile(r"\bForm\s+\d+[\-A-Z]?\b", re.IGNORECASE),
    re.compile(r"\bForm\s+(13[FGD]|S[\-]?\d|N[\-]?[A-Z]+|10[\-]?[KQ])\b", re.IGNORECASE),
    re.compile(r"\bSchedule\s+13[DG]\b", re.IGNORECASE),
    # Insider transaction filings
    re.compile(r"insider (sells|bought|sold|buys)\b.*shares\b", re.IGNORECASE),
    re.compile(r"sells \$\d", re.IGNORECASE),
    # Quarterly mass filings
    re.compile(r"\bquarterly (filing|report) For:", re.IGNORECASE),
]


def _is_parasitic(headline: Optional[str]) -> bool:
    if not headline or len(headline.strip()) < 8:
        return True
    return any(p.search(headline) for p in _PARASITIC_PATTERNS)


def _make_dedup_key(source_name: str, entry: dict) -> str:
    """Stable hash. Prefers entry GUID, falls back to link, then title."""
    raw = (
        entry.get("id")
        or entry.get("guid")
        or entry.get("link")
        or entry.get("title", "")
    )
    return hashlib.sha256(f"{source_name}|{raw}".encode("utf-8")).hexdigest()


def _entry_published_at(entry: dict) -> Optional[datetime]:
    """Best-effort UTC parse of published/updated time."""
    for key in ("published_parsed", "updated_parsed"):
        struct = entry.get(key)
        if struct:
            try:
                return datetime(*struct[:6], tzinfo=timezone.utc)
            except (TypeError, ValueError):
                continue
    return None


def _is_too_old(published: Optional[datetime]) -> bool:
    if published is None:
        # Can't parse → don't filter; let dedup handle it.
        return False
    age_hours = (datetime.now(timezone.utc) - published).total_seconds() / 3600
    return age_hours > MAX_ENTRY_AGE_HOURS


def _category_to_event_type(category: Optional[str]) -> str:
    return {
        "crypto":      "news_crypto",
        "tradfi":      "news_tradfi",
        "macro":       "news_macro",
        "geopolitics": "news_geo",
    }.get(category or "", "news_other")


def _fetch_feed_body(url: str) -> bytes:
    """HTTP GET with timeout + identifying UA. Raises on non-2xx."""
    resp = requests.get(
        url,
        timeout=HTTP_TIMEOUT_SEC,
        headers={"User-Agent": USER_AGENT, "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml"},
    )
    resp.raise_for_status()
    return resp.content


def _list_active_sources() -> list[dict]:
    client = get_client()
    res = (
        client.table("pulse_sources_config")
        .select("name, url, category, language, priority")
        .eq("is_active", True)
        .order("priority")
        .execute()
    )
    return res.data or []


def _existing_dedup_keys(keys: list[str]) -> set[str]:
    """Return subset of `keys` already present in pulse_candidates."""
    if not keys:
        return set()
    client = get_client()
    res = (
        client.table("pulse_candidates")
        .select("dedup_key")
        .in_("dedup_key", keys)
        .execute()
    )
    return {row["dedup_key"] for row in (res.data or [])}


def _update_source_status(name: str, success: bool, error_msg: Optional[str] = None) -> None:
    client = get_client()
    update = {
        "last_fetched_at": datetime.now(timezone.utc).isoformat(),
        "last_error":      None if success else (error_msg or "unknown")[:500],
    }
    try:
        client.table("pulse_sources_config").update(update).eq("name", name).execute()
    except Exception as e:
        log.warning("could not update source status for %s: %s", name, e)


def _fetch_one_source(source: dict) -> dict:
    """
    Fetch one RSS source and insert any new candidates.
    Returns: {fetched, new, skipped_old, skipped_dup, skipped_parasitic}.
    """
    name      = source["name"]
    url       = source["url"]
    priority  = source.get("priority", 3)
    category  = source.get("category", "other")
    language  = source.get("language")
    event_type = _category_to_event_type(category)

    stats = {"fetched": 0, "new": 0, "skipped_old": 0, "skipped_dup": 0, "skipped_parasitic": 0}

    body = _fetch_feed_body(url)
    parsed = feedparser.parse(body)
    entries = parsed.entries[:MAX_ENTRIES_PER_SOURCE]
    stats["fetched"] = len(entries)
    if not entries:
        return stats

    # Build candidate dicts in memory, skipping old/empty/parasitic entries.
    candidates_by_key: dict[str, dict] = {}
    for entry in entries:
        published = _entry_published_at(entry)
        if _is_too_old(published):
            stats["skipped_old"] += 1
            continue
        headline = (entry.get("title") or "").strip()[:500]
        link     = entry.get("link") or ""
        if not headline or not link:
            continue
        if _is_parasitic(headline):
            stats.setdefault("skipped_parasitic", 0)
            stats["skipped_parasitic"] += 1
            continue
        dedup_key = _make_dedup_key(name, entry)
        candidates_by_key[dedup_key] = {
            "event_type":  event_type,
            "priority":    priority,
            "headline":    headline,
            "source":      name,
            "source_url":  link,
            "payload": {
                "summary":       (entry.get("summary") or "")[:2000],
                "language":      language,
                "category":      category,
                "raw_published": entry.get("published"),
            },
            "dedup_key": dedup_key,
            "status":    "pending",
        }

    if not candidates_by_key:
        return stats

    # Dedup against DB.
    existing = _existing_dedup_keys(list(candidates_by_key.keys()))
    new_candidates = [c for k, c in candidates_by_key.items() if k not in existing]
    stats["skipped_dup"] = len(candidates_by_key) - len(new_candidates)

    if new_candidates:
        client = get_client()
        client.table("pulse_candidates").insert(new_candidates).execute()
        stats["new"] = len(new_candidates)

    return stats


def run_one_cycle() -> dict:
    """
    Fetch all active sources once. A failing source does NOT abort the cycle.
    Returns aggregated stats.
    """
    sources = _list_active_sources()
    totals = {
        "sources":           len(sources),
        "source_ok":         0,
        "source_errors":     0,
        "fetched":           0,
        "new":               0,
        "skipped_old":       0,
        "skipped_dup":       0,
        "skipped_parasitic": 0,
    }

    for src in sources:
        name = src["name"]
        try:
            s = _fetch_one_source(src)
            totals["source_ok"]         += 1
            totals["fetched"]           += s["fetched"]
            totals["new"]               += s["new"]
            totals["skipped_old"]       += s["skipped_old"]
            totals["skipped_dup"]       += s["skipped_dup"]
            totals["skipped_parasitic"] += s.get("skipped_parasitic", 0)
            log.info(
                "  %-22s fetched=%2d new=%2d skip_old=%2d skip_dup=%2d skip_paras=%2d",
                name, s["fetched"], s["new"], s["skipped_old"], s["skipped_dup"], s.get("skipped_parasitic", 0),
            )
            _update_source_status(name, success=True)
        except Exception as e:
            totals["source_errors"] += 1
            log.warning("  %-22s FAILED: %s", name, e)
            _update_source_status(name, success=False, error_msg=str(e))

    return totals
