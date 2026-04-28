"""
Editorial generator — Bloque 6a + 6b.

Per cycle:
  1. Fetch snapshot ONCE (backend caches 2 min, almost free).
  2. For each pending candidate (up to MAX_PER_CYCLE):
     a. Call /api/analysis/news-angle in Spanish (lang='es' hardcoded).
     b. Detect if headline mentions a tracked asset (BTC/ETH/SOL/SPY/Gold).
     c. If yes → semáforo = the live one from snapshot for that asset.
        If no  → semáforo = 'neutral'.
     d. Compose pulse_post with copy + asset_affected + compliance_flags
        (includes market context: F&G, stress flags, calendar source).
     e. Insert pulse_post (status='generated'), mark candidate processed.

Failure handling:
  - Snapshot fetch fails → all posts in the cycle get neutral semáforo + a flag
    `snapshot_unavailable: true` in compliance_flags. Cycle does NOT abort.
  - news-angle call fails → candidate stays 'pending', retries next cycle.
  - Post insert fails → candidate stays 'pending'. Same retry path.
  - Mark-as-processed fails after post insert → logged loudly to avoid
    silent post duplication on next cycle.

Deferred to Bloque 6c:
  - Card image generation (card_image_url/path remain NULL for now).
"""
import logging
from datetime import datetime, timezone
from typing import Optional

from . import wastake_client
from .supabase_client import get_client

log = logging.getLogger("editorial-gen")

# Throughput knob. Cycles run every 5 min; 5 candidates/cycle = 60/hour.
MAX_PER_CYCLE = 5

# pulse_posts.headline is varchar — keep it sane.
MAX_HEADLINE_LEN = 500
# Twitter hard limit.
MAX_TWEET_LEN = 280

# Asset detection. Snapshot exposes 5 assets. We do conservative substring matching
# on the (lowercased) headline padded with spaces, so we match whole words and
# avoid false positives like "method" containing "eth".
ASSET_KEYWORDS = {
    "bitcoin":  ["bitcoin", " btc ", " btc.", " btc,", " btc:", "satoshi"],
    "ethereum": ["ethereum", " eth ", " eth.", " eth,", " eth:", "vitalik", "ether "],
    "solana":   ["solana", " sol ", " sol.", " sol,", " sol:"],
    "spy":      ["s&p 500", "s&p500", " spx ", " spy ", " spy.", "standard & poor"],
    "gc=f":     [" oro ", " oro.", " oro,", "gold price", "precio del oro", " xau", "gold rises", "gold falls", "oro sube", "oro baja"],
}


def _detect_asset(headline: Optional[str]) -> Optional[str]:
    if not headline:
        return None
    h = " " + headline.lower() + " "
    for asset_id, kws in ASSET_KEYWORDS.items():
        for kw in kws:
            if kw in h:
                return asset_id
    return None


def _semaforo_for(asset_id: Optional[str], snapshot: Optional[dict]) -> str:
    """Look up snapshot semáforo for an asset. Defaults to neutral on any miss."""
    if not asset_id or not snapshot:
        return "neutral"
    for s in snapshot.get("semaforos") or []:
        if s.get("id") == asset_id:
            return s.get("semaforo") or "neutral"
    return "neutral"


def _market_context(snapshot: Optional[dict]) -> dict:
    """Extract the macro signals worth carrying with each post for human review."""
    if not snapshot:
        return {"snapshot_unavailable": True}
    market = snapshot.get("market") or {}
    flags  = snapshot.get("flags")  or {}
    fg     = market.get("fearGreed") or {}
    return {
        "fearGreed":         fg.get("value"),
        "fearGreedClass":    fg.get("classification"),
        "marketStressed":    flags.get("marketStressed"),
        "macroEventSoon":    flags.get("macroEventSoon"),
        "bigWhaleActivity":  flags.get("bigWhaleActivity"),
        "feargreedExtreme":  flags.get("feargreedExtreme"),
        "calendarSource":    snapshot.get("calendarSource"),
    }


def _list_pending(limit: int) -> list[dict]:
    client = get_client()
    res = (
        client.table("pulse_candidates")
        .select("id, headline, source, source_url, payload, event_type, asset_id, asset_type, priority")
        .eq("status", "pending")
        .order("priority", desc=True)
        .order("detected_at", desc=False)
        .limit(limit)
        .execute()
    )
    return res.data or []


def _mark_candidate(candidate_id: int, status: str) -> None:
    client = get_client()
    update: dict = {"status": status}
    if status == "processed":
        update["processed_at"] = datetime.now(timezone.utc).isoformat()
    client.table("pulse_candidates").update(update).eq("id", candidate_id).execute()


def _compose_post(candidate: dict, angle: dict, snapshot: Optional[dict]) -> dict:
    """Build the pulse_posts row from candidate + angle + (optional) snapshot."""
    tweets    = angle.get("tweets") or []
    headlines = angle.get("headlines") or []
    hook      = (angle.get("hook") or "").strip()

    copy_twitter    = (tweets[0] if tweets else hook or candidate["headline"]).strip()[:MAX_TWEET_LEN]
    ig_caption      = (angle.get("instagram_caption") or "").strip() or copy_twitter
    chosen_headline = (headlines[0] if headlines else candidate["headline"])[:MAX_HEADLINE_LEN]

    asset_id  = _detect_asset(candidate.get("headline"))
    semaforo  = _semaforo_for(asset_id, snapshot)
    asset_aff = asset_id or candidate.get("asset_id")  # detected wins, else upstream

    return {
        "candidate_id":    candidate["id"],
        "headline":        chosen_headline,
        "semaforo":        semaforo,
        "asset_affected":  asset_aff,
        "copy_twitter":    copy_twitter,
        "copy_instagram":  ig_caption,
        "copy_facebook":   ig_caption,
        "copy_tiktok":     copy_twitter,
        "card_image_url":  None,    # 6c will fill
        "card_image_path": None,
        "wastake_link":    "https://wastake.vercel.app",
        "source_link":     candidate.get("source_url"),
        "status":          "generated",
        "compliance_flags": {
            "angle_strength":   angle.get("strength"),
            "angle_reasoning":  angle.get("reasoning"),
            "angle_hook":       hook,
            "angle_cached":     angle.get("cached", False),
            "asset_match": {
                "detected_asset":  asset_id,
                "semaforo_source": "snapshot" if asset_id and snapshot else "default-neutral",
            },
            "market_context": _market_context(snapshot),
        },
    }


def _process_one(candidate: dict, snapshot: Optional[dict]) -> Optional[str]:
    """Process one candidate. Returns None on success, error string on failure."""
    payload = candidate.get("payload") or {}
    summary = payload.get("summary", "") or ""
    # Force Spanish output regardless of source language. WaCapital is a Spanish-
    # speaking brand and Groq handles the translation implicitly while it builds
    # the angle/headlines/tweets — no separate translation pass needed.
    lang    = "es"

    try:
        angle = wastake_client.get_news_angle(
            title=candidate["headline"],
            summary=summary,
            link=candidate.get("source_url", "") or "",
            tickers=[],
            lang=lang,
        )
    except Exception as e:
        return f"news-angle call failed: {e}"

    if not angle.get("angle"):
        return "news-angle returned empty angle field"

    try:
        post = _compose_post(candidate, angle, snapshot)
        client = get_client()
        client.table("pulse_posts").insert(post).execute()
    except Exception as e:
        return f"post insert failed: {e}"

    try:
        _mark_candidate(candidate["id"], "processed")
    except Exception as e:
        log.error("post inserted but candidate %d not marked processed: %s", candidate["id"], e)
        return f"candidate mark failed (post duplicated risk): {e}"

    return None


def run_one_cycle() -> dict:
    candidates = _list_pending(MAX_PER_CYCLE)
    totals = {
        "pending_picked":      len(candidates),
        "generated":           0,
        "errors":              0,
        "snapshot_ok":         False,
        "asset_matched":       0,
    }
    if not candidates:
        return totals

    # Snapshot once for the whole batch — backend caches for 2 min internally.
    snapshot: Optional[dict] = None
    try:
        snapshot = wastake_client.get_snapshot()
        totals["snapshot_ok"] = True
    except Exception as e:
        log.warning("snapshot fetch failed (posts will use neutral semaforo): %s", e)

    for cand in candidates:
        head = (cand.get("headline") or "")[:70]
        asset_match = _detect_asset(cand.get("headline"))
        if asset_match:
            totals["asset_matched"] += 1
        log.info(
            "  candidate %d [%s] asset=%s :: %s",
            cand["id"], cand.get("source", "?"), asset_match or "-", head,
        )
        err = _process_one(cand, snapshot)
        if err:
            totals["errors"] += 1
            log.warning("    FAIL: %s", err)
        else:
            totals["generated"] += 1
            log.info("    OK -> pulse_posts (status=generated)")

    return totals
