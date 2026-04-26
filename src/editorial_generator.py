"""
Editorial generator — Bloque 6a.

Reads up to MAX_PER_CYCLE pending candidates, calls /api/analysis/news-angle
for each, composes a pulse_posts row, and marks the candidate as processed.

Failure handling:
  - On news-angle error: candidate stays 'pending', retried next cycle.
  - Candidates that stay pending past expires_at (NOW+6h) are effectively
    dead-lettered (no cleanup job yet — a future task can reap them).
  - On post insert error: candidate stays 'pending'. Same retry path.

Deferred to Bloque 6b/6c:
  - Snapshot context → real semáforo per asset (currently always 'neutral').
  - Card image generation (currently card_image_url/path = NULL).
"""
import logging
from datetime import datetime, timezone
from typing import Optional

from . import wastake_client
from .supabase_client import get_client

log = logging.getLogger("editorial-gen")

# Throughput knob. With cycles every 5 min, 5 candidates/cycle = 60/hour,
# enough to keep up with the RSS fetcher's typical ~3-5 new/cycle rate.
MAX_PER_CYCLE = 5

# pulse_posts.headline is varchar — keep it sane.
MAX_HEADLINE_LEN = 500
# Twitter hard limit.
MAX_TWEET_LEN = 280


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


def _compose_post(candidate: dict, angle: dict) -> dict:
    """Build the pulse_posts row from a candidate + news-angle response."""
    tweets    = angle.get("tweets") or []
    headlines = angle.get("headlines") or []
    hook      = (angle.get("hook") or "").strip()

    copy_twitter   = (tweets[0] if tweets else hook or candidate["headline"]).strip()[:MAX_TWEET_LEN]
    ig_caption     = (angle.get("instagram_caption") or "").strip() or copy_twitter
    chosen_headline = (headlines[0] if headlines else candidate["headline"])[:MAX_HEADLINE_LEN]

    return {
        "candidate_id":    candidate["id"],
        "headline":        chosen_headline,
        "semaforo":        "neutral",   # 6b will compute from snapshot
        "asset_affected":  candidate.get("asset_id"),
        "copy_twitter":    copy_twitter,
        "copy_instagram":  ig_caption,
        "copy_facebook":   ig_caption,        # FB shares IG copy for now
        "copy_tiktok":     copy_twitter,      # TikTok placeholder
        "card_image_url":  None,              # 6c will generate
        "card_image_path": None,
        "wastake_link":    "https://wastake.vercel.app",
        "source_link":     candidate.get("source_url"),
        "status":          "generated",
        "compliance_flags": {
            "angle_strength":  angle.get("strength"),
            "angle_reasoning": angle.get("reasoning"),
            "angle_hook":      hook,
            "angle_cached":    angle.get("cached", False),
        },
    }


def _process_one(candidate: dict) -> Optional[str]:
    """Process one candidate. Returns None on success, error string on failure."""
    payload  = candidate.get("payload") or {}
    summary  = payload.get("summary", "") or ""
    lang     = payload.get("language") or "es"

    try:
        angle = wastake_client.get_news_angle(
            title=candidate["headline"],
            summary=summary,
            link=candidate.get("source_url", "") or "",
            tickers=[],   # asset extraction is a future task
            lang=lang,
        )
    except Exception as e:
        return f"news-angle call failed: {e}"

    if not angle.get("angle"):
        return "news-angle returned empty angle field"

    try:
        post = _compose_post(candidate, angle)
        client = get_client()
        client.table("pulse_posts").insert(post).execute()
    except Exception as e:
        return f"post insert failed: {e}"

    try:
        _mark_candidate(candidate["id"], "processed")
    except Exception as e:
        # Post is in DB but candidate not marked — will get reprocessed next
        # cycle and create a duplicate post. Log loudly.
        log.error("post inserted but candidate %d not marked processed: %s", candidate["id"], e)
        return f"candidate mark failed (post duplicated risk): {e}"

    return None


def run_one_cycle() -> dict:
    candidates = _list_pending(MAX_PER_CYCLE)
    totals = {
        "pending_picked": len(candidates),
        "generated":      0,
        "errors":         0,
    }
    if not candidates:
        return totals

    for cand in candidates:
        head = (cand.get("headline") or "")[:70]
        log.info("  candidate %d [%s] %s", cand["id"], cand.get("source", "?"), head)
        err = _process_one(cand)
        if err:
            totals["errors"] += 1
            log.warning("    FAIL: %s", err)
        else:
            totals["generated"] += 1
            log.info("    OK -> pulse_posts (status=generated)")

    return totals
