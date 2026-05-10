"""
Twitter publisher — Bloque 8a (Make.com edition).

Per cycle:
  1. Query pulse_posts WHERE status='approved' AND not yet published to Twitter.
     Publication state lives in compliance_flags.published_platforms.twitter —
     no DB migration needed.
  2. For each post (up to MAX_PER_CYCLE):
     a. Skip if card_image_url is NULL.
     b. Fire Make.com webhook: { post_id, text, image_url }.
     c. On 2xx from Make: mark compliance_flags with make_queued=True + sent_at.
        This prevents re-queueing on the next cycle.
  3. Returns stats dict for main.py logging.

Why Make.com instead of tweepy direct:
  X API is "Pay Per Use" — posting via tweepy requires credits ($0 balance = 402).
  Make.com has their own X developer app; when the user connects their account via
  OAuth, Make posts on their behalf using Make's API quota. Free tier = 1,000
  ops/month, enough for 3-5 tweets/day.

Make.com scenario structure:
  Webhook (Custom) → HTTP: Get a file (image_url) → X: Create a Tweet (text + media)

Auth: none required on this side — just the webhook URL in MAKE_WEBHOOK_URL env var.
"""
import logging
from datetime import datetime, timezone
from typing import Optional

import requests

from . import config
from .supabase_client import get_client

log = logging.getLogger("twitter-pub")

# ─── Tunables ───────────────────────────────────────────────────────────────

# Max posts to send per 5-min cycle.
MAX_PER_CYCLE = 2

# Timeout waiting for Make.com to acknowledge the webhook (Make responds
# immediately with HTTP 200 before executing the scenario).
MAKE_TIMEOUT = 15

# Twitter hard limit.
MAX_TWEET_LEN = 280


# ─── Helpers ────────────────────────────────────────────────────────────────

def _already_published(post: dict) -> bool:
    """True if this post has already been sent (or queued) to Twitter."""
    flags     = post.get("compliance_flags") or {}
    platforms = flags.get("published_platforms") or {}
    return bool(platforms.get("twitter"))


def _build_tweet_text(post: dict) -> str:
    """Use copy_twitter if available, fall back to headline."""
    text = (post.get("copy_twitter") or post.get("headline") or "").strip()
    if len(text) > MAX_TWEET_LEN:
        text = text[:MAX_TWEET_LEN - 1] + "…"
    return text


def _fire_make_webhook(
    tweet_text: str,
    image_url: Optional[str],
    post_id: str,
) -> bool:
    """POST to Make.com webhook. Returns True on HTTP 200."""
    url = config.MAKE_WEBHOOK_URL
    if not url:
        log.error("  MAKE_WEBHOOK_URL not configured")
        return False

    payload = {
        "post_id":   str(post_id),
        "text":      tweet_text,
        "image_url": image_url or "",
    }
    try:
        r = requests.post(url, json=payload, timeout=MAKE_TIMEOUT)
        if r.status_code == 200:
            log.info(
                "  ✅ Make.com queued post=%s text=%.60s…",
                post_id, tweet_text,
            )
            return True
        else:
            log.warning(
                "  Make.com returned %s for post=%s body=%s",
                r.status_code, post_id, r.text[:200],
            )
            return False
    except Exception as e:
        log.error("  Make.com webhook error post=%s: %s", post_id, e)
        return False


def _mark_queued(post_id) -> None:
    """
    Patch compliance_flags.published_platforms.twitter = {make_queued: true, sent_at: ...}.
    This truthy value prevents _already_published() from re-queuing the post
    while Make.com processes it.
    """
    client = get_client()
    try:
        res = (
            client.table("pulse_posts")
            .select("compliance_flags")
            .eq("id", post_id)
            .limit(1)
            .execute()
        )
        row       = (res.data or [{}])[0]
        flags     = dict(row.get("compliance_flags") or {})
        platforms = dict(flags.get("published_platforms") or {})
        platforms["twitter"] = {
            "make_queued": True,
            "sent_at":     datetime.now(timezone.utc).isoformat(),
        }
        flags["published_platforms"] = platforms
        client.table("pulse_posts").update(
            {"compliance_flags": flags}
        ).eq("id", post_id).execute()
    except Exception as e:
        log.error("  failed to mark post %s as queued: %s", post_id, e)


# ─── Main cycle ─────────────────────────────────────────────────────────────

def run_one_cycle() -> dict:
    stats = {"eligible": 0, "published": 0, "skipped_no_card": 0, "errors": 0}

    if not config.MAKE_WEBHOOK_URL or not config.MAKE_WEBHOOK_URL.startswith("http"):
        log.warning("twitter-pub: MAKE_WEBHOOK_URL not set — skipping cycle")
        return stats

    client = get_client()

    res = (
        client.table("pulse_posts")
        .select(
            "id, headline, copy_twitter, card_image_url, compliance_flags"
        )
        .eq("status", "approved")
        .order("created_at", desc=False)
        .limit(MAX_PER_CYCLE * 10)
        .execute()
    )
    rows    = res.data or []
    pending = [r for r in rows if not _already_published(r)]
    stats["eligible"] = len(pending)

    if not pending:
        log.info("twitter-pub: no approved posts pending publication")
        return stats

    for post in pending[:MAX_PER_CYCLE]:
        post_id  = post["id"]
        card_url = post.get("card_image_url")

        if not card_url:
            log.info("  post %s has no card — skipping", post_id)
            stats["skipped_no_card"] += 1
            continue

        tweet_text = _build_tweet_text(post)

        ok = _fire_make_webhook(tweet_text, card_url, post_id)
        if not ok:
            stats["errors"] += 1
            continue

        _mark_queued(post_id)
        stats["published"] += 1

    return stats
