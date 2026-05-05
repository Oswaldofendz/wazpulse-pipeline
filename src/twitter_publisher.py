"""
Twitter publisher — Bloque 8a.

Per cycle:
  1. Query pulse_posts WHERE status='approved' AND not yet published to Twitter.
     We track publication inside compliance_flags.published_platforms.twitter so
     no DB migration is needed.
  2. For each post (up to MAX_PER_CYCLE):
     a. Download card image from card_image_url (Supabase Storage public URL).
        If card_image_url is NULL, skip — we won't post cardless tweets.
     b. Upload image to Twitter via media/upload (v1.1 API).
     c. Post tweet via Twitter API v2 with copy_twitter text + media_id.
     d. Patch compliance_flags with twitter publish metadata (tweet_id + timestamp).
  3. Returns stats dict for main.py logging.

Auth: OAuth 1.0a via tweepy. Keys come from env vars (config.py).
Rate limits: Free tier allows 500 posts/month (17/day). We post ≤3/cycle × 288
cycles/day = 864 ceiling — but only approved posts flow through, typically 1-5/day.
"""
import logging
from datetime import datetime, timezone
from io import BytesIO
from typing import Optional

import requests
import tweepy

from . import config
from .supabase_client import get_client

log = logging.getLogger("twitter-pub")

# ─── Tunables ───────────────────────────────────────────────────────────────

# Max tweets to publish per 5-min cycle. Keep low — approval flow already
# rate-limits output to ~3-5 posts/day.
MAX_PER_CYCLE = 2

# Fetch timeout for downloading the card image from Supabase Storage.
IMG_FETCH_TIMEOUT = 20

# Twitter max tweet length (hard limit).
MAX_TWEET_LEN = 280


# ─── Auth ───────────────────────────────────────────────────────────────────

def _make_clients() -> tuple[tweepy.Client, tweepy.API]:
    """Return (v2 Client for posting, v1.1 API for media upload)."""
    auth = tweepy.OAuth1UserHandler(
        consumer_key=config.TWITTER_API_KEY,
        consumer_secret=config.TWITTER_API_SECRET,
        access_token=config.TWITTER_ACCESS_TOKEN,
        access_token_secret=config.TWITTER_ACCESS_TOKEN_SECRET,
    )
    v1_api = tweepy.API(auth, wait_on_rate_limit=False)
    v2_client = tweepy.Client(
        consumer_key=config.TWITTER_API_KEY,
        consumer_secret=config.TWITTER_API_SECRET,
        access_token=config.TWITTER_ACCESS_TOKEN,
        access_token_secret=config.TWITTER_ACCESS_TOKEN_SECRET,
    )
    return v2_client, v1_api


# ─── Helpers ────────────────────────────────────────────────────────────────

def _already_published(post: dict) -> bool:
    """True if this post has already been sent to Twitter."""
    flags = post.get("compliance_flags") or {}
    platforms = flags.get("published_platforms") or {}
    return bool(platforms.get("twitter"))


def _fetch_image(url: str) -> Optional[bytes]:
    """Download card image bytes from Supabase Storage public URL."""
    try:
        r = requests.get(url, timeout=IMG_FETCH_TIMEOUT)
        r.raise_for_status()
        return r.content
    except Exception as e:
        log.warning("  image fetch failed url=%s err=%s", url, e)
        return None


def _upload_media(v1_api: tweepy.API, img_bytes: bytes) -> Optional[int]:
    """Upload image to Twitter, return media_id or None on failure."""
    try:
        media = v1_api.media_upload(
            filename="wacapital_card.png",
            file=BytesIO(img_bytes),
        )
        return media.media_id
    except Exception as e:
        log.warning("  media upload failed: %s", e)
        return None


def _build_tweet_text(post: dict) -> str:
    """Use copy_twitter if available, fall back to headline."""
    text = (post.get("copy_twitter") or post.get("headline") or "").strip()
    # Ensure within hard limit
    if len(text) > MAX_TWEET_LEN:
        text = text[:MAX_TWEET_LEN - 1] + "…"
    return text


def _mark_published(post_id, tweet_id: str) -> None:
    """Patch compliance_flags.published_platforms.twitter with metadata."""
    client = get_client()
    try:
        # Fetch current flags
        res = (
            client.table("pulse_posts")
            .select("compliance_flags")
            .eq("id", post_id)
            .limit(1)
            .execute()
        )
        row = (res.data or [{}])[0]
        flags = dict(row.get("compliance_flags") or {})
        platforms = dict(flags.get("published_platforms") or {})
        platforms["twitter"] = {
            "tweet_id":    tweet_id,
            "published_at": datetime.now(timezone.utc).isoformat(),
        }
        flags["published_platforms"] = platforms
        client.table("pulse_posts").update(
            {"compliance_flags": flags}
        ).eq("id", post_id).execute()
    except Exception as e:
        log.error("  failed to mark post %s as twitter-published: %s", post_id, e)


# ─── Main cycle ─────────────────────────────────────────────────────────────

def run_one_cycle() -> dict:
    stats = {"eligible": 0, "published": 0, "skipped_no_card": 0, "errors": 0}

    if not all([
        config.TWITTER_API_KEY,
        config.TWITTER_API_SECRET,
        config.TWITTER_ACCESS_TOKEN,
        config.TWITTER_ACCESS_TOKEN_SECRET,
    ]):
        log.warning("Twitter credentials not configured — skipping cycle")
        return stats

    client = get_client()

    # Fetch approved posts. We over-fetch and filter in Python because
    # PostgREST can't do nested JSONB key existence checks easily.
    res = (
        client.table("pulse_posts")
        .select(
            "id, headline, copy_twitter, card_image_url, compliance_flags, candidate_id"
        )
        .eq("status", "approved")
        .order("approved_at", desc=False)   # oldest approved first
        .limit(MAX_PER_CYCLE * 10)
        .execute()
    )
    rows = res.data or []

    # Filter to only unpublished
    pending = [r for r in rows if not _already_published(r)]
    stats["eligible"] = len(pending)

    if not pending:
        log.info("twitter-pub: no approved posts pending publication")
        return stats

    # Build tweepy clients once (shared across posts in this cycle)
    try:
        v2_client, v1_api = _make_clients()
    except Exception as e:
        log.error("twitter-pub: failed to init tweepy clients: %s", e)
        stats["errors"] += 1
        return stats

    for post in pending[:MAX_PER_CYCLE]:
        post_id = post["id"]
        card_url = post.get("card_image_url")

        if not card_url:
            log.info("  post %s has no card — skipping", post_id)
            stats["skipped_no_card"] += 1
            continue

        # 1. Download card
        img_bytes = _fetch_image(card_url)
        if not img_bytes:
            stats["errors"] += 1
            continue

        # 2. Upload to Twitter media
        media_id = _upload_media(v1_api, img_bytes)
        if not media_id:
            stats["errors"] += 1
            continue

        # 3. Post tweet
        tweet_text = _build_tweet_text(post)
        try:
            resp = v2_client.create_tweet(
                text=tweet_text,
                media_ids=[media_id],
            )
            tweet_id = str(resp.data["id"])
            log.info(
                "  ✅ tweeted post=%s tweet_id=%s text=%.60s…",
                post_id, tweet_id, tweet_text,
            )
        except Exception as e:
            log.error("  create_tweet failed post=%s: %s", post_id, e)
            stats["errors"] += 1
            continue

        # 4. Mark published
        _mark_published(post_id, tweet_id)
        stats["published"] += 1

    return stats
