"""
TikTok publisher — Bloque 8c (Photo Carousel).

Per cycle:
  1. Query pulse_posts WHERE status='approved' AND carousel_urls present
     AND not yet published to TikTok.
  2. For each post (up to MAX_PER_CYCLE):
     a. Refresh TikTok access_token using refresh_token (auto-rotation).
     b. If carousel_urls missing → generate slides + upload to Supabase first.
     c. POST to TikTok Content Posting API (PULL_FROM_URL, DIRECT_POST).
     d. Mark compliance_flags.published_platforms.tiktok with publish_id.
  3. Returns stats dict for main.py logging.

TikTok Photo Carousel API:
  POST https://open.tiktokapis.com/v2/post/publish/content/init/
  media_type: "PHOTO"
  post_mode: "DIRECT_POST"
  source_info.source: "PULL_FROM_URL"
  source_info.photo_images: [url1, url2, ...url7]

Token refresh:
  POST https://open.tiktokapis.com/v2/oauth/token/
  grant_type: refresh_token
  Refresh tokens last 365 days; each refresh may rotate the refresh_token.
  New tokens are logged — update Railway TIKTOK_REFRESH_TOKEN when prompted.

Sandbox note:
  In sandbox mode, posts go to a "sandbox feed" (not real TikTok timeline).
  publish_id is returned immediately; poll /v2/post/publish/status/fetch/
  for status confirmation.
"""
import logging
import time
from datetime import datetime, timezone
from typing import Optional

import requests

from . import config
from .carousel_generator import generate_carousel, upload_carousel_to_supabase
from .supabase_client import get_client

log = logging.getLogger("tiktok-pub")

# ─── Tunables ───────────────────────────────────────────────────────────────

MAX_PER_CYCLE   = 1          # TikTok is slower to process; 1/cycle is safe
POLL_ATTEMPTS   = 6          # how many times to poll publish status
POLL_INTERVAL   = 5          # seconds between polls
MAX_CAPTION_LEN = 2200       # TikTok hard limit
MAX_PHOTO_SLIDES = 35        # TikTok hard limit per carousel

# ─── TikTok API constants ────────────────────────────────────────────────────

TIKTOK_TOKEN_URL       = "https://open.tiktokapis.com/v2/oauth/token/"
TIKTOK_PUBLISH_URL     = "https://open.tiktokapis.com/v2/post/publish/content/init/"
TIKTOK_STATUS_URL      = "https://open.tiktokapis.com/v2/post/publish/status/fetch/"
TIKTOK_CREATOR_INFO_URL = "https://open.tiktokapis.com/v2/post/publish/creator_info/query/"

# Module-level access token cache (refreshed per cycle as needed)
_cached_access_token: Optional[str] = None


# ─── Token management ────────────────────────────────────────────────────────

def _refresh_access_token() -> Optional[str]:
    """
    Exchange the stored refresh_token for a new access_token.
    Logs the new tokens prominently — user must update Railway env vars
    when refresh_token rotates.
    Returns the new access_token, or None on failure.
    """
    global _cached_access_token

    client_key    = config.TIKTOK_CLIENT_KEY
    client_secret = config.TIKTOK_CLIENT_SECRET
    refresh_token = config.TIKTOK_REFRESH_TOKEN

    if not all([client_key, client_secret, refresh_token]):
        log.error("tiktok-pub: missing TIKTOK_CLIENT_KEY / CLIENT_SECRET / REFRESH_TOKEN")
        return None

    try:
        r = requests.post(
            TIKTOK_TOKEN_URL,
            data={
                "client_key":    client_key,
                "client_secret": client_secret,
                "grant_type":    "refresh_token",
                "refresh_token": refresh_token,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=20,
        )
        body = r.json()
    except Exception as e:
        log.error("tiktok-pub: token refresh request failed: %s", e)
        return None

    if r.status_code != 200 or body.get("error"):
        log.error(
            "tiktok-pub: token refresh failed status=%s body=%s",
            r.status_code, body,
        )
        return None

    new_access  = body.get("access_token")
    new_refresh = body.get("refresh_token")
    expires_in  = body.get("expires_in", 86400)

    if not new_access:
        log.error("tiktok-pub: no access_token in refresh response: %s", body)
        return None

    _cached_access_token = new_access
    log.info(
        "tiktok-pub: access_token refreshed (expires_in=%ss)", expires_in
    )

    # Warn if refresh_token rotated — user must update Railway
    if new_refresh and new_refresh != refresh_token:
        log.warning(
            "⚠️  TIKTOK REFRESH_TOKEN ROTATED — update Railway env var:\n"
            "    TIKTOK_REFRESH_TOKEN=%s", new_refresh
        )

    return new_access


def _get_token() -> Optional[str]:
    """Return cached token or refresh if not available."""
    global _cached_access_token
    if not _cached_access_token:
        _cached_access_token = _refresh_access_token()
    return _cached_access_token


def _query_creator_info(access_token: str) -> Optional[dict]:
    """
    POST to creator_info/query to learn allowed privacy_level_options.
    Logs full response so we can debug invalid_params issues.
    Returns data dict on success, None on failure.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type":  "application/json; charset=UTF-8",
    }
    try:
        r = requests.post(TIKTOK_CREATOR_INFO_URL, headers=headers, timeout=15)
        body = r.json()
    except Exception as e:
        log.error("tiktok-pub: creator_info request error: %s", e)
        return None

    log.info(
        "tiktok-pub: creator_info status=%s body=%s",
        r.status_code, body,
    )
    if r.status_code == 200:
        err_code = (body.get("error") or {}).get("code", "")
        if err_code in ("ok", ""):
            return body.get("data") or {}
    log.error("tiktok-pub: creator_info failed status=%s", r.status_code)
    return None


# ─── Guard helpers ───────────────────────────────────────────────────────────

def _already_published(post: dict) -> bool:
    """True if post has been sent to TikTok."""
    flags     = post.get("compliance_flags") or {}
    platforms = flags.get("published_platforms") or {}
    return bool(platforms.get("tiktok"))


def _get_carousel_urls(post: dict) -> list[str]:
    """Return existing carousel_urls from compliance_flags, or empty list."""
    flags = post.get("compliance_flags") or {}
    return flags.get("carousel_urls") or []


# ─── Caption builder ─────────────────────────────────────────────────────────

# TikTok hashtags for WaCapital finance content (static + dynamic)
STATIC_HASHTAGS = "#WaCapital #crypto #mercados #finanzas #bitcoin #inversiones"

def _build_caption(post: dict) -> str:
    """
    Build TikTok caption (<=2200 chars).
    Structure: hook + angle + hashtags.
    Uses copy_tiktok if available, falls back to hook -> headline.
    angle_hook and angle_reasoning live inside the metadata JSONB column.
    """

    flags    = post.get("compliance_flags") or {}
    copy     = (post.get("copy_tiktok")       or "").strip()
    hook     = (flags.get("angle_hook")        or "").strip()
    angle    = (flags.get("angle_reasoning")   or "").strip()

    # Primary copy
    if copy:
        body = copy
    elif hook:
        body = hook
    else:
        body = (post.get("headline") or "").strip()

    # Append editorial angle as context if fits
    budget = MAX_CAPTION_LEN - len(STATIC_HASHTAGS) - 4
    if angle and len(body) + len(angle) + 2 < budget:
        body = f"{body}\n\n{angle}"

    caption = f"{body}\n\n{STATIC_HASHTAGS}"
    return caption[:MAX_CAPTION_LEN]


# ─── TikTok API calls ────────────────────────────────────────────────────────

def _post_carousel(
    access_token: str,
    open_id: str,
    photo_urls: list[str],
    caption: str,
    privacy_level: str = "SELF_ONLY",
) -> Optional[str]:
    """
    POST carousel to TikTok Content Posting API.
    Returns publish_id on success, None on failure.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type":  "application/json; charset=UTF-8",
    }

    # Ensure title is non-empty after stripping
    title = caption.strip()
    if not title:
        title = "#WaCapital #crypto #finanzas"
        log.warning("tiktok-pub: caption was empty — using fallback hashtags")

    payload = {
        "post_info": {
            "title":           title,
            "privacy_level":   privacy_level,
            "disable_comment": False,
        },
        "source_info": {
            "source":             "PULL_FROM_URL",
            "photo_cover_index":  1,
            "photo_images":       photo_urls[:MAX_PHOTO_SLIDES],
        },
        "post_mode":  "DIRECT_POST",
        "media_type": "PHOTO",
    }

    log.info(
        "tiktok-pub: payload post_info=%s photo_count=%d title_len=%d",
        {k: v for k, v in payload["post_info"].items() if k != "title"},
        len(photo_urls),
        len(title),
    )

    try:
        r = requests.post(
            TIKTOK_PUBLISH_URL,
            json=payload,
            headers=headers,
            timeout=30,
        )
        body = r.json()
    except Exception as e:
        log.error("tiktok-pub: publish request error: %s", e)
        return None

    if r.status_code != 200:
        log.error(
            "tiktok-pub: publish HTTP %s body=%s", r.status_code, body
        )
        return None

    err_code = (body.get("error") or {}).get("code", "")
    if err_code not in ("ok", ""):
        log.error("tiktok-pub: API error code=%s body=%s", err_code, body)
        return None

    publish_id = (body.get("data") or {}).get("publish_id")
    if not publish_id:
        log.error("tiktok-pub: no publish_id in response: %s", body)
        return None

    log.info("tiktok-pub: ✅ publish_id=%s", publish_id)
    return publish_id


def _poll_publish_status(access_token: str, publish_id: str) -> str:
    """
    Poll TikTok until status resolves to PUBLISH_COMPLETE / FAILED / CANCELLED.
    Returns the final status string, or 'TIMEOUT' if exhausted.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type":  "application/json; charset=UTF-8",
    }
    for attempt in range(1, POLL_ATTEMPTS + 1):
        time.sleep(POLL_INTERVAL)
        try:
            r = requests.post(
                TIKTOK_STATUS_URL,
                json={"publish_id": publish_id},
                headers=headers,
                timeout=15,
            )
            body = r.json()
        except Exception as e:
            log.warning("tiktok-pub: status poll error attempt=%d: %s", attempt, e)
            continue

        status = (body.get("data") or {}).get("status", "")
        log.info(
            "tiktok-pub: publish_id=%s status=%s (attempt %d/%d)",
            publish_id, status, attempt, POLL_ATTEMPTS,
        )
        if status in ("PUBLISH_COMPLETE", "FAILED", "CANCELLED"):
            return status

    return "TIMEOUT"


# ─── Supabase flag writers ────────────────────────────────────────────────────

def _mark_published(post_id, publish_id: str, status: str) -> None:
    """
    Set compliance_flags.published_platforms.tiktok = {publish_id, status, published_at}.
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
        platforms["tiktok"] = {
            "publish_id":   publish_id,
            "status":       status,
            "published_at": datetime.now(timezone.utc).isoformat(),
        }
        flags["published_platforms"] = platforms
        client.table("pulse_posts").update(
            {"compliance_flags": flags}
        ).eq("id", post_id).execute()
        log.info("tiktok-pub: marked post=%s status=%s", post_id, status)
    except Exception as e:
        log.error("tiktok-pub: failed to mark post=%s: %s", post_id, e)


def _save_carousel_urls(post_id, urls: list[str]) -> None:
    """Persist carousel_urls into compliance_flags for reuse."""
    client = get_client()
    try:
        res = (
            client.table("pulse_posts")
            .select("compliance_flags")
            .eq("id", post_id)
            .limit(1)
            .execute()
        )
        row   = (res.data or [{}])[0]
        flags = dict(row.get("compliance_flags") or {})
        flags["carousel_urls"] = urls
        client.table("pulse_posts").update(
            {"compliance_flags": flags}
        ).eq("id", post_id).execute()
    except Exception as e:
        log.error("tiktok-pub: failed to save carousel_urls post=%s: %s", post_id, e)


# ─── Main cycle ─────────────────────────────────────────────────────────────

def run_one_cycle() -> dict:
    stats = {
        "eligible": 0,
        "published": 0,
        "generated_carousel": 0,
        "skipped_no_card": 0,
        "errors": 0,
    }

    # Guard: all 4 TikTok env vars required
    if not all([
        config.TIKTOK_CLIENT_KEY,
        config.TIKTOK_CLIENT_SECRET,
        config.TIKTOK_REFRESH_TOKEN,
        config.TIKTOK_OPEN_ID,
    ]):
        log.warning("tiktok-pub: TIKTOK_* env vars not set — skipping cycle")
        return stats

    client = get_client()

    # Fetch approved posts (broader set, we filter in Python)
    res = (
        client.table("pulse_posts")
        .select(
            "id, headline, copy_tiktok, card_image_url, compliance_flags, semaforo"
        )
        .eq("status", "approved")
        .order("created_at", desc=False)
        .limit(MAX_PER_CYCLE * 15)
        .execute()
    )
    rows    = res.data or []
    pending = [r for r in rows if not _already_published(r)]
    stats["eligible"] = len(pending)

    if not pending:
        log.info("tiktok-pub: no approved posts pending TikTok publication")
        return stats

    # Refresh token once per cycle (access_token expires in 24h)
    access_token = _refresh_access_token()
    if not access_token:
        log.error("tiktok-pub: could not obtain access_token — aborting cycle")
        stats["errors"] += 1
        return stats

    # Query creator info to get allowed privacy levels and settings
    creator_info = _query_creator_info(access_token)
    if creator_info:
        privacy_options = creator_info.get("privacy_level_options") or ["SELF_ONLY"]
        privacy_level = privacy_options[0]
        log.info("tiktok-pub: creator_info OK — using privacy_level=%s", privacy_level)
    else:
        privacy_level = "SELF_ONLY"
        log.warning("tiktok-pub: creator_info failed — defaulting to SELF_ONLY")

    open_id = config.TIKTOK_OPEN_ID

    for post in pending[:MAX_PER_CYCLE]:
        post_id = post["id"]

        # 1. Get or generate carousel slides
        carousel_urls = _get_carousel_urls(post)

        if not carousel_urls:
            # Generate slides + upload to Supabase Storage
            if not post.get("card_image_url"):
                log.info("tiktok-pub: post=%s has no card_image_url — skipping", post_id)
                stats["skipped_no_card"] += 1
                continue

            log.info("tiktok-pub: generating carousel for post=%s", post_id)
            try:
                slides = generate_carousel(post)
                carousel_urls = upload_carousel_to_supabase(post_id, slides)
                _save_carousel_urls(post_id, carousel_urls)
                stats["generated_carousel"] += 1
                log.info(
                    "tiktok-pub: carousel generated — %d slides for post=%s",
                    len(carousel_urls), post_id,
                )
            except Exception as e:
                log.error(
                    "tiktok-pub: carousel generation failed post=%s: %s",
                    post_id, e,
                )
                stats["errors"] += 1
                continue

        if not carousel_urls:
            log.error("tiktok-pub: empty carousel_urls for post=%s", post_id)
            stats["errors"] += 1
            continue

        # 2. Build caption
        caption = _build_caption(post)
        log.info(
            "tiktok-pub: posting %d slides to TikTok post=%s",
            len(carousel_urls), post_id,
        )

        # 3. Post to TikTok
        publish_id = _post_carousel(access_token, open_id, carousel_urls, caption, privacy_level)
        if not publish_id:
            stats["errors"] += 1
            continue

        # 4. Poll for status (non-blocking — mark queued even if timeout)
        final_status = _poll_publish_status(access_token, publish_id)
        log.info(
            "tiktok-pub: post=%s publish_id=%s final_status=%s",
            post_id, publish_id, final_status,
        )

        # 5. Mark in DB regardless of status (publish_id proves it was sent)
        _mark_published(post_id, publish_id, final_status)

        if final_status in ("PUBLISH_COMPLETE", "TIMEOUT"):
            # TIMEOUT means still processing -- not a failure
            stats["published"] += 1
        else:
            # FAILED or CANCELLED
            stats["errors"] += 1

    return stats
