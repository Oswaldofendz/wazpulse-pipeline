"""
Daily storage cleanup -- prevents Supabase free-tier storage from filling up.

Background:
  Free tier storage cap is ~1 GB. Each TikTok carousel is 5 slides ~250-400 KB
  each (~1.5-2 MB per post), and each card hero is ~300-800 KB. At ~5
  approved posts per day this fills the cap in a few weeks, after which
  Supabase blocks the ENTIRE project (every REST/Storage call returns 402
  Payment Required, taking down the pipeline). This is exactly what happened
  on 2026-05-22 and triggered the project migration.

Strategy:
  - carousel-slides: delete folders older than CAROUSEL_DAYS (default 7).
    Once the TikTok post is live, slides are no longer needed.
  - card-images:     delete files older than CARD_DAYS (default 30).
    Cards are lighter and we sometimes re-reference them, so we keep longer.

Runs at most once per RUN_INTERVAL_HOURS, gated by a row in pulse_state so
restarts / multiple cycles in the same hour don't re-run it. Errors are
swallowed -- a failed cleanup must never crash the pipeline tick.
"""
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from .supabase_client import get_client

log = logging.getLogger("storage-cleanup")

CAROUSEL_BUCKET = "carousel-slides"
CARD_BUCKET     = "card-images"

CAROUSEL_DAYS = 7
CARD_DAYS     = 30

RUN_INTERVAL_HOURS = 24
STATE_KEY          = "storage_cleanup_last_run"

# Batch size for `bucket.remove([...])` calls.
DELETE_BATCH = 100

# Page size when listing folders/files. 1000 is Supabase's max.
LIST_LIMIT = 1000


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


# ─── State (last-run tracking via pulse_state) ──────────────────────────────

def _get_last_run() -> Optional[datetime]:
    try:
        client = get_client()
        res = (
            client.table("pulse_state")
            .select("value")
            .eq("key", STATE_KEY)
            .limit(1)
            .execute()
        )
        rows = res.data or []
        if not rows:
            return None
        value = rows[0].get("value") or {}
        return _parse_iso(value.get("ts"))
    except Exception as e:
        log.warning("could not read last_run: %s", e)
        return None


def _set_last_run(when: datetime) -> None:
    try:
        client = get_client()
        client.table("pulse_state").upsert({
            "key":        STATE_KEY,
            "value":      {"ts": when.isoformat()},
            "updated_at": when.isoformat(),
        }).execute()
    except Exception as e:
        log.warning("could not save last_run: %s", e)


# ─── Bucket walkers ─────────────────────────────────────────────────────────

def _is_folder(item: dict) -> bool:
    # Supabase Storage marks "directories" with id == None and metadata == None.
    return item.get("id") is None


def _collect_old_keys_flat(bucket, prefix: str, cutoff: datetime) -> list[str]:
    """List files under a single flat prefix (no recursion). Returns keys
    whose `created_at` is older than `cutoff`."""
    out: list[str] = []
    offset = 0
    while True:
        items = bucket.list(prefix, {
            "limit":  LIST_LIMIT,
            "offset": offset,
            "sortBy": {"column": "created_at", "order": "asc"},
        }) or []
        if not items:
            break
        for it in items:
            if _is_folder(it):
                continue
            name = it.get("name")
            if not name:
                continue
            created = _parse_iso(it.get("created_at"))
            if created is None or created >= cutoff:
                continue
            key = f"{prefix}/{name}" if prefix else name
            out.append(key)
        if len(items) < LIST_LIMIT:
            break
        offset += LIST_LIMIT
    return out


def _collect_old_keys_two_level(bucket, cutoff: datetime) -> list[str]:
    """For buckets shaped like {folder}/{file}.ext (e.g. carousel-slides:
    {post_id}/slide_NN.jpg). Lists top-level folders, then for each folder
    lists its files; collects file keys with created_at < cutoff."""
    out: list[str] = []
    offset = 0
    while True:
        top = bucket.list("", {
            "limit":  LIST_LIMIT,
            "offset": offset,
            "sortBy": {"column": "name", "order": "asc"},
        }) or []
        if not top:
            break
        for it in top:
            if not _is_folder(it):
                # Stray file at root -- check it too.
                created = _parse_iso(it.get("created_at"))
                name    = it.get("name")
                if name and created and created < cutoff:
                    out.append(name)
                continue
            folder = it.get("name")
            if not folder:
                continue
            out.extend(_collect_old_keys_flat(bucket, folder, cutoff))
        if len(top) < LIST_LIMIT:
            break
        offset += LIST_LIMIT
    return out


def _delete_batch(bucket, keys: list[str]) -> tuple[int, int]:
    """Returns (deleted, errors). Batches in chunks of DELETE_BATCH."""
    deleted = 0
    errors  = 0
    for i in range(0, len(keys), DELETE_BATCH):
        chunk = keys[i:i + DELETE_BATCH]
        try:
            bucket.remove(chunk)
            deleted += len(chunk)
        except Exception as e:
            log.warning("remove failed for %d keys: %s", len(chunk), e)
            errors += 1
    return deleted, errors


# ─── Public entry point ─────────────────────────────────────────────────────

def run_if_due() -> None:
    """Run cleanup if last run was >= RUN_INTERVAL_HOURS ago. Swallows all
    errors -- this must never break the pipeline tick."""
    try:
        now  = _now()
        last = _get_last_run()
        if last and (now - last) < timedelta(hours=RUN_INTERVAL_HOURS):
            return  # too soon, skip silently

        log.info(
            "storage-cleanup: starting (carousel>%dd, cards>%dd)",
            CAROUSEL_DAYS, CARD_DAYS,
        )
        client = get_client()

        # carousel-slides: {post_id}/slide_NN.jpg
        try:
            bucket = client.storage.from_(CAROUSEL_BUCKET)
            cutoff = now - timedelta(days=CAROUSEL_DAYS)
            keys   = _collect_old_keys_two_level(bucket, cutoff)
            deleted, errors = _delete_batch(bucket, keys)
            log.info(
                "storage-cleanup: %s -- candidates=%d deleted=%d errors=%d",
                CAROUSEL_BUCKET, len(keys), deleted, errors,
            )
        except Exception as e:
            log.warning("storage-cleanup: %s failed: %s", CAROUSEL_BUCKET, e)

        # card-images: posts/{candidate_id}.png
        try:
            bucket = client.storage.from_(CARD_BUCKET)
            cutoff = now - timedelta(days=CARD_DAYS)
            keys   = _collect_old_keys_flat(bucket, "posts", cutoff)
            deleted, errors = _delete_batch(bucket, keys)
            log.info(
                "storage-cleanup: %s -- candidates=%d deleted=%d errors=%d",
                CARD_BUCKET, len(keys), deleted, errors,
            )
        except Exception as e:
            log.warning("storage-cleanup: %s failed: %s", CARD_BUCKET, e)

        _set_last_run(now)
        log.info("storage-cleanup: done")

    except Exception as e:
        # Outer safety net: never let cleanup take down the cycle.
        log.exception("storage-cleanup: unexpected error: %s", e)
