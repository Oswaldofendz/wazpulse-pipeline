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
import re
import time
from datetime import datetime, timezone
from typing import Optional

from . import wastake_client, card_generator, entity_detector
from .rss_fetcher import is_parasitic
from .supabase_client import get_client

log = logging.getLogger("editorial-gen")

# Throughput knob. Cycles run every 5 min; 2 candidates/cycle = 24/hour.
# Conservative because Groq is shared with snapshot sentiment fallback and the
# /narrative endpoint, so saturation triggers 502s from the backend. RSS fetcher
# typically adds ~3-5 candidates/cycle; 24/hour is enough to stay even.
MAX_PER_CYCLE = 2
# Pause between candidates to spread out Groq calls.
INTER_CANDIDATE_SLEEP_SEC = 5

# Drop posts that Groq self-rates as filler. After feedback that Groq is being
# too generous (rating boring earnings beats as 4/5), raised threshold from 3
# to 4. Combined with the stricter prompt update, only meaningful news survives.
MIN_ANGLE_STRENGTH_INSERT = 4

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


# Sentiment word banks. Used as a tie-breaker when no asset match — gives us
# more red/green polarity than the previous "everything is amarillo" macro fallback.
# Mixed Spanish + English because RSS sources publish in either language.
_POSITIVE_WORDS = re.compile(
    r"\b("
    r"soars?|surges?|jumps?|rallies|rally|gains?|record|highs?|booms?|breakthrough|approves?|approved|"
    r"beats?|crushes?|outperforms?|wins?|profitable|profits?|bullish|moonshot|"
    r"sube|salta|repunta|récord|maximo|máximo|alza|gana|aprueba|aprobad[oa]|supera|favorable|optimista"
    r")\b",
    re.IGNORECASE,
)
_NEGATIVE_WORDS = re.compile(
    r"\b("
    r"crashes?|plunges?|drops?|tumbles?|falls?|loses?|losses?|warning|alert|cuts?|slumps?|sinks?|"
    r"misses?|disappoints?|bearish|panic|crisis|risk|downgrade|"
    r"cae|desploma|hunde|pierde|pérdida|alerta|crisis|recorta|caída|baja|advierte|riesgo|miedo|temor"
    r")\b",
    re.IGNORECASE,
)


def _sentiment_semaforo(headline: Optional[str]) -> Optional[str]:
    """Light-weight sentiment from headline keywords. Returns 'verde'/'rojo' or
    None if mixed/neutral. Used as a tie-breaker for posts without an asset
    match so we stop defaulting everything to amarillo."""
    if not headline:
        return None
    pos = len(_POSITIVE_WORDS.findall(headline))
    neg = len(_NEGATIVE_WORDS.findall(headline))
    if pos and not neg:
        return "verde"
    if neg and not pos:
        return "rojo"
    if pos > neg:
        return "verde"
    if neg > pos:
        return "rojo"
    return None  # tied or no signal — let macro fallback decide


def _semaforo_for(
    asset_id: Optional[str],
    snapshot: Optional[dict],
    headline: Optional[str] = None,
) -> tuple[str, str]:
    """
    Resolve a semáforo for the post. Returns (semaforo, source) so we can audit
    in compliance_flags.

    Priority:
      1. snapshot-asset       — headline matched BTC/ETH/SOL/SPY/Gold → live snapshot.
      2. headline-sentiment   — no asset match but the headline carries clear
                                positive or negative wording → verde or rojo.
      3. snapshot-macro       — derive from market-wide flags (stress / momentum / F&G).
      4. default-neutral      — snapshot unavailable AND no sentiment signal.
    """
    # 1. Asset-specific lookup wins.
    if asset_id and snapshot:
        for s in snapshot.get("semaforos") or []:
            if s.get("id") == asset_id:
                return (s.get("semaforo") or "neutral"), "snapshot-asset"

    # 2. Headline sentiment tie-breaker — gives us polarity instead of defaulting amarillo.
    sentiment = _sentiment_semaforo(headline)
    if sentiment:
        return sentiment, "headline-sentiment"

    if not snapshot:
        return "neutral", "default-neutral"

    # 3. Macro fallback for truly neutral wording.
    flags  = snapshot.get("flags")  or {}
    market = snapshot.get("market") or {}
    fg     = market.get("fearGreed") or {}
    fg_val = fg.get("value", 50) or 50
    strong = flags.get("strongSignalsCount") or 0

    if flags.get("feargreedExtreme") or flags.get("marketStressed"):
        return "rojo", "snapshot-macro"
    if strong >= 4 and fg_val >= 45:
        return "verde", "snapshot-macro"
    return "amarillo", "snapshot-macro"


def _market_context(snapshot: Optional[dict]) -> dict:
    """Extract the macro signals worth carrying with each post for human review."""
    if not snapshot:
        return {"snapshot_unavailable": True}
    market = snapshot.get("market") or {}
    flags  = snapshot.get("flags")  or {}
    fg     = market.get("fearGreed") or {}
    return {
        "fearGreed":           fg.get("value"),
        "fearGreedClass":      fg.get("classification"),
        "marketStressed":      flags.get("marketStressed"),
        "macroEventSoon":      flags.get("macroEventSoon"),
        "bigWhaleActivity":    flags.get("bigWhaleActivity"),
        "feargreedExtreme":    flags.get("feargreedExtreme"),
        "strongSignalsCount":  flags.get("strongSignalsCount"),
        "calendarSource":      snapshot.get("calendarSource"),
    }


def _list_pending(limit: int) -> list[dict]:
    """
    Fetch a larger pool than `limit`, drop any parasitic ones (Form 144, etc.)
    discovered along the way, and return up to `limit` clean candidates.

    Parasitics found in the pool are also marked status='discarded' so they
    never come back into the queue. This catches anything that slipped past
    the RSS-time filter (older rows from before the filter existed, or
    headlines that match a regex we hadn't covered yet).
    """
    client = get_client()
    pool_size = max(limit * 10, 30)  # generous: aim to find `limit` cleans
    res = (
        client.table("pulse_candidates")
        .select("id, headline, source, source_url, payload, event_type, asset_id, asset_type, priority")
        .eq("status", "pending")
        .order("priority", desc=True)
        .order("detected_at", desc=False)
        .limit(pool_size)
        .execute()
    )
    rows = res.data or []
    if not rows:
        return []

    clean: list[dict] = []
    parasitic_ids: list = []
    for row in rows:
        if is_parasitic(row.get("headline")):
            parasitic_ids.append(row["id"])
            continue
        clean.append(row)
        if len(clean) >= limit:
            break

    if parasitic_ids:
        log.info("  pre-pick: discarding %d parasitic candidates from queue", len(parasitic_ids))
        try:
            client.table("pulse_candidates").update({
                "status":       "discarded",
                "processed_at": datetime.now(timezone.utc).isoformat(),
            }).in_("id", parasitic_ids).execute()
        except Exception as e:
            log.warning("  pre-pick: bulk discard failed: %s", e)

    return clean


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

    # Two-tier asset detection:
    # 1) Snapshot-tracked match (BTC/ETH/SOL/SPY/Gold) drives the live semáforo lookup.
    # 2) Richer entity detection (~28 entities incl Tesla/Apple/etc.) drives the
    #    visual: card_generator uses this to pick a logo. Entity-id wins for asset_affected
    #    so backfill renders see e.g. "tesla" or "meta" instead of NULL.
    snapshot_asset_id   = _detect_asset(candidate.get("headline"))
    rich_entity         = entity_detector.detect_entity(candidate.get("headline"))
    asset_aff           = (rich_entity["id"] if rich_entity else None) or snapshot_asset_id or candidate.get("asset_id")
    semaforo, sema_src  = _semaforo_for(snapshot_asset_id, snapshot, candidate.get("headline"))

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
            "angle_reasoning": angle.get("reasoning"),
            "angle_hook":       hook,
            "angle_cached":     angle.get("cached", False),
            "asset_match": {
                "detected_asset":  snapshot_asset_id,
                "rich_entity":     (rich_entity["id"] if rich_entity else None),
                "semaforo_source": sema_src,   # snapshot-asset | headline-sentiment | snapshot-macro | default-neutral
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

    # Quality gate: drop filler posts before they pollute pulse_posts and
    # before we spend cycles generating cards / showing them in Telegram.
    angle_strength = angle.get("strength") or 0
    if angle_strength and angle_strength < MIN_ANGLE_STRENGTH_INSERT:
        # Mark candidate as processed so we don't keep retrying it.
        try:
            _mark_candidate(candidate["id"], "processed")
        except Exception:
            pass
        return f"angle_strength={angle_strength} below threshold {MIN_ANGLE_STRENGTH_INSERT} (filler, dropped)"

    try:
        post = _compose_post(candidate, angle, snapshot)
    except Exception as e:
        return f"post compose failed: {e}"

    # Render + upload the card image. Best-effort: a failure here doesn't
    # block insertion — the post still goes in with NULL image URLs and
    # we'll backfill later from a maintenance script if needed.
    url, path = card_generator.render_and_upload(post, candidate_id=candidate["id"])
    if url:
        post["card_image_url"]  = url
        post["card_image_path"] = path

    try:
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


# ─── Card backfill ──────────────────────────────────────────────────────────

# How many cards to backfill per cycle. With 5min cycles and ~1500 posts
# missing cards, 10/cycle drains in ~12.5h. Goes up if we want faster.
CARD_BACKFILL_PER_CYCLE = 10


def backfill_cards() -> dict:
    """
    Pick N posts without a card_image_url and generate cards for them.
    Independent of the LLM — runs purely off the data already in pulse_posts.

    Targets posts that could still be published (generated / pending_approval /
    approved). Skips rejected/failed/archived because we won't post those anyway.
    """
    client = get_client()
    res = (
        client.table("pulse_posts")
        .select("id, candidate_id, headline, semaforo, asset_affected, copy_twitter, source_link, compliance_flags")
        .is_("card_image_url", "null")
        .in_("status", ["generated", "pending_approval", "approved"])
        .order("created_at", desc=True)
        .limit(CARD_BACKFILL_PER_CYCLE)
        .execute()
    )
    posts = res.data or []

    stats = {"picked": len(posts), "generated": 0, "errors": 0}
    if not posts:
        return stats

    for post in posts:
        # Storage path uses candidate_id when available, else falls back to post id.
        cid = post.get("candidate_id") or post.get("id")
        # Backfill skips Tier 1 (AI image) to protect daily Imagen 3 quota.
        # The historical backlog is huge; AI generation is reserved for fresh posts.
        url, path = card_generator.render_and_upload(post, candidate_id=cid, skip_ai=True)
        if not url:
            stats["errors"] += 1
            log.warning("  backfill render/upload failed post=%s", post["id"])
            continue
        try:
            client.table("pulse_posts").update({
                "card_image_url":  url,
                "card_image_path": path,
            }).eq("id", post["id"]).execute()
            stats["generated"] += 1
            log.info("  backfill OK post=%s -> %s", post["id"], path)
        except Exception as e:
            stats["errors"] += 1
            log.warning("  backfill DB update failed post=%s: %s", post["id"], e)

    return stats


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

    for i, cand in enumerate(candidates):
        # Throttle: pause between candidates to keep Groq under per-second rate limit.
        # Sleeping BEFORE the call (skipping the first one) means a cycle of N takes
        # roughly (N-1) * SLEEP + sum(api_times) seconds.
        if i > 0:
            time.sleep(INTER_CANDIDATE_SLEEP_SEC)

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
