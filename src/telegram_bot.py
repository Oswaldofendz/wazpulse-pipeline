"""
Telegram approval bot — Bloque 7.

Each cycle does two things in sequence:

  1. send_pending(): pick up to MAX_SEND_PER_CYCLE posts that are
       - status = 'generated'
       - angle_strength >= MIN_ANGLE_STRENGTH (filter out filler)
       - created in the last RECENT_HOURS (skip the old backlog by default)
     and send each one to Telegram with three inline buttons:
        ✅ Aprobar / ❌ Rechazar / ⏭ Skip
     The post moves to status='pending_approval' and stores telegram_message_id.

  2. process_callbacks(): poll Telegram getUpdates with offset persisted in
     pulse_state.key='telegram_last_update_id'. For each callback:
        approve → status='approved', approved_by_user=true, approved_at=now()
        reject  → status='rejected', rejection_reason='manual'
        skip    → no DB change, just edits the message to remove buttons
     The message is edited to reflect the outcome so chat history reads cleanly.

Designed to live inside main.py's tick loop — no separate process / no webhooks.
"""
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests

from . import config
from .supabase_client import get_client

log = logging.getLogger("telegram-bot")

# ─── Tunables ───────────────────────────────────────────────────────────────

# How many posts to push to Telegram per cycle. Higher = more inbox spam.
MAX_SEND_PER_CYCLE = 3
# Filter out filler posts. The news-angle endpoint rates 1-5.
# Lowered from 5 → 4 temporarily so the user can validate visual flow with
# more posts. With strength=4 in pulse_posts already filtered noise, this
# brings ~30-50 posts/day into Telegram for review. Bump back to 5 later
# if the inbox feels too noisy.
MIN_ANGLE_STRENGTH = 4
# Skip posts older than this — the 1400+ legacy backlog from Bloque 6a/b
# would otherwise flood Telegram. Newer posts go to the human first.
RECENT_HOURS = 24

# Telegram REST API
API_BASE        = "https://api.telegram.org/bot"
HTTP_TIMEOUT    = 15

STATE_KEY_OFFSET = "telegram_last_update_id"

SEMAFORO_EMOJI = {
    "verde":    "🟢",
    "amarillo": "🟡",
    "rojo":     "🔴",
    "neutral":  "⚪",
}


# ─── Telegram REST wrappers ─────────────────────────────────────────────────

def _api_url(method: str) -> str:
    return f"{API_BASE}{config.TELEGRAM_BOT_TOKEN}/{method}"


def _send_message(text: str, reply_markup: Optional[dict] = None) -> dict:
    body = {
        "chat_id": int(config.TELEGRAM_CHAT_ID),
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_markup:
        body["reply_markup"] = reply_markup
    resp = requests.post(_api_url("sendMessage"), json=body, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def _send_photo(photo_url: str, caption: str, reply_markup: Optional[dict] = None) -> dict:
    """sendPhoto with optional caption. Caption hard limit is 1024 chars."""
    body = {
        "chat_id":    int(config.TELEGRAM_CHAT_ID),
        "photo":      photo_url,
        "caption":    caption[:1024],   # safety truncate
        "parse_mode": "HTML",
    }
    if reply_markup:
        body["reply_markup"] = reply_markup
    resp = requests.post(_api_url("sendPhoto"), json=body, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def _get_updates(offset: int) -> list[dict]:
    # timeout=0 → short polling (returns immediately if nothing pending).
    body = {"offset": offset, "timeout": 0}
    resp = requests.post(_api_url("getUpdates"), json=body, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    return resp.json().get("result", []) or []


def _answer_callback(callback_id: str, text: str = "") -> None:
    body = {"callback_query_id": callback_id, "text": text}
    try:
        requests.post(_api_url("answerCallbackQuery"), json=body, timeout=HTTP_TIMEOUT)
    except Exception as e:
        log.warning("answerCallbackQuery failed: %s", e)


def _edit_message_text(chat_id: int, message_id: int, text: str) -> None:
    body = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        requests.post(_api_url("editMessageText"), json=body, timeout=HTTP_TIMEOUT)
    except Exception as e:
        log.warning("editMessageText failed for msg %d: %s", message_id, e)


# ─── State persistence (pulse_state table) ──────────────────────────────────

def _get_state(key: str) -> Optional[dict]:
    client = get_client()
    res = client.table("pulse_state").select("value").eq("key", key).limit(1).execute()
    rows = res.data or []
    return rows[0]["value"] if rows else None


def _set_state(key: str, value: dict) -> None:
    client = get_client()
    client.table("pulse_state").upsert({
        "key":   key,
        "value": value,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }, on_conflict="key").execute()


# ─── Message formatting ─────────────────────────────────────────────────────

def _escape_html(text: str) -> str:
    """HTML-escape for Telegram parse_mode=HTML (only & < > matter)."""
    return (text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _common_header(post: dict) -> tuple[str, str]:
    """Return (header_block, market_line) reused by both message styles."""
    semaforo = post.get("semaforo") or "neutral"
    flag     = SEMAFORO_EMOJI.get(semaforo, "⚪")
    asset    = post.get("asset_affected") or "general"

    flags    = post.get("compliance_flags") or {}
    strength = flags.get("angle_strength", 3)
    market   = flags.get("market_context") or {}

    fg       = market.get("fearGreed", "?")
    fg_class = market.get("fearGreedClass", "?")
    alerts   = []
    if market.get("marketStressed"):    alerts.append("⚠️ market stressed")
    if market.get("feargreedExtreme"):  alerts.append("🔥 F&G extreme")
    if market.get("macroEventSoon"):    alerts.append("📅 macro soon")
    if market.get("bigWhaleActivity"):  alerts.append("🐳 whale active")
    market_line = " · ".join(alerts) if alerts else f"F&G {fg} ({fg_class})"

    header = f"{flag} <b>{_escape_html(asset.upper())}</b> · ⭐ {strength}/5"
    return header, market_line


def _format_message_text(post: dict) -> str:
    """Full text-only message (used when no card image is available)."""
    header, market_line = _common_header(post)

    flags     = post.get("compliance_flags") or {}
    reasoning = flags.get("angle_reasoning") or ""

    headline  = (post.get("headline") or "")[:200]
    twitter   = (post.get("copy_twitter") or "")
    instagram = (post.get("copy_instagram") or "")
    source    = post.get("source_link") or ""

    parts = [
        header,
        _escape_html(market_line),
        "",
        f"<b>{_escape_html(headline)}</b>",
        "",
        f"🐦 <i>Twitter ({len(twitter)}/280):</i>",
        _escape_html(twitter[:280]),
        "",
        "📷 <i>Instagram:</i>",
        _escape_html(instagram[:600]),
    ]
    if source:
        parts += ["", f'🌐 <a href="{_escape_html(source)}">Fuente</a>']
    if reasoning:
        parts += ["", f"<i>🤖 {_escape_html(reasoning)}</i>"]
    return "\n".join(parts)


def _format_caption(post: dict) -> str:
    """Compact caption for sendPhoto (1024 char limit). Card itself carries
    the headline and asset visually, so we only need the contextual layer here."""
    header, market_line = _common_header(post)
    flags     = post.get("compliance_flags") or {}
    reasoning = (flags.get("angle_reasoning") or "")[:200]
    twitter   = (post.get("copy_twitter") or "")[:240]
    source    = post.get("source_link") or ""

    parts = [
        header,
        _escape_html(market_line),
        "",
        _escape_html(twitter),
    ]
    if source:
        parts += ["", f'🌐 <a href="{_escape_html(source)}">Fuente</a>']
    if reasoning:
        parts += ["", f"<i>🤖 {_escape_html(reasoning)}</i>"]
    return "\n".join(parts)


def _build_keyboard(post_id: int) -> dict:
    return {
        "inline_keyboard": [[
            {"text": "✅ Aprobar",  "callback_data": f"approve:{post_id}"},
            {"text": "❌ Rechazar", "callback_data": f"reject:{post_id}"},
            {"text": "⏭ Skip",      "callback_data": f"skip:{post_id}"},
        ]]
    }


# ─── Step 1: send pending posts ─────────────────────────────────────────────

def _list_eligible_posts(limit_pool: int = 50) -> list[dict]:
    """
    Fetch a pool of candidate posts then filter by angle_strength in Python
    (Supabase REST can't easily filter on a nested JSONB key like compliance_flags->>'angle_strength' via the JS-style client).
    """
    client = get_client()
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=RECENT_HOURS)).isoformat()
    res = (
        client.table("pulse_posts")
        .select("id, headline, semaforo, asset_affected, copy_twitter, copy_instagram, source_link, compliance_flags, created_at, card_image_url")
        .eq("status", "generated")
        .gte("created_at", cutoff)
        .order("created_at", desc=True)
        .limit(limit_pool)
        .execute()
    )
    return res.data or []


def send_pending() -> dict:
    pool = _list_eligible_posts()
    eligible = [
        p for p in pool
        if (p.get("compliance_flags") or {}).get("angle_strength", 0) >= MIN_ANGLE_STRENGTH
    ][:MAX_SEND_PER_CYCLE]

    stats = {"pool": len(pool), "eligible": len(eligible), "sent": 0, "send_errors": 0}
    if not eligible:
        return stats

    client = get_client()
    for post in eligible:
        post_id = post["id"]
        try:
            keyboard  = _build_keyboard(post_id)
            card_url  = post.get("card_image_url")
            if card_url:
                # Visual: photo + compact caption
                caption  = _format_caption(post)
                response = _send_photo(card_url, caption, reply_markup=keyboard)
            else:
                # Fallback: text-only (legacy posts without card)
                text     = _format_message_text(post)
                response = _send_message(text, reply_markup=keyboard)
            message_id = response.get("result", {}).get("message_id")
            if not message_id:
                raise RuntimeError(f"telegram returned no message_id: {response}")

            client.table("pulse_posts").update({
                "telegram_message_id": message_id,
                "status":              "pending_approval",
            }).eq("id", post_id).execute()

            stats["sent"] += 1
            log.info("  sent post %d (msg=%d, sema=%s, asset=%s)",
                     post_id, message_id, post.get("semaforo"), post.get("asset_affected") or "-")
        except Exception as e:
            stats["send_errors"] += 1
            log.warning("  failed to send post %d: %s", post_id, e)

    return stats


# ─── Step 2: process callback queries ───────────────────────────────────────

def _apply_action(action: str, post_id: int, headline_for_msg: str, chat_id: int, message_id: int) -> str:
    """Update DB + edit the original Telegram message. Returns short status text."""
    client = get_client()
    now_iso = datetime.now(timezone.utc).isoformat()
    hh_mm   = now_iso[11:16]
    headline_short = _escape_html(headline_for_msg[:200])

    if action == "approve":
        client.table("pulse_posts").update({
            "status":           "approved",
            "approved_by_user": True,
            "approved_at":      now_iso,
        }).eq("id", post_id).execute()
        _edit_message_text(chat_id, message_id,
            f"✅ <b>Aprobado</b> a las {hh_mm} UTC\n\n<i>{headline_short}</i>")
        return "✅ Aprobado"

    if action == "reject":
        client.table("pulse_posts").update({
            "status":           "rejected",
            "rejection_reason": "manual",
        }).eq("id", post_id).execute()
        _edit_message_text(chat_id, message_id,
            f"❌ <b>Rechazado</b> a las {hh_mm} UTC\n\n<i>{headline_short}</i>")
        return "❌ Rechazado"

    if action == "skip":
        # No DB change. Just remove buttons by editing the text.
        _edit_message_text(chat_id, message_id,
            f"⏭ <b>Skip</b> — queda en pending_approval\n\n<i>{headline_short}</i>")
        return "⏭ Skip"

    return "❓ acción desconocida"


def process_callbacks() -> dict:
    state  = _get_state(STATE_KEY_OFFSET) or {"offset": 0}
    offset = int(state.get("offset", 0) or 0)

    try:
        updates = _get_updates(offset=offset)
    except Exception as e:
        log.warning("getUpdates failed: %s", e)
        return {"polled": 0, "approved": 0, "rejected": 0, "skipped": 0, "ignored": 0}

    stats = {"polled": len(updates), "approved": 0, "rejected": 0, "skipped": 0, "ignored": 0}
    if not updates:
        return stats

    client = get_client()
    max_update_id = offset

    for update in updates:
        update_id = int(update.get("update_id", 0) or 0)
        if update_id >= max_update_id:
            max_update_id = update_id + 1

        cb = update.get("callback_query")
        if not cb:
            stats["ignored"] += 1
            continue

        cb_id      = cb.get("id")
        data       = cb.get("data", "") or ""
        message    = cb.get("message", {}) or {}
        chat_id    = (message.get("chat") or {}).get("id")
        message_id = message.get("message_id")

        if ":" not in data:
            _answer_callback(cb_id, "callback_data inválida")
            stats["ignored"] += 1
            continue

        action, post_id_str = data.split(":", 1)
        try:
            post_id = int(post_id_str)
        except ValueError:
            _answer_callback(cb_id, "post_id inválido")
            stats["ignored"] += 1
            continue

        # Fetch headline so the post-action message remains informative.
        res = client.table("pulse_posts").select("status, headline").eq("id", post_id).limit(1).execute()
        rows = res.data or []
        if not rows:
            _answer_callback(cb_id, "Post no encontrado")
            stats["ignored"] += 1
            continue
        post = rows[0]

        try:
            label = _apply_action(action, post_id, post.get("headline") or "", chat_id, message_id)
            _answer_callback(cb_id, label)
            if action == "approve": stats["approved"] += 1
            elif action == "reject": stats["rejected"] += 1
            elif action == "skip":   stats["skipped"]  += 1
            else:                    stats["ignored"]  += 1
            log.info("  post %d → %s", post_id, label)
        except Exception as e:
            _answer_callback(cb_id, "Error interno")
            log.warning("  post %d action %s failed: %s", post_id, action, e)
            stats["ignored"] += 1

    # Persist offset so we don't reprocess the same updates next cycle.
    if max_update_id != offset:
        try:
            _set_state(STATE_KEY_OFFSET, {"offset": max_update_id})
        except Exception as e:
            log.error("failed to persist telegram offset (will replay next cycle): %s", e)

    return stats


# ─── Cycle entry point ──────────────────────────────────────────────────────

def run_one_cycle() -> dict:
    log.info("step 1: send pending posts to Telegram")
    send_stats = send_pending()
    log.info("  pool=%d eligible=%d sent=%d errors=%d",
             send_stats["pool"], send_stats["eligible"], send_stats["sent"], send_stats["send_errors"])

    log.info("step 2: process Telegram callbacks")
    cb_stats = process_callbacks()
    log.info("  polled=%d approved=%d rejected=%d skipped=%d ignored=%d",
             cb_stats["polled"], cb_stats["approved"], cb_stats["rejected"],
             cb_stats["skipped"], cb_stats["ignored"])

    return {"send": send_stats, "callbacks": cb_stats}
