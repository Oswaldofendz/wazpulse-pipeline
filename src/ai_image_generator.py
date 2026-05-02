"""
AI image generator for Tier-1 dramatic cards (CryptoAlpha-style).

Provider strategy:
  1. Google Imagen 3 (imagen-3.0-generate-002) — top-tier quality.
     Tries each GEMINI_API_KEY{,_2,_3,_4,_5} in order; on 429/403 it marks
     that key as exhausted for an hour and rolls to the next one.
  2. Pollinations.ai (Flux model) — zero-auth public endpoint, used as a
     final fallback when ALL Gemini keys are on cooldown.

Both providers return PNG bytes. We cache by SHA256(prompt) in /tmp/ so
re-renders within a deploy don't waste quota.

Image is generated at 9:16 (Imagen) or 1080x1350 (Pollinations) for portrait
feed (Instagram, TikTok). The card_generator composes text overlay on top.

Failure mode: returns None when both providers fail. The card generator falls
back to Tier-2 (logo card) when this returns None.
"""
import base64
import hashlib
import logging
import os
import time
from io import BytesIO
from typing import Optional
from urllib.parse import quote

import requests
from PIL import Image

log = logging.getLogger("ai-image")

CACHE_DIR    = "/tmp/wacapital_ai_images"
HTTP_TIMEOUT = 90    # AI gen can be slow
PROMPT_MAX   = 480   # safety cap for prompt length

# How long to skip a key after a 429. 1 hour is generous; daily quotas reset
# at UTC 00:00 so an hour cooldown handles per-minute hiccups too.
KEY_COOLDOWN_SEC = 3600

try:
    os.makedirs(CACHE_DIR, exist_ok=True)
except Exception as e:
    log.warning("could not create cache dir %s: %s", CACHE_DIR, e)


# ─── Key rotation state ─────────────────────────────────────────────────────

# {key_id_short: epoch_when_usable_again}
_key_cooldowns: dict[str, float] = {}


def _gemini_keys() -> list[str]:
    """Read all GEMINI_API_KEY{,_2,_3,_4,_5} env vars. Returns non-empty in order."""
    out: list[str] = []
    for var in ("GEMINI_API_KEY", "GEMINI_API_KEY_2", "GEMINI_API_KEY_3",
                "GEMINI_API_KEY_4", "GEMINI_API_KEY_5"):
        v = (os.getenv(var) or "").strip()
        if v:
            out.append(v)
    return out


def _key_label(key: str) -> str:
    """Short, log-safe identifier for a key."""
    return f"...{key[-6:]}" if len(key) > 6 else "?"


def _key_on_cooldown(key: str) -> bool:
    until = _key_cooldowns.get(_key_label(key), 0.0)
    return time.time() < until


def _mark_key_exhausted(key: str) -> None:
    _key_cooldowns[_key_label(key)] = time.time() + KEY_COOLDOWN_SEC


# ─── Cache ──────────────────────────────────────────────────────────────────

def _cache_path(prompt: str) -> str:
    h = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
    return os.path.join(CACHE_DIR, f"{h}.png")


# ─── Provider: Google Imagen 3 ──────────────────────────────────────────────

def _imagen3(prompt: str, key: str) -> bytes:
    """
    POST to Imagen 3 predict endpoint. Returns raw PNG bytes.
    Raises requests.HTTPError on non-2xx so caller can rotate keys.
    """
    url = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        f"imagen-3.0-generate-002:predict?key={key}"
    )
    body = {
        "instances": [{"prompt": prompt[:PROMPT_MAX]}],
        "parameters": {
            "sampleCount": 1,
            "aspectRatio": "3:4",   # portrait, ~1024x1408 → fits 1080x1350 nicely
        },
    }
    resp = requests.post(url, json=body, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    preds = data.get("predictions") or []
    if not preds or "bytesBase64Encoded" not in preds[0]:
        raise RuntimeError(f"Imagen 3 returned no image: {str(data)[:200]}")
    return base64.b64decode(preds[0]["bytesBase64Encoded"])


# ─── Provider: Pollinations.ai (Flux) ───────────────────────────────────────

def _pollinations(prompt: str) -> bytes:
    """Pollinations returns the PNG bytes directly via GET."""
    safe = quote(prompt[:PROMPT_MAX])
    url = (
        f"https://image.pollinations.ai/prompt/{safe}"
        "?width=1080&height=1350&model=flux&nologo=true"
    )
    resp = requests.get(url, timeout=HTTP_TIMEOUT, headers={
        "User-Agent": "WaCapital-PulseEngine/1.0",
    })
    resp.raise_for_status()
    return resp.content


# ─── Public entry point ─────────────────────────────────────────────────────

def generate(prompt: str) -> Optional[Image.Image]:
    """
    Generate a portrait image from `prompt`. Returns PIL.Image (RGB) or None
    if both Gemini and Pollinations failed.

    Caches in /tmp/ keyed by sha256(prompt) — same prompt will return the
    cached result without re-billing.
    """
    if not prompt or not prompt.strip():
        return None

    # Cache hit?
    path = _cache_path(prompt)
    if os.path.exists(path):
        try:
            return Image.open(path).convert("RGB")
        except Exception:
            try:
                os.remove(path)
            except Exception:
                pass

    # Try each Gemini key in order, skipping ones on cooldown.
    for key in _gemini_keys():
        if _key_on_cooldown(key):
            log.info("[ai-image] skipping key %s (on cooldown)", _key_label(key))
            continue
        try:
            png = _imagen3(prompt, key)
            img = Image.open(BytesIO(png)).convert("RGB")
            try:
                img.save(path, format="PNG", optimize=True)
            except Exception as e:
                log.warning("[ai-image] cache write failed: %s", e)
            log.info("[ai-image] OK via Imagen 3 (key %s)", _key_label(key))
            return img
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else "?"
            if status in (429, 403):
                _mark_key_exhausted(key)
                log.warning("[ai-image] Imagen 3 quota hit on key %s (status=%s) → next key",
                            _key_label(key), status)
                continue
            log.warning("[ai-image] Imagen 3 failed on key %s: status=%s body=%s",
                        _key_label(key), status, str(e.response.text)[:200] if e.response else "?")
            continue
        except Exception as e:
            log.warning("[ai-image] Imagen 3 error on key %s: %s", _key_label(key), e)
            continue

    # All Gemini keys exhausted or absent → Pollinations fallback.
    try:
        log.info("[ai-image] falling back to Pollinations (Flux)")
        png = _pollinations(prompt)
        img = Image.open(BytesIO(png)).convert("RGB")
        try:
            img.save(path, format="PNG", optimize=True)
        except Exception as e:
            log.warning("[ai-image] cache write failed: %s", e)
        log.info("[ai-image] OK via Pollinations")
        return img
    except Exception as e:
        log.error("[ai-image] all providers failed (Pollinations: %s)", e)
        return None


# ─── Prompt crafting ────────────────────────────────────────────────────────

# Cinematic style modifiers appended to every Tier-1 prompt. Tuned for
# Imagen 3 / Flux — produces dramatic editorial photography aesthetic close to
# CryptoAlpha / WatcherGuru visual language.
_STYLE_TAIL = (
    "professional financial news editorial photography, dramatic cinematic lighting, "
    "moody dark background, high contrast, photorealistic, hyper-detailed, "
    "vertical composition, 8k, sharp focus, no text, no watermark"
)


def _subject_from_entity(entity: Optional[dict], headline: str) -> str:
    """Build the focal-subject phrase based on the matched entity, if any."""
    if not entity:
        # No clear subject — describe via the headline keywords abstractly.
        return f"financial news scene depicting: {headline[:120]}"

    e_type = entity.get("type")
    name   = entity.get("display") or entity.get("id") or "subject"

    if e_type == "crypto":
        return f"a glowing {name} cryptocurrency coin floating in mid-air, metallic golden glow"
    if e_type == "company":
        return f"the {name} company logo prominently displayed, illuminated, corporate environment"
    if e_type == "person":
        return f"a portrait of {name}, intense expression, dramatic lighting on face"
    if e_type in ("index", "commodity"):
        return f"abstract financial visualization of {name} market, charts and graphs"

    return f"editorial visualization of {name}"


def craft_prompt(headline: str, hook: str = "", entity: Optional[dict] = None) -> str:
    """
    Build a single-line prompt for Imagen 3 / Flux from a headline and entity.

    Always English (image models perform better in English regardless of the
    headline language). Output is a long single string with the subject first,
    contextual modifiers, and the cinematic style tail.
    """
    subject = _subject_from_entity(entity, headline)
    context = headline.strip().rstrip(".!?")
    if hook:
        context = f"{context}. {hook.strip().rstrip('.!?')}"
    parts = [subject, f"context: {context[:160]}", _STYLE_TAIL]
    return ", ".join(parts)
