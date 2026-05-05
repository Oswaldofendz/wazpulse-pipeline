"""
Card image generator — Bloque 6c V2.2 (CryptoAlpha-style portrait).

Output: 1080×1350 PNG (portrait 4:5) — optimal for IG/TikTok feed and works
fine for Twitter (renders as portrait card).

Three tiers, dispatched by `render()`:
  • Tier 1 — AI hero (Imagen 3 / Pollinations Flux generated dramatic photo).
            Triggered for posts with angle_strength == 5 AND a matched entity.
  • Tier 2 — Logo hero (Clearbit company logo / cryptologos.cc crypto logo).
            For posts with entity match but lower strength.
  • Tier 3 — Solid color hero with big asset typography.
            For posts without entity match — keeps the brand consistent.

Common elements across all tiers:
  – Top stripe (90 px) in semáforo color
  – Hero area (760 px tall, varies by tier)
  – Dark text overlay box (430 px tall, rounded) with:
        headline in WHITE bold
        hook in CYAN bold (the punchy CryptoAlpha-style accent)
  – Brand "WaCapital" centered at the very bottom

Failure handling: every tier degrades to the next on error so a card always
renders. card_image_url stays NULL only if all three tiers crash.
"""
from io import BytesIO
import logging
import os
from typing import Optional

from PIL import Image, ImageDraw, ImageFont

from . import config, image_fetcher, entity_detector, ai_image_generator
from .supabase_client import get_client

log = logging.getLogger("card-generator")

# ─── Canvas (portrait 4:5) ──────────────────────────────────────────────────

CARD_W = 1080
CARD_H = 1350

# ─── Colors ─────────────────────────────────────────────────────────────────

SEMAFORO_COLORS = {
    "verde":    (34, 197, 94),     # green-500
    "amarillo": (234, 179, 8),     # yellow-500
    "rojo":     (239, 68, 68),     # red-500
    "neutral":  (148, 163, 184),   # slate-400
}

BG_COLOR        = (6, 8, 14)       # near-black — base/fallback hero
HERO_DARK       = (8, 12, 22)      # slightly bluer dark for hero panels
OVERLAY_BG      = (6, 8, 12)       # pure near-black for gradient endpoint
TEXT_PRIMARY    = (255, 255, 255)  # pure white headline
TEXT_SECONDARY  = (203, 213, 225)  # slate-300
TEXT_MUTED      = (148, 163, 184)  # slate-400
ACCENT_CYAN     = (56, 189, 248)   # sky-400 — the CryptoAlpha-style hook color

# ─── Layout (CryptoAlpha V3: full-bleed hero + gradient fade, no separate box) ────
#
# Hero image fills the FULL card height (0 → CARD_H). A gradient overlay fades
# from transparent at GRADIENT_START to solid near-black at GRADIENT_SOLID, then
# stays solid to the bottom. Text sits on the solid zone. No rounded rectangle box.
# This is the CryptoAlpha / WatcherGuru visual signature.

HERO_TOP       = 0
HERO_BOTTOM    = CARD_H           # full-bleed: image fills entire card
HERO_HEIGHT    = CARD_H

GRADIENT_START = 680              # gradient begins here (fully transparent)
GRADIENT_SOLID = 900              # gradient is fully opaque from here down

# Text area starts just below gradient_solid with padding
TEXT_AREA_TOP  = 915
TEXT_AREA_BOT  = 1285
TEXT_PAD_X     = 40               # horizontal padding from card edge

# Semáforo accent — thin horizontal bar above the text block
SEMAFORO_BAR_Y = TEXT_AREA_TOP - 10
SEMAFORO_BAR_H = 4

# Type sizes (base — auto-scale UP fills the text area for short text)
SIZE_HEADLINE = 52
SIZE_HOOK     = 42
SIZE_BRAND    = 30     # WaCapital wordmark
SIZE_FOOTER   = 16

# Brand position: last ~55px of card
BRAND_Y = 1293

# Max wrapped lines per text block
MAX_HEADLINE_LINES = 3
MAX_HOOK_LINES     = 3


# ─── Font loading with multi-path fallback ──────────────────────────────────

_HERE = os.path.dirname(os.path.abspath(__file__))
FONT_BUNDLED_BOLD    = os.path.join(_HERE, "assets", "fonts", "Inter-Bold.ttf")
FONT_BUNDLED_SEMI    = os.path.join(_HERE, "assets", "fonts", "Inter-SemiBold.ttf")
FONT_BUNDLED_REGULAR = os.path.join(_HERE, "assets", "fonts", "Inter-Regular.ttf")

SYSTEM_BOLD = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
]
SYSTEM_REGULAR = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
]


def _load_font(paths: list[str], size: int) -> ImageFont.FreeTypeFont:
    last_err: Optional[Exception] = None
    for path in paths:
        try:
            return ImageFont.truetype(path, size)
        except (OSError, IOError) as e:
            last_err = e
            continue
    log.error(
        "TTF load FAILED for size=%d. Tried paths=%s. Last error=%s.",
        size, paths, last_err,
    )
    return ImageFont.load_default()


def _font_bold(size: int) -> ImageFont.FreeTypeFont:
    return _load_font([FONT_BUNDLED_BOLD] + SYSTEM_BOLD, size)


def _font_semi(size: int) -> ImageFont.FreeTypeFont:
    return _load_font([FONT_BUNDLED_SEMI, FONT_BUNDLED_BOLD] + SYSTEM_BOLD, size)


def _font_regular(size: int) -> ImageFont.FreeTypeFont:
    return _load_font([FONT_BUNDLED_REGULAR] + SYSTEM_REGULAR, size)


# ─── Text helpers ───────────────────────────────────────────────────────────

def _wrap(draw: ImageDraw.ImageDraw, text: str, font, max_w: int) -> list[str]:
    words = (text or "").split()
    lines: list[str] = []
    cur: list[str] = []
    for w in words:
        cand = " ".join(cur + [w])
        bb = draw.textbbox((0, 0), cand, font=font)
        if (bb[2] - bb[0]) <= max_w:
            cur.append(w)
        else:
            if cur:
                lines.append(" ".join(cur))
            cur = [w]
    if cur:
        lines.append(" ".join(cur))
    return lines


def _ellipsize(lines: list[str], max_lines: int) -> list[str]:
    if len(lines) <= max_lines:
        return lines
    keep = lines[:max_lines]
    keep[-1] = keep[-1].rstrip(".,;:") + "…"
    return keep


def _domain(url: Optional[str]) -> str:
    if not url:
        return ""
    try:
        host = url.split("//", 1)[-1].split("/", 1)[0]
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


# ─── Common card pieces ─────────────────────────────────────────────────────

def _new_canvas() -> Image.Image:
    return Image.new("RGB", (CARD_W, CARD_H), BG_COLOR)


def _draw_hero_subtle(img: Image.Image, semaforo: str) -> None:
    """
    Last-resort hero (when both AI providers AND the logo fetch fail).
    Full-card dark background with a soft semáforo glow — gradient overlay
    will be applied on top by _render_with_hero.
    """
    draw = ImageDraw.Draw(img)
    draw.rectangle([(0, 0), (CARD_W, CARD_H)], fill=HERO_DARK)

    glow_color = SEMAFORO_COLORS.get(semaforo, SEMAFORO_COLORS["neutral"])
    glow = Image.new("RGBA", (CARD_W, CARD_H), (0, 0, 0, 0))
    g_draw = ImageDraw.Draw(glow)
    cx = CARD_W // 2
    cy = CARD_H // 3   # glow centered in upper 1/3 of card
    for i, alpha in enumerate([10, 18, 30, 45]):
        r = 420 - i * 90
        g_draw.ellipse([(cx - r, cy - r), (cx + r, cy + r)], fill=(*glow_color, alpha))
    img.paste(glow, (0, 0), glow)


def _draw_hero_logo(img: Image.Image, logo: Image.Image) -> None:
    """T2 hero — dark full-card background with logo centered in upper area.
    Gradient overlay applied on top by _render_with_hero."""
    draw = ImageDraw.Draw(img)
    draw.rectangle([(0, 0), (CARD_W, CARD_H)], fill=HERO_DARK)

    panel_pad = 100
    panel_top    = 80
    panel_bottom = GRADIENT_START - 40   # logo stays above gradient zone
    panel_left   = panel_pad
    panel_right  = CARD_W - panel_pad
    draw.rounded_rectangle(
        [(panel_left, panel_top), (panel_right, panel_bottom)],
        radius=28,
        fill=(241, 245, 249),
    )
    inner_w = (panel_right - panel_left) - 80
    inner_h = (panel_bottom - panel_top) - 80
    fitted  = image_fetcher.fit_into(logo, inner_w, inner_h)
    iw, ih = fitted.size
    cx = (CARD_W - iw) // 2
    cy = panel_top + ((panel_bottom - panel_top) - ih) // 2
    img.paste(fitted, (cx, cy), fitted if fitted.mode == "RGBA" else None)


def _draw_gradient_overlay(img: Image.Image) -> None:
    """CryptoAlpha-style gradient: transparent at GRADIENT_START → solid at GRADIENT_SOLID.
    Applied on top of the hero image so text sits on a clean dark surface."""
    r, g, b = OVERLAY_BG
    span = GRADIENT_SOLID - GRADIENT_START

    # Build a full-card RGBA layer and alpha-composite it.
    grad = Image.new("RGBA", (CARD_W, CARD_H), (0, 0, 0, 0))
    draw = ImageDraw.Draw(grad)

    # Gradient band: linear alpha from 0 → 255
    for y in range(GRADIENT_START, GRADIENT_SOLID):
        alpha = int(255 * (y - GRADIENT_START) / span)
        draw.line([(0, y), (CARD_W, y)], fill=(r, g, b, alpha))

    # Solid zone from GRADIENT_SOLID to bottom
    draw.rectangle([(0, GRADIENT_SOLID), (CARD_W, CARD_H)], fill=(r, g, b, 255))

    base = img.convert("RGBA")
    merged = Image.alpha_composite(base, grad)
    img.paste(merged.convert("RGB"))


def _draw_hero_ai(img: Image.Image, ai: Image.Image) -> None:
    """T1 hero — full-bleed AI image cropped/scaled to fill the entire card."""
    iw, ih = ai.size
    scale = max(CARD_W / iw, CARD_H / ih)
    nw = max(1, int(iw * scale))
    nh = max(1, int(ih * scale))
    ai_resized = ai.resize((nw, nh), Image.LANCZOS)
    cx_off = (nw - CARD_W) // 2
    cy_off = (nh - CARD_H) // 2
    cropped = ai_resized.crop((cx_off, cy_off, cx_off + CARD_W, cy_off + CARD_H))
    img.paste(cropped, (0, 0))


def _compute_overlay_fonts(
    draw: ImageDraw.ImageDraw,
    headline: str,
    hook: str,
    max_w: int,
    available_h: int,
) -> tuple:
    """
    Scale headline + hook fonts UP from the base sizes so the text fills as
    much of the overlay box as possible.  Returns:
        (size_head, size_hook, head_lines, hook_lines, head_lh, hook_lh)

    Strategy: iterate from SIZE_HEADLINE upward (step 2px) until the combined
    text height would exceed the box — then use the last valid size.
    Ceiling: 1.7× the base sizes so ultra-short text doesn't look absurd.
    """
    MAX_HEAD = int(SIZE_HEADLINE * 1.7)   # ~85 px ceiling for headline
    TOP_PAD  = 25
    BOT_PAD  = 22
    GAP      = 16   # vertical gap between headline block and hook block
    inner_h  = available_h - TOP_PAD - BOT_PAD

    best = None

    for size_head in range(SIZE_HEADLINE, MAX_HEAD + 1, 2):
        # Hook scales proportionally with the headline size.
        size_hook = min(int(SIZE_HOOK * 1.7), int(size_head * SIZE_HOOK / SIZE_HEADLINE))

        f_head   = _font_bold(size_head)
        f_hook   = _font_bold(size_hook)
        head_lh  = size_head + 8
        hook_lh  = size_hook + 6

        head_lines = _ellipsize(_wrap(draw, headline, f_head, max_w), MAX_HEADLINE_LINES)
        head_h     = len(head_lines) * head_lh

        if hook:
            remaining    = inner_h - head_h - GAP
            physical_max = max(1, remaining // hook_lh)
            hook_lines   = _ellipsize(
                _wrap(draw, hook, f_hook, max_w),
                min(MAX_HOOK_LINES, physical_max),
            )
            hook_h  = len(hook_lines) * hook_lh
            total_h = head_h + GAP + hook_h
        else:
            hook_lines = []
            total_h    = head_h

        if total_h <= inner_h:
            best = (size_head, size_hook, head_lines, hook_lines, head_lh, hook_lh)
        else:
            break   # this size overflows — stop, use previous best

    # Guaranteed fallback to base sizes (should never be None in practice).
    if best is None:
        f_head = _font_bold(SIZE_HEADLINE)
        f_hook = _font_bold(SIZE_HOOK)
        head_lh  = SIZE_HEADLINE + 8
        hook_lh  = SIZE_HOOK + 6
        head_lines = _ellipsize(_wrap(draw, headline, f_head, max_w), MAX_HEADLINE_LINES)
        hook_lines = _ellipsize(_wrap(draw, hook, f_hook, max_w), MAX_HOOK_LINES) if hook else []
        best = (SIZE_HEADLINE, SIZE_HOOK, head_lines, hook_lines, head_lh, hook_lh)

    return best


def _draw_overlay_text(img: Image.Image, headline: str, hook: str, semaforo: str) -> None:
    """
    CryptoAlpha V3 text overlay: no separate box — text sits directly on the
    gradient that _draw_gradient_overlay() already painted. Font sizes auto-scale
    UP to fill the available text area so there's no wasted empty space.
    Thin semáforo-colored bar provides a subtle accent above the text block.
    """
    draw = ImageDraw.Draw(img)

    # Thin semáforo accent bar above text block
    bar_color = SEMAFORO_COLORS.get(semaforo, SEMAFORO_COLORS["neutral"])
    draw.rectangle(
        [(TEXT_PAD_X, SEMAFORO_BAR_Y),
         (CARD_W - TEXT_PAD_X, SEMAFORO_BAR_Y + SEMAFORO_BAR_H)],
        fill=bar_color,
    )

    max_w       = CARD_W - TEXT_PAD_X * 2
    available_h = TEXT_AREA_BOT - TEXT_AREA_TOP

    size_head, size_hook, head_lines, hook_lines, head_lh, hook_lh = \
        _compute_overlay_fonts(draw, headline, hook, max_w, available_h)

    f_head = _font_bold(size_head)
    f_hook = _font_bold(size_hook)

    # Headline (white)
    head_y = TEXT_AREA_TOP
    for i, line in enumerate(head_lines):
        draw.text((TEXT_PAD_X, head_y + i * head_lh), line, font=f_head, fill=TEXT_PRIMARY)

    # Hook (cyan accent)
    if hook_lines:
        hook_y = head_y + len(head_lines) * head_lh + 16
        for i, line in enumerate(hook_lines):
            draw.text((TEXT_PAD_X, hook_y + i * hook_lh), line, font=f_hook, fill=ACCENT_CYAN)


def _draw_brand_footer(img: Image.Image, source: str) -> None:
    """Small WaCapital wordmark centered at the very bottom — CryptoAlpha-style."""
    draw = ImageDraw.Draw(img)
    f_brand = _font_semi(SIZE_BRAND)
    brand_text = "WaCapital"
    bb = draw.textbbox((0, 0), brand_text, font=f_brand)
    tw = bb[2] - bb[0]
    cx = (CARD_W - tw) // 2
    draw.text((cx, BRAND_Y), brand_text, font=f_brand, fill=TEXT_SECONDARY)
    # Source domain microcopy below the brand
    if source:
        f_foot = _font_regular(SIZE_FOOTER)
        bb2 = draw.textbbox((0, 0), source, font=f_foot)
        tw2 = bb2[2] - bb2[0]
        cx2 = (CARD_W - tw2) // 2
        draw.text((cx2, BRAND_Y + SIZE_BRAND + 2), source, font=f_foot, fill=TEXT_MUTED)


# ─── Tier renderers ─────────────────────────────────────────────────────────

def _render_with_hero(post: dict, hero_drawer) -> bytes:
    """Common renderer — hero_drawer fills the canvas, then gradient + text overlay."""
    semaforo = (post.get("semaforo") or "neutral").lower()
    headline = (post.get("headline")    or "").strip()
    flags    = post.get("compliance_flags") or {}
    hook     = (flags.get("angle_hook") or "").strip()
    source   = _domain(post.get("source_link"))

    img = _new_canvas()
    hero_drawer(img)
    _draw_gradient_overlay(img)   # CryptoAlpha-style fade: transparent → solid black
    _draw_overlay_text(img, headline, hook, semaforo)
    _draw_brand_footer(img, source)

    out = BytesIO()
    img.save(out, format="PNG", optimize=True)
    return out.getvalue()


def _render_tier1_ai(post: dict, entity: Optional[dict], ai_image: Image.Image) -> bytes:
    return _render_with_hero(post, lambda im: _draw_hero_ai(im, ai_image))


def _render_tier2_logo(post: dict, entity: dict, logo: Image.Image) -> bytes:
    return _render_with_hero(post, lambda im: _draw_hero_logo(im, logo))


def _render_tier3_subtle(post: dict) -> bytes:
    semaforo = (post.get("semaforo") or "neutral").lower()
    return _render_with_hero(post, lambda im: _draw_hero_subtle(im, semaforo))


# ─── Tier dispatch ──────────────────────────────────────────────────────────

def _resolve_entity(post: dict) -> Optional[dict]:
    # Prefer asset_affected (set at editorial time from original English headline).
    e = entity_detector.find_by_id(post.get("asset_affected"))
    if e is not None:
        return e
    # Fallback: detect on the (translated) headline.
    return entity_detector.detect_entity(post.get("headline") or "")


def render(post: dict, *, ai_quality: str = "best") -> bytes:
    """
    Render entry point. AI image is the GOAL for every card — text-only
    fallbacks only when both providers fail.

    ai_quality:
      "best"  — try Imagen 3 (multi-key) first, then Pollinations.
                For fresh posts where we want top quality.
      "cheap" — skip Imagen, use Pollinations only.
                For backfill of 1500+ historical posts so we don't burn
                Imagen 3 daily quota; Pollinations is free + unlimited.
      "none"  — no AI at all (debugging only).
    """
    entity = _resolve_entity(post)

    # Tier 1: AI hero (the default). Provider chosen by ai_quality.
    if ai_quality != "none":
        try_imagen = (ai_quality == "best")
        prompt = ai_image_generator.craft_prompt(
            headline=post.get("headline") or "",
            hook=(post.get("compliance_flags") or {}).get("angle_hook") or "",
            entity=entity,
        )
        log.info("[tier1] generating AI image (entity=%s, quality=%s)",
                 entity["id"] if entity else "-", ai_quality)
        ai_img = ai_image_generator.generate(prompt, try_imagen=try_imagen)
        if ai_img is not None:
            try:
                return _render_tier1_ai(post, entity, ai_img)
            except Exception as e:
                log.warning("[tier1] render failed, falling back to T2/T3: %s", e)
        else:
            log.warning("[tier1] AI returned None — falling back to T2/T3")

    # Tier 2: logo hero (only when both AI providers failed AND entity matched).
    if entity and entity.get("logo_url"):
        logo = image_fetcher.fetch(entity["logo_url"])
        if logo is not None:
            try:
                log.info("[tier2] entity=%s logo card", entity["id"])
                return _render_tier2_logo(post, entity, logo)
            except Exception as e:
                log.warning("[tier2] render failed, falling back to T3: %s", e)
        else:
            log.warning("[tier2] entity=%s logo fetch returned None — falling back to T3", entity["id"])

    # Tier 3: subtle hero (last resort — no big text, no WaCapital watermark).
    log.info("[tier3] subtle hero card (fallback)")
    return _render_tier3_subtle(post)


# ─── Upload ─────────────────────────────────────────────────────────────────

CARD_BUCKET = "card-images"


def upload(card_bytes: bytes, candidate_id) -> tuple[str, str]:
    """Upload PNG to Supabase Storage. Returns (public_url, storage_path).

    Uses upsert=true so an existing file is overwritten in a single POST
    (no more duplicate-detection dance, no more 400s from re-renders).
    """
    client = get_client()
    path   = f"posts/{candidate_id}.png"
    bucket = client.storage.from_(CARD_BUCKET)
    file_options = {
        "content-type":  "image/png",
        "cache-control": "3600",
        "upsert":        "true",   # overwrite if already exists
    }
    try:
        bucket.upload(path=path, file=card_bytes, file_options=file_options)
    except Exception as e:
        # Belt-and-suspenders: if upsert isn't honored for some reason,
        # fall back to an explicit PUT update.
        msg = str(e).lower()
        if any(k in msg for k in ("duplicate", "already exists", "409", "400", "exists")):
            bucket.update(path=path, file=card_bytes, file_options=file_options)
        else:
            raise
    return bucket.get_public_url(path), path


def render_and_upload(post: dict, candidate_id, *, ai_quality: str = "best") -> tuple[Optional[str], Optional[str]]:
    try:
        png = render(post, ai_quality=ai_quality)
        return upload(png, candidate_id)
    except Exception as e:
        log.warning("card render/upload failed for candidate %s: %s", candidate_id, e)
        return None, None
