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

BG_COLOR        = (15, 23, 42)     # slate-900 — base/fallback hero
HERO_DARK       = (10, 18, 35)     # slightly bluer dark for hero panels
OVERLAY_BG      = (10, 14, 24)     # dark text-box background (almost black)
TEXT_PRIMARY    = (248, 250, 252)  # slate-50 — headline white
TEXT_SECONDARY  = (203, 213, 225)  # slate-300
TEXT_MUTED      = (148, 163, 184)  # slate-400
ACCENT_CYAN     = (56, 189, 248)   # sky-400 — the CryptoAlpha-style hook color

# ─── Layout ─────────────────────────────────────────────────────────────────

TOP_STRIPE_H   = 90

HERO_TOP       = TOP_STRIPE_H            # 90
HERO_BOTTOM    = 850
HERO_HEIGHT    = HERO_BOTTOM - HERO_TOP  # 760

OVERLAY_TOP    = 850
OVERLAY_BOTTOM = 1280
OVERLAY_PAD_X  = 40
OVERLAY_PAD_INNER = 50

BRAND_Y        = 1300  # brand sits below the overlay box

# Type sizes
SIZE_ASSET    = 88     # big asset name in T3 hero (when no logo)
SIZE_HEADLINE = 60     # white text in overlay
SIZE_HOOK     = 50     # cyan accent text
SIZE_BRAND    = 28
SIZE_FOOTER   = 22


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


def _draw_top_stripe(img: Image.Image, semaforo: str) -> None:
    color = SEMAFORO_COLORS.get(semaforo, SEMAFORO_COLORS["neutral"])
    ImageDraw.Draw(img).rectangle([(0, 0), (CARD_W, TOP_STRIPE_H)], fill=color)


def _draw_hero_solid(img: Image.Image, asset_label: str) -> None:
    """T3 hero — dark slate background with a giant centered asset label."""
    draw = ImageDraw.Draw(img)
    draw.rectangle([(0, HERO_TOP), (CARD_W, HERO_BOTTOM)], fill=HERO_DARK)
    label = (asset_label or "GENERAL").upper()
    f = _font_bold(SIZE_ASSET)
    bb = draw.textbbox((0, 0), label, font=f)
    tw, th = bb[2] - bb[0], bb[3] - bb[1]
    cx = (CARD_W - tw) // 2
    cy = HERO_TOP + (HERO_HEIGHT - th) // 2
    draw.text((cx, cy), label, font=f, fill=TEXT_PRIMARY)


def _draw_hero_logo(img: Image.Image, logo: Image.Image) -> None:
    """T2 hero — dark panel with logo centered. Slight white pad behind logo
    so dark/transparent logos still read."""
    draw = ImageDraw.Draw(img)
    draw.rectangle([(0, HERO_TOP), (CARD_W, HERO_BOTTOM)], fill=HERO_DARK)

    # Inner white-ish panel for the logo (helps Clearbit logos with white BG fit in)
    panel_pad = 80
    panel_top    = HERO_TOP + panel_pad
    panel_bottom = HERO_BOTTOM - panel_pad
    panel_left   = panel_pad
    panel_right  = CARD_W - panel_pad
    draw.rounded_rectangle(
        [(panel_left, panel_top), (panel_right, panel_bottom)],
        radius=24,
        fill=(241, 245, 249),  # slate-100
    )
    inner_w = (panel_right - panel_left) - 60
    inner_h = (panel_bottom - panel_top) - 60
    fitted  = image_fetcher.fit_into(logo, inner_w, inner_h)
    iw, ih = fitted.size
    cx = (CARD_W - iw) // 2
    cy = panel_top + ((panel_bottom - panel_top) - ih) // 2
    img.paste(fitted, (cx, cy), fitted if fitted.mode == "RGBA" else None)


def _draw_hero_ai(img: Image.Image, ai: Image.Image) -> None:
    """T1 hero — full-bleed AI image cropped/scaled to the hero area."""
    target_w = CARD_W
    target_h = HERO_HEIGHT
    iw, ih = ai.size
    # Scale to cover, then center-crop.
    scale = max(target_w / iw, target_h / ih)
    nw = max(1, int(iw * scale))
    nh = max(1, int(ih * scale))
    ai_resized = ai.resize((nw, nh), Image.LANCZOS)
    cx_off = (nw - target_w) // 2
    cy_off = (nh - target_h) // 2
    cropped = ai_resized.crop((cx_off, cy_off, cx_off + target_w, cy_off + target_h))
    img.paste(cropped, (0, HERO_TOP))


def _draw_overlay_text(img: Image.Image, headline: str, hook: str) -> None:
    """
    Bottom text panel: dark rounded box with white headline + cyan hook.
    """
    draw = ImageDraw.Draw(img)
    draw.rounded_rectangle(
        [(OVERLAY_PAD_X, OVERLAY_TOP), (CARD_W - OVERLAY_PAD_X, OVERLAY_BOTTOM)],
        radius=28,
        fill=OVERLAY_BG,
    )

    inner_x_left  = OVERLAY_PAD_X + OVERLAY_PAD_INNER
    inner_x_right = CARD_W - OVERLAY_PAD_X - OVERLAY_PAD_INNER
    max_w = inner_x_right - inner_x_left

    # Headline (white)
    f_head = _font_bold(SIZE_HEADLINE)
    head_lines = _ellipsize(_wrap(draw, headline, f_head, max_w), 3)
    head_lh    = SIZE_HEADLINE + 10
    head_y     = OVERLAY_TOP + 35
    for i, line in enumerate(head_lines):
        draw.text((inner_x_left, head_y + i * head_lh), line, font=f_head, fill=TEXT_PRIMARY)

    # Hook (cyan accent), 2 lines max
    if hook:
        f_hook = _font_bold(SIZE_HOOK)
        hook_lines = _ellipsize(_wrap(draw, hook, f_hook, max_w), 2)
        hook_lh    = SIZE_HOOK + 8
        hook_y     = head_y + len(head_lines) * head_lh + 20
        # Don't overflow the box
        max_hook_lines = max(0, (OVERLAY_BOTTOM - hook_y - 20) // hook_lh)
        for i, line in enumerate(hook_lines[:max_hook_lines]):
            draw.text((inner_x_left, hook_y + i * hook_lh), line, font=f_hook, fill=ACCENT_CYAN)


def _draw_brand_footer(img: Image.Image, source: str) -> None:
    draw = ImageDraw.Draw(img)
    f_brand  = _font_semi(SIZE_BRAND)
    brand_text = "WaCapital"
    bb = draw.textbbox((0, 0), brand_text, font=f_brand)
    tw = bb[2] - bb[0]
    cx = (CARD_W - tw) // 2
    draw.text((cx, BRAND_Y), brand_text, font=f_brand, fill=TEXT_SECONDARY)

    # Tiny source line below
    if source:
        f_foot = _font_regular(SIZE_FOOTER)
        bb2 = draw.textbbox((0, 0), source, font=f_foot)
        tw2 = bb2[2] - bb2[0]
        cx2 = (CARD_W - tw2) // 2
        draw.text((cx2, BRAND_Y + SIZE_BRAND + 4), source, font=f_foot, fill=TEXT_MUTED)


# ─── Tier renderers ─────────────────────────────────────────────────────────

def _render_tier1_ai(post: dict, entity: dict, ai_image: Image.Image) -> bytes:
    semaforo = (post.get("semaforo") or "neutral").lower()
    headline = (post.get("headline")    or "").strip()
    flags    = post.get("compliance_flags") or {}
    hook     = (flags.get("angle_hook") or "").strip()
    source   = _domain(post.get("source_link"))

    img = _new_canvas()
    _draw_top_stripe(img, semaforo)
    _draw_hero_ai(img, ai_image)
    _draw_overlay_text(img, headline, hook)
    _draw_brand_footer(img, source)

    out = BytesIO()
    img.save(out, format="PNG", optimize=True)
    return out.getvalue()


def _render_tier2_logo(post: dict, entity: dict, logo: Image.Image) -> bytes:
    semaforo = (post.get("semaforo") or "neutral").lower()
    headline = (post.get("headline")    or "").strip()
    flags    = post.get("compliance_flags") or {}
    hook     = (flags.get("angle_hook") or "").strip()
    source   = _domain(post.get("source_link"))

    img = _new_canvas()
    _draw_top_stripe(img, semaforo)
    _draw_hero_logo(img, logo)
    _draw_overlay_text(img, headline, hook)
    _draw_brand_footer(img, source)

    out = BytesIO()
    img.save(out, format="PNG", optimize=True)
    return out.getvalue()


def _render_tier3_text(post: dict) -> bytes:
    semaforo = (post.get("semaforo") or "neutral").lower()
    headline = (post.get("headline")    or "").strip()
    flags    = post.get("compliance_flags") or {}
    hook     = (flags.get("angle_hook") or "").strip()
    source   = _domain(post.get("source_link"))
    asset    = (post.get("asset_affected") or "general").upper()

    img = _new_canvas()
    _draw_top_stripe(img, semaforo)
    _draw_hero_solid(img, asset)
    _draw_overlay_text(img, headline, hook)
    _draw_brand_footer(img, source)

    out = BytesIO()
    img.save(out, format="PNG", optimize=True)
    return out.getvalue()


# ─── Tier dispatch ──────────────────────────────────────────────────────────

def _wants_tier1(post: dict, entity: Optional[dict]) -> bool:
    """T1 only for highest-value posts with a clear visual subject."""
    if entity is None:
        return False
    flags = post.get("compliance_flags") or {}
    strength = flags.get("angle_strength") or 0
    return strength >= 5


def _resolve_entity(post: dict) -> Optional[dict]:
    # Prefer asset_affected (set at editorial time from original English headline).
    e = entity_detector.find_by_id(post.get("asset_affected"))
    if e is not None:
        return e
    # Fallback: detect on the (translated) headline.
    return entity_detector.detect_entity(post.get("headline") or "")


def render(post: dict, *, skip_ai: bool = False) -> bytes:
    """
    Public render entry point. Resolves entity, decides tier, and renders.
    Always returns valid PNG bytes — degrades through tiers on failure.

    skip_ai=True forces Tier-2 or Tier-3, skipping Imagen 3. Used by the
    card backfill so re-rendering the historical backlog doesn't drain
    daily AI image quota.
    """
    entity = _resolve_entity(post)

    # Tier 1: AI hero
    if not skip_ai and _wants_tier1(post, entity):
        prompt = ai_image_generator.craft_prompt(
            headline=post.get("headline") or "",
            hook=(post.get("compliance_flags") or {}).get("angle_hook") or "",
            entity=entity,
        )
        log.info("[tier1] strength==5 + entity=%s — generating AI image", entity["id"])
        ai_img = ai_image_generator.generate(prompt)
        if ai_img is not None:
            try:
                return _render_tier1_ai(post, entity, ai_img)
            except Exception as e:
                log.warning("[tier1] render failed, falling back to T2/T3: %s", e)
        else:
            log.warning("[tier1] AI image generation returned None — falling back")

    # Tier 2: logo hero
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

    # Tier 3: text-only hero
    log.info("[tier3] text-only card")
    return _render_tier3_text(post)


# ─── Upload ─────────────────────────────────────────────────────────────────

CARD_BUCKET = "card-images"


def upload(card_bytes: bytes, candidate_id) -> tuple[str, str]:
    """Upload PNG to Supabase Storage. Returns (public_url, storage_path)."""
    client = get_client()
    path   = f"posts/{candidate_id}.png"
    bucket = client.storage.from_(CARD_BUCKET)
    file_options = {
        "content-type": "image/png",
        "cache-control": "3600",
    }
    try:
        bucket.upload(path=path, file=card_bytes, file_options=file_options)
    except Exception as e:
        msg = str(e).lower()
        if "duplicate" in msg or "already exists" in msg or "409" in msg:
            bucket.update(path=path, file=card_bytes, file_options=file_options)
        else:
            raise
    return bucket.get_public_url(path), path


def render_and_upload(post: dict, candidate_id, *, skip_ai: bool = False) -> tuple[Optional[str], Optional[str]]:
    try:
        png = render(post, skip_ai=skip_ai)
        return upload(png, candidate_id)
    except Exception as e:
        log.warning("card render/upload failed for candidate %s: %s", candidate_id, e)
        return None, None
