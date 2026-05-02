"""
Card image generator — Bloque 6c MVP.

Renders a 1080x1080 PNG branded card per pulse_post using Pillow.
Square ratio works across all Phase 1 platforms (Twitter, Instagram Feed,
TikTok Photo). The carousel evolution (Bloque 6c-CAROUSEL) will produce
multiple slides per post.

Layout (top → bottom, y coordinates approximate):
   0 –  90   Header bar in the post's semáforo color
 130 – 210   Asset name (left) + 'WaCapital' wordmark (right)
 270 – 700   Headline, large bold, wraps up to 4 lines
 760 – 920   Hook / copy_twitter excerpt, secondary text, 3 lines
1000 – 1040  Footer: 'source · WaCapital — Powered by WaStake'

Storage:
  Supabase Storage bucket 'card-images', uploaded to path
  'posts/{candidate_id}.png' (one image per candidate). The public URL
  is written back to pulse_posts.card_image_url.

Failure handling:
  Render failures are non-fatal — the post still gets inserted in
  pulse_posts with card_image_url=NULL. The editorial generator logs
  a warning so we can audit.
"""
from io import BytesIO
import logging
import os
from typing import Optional

from PIL import Image, ImageDraw, ImageFont

from . import config, image_fetcher, entity_detector
from .supabase_client import get_client

log = logging.getLogger("card-generator")

# ─── Canvas ─────────────────────────────────────────────────────────────────

CARD_W = 1080
CARD_H = 1080

# ─── Colors ─────────────────────────────────────────────────────────────────

SEMAFORO_COLORS = {
    "verde":    (34, 197, 94),     # green-500
    "amarillo": (234, 179, 8),     # yellow-500
    "rojo":     (239, 68, 68),     # red-500
    "neutral":  (148, 163, 184),   # slate-400
}

BG_COLOR        = (15, 23, 42)     # slate-900
TEXT_PRIMARY    = (248, 250, 252)  # slate-50
TEXT_SECONDARY  = (203, 213, 225)  # slate-300
TEXT_MUTED      = (148, 163, 184)  # slate-400
SEPARATOR       = (51, 65, 85)     # slate-700

# ─── Layout ─────────────────────────────────────────────────────────────────

HEADER_BAR_H  = 90
PADDING_X     = 80
HEAD_GAP      = 40
ASSET_Y       = HEADER_BAR_H + HEAD_GAP

# Type sizes
SIZE_ASSET    = 56
SIZE_BRAND    = 32
SIZE_HEADLINE = 60
SIZE_HOOK     = 34
SIZE_FOOTER   = 24

# ─── Font loading with multi-path fallback ──────────────────────────────────

# Resolve repo-bundled fonts relative to this file. /app/src/card_generator.py
# on Railway → /app/src/assets/fonts/...
_HERE = os.path.dirname(os.path.abspath(__file__))
FONT_BUNDLED_BOLD    = os.path.join(_HERE, "assets", "fonts", "Inter-Bold.ttf")
FONT_BUNDLED_SEMI    = os.path.join(_HERE, "assets", "fonts", "Inter-SemiBold.ttf")
FONT_BUNDLED_REGULAR = os.path.join(_HERE, "assets", "fonts", "Inter-Regular.ttf")

# System fallback chain in case the bundled fonts go missing.
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
        "TTF load FAILED for size=%d. Tried paths=%s. Last error=%s. "
        "Cards will render with the unscaled bitmap fallback (text will look tiny). "
        "Make sure nixpacks.toml installs fonts-dejavu-core and fonts-liberation.",
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

def _wrap_text(draw: ImageDraw.ImageDraw, text: str, font, max_width: int) -> list[str]:
    """Greedy word wrap to fit max_width pixels."""
    words = (text or "").split()
    lines: list[str] = []
    current: list[str] = []
    for word in words:
        candidate = " ".join(current + [word])
        bbox = draw.textbbox((0, 0), candidate, font=font)
        if (bbox[2] - bbox[0]) <= max_width:
            current.append(word)
        else:
            if current:
                lines.append(" ".join(current))
            current = [word]
    if current:
        lines.append(" ".join(current))
    return lines


def _ellipsize_lines(lines: list[str], max_lines: int) -> list[str]:
    if len(lines) <= max_lines:
        return lines
    keep = lines[:max_lines]
    keep[-1] = keep[-1].rstrip(".,;:") + "…"
    return keep


def _domain_from_url(url: Optional[str]) -> str:
    if not url:
        return ""
    try:
        # naive parse, no urllib because the value should already be sane
        host = url.split("//", 1)[-1].split("/", 1)[0]
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


# ─── Render ─────────────────────────────────────────────────────────────────

def _render_text_only(post: dict) -> bytes:
    """Original layout (no entity match → text-only card)."""
    semaforo = (post.get("semaforo") or "neutral").lower()
    asset    = (post.get("asset_affected") or "GENERAL").upper()
    headline = (post.get("headline") or "").strip()
    hook_src = (post.get("copy_twitter") or "").strip()
    source   = _domain_from_url(post.get("source_link"))

    img  = Image.new("RGB", (CARD_W, CARD_H), BG_COLOR)
    draw = ImageDraw.Draw(img)

    bar_color = SEMAFORO_COLORS.get(semaforo, SEMAFORO_COLORS["neutral"])
    draw.rectangle([(0, 0), (CARD_W, HEADER_BAR_H)], fill=bar_color)

    f_asset = _font_bold(SIZE_ASSET)
    f_brand = _font_semi(SIZE_BRAND)
    draw.text((PADDING_X, ASSET_Y), asset, font=f_asset, fill=TEXT_PRIMARY)
    brand_text = "WaCapital"
    bbox = draw.textbbox((0, 0), brand_text, font=f_brand)
    brand_w = bbox[2] - bbox[0]
    draw.text((CARD_W - PADDING_X - brand_w, ASSET_Y + (SIZE_ASSET - SIZE_BRAND) // 2 + 4),
              brand_text, font=f_brand, fill=TEXT_PRIMARY)

    sep_y = ASSET_Y + SIZE_ASSET + 30
    draw.line([(PADDING_X, sep_y), (CARD_W - PADDING_X, sep_y)], fill=SEPARATOR, width=2)

    f_headline = _font_bold(SIZE_HEADLINE)
    max_w      = CARD_W - 2 * PADDING_X
    headline_lines = _ellipsize_lines(_wrap_text(draw, headline, f_headline, max_w), 4)
    headline_y = sep_y + 50
    line_h     = SIZE_HEADLINE + 14
    for i, line in enumerate(headline_lines):
        draw.text((PADDING_X, headline_y + i * line_h), line, font=f_headline, fill=TEXT_PRIMARY)

    f_hook    = _font_regular(SIZE_HOOK)
    hook_y    = headline_y + len(headline_lines) * line_h + 50
    hook_lines = _ellipsize_lines(_wrap_text(draw, hook_src, f_hook, max_w), 3)
    hook_lh    = SIZE_HOOK + 10
    for i, line in enumerate(hook_lines):
        draw.text((PADDING_X, hook_y + i * hook_lh), line, font=f_hook, fill=TEXT_SECONDARY)

    f_footer = _font_regular(SIZE_FOOTER)
    footer_y = CARD_H - 60
    footer_text = (f"{source} · " if source else "") + "WaCapital — Powered by WaStake"
    draw.text((PADDING_X, footer_y), footer_text, font=f_footer, fill=TEXT_MUTED)

    out = BytesIO()
    img.save(out, format="PNG", optimize=True)
    return out.getvalue()


def _render_with_image(post: dict, entity: dict, logo: Image.Image) -> bytes:
    """
    WatcherGuru-style layout v2.1: image dominates 45% of the card.

    Layout (1080x1080):
      0–90     top stripe in semáforo color
      120–190  asset (e.g. "BTC", "TESLA") + WaCapital wordmark
      220–710  image panel (490px tall) with rounded corners + colored bottom border
      750–960  headline, large bold, up to 3 lines
      1000–    footer: source · WaCapital — Powered by WaStake
    """
    semaforo = (post.get("semaforo") or "neutral").lower()
    asset    = entity["display"].upper()
    headline = (post.get("headline") or "").strip()
    source   = _domain_from_url(post.get("source_link"))

    img  = Image.new("RGB", (CARD_W, CARD_H), BG_COLOR)
    draw = ImageDraw.Draw(img)

    bar_color = SEMAFORO_COLORS.get(semaforo, SEMAFORO_COLORS["neutral"])

    # 1. Top stripe in semáforo color
    draw.rectangle([(0, 0), (CARD_W, HEADER_BAR_H)], fill=bar_color)

    # 2. Asset + WaCapital wordmark (compact header)
    f_asset = _font_bold(SIZE_ASSET)
    f_brand = _font_semi(SIZE_BRAND)
    asset_y = 120
    draw.text((PADDING_X, asset_y), asset, font=f_asset, fill=TEXT_PRIMARY)
    brand_text = "WaCapital"
    bbox = draw.textbbox((0, 0), brand_text, font=f_brand)
    brand_w = bbox[2] - bbox[0]
    draw.text((CARD_W - PADDING_X - brand_w, asset_y + (SIZE_ASSET - SIZE_BRAND) // 2 + 4),
              brand_text, font=f_brand, fill=TEXT_PRIMARY)

    # 3. Image panel — bigger, with semáforo-colored bottom border accent
    image_top      = 220
    image_bottom   = 710
    image_h        = image_bottom - image_top   # 490 px
    panel_padding  = 40
    panel_left     = PADDING_X
    panel_right    = CARD_W - PADDING_X
    border_h       = 6  # colored bottom accent

    # Main panel (light off-white, less harsh than pure white)
    draw.rounded_rectangle(
        [(panel_left, image_top), (panel_right, image_bottom - border_h)],
        radius=20,
        fill=(241, 245, 249),  # slate-100
    )
    # Bottom colored border (semáforo accent)
    draw.rectangle(
        [(panel_left + 20, image_bottom - border_h), (panel_right - 20, image_bottom)],
        fill=bar_color,
    )

    # Fit logo inside the panel.
    inner_w = (panel_right - panel_left) - 2 * panel_padding
    inner_h = (image_h - border_h) - 2 * panel_padding
    fitted  = image_fetcher.fit_into(logo, inner_w, inner_h)
    iw, ih  = fitted.size
    cx = (CARD_W - iw) // 2
    cy = image_top + ((image_h - border_h) - ih) // 2
    img.paste(fitted, (cx, cy), fitted if fitted.mode == "RGBA" else None)

    # 4. Headline below image — bigger, more dramatic
    f_headline = _font_bold(SIZE_HEADLINE)
    max_w      = CARD_W - 2 * PADDING_X
    headline_lines = _ellipsize_lines(_wrap_text(draw, headline, f_headline, max_w), 3)
    headline_y = 750
    line_h     = SIZE_HEADLINE + 14
    for i, line in enumerate(headline_lines):
        draw.text((PADDING_X, headline_y + i * line_h), line, font=f_headline, fill=TEXT_PRIMARY)

    # 5. Footer
    f_footer = _font_regular(SIZE_FOOTER)
    footer_y = CARD_H - 60
    footer_text = (f"{source} · " if source else "") + "WaCapital — Powered by WaStake"
    draw.text((PADDING_X, footer_y), footer_text, font=f_footer, fill=TEXT_MUTED)

    out = BytesIO()
    img.save(out, format="PNG", optimize=True)
    return out.getvalue()


def render(post: dict) -> bytes:
    """
    Public render entry point. Tries two paths to find an entity:

    1. Lookup by `asset_affected` field — populated at editorial time from the
       ORIGINAL English RSS headline. Groq later rewrites the headline in
       Spanish and may drop the entity name, so this is the authoritative source.
    2. Fallback: detect on the (translated) headline. Catches entities the
       editorial step missed.

    If we find an entity AND its logo fetches successfully, render with the
    WatcherGuru-style image layout. Otherwise fall back to text-only.
    """
    # Path 1: asset_affected lookup (covers most real cases)
    entity = entity_detector.find_by_id(post.get("asset_affected"))

    # Path 2: detect on the translated headline as a safety net
    if entity is None:
        entity = entity_detector.detect_entity(post.get("headline") or "")

    if entity and entity.get("logo_url"):
        log.info("rendering with image: entity=%s logo=%s", entity["id"], entity["logo_url"])
        logo = image_fetcher.fetch(entity["logo_url"])
        if logo is not None:
            try:
                return _render_with_image(post, entity, logo)
            except Exception as e:
                log.warning("image-card render failed for entity=%s, falling back to text-only: %s",
                            entity["id"], e)
        else:
            log.warning("entity=%s matched but logo fetch returned None — falling back to text-only", entity["id"])

    return _render_text_only(post)


# ─── Upload ─────────────────────────────────────────────────────────────────

CARD_BUCKET = "card-images"


def upload(card_bytes: bytes, candidate_id) -> tuple[str, str]:
    """
    Upload PNG to Supabase Storage. Returns (public_url, storage_path).
    Uses upsert via update-on-duplicate so reprocessed candidates overwrite.
    """
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
        # 409 / "duplicate" / "resource already exists" → overwrite via update.
        msg = str(e).lower()
        if "duplicate" in msg or "already exists" in msg or "409" in msg:
            bucket.update(path=path, file=card_bytes, file_options=file_options)
        else:
            raise

    public_url = bucket.get_public_url(path)
    return public_url, path


def render_and_upload(post: dict, candidate_id) -> tuple[Optional[str], Optional[str]]:
    """One-shot helper used by editorial_generator. Returns (url, path) or (None, None) on failure."""
    try:
        png = render(post)
        return upload(png, candidate_id)
    except Exception as e:
        log.warning("card render/upload failed for candidate %s: %s", candidate_id, e)
        return None, None
