"""
Carousel generator — Bloque 6c-CAROUSEL.

Generates 7 slides (1080×1920, 9:16 vertical) for TikTok photo carousel.
Algorithm research (2026): 5-10 slides optimal for completion rate signal;
7 is the sweet spot for finance/news — delivers value without drop-off.

Slide structure:
  1. HOOK      — card hero image (full-bleed) + stop-scroll headline
  2. CONTEXTO  — ¿Qué pasó? (headline expanded)
  3. DATO      — El número/dato clave (hook text)
  4. ÁNGULO    — Análisis editorial (angle)
  5. IMPACTO   — ¿Cómo te afecta? (reasoning)
  6. SEMÁFORO  — Señal de mercado con color
  7. CTA       — Síguenos @WaCapital + activa 🔔

Output: list[bytes] (PNG), one per slide.
Stored in Supabase Storage under carousels/{post_id}/slide_{n}.png
URLs tracked in compliance_flags.carousel_urls (no DB migration needed).
"""
import logging
import os
import textwrap
from io import BytesIO
from typing import Optional

import requests
from PIL import Image, ImageDraw, ImageFont

log = logging.getLogger("carousel-gen")

# ─── Canvas ─────────────────────────────────────────────────────────────────

W, H = 1080, 1920       # 9:16 TikTok native

# ─── Colors ─────────────────────────────────────────────────────────────────

BG            = (6,  8,  14)        # near-black base
BG_CARD       = (10, 13, 22)        # slightly lighter for content slides
ACCENT_CYAN   = (56, 189, 248)      # sky-400 — WaCapital accent
ACCENT_RED    = (239, 68,  68)      # red-500
ACCENT_GREEN  = (34, 197,  94)      # green-500
ACCENT_YELLOW = (234, 179,  8)      # yellow-500
WHITE         = (255, 255, 255)
GRAY_300      = (203, 213, 225)
GRAY_500      = (100, 116, 139)
GRAY_800      = (22,  33,  55)

SEMAFORO_COLOR = {
    "verde":    ACCENT_GREEN,
    "amarillo": ACCENT_YELLOW,
    "rojo":     ACCENT_RED,
    "neutral":  (148, 163, 184),
}
SEMAFORO_EMOJI = {
    "verde": "🟢 BULLISH",
    "amarillo": "🟡 CAUTION",
    "rojo": "🔴 BEARISH",
    "neutral": "⚪ NEUTRAL",
}

# ─── Font paths ──────────────────────────────────────────────────────────────

_HERE = os.path.dirname(__file__)
_BOLD    = os.path.join(_HERE, "assets", "fonts", "Inter-Bold.ttf")
_SEMI    = os.path.join(_HERE, "assets", "fonts", "Inter-SemiBold.ttf")
_REGULAR = os.path.join(_HERE, "assets", "fonts", "Inter-Regular.ttf")
_BOLD_SYS    = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
_REGULAR_SYS = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"


def _font(path_primary: str, fallback: str, size: int) -> ImageFont.FreeTypeFont:
    for p in [path_primary, fallback]:
        try:
            return ImageFont.truetype(p, size)
        except Exception:
            pass
    return ImageFont.load_default()

def _bold(size):    return _font(_BOLD,    _BOLD_SYS,    size)
def _semi(size):    return _font(_SEMI,    _BOLD_SYS,    size)
def _regular(size): return _font(_REGULAR, _REGULAR_SYS, size)


# ─── Drawing helpers ─────────────────────────────────────────────────────────

def _wrap_text(draw, text: str, font, max_w: int) -> list[str]:
    """Word-wrap text to fit within max_w pixels."""
    words = text.split()
    lines, current = [], ""
    for word in words:
        trial = (current + " " + word).strip()
        bb = draw.textbbox((0, 0), trial, font=font)
        if bb[2] - bb[0] <= max_w:
            current = trial
        else:
            if current:
                lines.append(current)
            current = word
    if current:
        lines.append(current)
    return lines


def _draw_text_block(draw, lines: list[str], font, color, start_y: int,
                     center_x: int, line_spacing: int = 12) -> int:
    """Draw centered multiline text. Returns y after last line."""
    y = start_y
    for line in lines:
        bb = draw.textbbox((0, 0), line, font=font)
        w = bb[2] - bb[0]
        draw.text((center_x - w // 2, y), line, font=font, fill=color)
        y += (bb[3] - bb[1]) + line_spacing
    return y


def _base_slide(label: str, slide_num: int, total: int = 7,
                accent: tuple = ACCENT_CYAN) -> tuple[Image.Image, ImageDraw.ImageDraw]:
    """Create a dark base slide with progress dots, label, and WaCapital brand."""
    img  = Image.new("RGB", (W, H), BG)
    draw = ImageDraw.Draw(img)

    # ── Subtle grid pattern (very faint) ─────────────────────────────────────
    for x in range(0, W, 60):
        draw.line([(x, 0), (x, H)], fill=(255, 255, 255, 4), width=1)
    for y in range(0, H, 60):
        draw.line([(0, y), (W, y)], fill=(255, 255, 255, 4), width=1)

    # ── Top accent bar (2px) ─────────────────────────────────────────────────
    draw.rectangle([0, 0, W, 4], fill=accent)

    # ── WaCapital brand — top left ───────────────────────────────────────────
    brand_font = _bold(32)
    draw.text((48, 36), "WaCapital", font=brand_font, fill=WHITE)

    # ── Progress dots — top right ────────────────────────────────────────────
    dot_r, dot_gap = 7, 18
    dot_total_w = total * (dot_r * 2) + (total - 1) * dot_gap
    dot_x_start = W - 48 - dot_total_w
    dot_y = 48
    for i in range(total):
        cx = dot_x_start + i * (dot_r * 2 + dot_gap) + dot_r
        color = WHITE if i == slide_num - 1 else GRAY_500
        draw.ellipse([cx - dot_r, dot_y - dot_r, cx + dot_r, dot_y + dot_r], fill=color)

    # ── Slide label ──────────────────────────────────────────────────────────
    label_font = _semi(28)
    label_bb   = draw.textbbox((0, 0), label, font=label_font)
    lw = label_bb[2] - label_bb[0]
    label_x = (W - lw) // 2
    label_y  = 110
    # Accent pill behind label
    pad = 20
    draw.rounded_rectangle(
        [label_x - pad, label_y - 8, label_x + lw + pad, label_y + 36],
        radius=20, fill=(*accent, 40)
    )
    draw.text((label_x, label_y), label, font=label_font, fill=accent)

    # ── Bottom brand bar ──────────────────────────────────────────────────────
    draw.rectangle([0, H - 80, W, H], fill=GRAY_800)
    handle_font = _regular(28)
    handle = "@WaCapital • Finanzas que importan"
    hbb    = draw.textbbox((0, 0), handle, font=handle_font)
    hw     = hbb[2] - hbb[0]
    draw.text(((W - hw) // 2, H - 55), handle, font=handle_font, fill=GRAY_500)

    return img, draw


# ─── Individual slide builders ───────────────────────────────────────────────

def _slide1_hook(post: dict) -> bytes:
    """Slide 1: Full-bleed hero + stop-scroll headline."""
    card_url = post.get("card_image_url")
    headline = (post.get("headline") or "").strip()
    semaforo = post.get("semaforo", "neutral")
    accent   = SEMAFORO_COLOR.get(semaforo, ACCENT_CYAN)

    # Load hero image (the existing card), scale to 9:16
    hero = None
    if card_url:
        try:
            r = requests.get(card_url, timeout=20)
            r.raise_for_status()
            hero = Image.open(BytesIO(r.content)).convert("RGB")
        except Exception as e:
            log.warning("slide1: could not load card image: %s", e)

    img = Image.new("RGB", (W, H), BG)

    if hero:
        # Scale to fill width, crop vertically centered
        ratio = W / hero.width
        new_h = int(hero.height * ratio)
        hero  = hero.resize((W, max(new_h, H)), Image.LANCZOS)
        top   = max(0, (hero.height - H) // 2)
        hero  = hero.crop((0, top, W, top + H))
        img.paste(hero, (0, 0))

    draw = ImageDraw.Draw(img)

    # Gradient overlay: bottom 2/3 fades to near-black
    fade_start = H // 3
    for y in range(fade_start, H):
        t = (y - fade_start) / (H - fade_start)
        alpha = int(220 * min(t * 1.4, 1.0))
        draw.line([(0, y), (W, y)], fill=(*BG, alpha))

    # Top accent bar
    draw.rectangle([0, 0, W, 6], fill=accent)

    # WaCapital brand
    draw.text((48, 36), "WaCapital", font=_bold(36), fill=WHITE)

    # Slide number
    draw.text((W - 80, 44), "1/7", font=_regular(28), fill=GRAY_300)

    # Semáforo badge
    badge_text = SEMAFORO_EMOJI.get(semaforo, "⚪ NEUTRAL")
    badge_font = _semi(30)
    bb   = draw.textbbox((0, 0), badge_text, font=badge_font)
    bw   = bb[2] - bb[0]
    bx   = (W - bw) // 2
    by   = H - 520
    draw.rounded_rectangle([bx - 24, by - 8, bx + bw + 24, by + 44],
                            radius=24, fill=(*accent, 50))
    draw.text((bx, by), badge_text, font=badge_font, fill=accent)

    # Headline — big, centered, bottom third
    h_font = _bold(62)
    lines  = _wrap_text(draw, headline, h_font, W - 80)
    if len(lines) > 4:
        lines = lines[:4]
        lines[-1] = lines[-1][:-3] + "…"
    _draw_text_block(draw, lines, h_font, WHITE, H - 460, W // 2, line_spacing=14)

    # Bottom brand bar
    draw.rectangle([0, H - 80, W, H], fill=(*BG, 200))
    draw.text(((W - 300) // 2, H - 55),
              "@WaCapital • Finanzas que importan",
              font=_regular(26), fill=GRAY_500)

    buf = BytesIO()
    img.save(buf, "PNG", optimize=True)
    return buf.getvalue()


def _slide_text(slide_num: int, label: str, title: str, body: str,
                accent: tuple = ACCENT_CYAN, post: dict = None) -> bytes:
    """Generic dark slide with label, title and body text."""
    semaforo = (post or {}).get("semaforo", "neutral")
    acc      = accent if accent != ACCENT_CYAN else SEMAFORO_COLOR.get(semaforo, ACCENT_CYAN)
    img, draw = _base_slide(label, slide_num, accent=acc)

    cx = W // 2
    content_top = 200

    # Title
    t_font = _bold(66)
    t_lines = _wrap_text(draw, title, t_font, W - 100)
    if len(t_lines) > 3:
        t_lines = t_lines[:3]
        t_lines[-1] = t_lines[-1][:-3] + "…"
    y = _draw_text_block(draw, t_lines, t_font, WHITE, content_top, cx, 16)

    # Divider
    y += 40
    draw.rectangle([cx - 120, y, cx + 120, y + 3], fill=acc)
    y += 36

    # Body
    b_font  = _semi(44)
    b_lines = _wrap_text(draw, body, b_font, W - 120)
    if len(b_lines) > 7:
        b_lines = b_lines[:7]
        b_lines[-1] = b_lines[-1][:-3] + "…"
    _draw_text_block(draw, b_lines, b_font, GRAY_300, y, cx, 20)

    buf = BytesIO()
    img.save(buf, "PNG", optimize=True)
    return buf.getvalue()


def _slide6_semaforo(post: dict) -> bytes:
    """Slide 6: Big visual semáforo signal."""
    semaforo = post.get("semaforo", "neutral")
    accent   = SEMAFORO_COLOR.get(semaforo, ACCENT_CYAN)
    headline = (post.get("headline") or "").strip()

    img, draw = _base_slide("SEÑAL DE MERCADO", 6, accent=accent)
    cx = W // 2

    # Giant colored circle
    cy = 680
    r  = 240
    draw.ellipse([cx - r, cy - r, cx + r, cy + r],
                 fill=(*accent, 30), outline=accent, width=8)

    # Inner ring
    r2 = 180
    draw.ellipse([cx - r2, cy - r2, cx + r2, cy + r2],
                 fill=(*accent, 15), outline=(*accent, 120), width=4)

    # Semáforo text inside circle
    labels = {
        "verde":    ("BULL", "MERCADO ALCISTA"),
        "amarillo": ("WAIT", "PRECAUCIÓN"),
        "rojo":     ("BEAR", "MERCADO BAJISTA"),
        "neutral":  ("HOLD", "NEUTRAL"),
    }
    short, long_ = labels.get(semaforo, ("HOLD", "NEUTRAL"))
    draw.text((cx - draw.textbbox((0,0), short, font=_bold(100))[2]//2 +
               draw.textbbox((0,0), short, font=_bold(100))[0]//2,
               cy - 70), short, font=_bold(100), fill=accent)

    # Re-draw short text properly centered
    sf = _bold(100)
    sbb = draw.textbbox((0, 0), short, font=sf)
    sw = sbb[2] - sbb[0]
    draw.text((cx - sw//2, cy - 65), short, font=sf, fill=accent)

    lf  = _semi(36)
    lbb = draw.textbbox((0, 0), long_, font=lf)
    lw  = lbb[2] - lbb[0]
    draw.text((cx - lw//2, cy + 60), long_, font=lf, fill=WHITE)

    # Context: post headline (brief)
    y = cy + r + 60
    draw.rectangle([cx - 120, y, cx + 120, y + 3], fill=(*accent, 150))
    y += 36

    h_font  = _semi(40)
    h_lines = _wrap_text(draw, headline, h_font, W - 120)
    if len(h_lines) > 3:
        h_lines = h_lines[:3]
    _draw_text_block(draw, h_lines, h_font, GRAY_300, y, cx, 16)

    buf = BytesIO()
    img.save(buf, "PNG", optimize=True)
    return buf.getvalue()


def _slide7_cta(post: dict) -> bytes:
    """Slide 7: CTA — follow + notifications."""
    semaforo = post.get("semaforo", "neutral")
    accent   = SEMAFORO_COLOR.get(semaforo, ACCENT_CYAN)

    img, draw = _base_slide("NO TE LO PIERDAS", 7, accent=accent)
    cx = W // 2

    # WaCapital big logo text
    logo_f = _bold(120)
    logo_t = "Wa"
    lbb    = draw.textbbox((0, 0), logo_t, font=logo_f)
    lw     = lbb[2] - lbb[0]
    draw.text((cx - lw//2, 260), logo_t, font=logo_f, fill=WHITE)

    cap_f = _bold(120)
    cap_t = "Capital"
    cbb   = draw.textbbox((0, 0), cap_t, font=cap_f)
    cw    = cbb[2] - cbb[0]
    draw.text((cx - cw//2, 380), cap_t, font=cap_f, fill=accent)

    # Tagline
    tag_f = _regular(40)
    tag   = "Finanzas que importan"
    tbb   = draw.textbbox((0, 0), tag, font=tag_f)
    tw    = tbb[2] - tbb[0]
    draw.text((cx - tw//2, 560), tag, font=tag_f, fill=GRAY_300)

    # Divider
    y = 660
    draw.rectangle([cx - 200, y, cx + 200, y + 3], fill=(*accent, 150))
    y += 60

    # CTA lines
    cta_lines = [
        "Seguinos en TikTok",
        "@WaCapital",
        "",
        "Activa las notificaciones 🔔",
        "para no perderte ningún análisis",
    ]
    for line in cta_lines:
        if not line:
            y += 20
            continue
        is_handle = line.startswith("@")
        f     = _bold(58) if is_handle else _semi(42)
        color = accent if is_handle else WHITE if "Activa" in line else GRAY_300
        bb    = draw.textbbox((0, 0), line, font=f)
        lw    = bb[2] - bb[0]
        draw.text((cx - lw//2, y), line, font=f, fill=color)
        y += (bb[3] - bb[1]) + 20

    # Arrow pointing up (swipe hint) at bottom
    y2 = H - 160
    arr_f = _bold(36)
    arr   = "↑ Swipe para volver al inicio"
    abb   = draw.textbbox((0, 0), arr, font=arr_f)
    aw    = abb[2] - abb[0]
    draw.text((cx - aw//2, y2), arr, font=arr_f, fill=GRAY_500)

    buf = BytesIO()
    img.save(buf, "PNG", optimize=True)
    return buf.getvalue()


# ─── Main entry point ────────────────────────────────────────────────────────

def generate_carousel(post: dict) -> list[bytes]:
    """
    Generate 7 carousel slides for a post.
    Returns list of PNG bytes, one per slide.
    Returns empty list on total failure.
    """
    # angle_hook/angle_reasoning are in compliance_flags JSONB
    flags     = post.get("compliance_flags") or {}
    headline  = (post.get("headline")                      or "Sin titulo").strip()
    hook      = (flags.get("angle_hook")                   or headline).strip()
    angle     = (flags.get("angle_reasoning")              or "Analisis en curso.").strip()
    reasoning = (flags.get("angle_reasoning")              or "Este evento podria impactar los mercados.").strip()
    semaforo  = post.get("semaforo", "neutral")
    accent    = SEMAFORO_COLOR.get(semaforo, ACCENT_CYAN)

    slides: list[bytes] = []
    builders = [
        # (fn, args)
        (lambda: _slide1_hook(post),),
        (lambda: _slide_text(2, "¿QUÉ PASÓ?",
                             "El contexto",
                             headline, accent, post),),
        (lambda: _slide_text(3, "EL DATO CLAVE",
                             "El número que importa",
                             hook, accent, post),),
        (lambda: _slide_text(4, "EL ANÁLISIS",
                             "¿Qué significa esto?",
                             angle, accent, post),),
        (lambda: _slide_text(5, "¿CÓMO TE AFECTA?",
                             "El impacto real",
                             reasoning, accent, post),),
        (lambda: _slide6_semaforo(post),),
        (lambda: _slide7_cta(post),),
    ]

    for i, (builder,) in enumerate(builders):
        try:
            slides.append(builder())
            log.info("  slide %d/7 OK (%d bytes)", i + 1, len(slides[-1]))
        except Exception as e:
            log.error("  slide %d/7 FAILED: %s", i + 1, e)
            # Insert blank fallback so slide count stays at 7
            try:
                img = Image.new("RGB", (W, H), BG)
                buf = BytesIO()
                img.save(buf, "PNG")
                slides.append(buf.getvalue())
            except Exception:
                pass

    log.info("carousel_generator: %d/7 slides generated for post %s",
             len(slides), post.get("id", "?"))
    return slides


def upload_carousel_to_supabase(post_id: str, slides: list[bytes]) -> list[str]:
    """
    Upload slides to Supabase Storage bucket 'carousel-slides'.
    Returns list of public URLs.
    """
    from .supabase_client import get_client
    from . import config

    client = get_client()
    urls   = []

    for i, slide_bytes in enumerate(slides):
        path = f"{post_id}/slide_{i+1:02d}.png"
        try:
            client.storage.from_("carousel-slides").upload(
                path,
                slide_bytes,
                {"content-type": "image/png", "upsert": "true"},
            )
            public_url = (
                f"{config.SUPABASE_URL}/storage/v1/object/public/carousel-slides/{path}"
            )
            urls.append(public_url)
            log.info("  uploaded slide %d/%d: %s", idx + 1, len(slides), path)
        except Exception as e:
            log.error("  failed to upload slide %d: %s", idx + 1, e)

    log.info("carousel: %d/%d slides uploaded for post %s", len(urls), len(slides), post_id)
    return urls
