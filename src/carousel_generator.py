"""
Carousel generator — Bloque 6c-CAROUSEL.

Generates 5 slides (1080×1920, 9:16 vertical) for TikTok photo carousel.

Slide structure:
  1. HOOK      — original WaCapital card (1080×1350 stretched), full headline
  2. DATO      — angle_hook (punchy data point) — blurred hero as bg
  3. ANÁLISIS  — angle_reasoning (deeper analysis) — blurred hero as bg
  4. SEMÁFORO  — market signal with color — blurred hero as bg
  5. CTA       — @WaCapital + follow + 🔔 — clean brand slide, no bg

Background strategy on content slides (2-4):
  • The WaCapital card has built-in headline text rendered into its bottom half.
    Using the FULL card as a slide background caused that white headline text
    to bleed through any dark overlay and visually fight with the slide's own
    body copy. Fix: crop ONLY the top half of the card (the bare AI image,
    before the gradient/text overlay) before using as background.
  • Heavy Gaussian blur (radius=40) hides the necessary upscaling artifacts
    and turns the hero into a moody color wash that maintains visual identity
    without competing with the foreground text.

History:
  • Original: 7 slides — slides 4 and 5 used the same angle_reasoning field,
    producing duplicated content.
  • v2: 6 slides — dropped the duplicated IMPACTO slide. Added card-as-bg
    which introduced the text bleed problem.
  • v3 (this): 5 slides — also dropped CONTEXTO (was just the headline,
    already in slide 1). Card-as-bg now uses only the bare AI part, blurred.

Output: list[bytes] (JPEG), one per slide.
Stored in Supabase Storage under carousel-slides/{post_id}/slide_NN.jpg
URLs tracked in compliance_flags.carousel_urls (no DB migration needed).
"""
import logging
import os
import textwrap
from io import BytesIO
from typing import Optional

import requests
from PIL import Image, ImageDraw, ImageFilter, ImageFont

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

# ─── Logo ────────────────────────────────────────────────────────────────────

_LOGO_PATH       = os.path.join(_HERE, "assets", "logo", "wacapital.png")
_logo_cache: Optional[Image.Image] = None


def _load_logo() -> Optional[Image.Image]:
    """Load WaCapital logo from disk once and cache it for the process lifetime."""
    global _logo_cache
    if _logo_cache is not None:
        return _logo_cache
    try:
        _logo_cache = Image.open(_LOGO_PATH).convert("RGBA")
        log.info("logo loaded: %dx%d", _logo_cache.width, _logo_cache.height)
        return _logo_cache
    except Exception as e:
        log.warning("could not load logo from %s: %s", _LOGO_PATH, e)
        return None


def _paste_logo(img: Image.Image, x: int, y: int, height: int) -> None:
    """
    Paste WaCapital logo onto `img` at (x, y), scaled to `height` px tall.

    Preserves aspect ratio. Uses the logo's alpha channel as a paste mask so
    transparent edges don't clobber the background.
    """
    logo = _load_logo()
    if logo is None:
        return
    ratio  = height / logo.height
    new_w  = int(logo.width * ratio)
    scaled = logo.resize((new_w, height), Image.LANCZOS)
    mask   = scaled.split()[3] if scaled.mode == "RGBA" else None
    img.paste(scaled, (x, y), mask)


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

def _load_hero(url: Optional[str], blur: bool = False) -> Optional[Image.Image]:
    """
    Download the card image and return it as a 1080×1920 (9:16) background.

    Critical detail: the WaCapital card (1080×1350) has a dark gradient + headline
    text rendered into its bottom 50%. If we used the full card as a carousel
    background, that built-in white headline would bleed through any dark overlay
    and visually fight with the slide's own text on top.

    To avoid this we crop ONLY the top half of the card (the bare AI image, before
    the gradient/text overlay starts), and optionally blur it. Result: a moody,
    abstract background that keeps the color identity of the post but is
    completely free of any text artifacts.
    """
    if not url:
        return None
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        img = Image.open(BytesIO(r.content)).convert("RGB")

        # The card layout has GRADIENT_START at y=680 (of 1350). Crop above that
        # to keep only the bare AI image. Threshold check (height >= 1000) avoids
        # touching non-card sources.
        if img.height >= 1000:
            img = img.crop((0, 0, img.width, min(680, img.height)))

        # Scale to fill width, then crop or pad vertically to 1080×1920
        ratio = W / img.width
        new_h = int(img.height * ratio)
        img = img.resize((W, max(new_h, H)), Image.LANCZOS)
        top = max(0, (img.height - H) // 2)
        img = img.crop((0, top, W, top + H))

        if blur:
            # Heavy gaussian blur turns the (necessarily stretched) hero into a
            # soft color wash. Hides any compression / stretch artifacts and
            # makes any remaining detail unobtrusive.
            img = img.filter(ImageFilter.GaussianBlur(radius=40))

        return img
    except Exception as e:
        log.warning("could not load hero from %s: %s", url, e)
        return None


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


def _base_slide(label: str, slide_num: int, total: int = 5,
                accent: tuple = ACCENT_CYAN,
                hero_url: Optional[str] = None,
                darkness: float = 0.62) -> tuple[Image.Image, ImageDraw.ImageDraw]:
    """
    Create a slide canvas with WaCapital brand frame.

    If hero_url is provided, the BARE AI hero (top half of the card, blurred)
    is used as background and darkened with `darkness` (0=no overlay, 1=fully
    black) so the text on top stays readable. The blur means we can use a
    lighter overlay (more visual character) without losing legibility, since
    there is no detail left to fight the text.

    If hero_url is None, falls back to a dark base with subtle grid pattern.
    """
    hero = _load_hero(hero_url, blur=True) if hero_url else None

    if hero:
        # Blend blurred hero with near-black for a moody color-tinted backdrop
        dark = Image.new("RGB", (W, H), BG)
        img  = Image.blend(hero, dark, darkness)
    else:
        img = Image.new("RGB", (W, H), BG)

    draw = ImageDraw.Draw(img)

    # ── Top accent bar (4px) ─────────────────────────────────────────────────
    # (no grid pattern — it was visually noisy and confused viewers)
    draw.rectangle([0, 0, W, 4], fill=accent)

    # ── WaCapital brand mark (logo) — top left ───────────────────────────────
    _paste_logo(img, x=36, y=28, height=80)

    # ── Progress dots — top right ────────────────────────────────────────────
    dot_r, dot_gap = 7, 18
    dot_total_w = total * (dot_r * 2) + (total - 1) * dot_gap
    dot_x_start = W - 48 - dot_total_w
    dot_y = 48
    for i in range(total):
        cx = dot_x_start + i * (dot_r * 2 + dot_gap) + dot_r
        color = WHITE if i == slide_num - 1 else GRAY_500
        draw.ellipse([cx - dot_r, dot_y - dot_r, cx + dot_r, dot_y + dot_r], fill=color)

    # ── Slide label (pill with accent outline) ──────────────────────────────
    label_font = _semi(28)
    label_bb   = draw.textbbox((0, 0), label, font=label_font)
    lw = label_bb[2] - label_bb[0]
    label_x = (W - lw) // 2
    label_y  = 130
    pad = 24
    # Solid dark fill + accent-colored outline. Previous version used a
    # tuple with alpha (`(*accent, 40)`) which Pillow ignores on RGB
    # canvases — the pill came out as a SOLID accent bar.
    draw.rounded_rectangle(
        [label_x - pad, label_y - 12, label_x + lw + pad, label_y + 40],
        radius=22,
        fill=BG_CARD,
        outline=accent,
        width=2,
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
    """
    Slide 1: Full-bleed hero + stop-scroll headline.

    The previous version used `draw.line(..., fill=(*BG, alpha))` on an RGB
    canvas. Pillow ignores the alpha component on RGB images, so every line
    from y=H/3 down was painted SOLID BLACK — collapsing the bottom 2/3 of
    the slide into a flat wall and hiding the hero. Fixed here by building
    a separate RGBA overlay and alpha_composite-ing onto the base.
    """
    card_url = post.get("card_image_url")
    headline = (post.get("headline") or "").strip()
    semaforo = post.get("semaforo", "neutral")
    accent   = SEMAFORO_COLOR.get(semaforo, ACCENT_CYAN)

    # Load hero (the existing card) and scale to fill 1080×1920 (cover-fit)
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
        # Cover-fit: scale so the image fills both dimensions, then center-crop
        scale = max(W / hero.width, H / hero.height)
        nw, nh = int(hero.width * scale), int(hero.height * scale)
        hero = hero.resize((nw, nh), Image.LANCZOS)
        left = max(0, (nw - W) // 2)
        top  = max(0, (nh - H) // 2)
        hero = hero.crop((left, top, left + W, top + H))
        img.paste(hero, (0, 0))

    # ── Gradient overlay — proper alpha compositing ─────────────────────────
    # Fade from 0% black at y=H*0.55 to ~88% black at y=H. Keeps the top of
    # the hero pristine and only darkens the bottom where text will sit.
    overlay     = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    over_draw   = ImageDraw.Draw(overlay)
    fade_start  = int(H * 0.55)
    fade_end    = H
    for y in range(fade_start, fade_end):
        t = (y - fade_start) / (fade_end - fade_start)
        alpha = int(225 * (t ** 1.4))     # eased curve, gentler middle, dense bottom
        over_draw.line([(0, y), (W, y)], fill=(BG[0], BG[1], BG[2], alpha))
    base_rgba = img.convert("RGBA")
    img = Image.alpha_composite(base_rgba, overlay).convert("RGB")
    draw = ImageDraw.Draw(img)

    # Top accent bar
    draw.rectangle([0, 0, W, 6], fill=accent)

    # WaCapital brand mark (logo) — top left
    _paste_logo(img, x=36, y=28, height=80)

    # Slide number — top right
    draw.text((W - 86, 44), "1/5", font=_regular(28), fill=GRAY_300)

    # Semáforo badge — sits just above the headline block
    badge_text = SEMAFORO_EMOJI.get(semaforo, "⚪ NEUTRAL")
    badge_font = _semi(32)
    bb   = draw.textbbox((0, 0), badge_text, font=badge_font)
    bw   = bb[2] - bb[0]
    bx   = (W - bw) // 2
    by   = H - 540
    # Solid pill (not alpha-tuple — was broken on RGB)
    draw.rounded_rectangle([bx - 26, by - 10, bx + bw + 26, by + 50],
                            radius=26, fill=BG_CARD, outline=accent, width=2)
    draw.text((bx, by), badge_text, font=badge_font, fill=accent)

    # Headline — big, centered, bottom third
    h_font = _bold(64)
    lines  = _wrap_text(draw, headline, h_font, W - 100)
    if len(lines) > 4:
        lines = lines[:4]
        lines[-1] = lines[-1].rstrip(".,;:") + "…"
    _draw_text_block(draw, lines, h_font, WHITE, H - 460, W // 2, line_spacing=14)

    # Bottom brand bar
    draw.rectangle([0, H - 80, W, H], fill=GRAY_800)
    handle = "@WaCapital • Finanzas que importan"
    hf     = _regular(26)
    hbb    = draw.textbbox((0, 0), handle, font=hf)
    hw     = hbb[2] - hbb[0]
    draw.text(((W - hw) // 2, H - 55), handle, font=hf, fill=GRAY_500)

    buf = BytesIO()
    img.save(buf, "JPEG", quality=90)
    return buf.getvalue()


def _slide_text(slide_num: int, label: str, title: str, body: str,
                accent: tuple = ACCENT_CYAN, post: dict = None) -> bytes:
    """
    Text slide — blurred-hero background, big body text centered vertically.

    Previous version pinned the title to y=200 and the body below it, leaving
    most of the slide empty at the bottom. Now we vertically center the body
    block in the available area (between top brand and bottom brand bars),
    so the slide feels balanced and the eye lands on the content.
    """
    semaforo = (post or {}).get("semaforo", "neutral")
    acc      = accent if accent != ACCENT_CYAN else SEMAFORO_COLOR.get(semaforo, ACCENT_CYAN)
    hero_url = (post or {}).get("card_image_url")
    img, draw = _base_slide(label, slide_num, total=5, accent=acc, hero_url=hero_url)

    cx = W // 2

    # ── Build the body block first so we can vertical-center it ─────────────
    b_font  = _bold(68)                                 # bigger than before (was 44 semi)
    b_lines = _wrap_text(draw, body, b_font, W - 120)
    # If body wraps too long at the chosen size, shrink the font instead of
    # ellipsizing — we'd rather see all the text smaller than have "..."
    for size in (68, 62, 56, 50, 46):
        b_font  = _bold(size)
        b_lines = _wrap_text(draw, body, b_font, W - 120)
        if len(b_lines) <= 8:
            break
    if len(b_lines) > 8:
        b_lines = b_lines[:8]
        b_lines[-1] = b_lines[-1].rstrip(".,;:") + "…"

    line_h     = b_font.size + 16
    body_h     = len(b_lines) * line_h
    title_font = _semi(42)
    title_h    = title_font.size + 18
    divider_h  = 60

    block_h    = title_h + divider_h + body_h
    safe_top   = 250                       # below label pill
    safe_bot   = H - 130                   # above brand bar
    available  = safe_bot - safe_top
    block_y    = safe_top + max(0, (available - block_h) // 2)

    # Title (smaller, sits above the body as a subhead)
    tbb   = draw.textbbox((0, 0), title, font=title_font)
    tw    = tbb[2] - tbb[0]
    draw.text((cx - tw // 2, block_y), title, font=title_font, fill=acc)

    # Divider
    div_y = block_y + title_h + 20
    draw.rectangle([cx - 110, div_y, cx + 110, div_y + 4], fill=acc)

    # Body
    body_y = div_y + divider_h - 10
    _draw_text_block(draw, b_lines, b_font, WHITE, body_y, cx, 16)

    buf = BytesIO()
    img.save(buf, "JPEG", quality=90)
    return buf.getvalue()


def _slide4_semaforo(post: dict) -> bytes:
    """
    Slide 4: Real traffic light — 3 stacked circles (red/yellow/green), only the
    one matching the post's semáforo state is fully lit. The other two are
    dim/gray-out, like a real signal.
    """
    semaforo = post.get("semaforo", "neutral")
    accent   = SEMAFORO_COLOR.get(semaforo, ACCENT_CYAN)
    headline = (post.get("headline") or "").strip()
    hero_url = post.get("card_image_url")

    # Stronger darkening so the colored circle is unambiguous
    img, draw = _base_slide("SEÑAL DE MERCADO", 4, total=5, accent=accent,
                            hero_url=hero_url, darkness=0.78)
    cx = W // 2

    # ── Housing: tall rounded rectangle (the traffic light body) ────────────
    housing_w  = 360
    housing_h  = 920
    housing_x  = cx - housing_w // 2
    housing_y  = 280
    draw.rounded_rectangle(
        [housing_x, housing_y, housing_x + housing_w, housing_y + housing_h],
        radius=60,
        fill=(18, 22, 32),
        outline=(60, 70, 90),
        width=6,
    )

    # ── Three lights, stacked vertically ────────────────────────────────────
    light_r       = 130
    inner_gap_y   = 70                                  # padding from housing edges
    span_y        = housing_h - 2 * inner_gap_y
    slot_h        = span_y / 3
    states_order  = [
        ("rojo",     ACCENT_RED,    "MERCADO BAJISTA"),
        ("amarillo", ACCENT_YELLOW, "PRECAUCIÓN"),
        ("verde",    ACCENT_GREEN,  "MERCADO ALCISTA"),
    ]
    # Neutral maps to "no light on" — handled below
    active_state = semaforo if semaforo in ("rojo", "amarillo", "verde") else None

    for i, (state, color, _label) in enumerate(states_order):
        ly = int(housing_y + inner_gap_y + slot_h * (i + 0.5))
        is_on = (state == active_state)
        if is_on:
            # Outer glow halo
            for halo_r, halo_alpha in [(light_r + 36, 32), (light_r + 22, 56), (light_r + 10, 90)]:
                halo = Image.new("RGBA", (W, H), (0, 0, 0, 0))
                hd   = ImageDraw.Draw(halo)
                hd.ellipse(
                    [cx - halo_r, ly - halo_r, cx + halo_r, ly + halo_r],
                    fill=(*color, halo_alpha),
                )
                img_rgba = img.convert("RGBA")
                img      = Image.alpha_composite(img_rgba, halo).convert("RGB")
                draw     = ImageDraw.Draw(img)
            # Lit bulb
            draw.ellipse(
                [cx - light_r, ly - light_r, cx + light_r, ly + light_r],
                fill=color, outline=WHITE, width=3,
            )
            # Inner highlight (gives a "glass bulb" feel)
            hl_r = light_r // 2
            hl_x = cx - light_r // 3
            hl_y = ly - light_r // 3
            highlight = Image.new("RGBA", (W, H), (0, 0, 0, 0))
            hd2 = ImageDraw.Draw(highlight)
            hd2.ellipse(
                [hl_x - hl_r, hl_y - hl_r, hl_x + hl_r, hl_y + hl_r],
                fill=(255, 255, 255, 60),
            )
            img_rgba = img.convert("RGBA")
            img      = Image.alpha_composite(img_rgba, highlight).convert("RGB")
            draw     = ImageDraw.Draw(img)
        else:
            # Off — dim, dark version of the color
            dim_color = (color[0] // 5, color[1] // 5, color[2] // 5)
            draw.ellipse(
                [cx - light_r, ly - light_r, cx + light_r, ly + light_r],
                fill=dim_color, outline=(70, 80, 100), width=3,
            )

    # ── Label below the traffic light ────────────────────────────────────────
    label_text_map = {
        "verde":    "MERCADO ALCISTA",
        "amarillo": "PRECAUCIÓN",
        "rojo":     "MERCADO BAJISTA",
        "neutral":  "SEÑAL NEUTRAL",
    }
    label_color   = SEMAFORO_COLOR.get(semaforo, GRAY_300)
    state_label   = label_text_map.get(semaforo, "SEÑAL NEUTRAL")
    state_font    = _bold(56)
    sbb           = draw.textbbox((0, 0), state_label, font=state_font)
    sw            = sbb[2] - sbb[0]
    state_y       = housing_y + housing_h + 50
    draw.text((cx - sw // 2, state_y), state_label, font=state_font, fill=label_color)

    # Small headline context under the label, muted
    ctx_y = state_y + 90
    ctx_f = _regular(32)
    ctx_lines = _wrap_text(draw, headline, ctx_f, W - 160)
    if len(ctx_lines) > 2:
        ctx_lines = ctx_lines[:2]
        ctx_lines[-1] = ctx_lines[-1].rstrip(".,;:") + "…"
    _draw_text_block(draw, ctx_lines, ctx_f, GRAY_300, ctx_y, cx, 8)

    buf = BytesIO()
    img.save(buf, "JPEG", quality=90)
    return buf.getvalue()


def _slide5_cta(post: dict) -> bytes:
    """Slide 5: CTA — follow + notifications. Clean brand slide, no hero bg."""
    semaforo = post.get("semaforo", "neutral")
    accent   = SEMAFORO_COLOR.get(semaforo, ACCENT_CYAN)

    # No hero on CTA — keeps the brand block crisp and distinct from content slides
    img, draw = _base_slide("NO TE LO PIERDAS", 5, total=5, accent=accent)
    cx = W // 2

    # ── Big WaCapital logo as centerpiece ────────────────────────────────────
    logo = _load_logo()
    if logo is not None:
        logo_height = 460
        ratio  = logo_height / logo.height
        new_w  = int(logo.width * ratio)
        scaled = logo.resize((new_w, logo_height), Image.LANCZOS)
        mask   = scaled.split()[3] if scaled.mode == "RGBA" else None
        img.paste(scaled, (cx - new_w // 2, 240), mask)
    else:
        # Fallback: text version if logo missing
        draw.text((cx - draw.textbbox((0, 0), "Wa", font=_bold(120))[2] // 2,
                   260), "Wa", font=_bold(120), fill=WHITE)
        draw.text((cx - draw.textbbox((0, 0), "Capital", font=_bold(120))[2] // 2,
                   380), "Capital", font=_bold(120), fill=accent)

    # Tagline directly under the logo
    tag_f = _regular(42)
    tag   = "Finanzas que importan"
    tbb   = draw.textbbox((0, 0), tag, font=tag_f)
    tw    = tbb[2] - tbb[0]
    draw.text((cx - tw // 2, 730), tag, font=tag_f, fill=GRAY_300)

    # Divider (solid accent color — no alpha tuple, RGB ignores it anyway)
    y = 820
    draw.rectangle([cx - 200, y, cx + 200, y + 4], fill=accent)
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
    img.save(buf, "JPEG", quality=90)
    return buf.getvalue()


# ─── Main entry point ────────────────────────────────────────────────────────

def generate_carousel(post: dict) -> list[bytes]:
    """
    Generate 5 carousel slides for a post.
    Returns list of JPEG bytes, one per slide.

    Structure:
      1. HOOK      — card image + headline (the original WaCapital card)
      2. DATO      — angle_hook (punchy data point)
      3. ANÁLISIS  — angle_reasoning (deeper analysis)
      4. SEMÁFORO  — market signal with color
      5. CTA       — brand + follow

    The redundant "CONTEXTO" slide that re-rendered the headline (already shown
    in slide 1) was dropped. The hero background on slides 2-4 is sourced from
    only the top half of the card (the bare AI image) and blurred, so the
    card's built-in headline text never bleeds through and competes with the
    slide's own copy.
    """
    flags    = post.get("compliance_flags") or {}
    hook     = (flags.get("angle_hook")        or (post.get("headline") or "Sin titulo")).strip()
    angle    = (flags.get("angle_reasoning")   or "Análisis en curso.").strip()
    semaforo = post.get("semaforo", "neutral")
    accent   = SEMAFORO_COLOR.get(semaforo, ACCENT_CYAN)

    slides: list[bytes] = []
    builders = [
        lambda: _slide1_hook(post),
        lambda: _slide_text(2, "EL DATO CLAVE",
                            "El número que importa",
                            hook, accent, post),
        lambda: _slide_text(3, "EL ANÁLISIS",
                            "¿Qué significa esto?",
                            angle, accent, post),
        lambda: _slide4_semaforo(post),
        lambda: _slide5_cta(post),
    ]

    total = len(builders)
    for i, builder in enumerate(builders):
        try:
            slides.append(builder())
            log.info("  slide %d/%d OK (%d bytes)", i + 1, total, len(slides[-1]))
        except Exception as e:
            log.error("  slide %d/%d FAILED: %s", i + 1, total, e)
            # Insert blank fallback so slide count stays consistent
            try:
                img = Image.new("RGB", (W, H), BG)
                buf = BytesIO()
                img.save(buf, "JPEG", quality=90)
                slides.append(buf.getvalue())
            except Exception:
                pass

    log.info("carousel_generator: %d/%d slides generated for post %s",
             len(slides), total, post.get("id", "?"))
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
        slide_path = f"{post_id}/slide_{i+1:02d}.jpg"
        try:
            client.storage.from_("carousel-slides").upload(
                slide_path,
                slide_bytes,
                {"content-type": "image/jpeg", "x-upsert": "true"},
            )
            public_url = (
                f"{config.SUPABASE_URL}/storage/v1/object/public/carousel-slides/{slide_path}"
            )
            urls.append(public_url)
            log.info("  uploaded slide %d/%d: %s", i + 1, len(slides), slide_path)
        except Exception as e:
            log.error("  failed to upload slide %d: %s", i + 1, e)

    log.info("carousel: %d/%d slides uploaded for post %s", len(urls), len(slides), post_id)
    return urls
