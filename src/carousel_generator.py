"""
Carousel generator — Bloque 6c-CAROUSEL v4 (Luxury Mindset style).

Generates 5 slides (1080×1920, 9:16 vertical) for TikTok photo carousel.

Slide structure:
  1. HOOK      — original WaCapital card pasted full-bleed, NO text overlay.
                 The card already has the headline + hook baked in.
  2. EL DATO   — full-bleed bare-AI hero (heavily darkened 78%) + top pill
                 "EL DATO" + HUGE angle_hook + bottom pill takeaway.
  3. ANÁLISIS  — same Luxury layout, title = first sentence of angle_reasoning,
                 subtitle = next 1-2 sentences.
  4. SEMÁFORO  — smaller traffic light + "¿POR QUÉ?" pill + 3 bullet points
                 sourced from angle_reasoning.
  5. CTA       — clean brand slide with logo + tagline + follow.

Design inspiration: @luxurymindset and similar finance/luxury TikTok carousels.
Key pattern: hero image is darkened to 22% visibility so it gives identity to
the slide without ever competing with the text. The text is the message; the
image is the mood.

History notes (failure modes I've fixed across iterations):
  • Original: 7 slides, slides 4/5 duplicated content (same angle_reasoning).
  • v2: 6 slides; introduced card-as-background which made the card's built-in
    headline text bleed through and fight slide-specific text.
  • v3: 5 slides; cropped to bare AI image + blurred to kill text bleed but
    layout was top-heavy with empty bottom half.
  • v4 (this): the Luxury Mindset layout — pill / big title / subtitle /
    bottom pill, body vertically centered, hero darkened to a near-black wash.

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
    Slide 1: the WaCapital card, full-bleed, no text overlay.

    The card itself already contains everything we want users to see:
    the AI hero image, the headline, the cyan hook, the WaCapital wordmark
    at the bottom. Previously we ADDED another headline overlay on top of
    the card, which produced visible double-text (card headline + slide
    headline both saying the same thing). Fixed here by just pasting the
    card as the slide image and trusting the card design.
    """
    card_url = post.get("card_image_url")
    img      = Image.new("RGB", (W, H), BG)

    if not card_url:
        # Fallback: no card image → render a minimal logo-only slide
        _paste_logo(img, x=(W - 460) // 2, y=(H - 460) // 2, height=460)
        buf = BytesIO()
        img.save(buf, "JPEG", quality=90)
        return buf.getvalue()

    try:
        r = requests.get(card_url, timeout=20)
        r.raise_for_status()
        card = Image.open(BytesIO(r.content)).convert("RGB")
    except Exception as e:
        log.warning("slide1: could not load card image: %s", e)
        _paste_logo(img, x=(W - 460) // 2, y=(H - 460) // 2, height=460)
        buf = BytesIO()
        img.save(buf, "JPEG", quality=90)
        return buf.getvalue()

    # Cover-fit the card so it fills the slide. The card is 4:5 (1080×1350)
    # and the slide is 9:16 (1080×1920), so we scale to fill height and crop
    # whatever sticks out horizontally (very little — only the sides).
    scale = max(W / card.width, H / card.height)
    nw, nh = int(card.width * scale), int(card.height * scale)
    card = card.resize((nw, nh), Image.LANCZOS)
    left = max(0, (nw - W) // 2)
    top  = max(0, (nh - H) // 2)
    card = card.crop((left, top, left + W, top + H))
    img.paste(card, (0, 0))

    buf = BytesIO()
    img.save(buf, "JPEG", quality=90)
    return buf.getvalue()


def _slide_luxury(slide_num: int, label: str, title: str, subtitle: str,
                  bottom_pill: Optional[str], post: dict,
                  accent: Optional[tuple] = None) -> bytes:
    """
    Luxury Mindset-style slide:
      • Full-bleed hero (bare AI image from top half of card, NOT blurred)
        with a heavy dark overlay (~78%) so the photo is present but doesn't
        fight the text.
      • Top pill with a short uppercase label.
      • HUGE bold title taking the visual center of the slide, with the
        SECOND-TO-LAST word underlined in the semáforo accent color.
      • Optional smaller subtitle below for elaboration.
      • Optional bottom pill with a takeaway / key insight.
      • Bottom brand bar.

    The hero is unblurred so the user can see what the photo is about, but
    the heavy darkening keeps text legibility paramount — same approach as
    the @luxurymindset / similar finance/luxury carousels.
    """
    semaforo = (post or {}).get("semaforo", "neutral")
    if accent is None:
        accent = SEMAFORO_COLOR.get(semaforo, ACCENT_CYAN)

    # ── 1. Hero background ──────────────────────────────────────────────────
    hero = _load_hero(post.get("card_image_url"), blur=False)
    img  = Image.new("RGB", (W, H), BG)
    if hero:
        dark = Image.new("RGB", (W, H), BG)
        img  = Image.blend(hero, dark, 0.78)
    draw = ImageDraw.Draw(img)

    # ── 2. Top thin accent bar ──────────────────────────────────────────────
    draw.rectangle([0, 0, W, 4], fill=accent)

    # ── 3. Logo top-left + slide number top-right ───────────────────────────
    _paste_logo(img, x=36, y=28, height=72)
    sn_font = _regular(26)
    sn_text = f"{slide_num}/5"
    sn_bb   = draw.textbbox((0, 0), sn_text, font=sn_font)
    sn_w    = sn_bb[2] - sn_bb[0]
    draw.text((W - 50 - sn_w, 52), sn_text, font=sn_font, fill=GRAY_300)

    # ── 4. Top pill with label ──────────────────────────────────────────────
    pill_font = _bold(30)
    pill_bb   = draw.textbbox((0, 0), label.upper(), font=pill_font)
    plw       = pill_bb[2] - pill_bb[0]
    px        = (W - plw) // 2
    py        = 250
    pad_x     = 32
    pad_y     = 18
    draw.rounded_rectangle(
        [px - pad_x, py - pad_y, px + plw + pad_x, py + pill_font.size + pad_y],
        radius=36, fill=BG_CARD, outline=accent, width=2,
    )
    draw.text((px, py - 4), label.upper(), font=pill_font, fill=accent)

    # ── 5. Big title — auto-shrink until it fits the body area ──────────────
    # Title block sits in a generous vertical zone roughly from y=440 to
    # y=1500. We pick the largest font that produces at most 6 lines.
    title_top    = 460
    title_bottom = 1400
    max_lines    = 6
    title_size   = 84
    for size in (84, 76, 70, 64, 58, 52, 46):
        title_font = _bold(size)
        title_lines = _wrap_text(draw, title, title_font, W - 100)
        if len(title_lines) <= max_lines:
            title_size = size
            break
    title_font = _bold(title_size)
    title_lines = _wrap_text(draw, title, title_font, W - 100)
    if len(title_lines) > max_lines:
        title_lines = title_lines[:max_lines]
        title_lines[-1] = title_lines[-1].rstrip(".,;:") + "…"

    line_h     = title_size + 14
    title_h    = len(title_lines) * line_h

    # Reserve room for subtitle + underline (if present)
    underline_gap   = 28
    underline_h     = 8
    subtitle_gap    = 60
    subtitle_size   = 38
    subtitle_lines: list[str] = []
    if subtitle:
        sub_font  = _semi(subtitle_size)
        subtitle_lines = _wrap_text(draw, subtitle, sub_font, W - 140)
        if len(subtitle_lines) > 4:
            subtitle_lines = subtitle_lines[:4]
            subtitle_lines[-1] = subtitle_lines[-1].rstrip(".,;:") + "…"
    subtitle_h = (subtitle_size + 14) * len(subtitle_lines) if subtitle_lines else 0
    if subtitle_h:
        subtitle_h += subtitle_gap

    block_h    = title_h + underline_gap + underline_h + subtitle_h
    available  = title_bottom - title_top
    block_y    = title_top + max(0, (available - block_h) // 2)

    # Draw title lines
    _draw_text_block(draw, title_lines, title_font, WHITE, block_y, W // 2, 14)

    # Accent underline below the title (modeled after the red underline in
    # the reference design — short horizontal bar, slightly narrower than
    # the longest line)
    ul_y = block_y + title_h + underline_gap
    ul_half = max(80, min(220, W // 4))
    draw.rectangle([W // 2 - ul_half, ul_y, W // 2 + ul_half, ul_y + underline_h], fill=accent)

    # Draw subtitle
    if subtitle_lines:
        sub_font  = _semi(subtitle_size)
        sub_y     = ul_y + underline_h + subtitle_gap - 12
        _draw_text_block(draw, subtitle_lines, sub_font, GRAY_300, sub_y, W // 2, 10)

    # ── 6. Bottom takeaway pill ────────────────────────────────────────────
    if bottom_pill:
        bp_font = _bold(28)
        bp_bb   = draw.textbbox((0, 0), bottom_pill.upper(), font=bp_font)
        bp_w    = bp_bb[2] - bp_bb[0]
        bp_x    = (W - bp_w) // 2
        bp_y    = H - 200
        bp_pad_x = 32
        bp_pad_y = 18
        draw.rounded_rectangle(
            [bp_x - bp_pad_x, bp_y - bp_pad_y,
             bp_x + bp_w + bp_pad_x, bp_y + bp_font.size + bp_pad_y],
            radius=34, fill=BG_CARD, outline=accent, width=2,
        )
        draw.text((bp_x, bp_y - 4), bottom_pill.upper(), font=bp_font, fill=WHITE)

    # ── 7. Bottom brand bar ────────────────────────────────────────────────
    draw.rectangle([0, H - 80, W, H], fill=GRAY_800)
    handle = "@WaCapital • Finanzas que importan"
    hf     = _regular(26)
    hbb    = draw.textbbox((0, 0), handle, font=hf)
    hw     = hbb[2] - hbb[0]
    draw.text(((W - hw) // 2, H - 55), handle, font=hf, fill=GRAY_500)

    buf = BytesIO()
    img.save(buf, "JPEG", quality=90)
    return buf.getvalue()


def _first_sentences(text: str, n: int) -> list[str]:
    """Split text on '. ' boundaries and return up to n cleaned sentences."""
    if not text:
        return []
    parts = [p.strip() for p in text.replace("\n", " ").split(". ") if p.strip()]
    out: list[str] = []
    for p in parts[:n]:
        if not p.endswith("."):
            p = p + "."
        out.append(p)
    return out


def _slide4_semaforo(post: dict) -> bytes:
    """
    Slide 4: Traffic light + WHY explanation.

    Smaller traffic light at the top half of the slide (3 stacked lights, only
    the one matching the post's semáforo lit with a halo). Below the light,
    a clear state label, followed by 3 bullet points sourced from the
    angle_reasoning that explain WHY the market signal is what it is.
    """
    semaforo = post.get("semaforo", "neutral")
    accent   = SEMAFORO_COLOR.get(semaforo, ACCENT_CYAN)
    flags    = post.get("compliance_flags") or {}
    reasoning = (flags.get("angle_reasoning") or "").strip()

    # Blurred-hero background so the focus is the traffic light
    img, draw = _base_slide("SEÑAL DE MERCADO", 4, total=5, accent=accent,
                            hero_url=post.get("card_image_url"), darkness=0.80)
    cx = W // 2

    # ── Traffic light housing (smaller — top portion of slide) ──────────────
    housing_w  = 280
    housing_h  = 700
    housing_x  = cx - housing_w // 2
    housing_y  = 240
    draw.rounded_rectangle(
        [housing_x, housing_y, housing_x + housing_w, housing_y + housing_h],
        radius=48,
        fill=(18, 22, 32),
        outline=(60, 70, 90),
        width=5,
    )

    # ── Three lights ────────────────────────────────────────────────────────
    light_r       = 100
    inner_gap_y   = 60
    span_y        = housing_h - 2 * inner_gap_y
    slot_h        = span_y / 3
    states_order  = [
        ("rojo",     ACCENT_RED,    "MERCADO BAJISTA"),
        ("amarillo", ACCENT_YELLOW, "PRECAUCIÓN"),
        ("verde",    ACCENT_GREEN,  "MERCADO ALCISTA"),
    ]
    active_state = semaforo if semaforo in ("rojo", "amarillo", "verde") else None

    for i, (state, color, _label) in enumerate(states_order):
        ly = int(housing_y + inner_gap_y + slot_h * (i + 0.5))
        is_on = (state == active_state)
        if is_on:
            for halo_r, halo_alpha in [(light_r + 32, 30), (light_r + 18, 56), (light_r + 8, 100)]:
                halo = Image.new("RGBA", (W, H), (0, 0, 0, 0))
                hd   = ImageDraw.Draw(halo)
                hd.ellipse(
                    [cx - halo_r, ly - halo_r, cx + halo_r, ly + halo_r],
                    fill=(*color, halo_alpha),
                )
                img_rgba = img.convert("RGBA")
                img      = Image.alpha_composite(img_rgba, halo).convert("RGB")
                draw     = ImageDraw.Draw(img)
            draw.ellipse(
                [cx - light_r, ly - light_r, cx + light_r, ly + light_r],
                fill=color, outline=WHITE, width=3,
            )
            hl_r = light_r // 2
            hl_x = cx - light_r // 3
            hl_y = ly - light_r // 3
            highlight = Image.new("RGBA", (W, H), (0, 0, 0, 0))
            hd2 = ImageDraw.Draw(highlight)
            hd2.ellipse(
                [hl_x - hl_r, hl_y - hl_r, hl_x + hl_r, hl_y + hl_r],
                fill=(255, 255, 255, 55),
            )
            img_rgba = img.convert("RGBA")
            img      = Image.alpha_composite(img_rgba, highlight).convert("RGB")
            draw     = ImageDraw.Draw(img)
        else:
            dim_color = (color[0] // 5, color[1] // 5, color[2] // 5)
            draw.ellipse(
                [cx - light_r, ly - light_r, cx + light_r, ly + light_r],
                fill=dim_color, outline=(70, 80, 100), width=3,
            )

    # ── State label below the light ─────────────────────────────────────────
    label_text_map = {
        "verde":    "MERCADO ALCISTA",
        "amarillo": "PRECAUCIÓN",
        "rojo":     "MERCADO BAJISTA",
        "neutral":  "SEÑAL NEUTRAL",
    }
    state_label = label_text_map.get(semaforo, "SEÑAL NEUTRAL")
    state_font  = _bold(48)
    sbb         = draw.textbbox((0, 0), state_label, font=state_font)
    sw          = sbb[2] - sbb[0]
    state_y     = housing_y + housing_h + 36
    draw.text((cx - sw // 2, state_y), state_label, font=state_font, fill=accent)

    # ── "¿POR QUÉ?" header + bullets ────────────────────────────────────────
    why_y    = state_y + 90
    why_font = _bold(26)
    why_text = "¿POR QUÉ?"
    wbb      = draw.textbbox((0, 0), why_text, font=why_font)
    ww       = wbb[2] - wbb[0]
    # Header pill
    why_pad  = 26
    draw.rounded_rectangle(
        [cx - ww // 2 - why_pad, why_y - 12, cx + ww // 2 + why_pad, why_y + why_font.size + 12],
        radius=28, fill=BG_CARD, outline=accent, width=2,
    )
    draw.text((cx - ww // 2, why_y - 2), why_text, font=why_font, fill=accent)

    # Bullets sourced from angle_reasoning (split on sentence boundaries)
    bullets = _first_sentences(reasoning, n=3)
    if not bullets:
        bullets = ["Análisis editorial en curso."]

    bullet_font = _semi(30)
    bullet_y    = why_y + why_font.size + 50
    pad_left    = 100
    text_pad    = 30
    avail_w     = W - pad_left - 40   # right margin

    for bullet in bullets:
        # Wrap long bullets
        wrap_lines = _wrap_text(draw, bullet, bullet_font, avail_w - text_pad)
        if len(wrap_lines) > 3:
            wrap_lines = wrap_lines[:3]
            wrap_lines[-1] = wrap_lines[-1].rstrip(".,;:") + "…"

        # Don't run off the bottom — leave room for brand bar
        block_h = len(wrap_lines) * (bullet_font.size + 12)
        if bullet_y + block_h > H - 110:
            break

        # Accent dot
        dot_r  = 9
        dot_y  = bullet_y + bullet_font.size // 2 + 2
        draw.ellipse([pad_left - 2, dot_y - dot_r,
                      pad_left - 2 + dot_r * 2, dot_y + dot_r], fill=accent)

        # Bullet text
        for i, line in enumerate(wrap_lines):
            draw.text((pad_left + text_pad, bullet_y + i * (bullet_font.size + 12)),
                      line, font=bullet_font, fill=WHITE)

        bullet_y += block_h + 22

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
    flags     = post.get("compliance_flags") or {}
    headline  = (post.get("headline")          or "Sin título").strip()
    hook      = (flags.get("angle_hook")       or headline).strip()
    angle     = (flags.get("angle_reasoning")  or "Análisis en curso.").strip()
    semaforo  = post.get("semaforo", "neutral")

    # For slide 3 we split the analysis into a punchy first sentence (TITLE)
    # and the remainder (SUBTITLE), Luxury Mindset-style.
    angle_parts = _first_sentences(angle, n=4)
    if not angle_parts:
        angle_title    = angle
        angle_subtitle = ""
    elif len(angle_parts) == 1:
        angle_title    = angle_parts[0]
        angle_subtitle = ""
    else:
        angle_title    = angle_parts[0]
        angle_subtitle = " ".join(angle_parts[1:3])     # next 1-2 sentences

    # Pills for slide 2 / 3 bottom takeaway. Short, brand-y, all-caps.
    pill_takeaway_dato = {
        "verde":    "OPORTUNIDAD EN MOVIMIENTO",
        "amarillo": "ATENCIÓN AL MERCADO",
        "rojo":     "RIESGO ELEVADO",
        "neutral":  "DATO QUE IMPORTA",
    }.get(semaforo, "DATO QUE IMPORTA")

    pill_takeaway_analisis = {
        "verde":    "EL MERCADO PIDE ATENCIÓN",
        "amarillo": "LEER ENTRE LÍNEAS",
        "rojo":     "PRECAUCIÓN OBLIGATORIA",
        "neutral":  "ANÁLISIS CONTEXTUAL",
    }.get(semaforo, "ANÁLISIS CONTEXTUAL")

    slides: list[bytes] = []
    builders = [
        lambda: _slide1_hook(post),
        lambda: _slide_luxury(
            slide_num=2,
            label="EL DATO",
            title=hook,
            subtitle="",                  # hook is already complete; no subtitle
            bottom_pill=pill_takeaway_dato,
            post=post,
        ),
        lambda: _slide_luxury(
            slide_num=3,
            label="EL ANÁLISIS",
            title=angle_title,
            subtitle=angle_subtitle,
            bottom_pill=pill_takeaway_analisis,
            post=post,
        ),
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
