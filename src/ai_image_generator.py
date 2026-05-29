"""
AI image generator for Tier-1 dramatic cards (CryptoAlpha-style).

Provider strategy:
  1. Google Imagen 3 (imagen-3.0-generate-002) — top-tier quality.
     Tries each GEMINI_API_KEY{,_2,_3,_4,_5} in order; on 429/403 it marks
     that key as exhausted for an hour and rolls to the next one.
  2. Pollinations.ai (Flux model) — zero-auth public endpoint, used as a
     final fallback when ALL Gemini keys are on cooldown.

Both providers return PNG bytes. Cache is keyed by sha256(prompt+seed) so
different seeds always produce different images even for the same prompt.

Image is generated at 9:16 (Imagen) or 1080x1350 (Pollinations) for portrait
feed (Instagram, TikTok). The card_generator composes text overlay on top.

Failure mode: returns None when both providers fail. The card generator falls
back to Tier-2 (logo card) when this returns None.
"""
import base64
import hashlib
import logging
import os
import random
import time
from io import BytesIO
from typing import Optional
from urllib.parse import quote

import requests
from PIL import Image

log = logging.getLogger("ai-image")

CACHE_DIR    = "/tmp/wacapital_ai_images"
HTTP_TIMEOUT = 120
PROMPT_MAX   = 480

KEY_COOLDOWN_SEC = 3600

# Imagen 3 / Imagen 4 are NOT in Google AI Studio free tier (validated via
# probe scripts: GA endpoints return 404, paid models return 429 with
# `limit: 0`). Until we move to a paid plan or Google opens free image
# generation, skip the entire Imagen pre-stage so we don't waste a request
# per cycle and clutter logs with 5 cooldown lines. Flip to True if access
# changes — code path is preserved intact below.
_IMAGEN_ENABLED = False

try:
    os.makedirs(CACHE_DIR, exist_ok=True)
except Exception as e:
    log.warning("could not create cache dir %s: %s", CACHE_DIR, e)


# ─── Key rotation state ─────────────────────────────────────────────────────

_key_cooldowns: dict[str, float] = {}


def _gemini_keys() -> list[str]:
    out: list[str] = []
    for var in ("GEMINI_API_KEY", "GEMINI_API_KEY_2", "GEMINI_API_KEY_3",
                "GEMINI_API_KEY_4", "GEMINI_API_KEY_5"):
        v = (os.getenv(var) or "").strip()
        if v:
            out.append(v)
    return out


def _key_label(key: str) -> str:
    return f"...{key[-6:]}" if len(key) > 6 else "?"


def _key_on_cooldown(key: str) -> bool:
    until = _key_cooldowns.get(_key_label(key), 0.0)
    return time.time() < until


def _mark_key_exhausted(key: str) -> None:
    _key_cooldowns[_key_label(key)] = time.time() + KEY_COOLDOWN_SEC


# ─── Cache ──────────────────────────────────────────────────────────────────

def _cache_path(prompt: str, seed: int) -> str:
    h = hashlib.sha256(f"{prompt}|{seed}".encode("utf-8")).hexdigest()
    return os.path.join(CACHE_DIR, f"{h}.png")


# ─── Provider: Google Imagen 3 ──────────────────────────────────────────────

def _imagen3(prompt: str, key: str) -> bytes:
    url = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        f"imagen-3.0-generate-002:predict?key={key}"
    )
    body = {
        "instances": [{"prompt": prompt[:PROMPT_MAX]}],
        "parameters": {
            "sampleCount": 1,
            "aspectRatio": "3:4",
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

def _pollinations(prompt: str, seed: int) -> bytes:
    """Pollinations GET with explicit seed so every post gets a unique image
    even when the prompt is identical (fixes the server-room repetition bug).

    Model: flux-realism — photorealistic editorial output, much better for
    finance content than `turbo` (which was fast but flat). Validated against
    the Trump+flag editorial test image: faithful subjects, dramatic lighting,
    no text artifacts.
    """
    safe = quote(prompt[:PROMPT_MAX])
    url = (
        f"https://image.pollinations.ai/prompt/{safe}"
        f"?width=1080&height=1350&model=flux-realism&nologo=true&seed={seed}"
    )
    resp = requests.get(url, timeout=HTTP_TIMEOUT, headers={
        "User-Agent": "WaCapital-PulseEngine/1.0",
    })
    resp.raise_for_status()
    return resp.content


# ─── Public entry point ─────────────────────────────────────────────────────

def generate(prompt: str, *, try_imagen: bool = True, seed: Optional[int] = None) -> Optional[Image.Image]:
    """
    Generate a portrait image from `prompt`. Returns PIL.Image (RGB) or None.

    seed: pass candidate_id or any int to force a unique image per post.
          If None, a random seed is chosen automatically.
    """
    if not prompt or not prompt.strip():
        return None

    if seed is None:
        seed = random.randint(1, 999_999)

    path = _cache_path(prompt, seed)
    if os.path.exists(path):
        try:
            return Image.open(path).convert("RGB")
        except Exception:
            try:
                os.remove(path)
            except Exception:
                pass

    if try_imagen and _IMAGEN_ENABLED:
        for key in _gemini_keys():
            if _key_on_cooldown(key):
                log.debug("[ai-image] skipping key %s (on cooldown)", _key_label(key))
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
                body_snippet = "?"
                if e.response is not None:
                    try:
                        body_snippet = e.response.text[:300]
                    except Exception:
                        body_snippet = "<unreadable>"
                if status in (429, 403):
                    _mark_key_exhausted(key)
                    log.warning("[ai-image] Imagen 3 quota/access on key %s (status=%s): %s",
                                _key_label(key), status, body_snippet)
                    continue
                if status == 404:
                    _mark_key_exhausted(key)
                    log.warning("[ai-image] Imagen 3 NOT available for key %s (status=404). Body: %s",
                                _key_label(key), body_snippet)
                    continue
                log.warning("[ai-image] Imagen 3 failed on key %s: status=%s body=%s",
                            _key_label(key), status, body_snippet)
                continue
            except Exception as e:
                log.warning("[ai-image] Imagen 3 error on key %s: %s", _key_label(key), e)
                continue

    try:
        if try_imagen:
            log.info("[ai-image] falling back to Pollinations (Flux) seed=%d", seed)
        else:
            log.info("[ai-image] using Pollinations (Flux) seed=%d — Imagen skipped", seed)
        png = _pollinations(prompt, seed)
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

# Base style tail — used for strength < 4 posts (most of the volume).
# Tuned for Pollinations `flux-realism`: lens spec, lighting, color grading,
# magazine reference. Anti-text rules kept identical (these are non-negotiable
# because the text overlay is composed afterwards in Pillow).
_STYLE_TAIL = (
    "editorial photography, shot on 85mm lens, dramatic rim lighting, "
    "photorealistic, cinematic color grading, deep contrast, sharp detail, "
    "vertical 1080x1350 composition, professional finance magazine aesthetic, "
    "ABSOLUTELY no text, no letters, no readable writing, no captions, no labels, "
    "no logos with text, no signs, no numbers, no watermark"
)

# Rich style tail — reserved for strength >= 4 posts (the 4-5 star news that
# define the brand). Heavier descriptors trade ~3-5s of extra generation time
# for noticeably better composition. Magazine references chosen to anchor the
# model on a recognizable editorial style.
_STYLE_TAIL_RICH = (
    "award-winning editorial photography, shot on 85mm lens at f/1.4, "
    "dramatic rim lighting with deep shadows, photorealistic ultra-detailed, "
    "cinematic color grading, rich contrast, magazine cover composition, "
    "vertical 1080x1350 framing, Bloomberg Businessweek / Vanity Fair aesthetic, "
    "ABSOLUTELY no text, no letters, no readable writing, no captions, no labels, "
    "no logos with text, no signs, no numbers, no watermark"
)

# ─── Dual-subject catalog (~250 entries) ────────────────────────────────────
# Each entry: (regex_pattern, category, english_visual_description)
import re as _re

_SUBJECTS = [
    # ══ POLITICIANS & PUBLIC FIGURES ══════════════════════════════════════════
    (_re.compile(r'\btrump\b', _re.I),
        'person', 'Donald Trump, intense expression, dark suit, power pose, American flag'),
    (_re.compile(r'\bpowell\b', _re.I),
        'person', 'Jerome Powell, stern expression, Federal Reserve formal attire'),
    (_re.compile(r'\bbiden\b', _re.I),
        'person', 'Joe Biden, formal presidential portrait, American flag background'),
    (_re.compile(r'\byellen\b', _re.I),
        'person', 'Janet Yellen, Treasury Secretary, formal portrait, Washington DC'),
    (_re.compile(r'\bbessent\b', _re.I),
        'person', 'Scott Bessent, US Treasury Secretary, formal suit, financial backdrop'),
    (_re.compile(r'\blagarde\b', _re.I),
        'person', 'Christine Lagarde, ECB president, elegant formal attire'),
    (_re.compile(r'\bdraghi\b', _re.I),
        'person', 'Mario Draghi, former ECB president, formal European setting'),
    (_re.compile(r'\bmusk\b', _re.I),
        'person', 'Elon Musk, tech visionary, intense focused look, futuristic backdrop'),
    (_re.compile(r'\bxi\s*jinping\b', _re.I),
        'person', 'Xi Jinping, Chinese president, formal portrait, red background'),
    (_re.compile(r'\brutte\b', _re.I),
        'person', 'Mark Rutte, NATO Secretary General, formal diplomatic attire'),
    (_re.compile(r'\bzelensky\b|zelenskyy\b', _re.I),
        'person', 'Volodymyr Zelensky, military olive uniform, determined expression'),
    (_re.compile(r'\bmacron\b', _re.I),
        'person', 'Emmanuel Macron, French president, formal suit, Élysée Palace'),
    (_re.compile(r'\bscholz\b', _re.I),
        'person', 'Olaf Scholz, German chancellor, formal attire, European backdrop'),
    (_re.compile(r'\bmilei\b', _re.I),
        'person', 'Javier Milei, Argentine president, passionate expression, chainsaw motif'),
    (_re.compile(r'\blula\b', _re.I),
        'person', 'Luiz Inácio Lula da Silva, Brazilian president, formal portrait'),
    (_re.compile(r'\bpetro\b', _re.I),
        'person', 'Gustavo Petro, Colombian president, formal portrait'),
    (_re.compile(r'\bputin\b', _re.I),
        'person', 'Vladimir Putin, Russian president, cold intense gaze, Kremlin backdrop'),
    (_re.compile(r'\bnetanyahu\b', _re.I),
        'person', 'Benjamin Netanyahu, Israeli PM, formal suit, Israeli flag backdrop'),
    (_re.compile(r'\bmodi\b|\bnarendra\b', _re.I),
        'person', 'Narendra Modi, Indian PM, traditional kurta, vibrant backdrop'),
    (_re.compile(r'\berdogan\b|\berdoğan\b', _re.I),
        'person', 'Recep Tayyip Erdogan, Turkish president, formal portrait, Ottoman motifs'),
    (_re.compile(r'\bbukele\b', _re.I),
        'person', 'Nayib Bukele, El Salvador president, casual hoodie, Bitcoin symbol'),
    (_re.compile(r'\bbuffett\b|\bwarren\s*buffett\b', _re.I),
        'person', 'Warren Buffett, legendary investor, warm smile, Berkshire boardroom'),
    (_re.compile(r'\bdimon\b|\bjamie\s*dimon\b', _re.I),
        'person', 'Jamie Dimon, JPMorgan CEO, confident executive portrait'),
    (_re.compile(r'\baltman\b|\bsam\s*altman\b', _re.I),
        'person', 'Sam Altman, OpenAI CEO, tech leader, futuristic AI backdrop'),
    (_re.compile(r'\bzuckerberg\b', _re.I),
        'person', 'Mark Zuckerberg, Meta CEO, casual tech style, VR metaverse backdrop'),
    (_re.compile(r'\bjensen\s*huang\b|\bjensen\b|\bnvidia.*ceo\b', _re.I),
        'person', 'Jensen Huang, Nvidia CEO, signature leather jacket, GPU chip backdrop'),
    (_re.compile(r'\btim\s*cook\b', _re.I),
        'person', 'Tim Cook, Apple CEO, minimalist style, Apple Park backdrop'),
    (_re.compile(r'\bsatya\s*nadella\b|\bnadella\b', _re.I),
        'person', 'Satya Nadella, Microsoft CEO, formal portrait, cloud tech backdrop'),
    (_re.compile(r'\bsundar\s*pichai\b|\bpichai\b', _re.I),
        'person', 'Sundar Pichai, Google CEO, formal portrait, colorful Google campus'),
    (_re.compile(r'\bbezos\b', _re.I),
        'person', 'Jeff Bezos, Amazon founder, intense gaze, rocket and warehouse backdrop'),
    (_re.compile(r'\bgates\b|\bbill\s*gates\b', _re.I),
        'person', 'Bill Gates, philanthropist, glasses, global health and tech backdrop'),
    (_re.compile(r'\bsoros\b', _re.I),
        'person', 'George Soros, financier, formal portrait, global markets backdrop'),
    (_re.compile(r'\bdalio\b|\bray\s*dalio\b', _re.I),
        'person', 'Ray Dalio, Bridgewater founder, calm wisdom, global macro backdrop'),
    (_re.compile(r'\bcathie\s*wood\b|\bark\s*invest\b', _re.I),
        'person', 'Cathie Wood, ARK Invest, visionary portrait, disruptive tech backdrop'),
    (_re.compile(r'\bmichael\s*saylor\b|\bsaylor\b', _re.I),
        'person', 'Michael Saylor, MicroStrategy, intense conviction, Bitcoin backdrop'),
    (_re.compile(r'\bvitalik\b|\bbuterin\b', _re.I),
        'person', 'Vitalik Buterin, Ethereum creator, casual intellectual, blockchain visualization'),
    (_re.compile(r'\bcz\b|\bchangpeng\b|\bzhao\b', _re.I),
        'person', 'CZ Changpeng Zhao, Binance founder, casual portrait, crypto exchange backdrop'),
    (_re.compile(r'\bfink\b|\blarry\s*fink\b', _re.I),
        'person', 'Larry Fink, BlackRock CEO, authoritative portrait, global finance backdrop'),
    (_re.compile(r'\bpellegrini\b', _re.I),
        'person', 'Robert Fico or Slovak leader, formal European political portrait'),
    (_re.compile(r'\borban\b|\borbán\b', _re.I),
        'person', 'Viktor Orbán, Hungarian PM, strong nationalist portrait, EU backdrop'),

    # ══ COUNTRIES & REGIONS ════════════════════════════════════════════════════
    (_re.compile(r'\beuropa\b|\bue\b|\beurope\b|\beuropeos?\b|\beuropean\b|\beurop[aeo]\b', _re.I),
        'place', 'European Union flag, golden stars circle on deep blue'),
    (_re.compile(r'\bee\.uu\.|\busa\b|\bamerica\b|\bamerican\b|\bunited\s*states\b|\bestados\s*unidos\b', _re.I),
        'place', 'American flag, stars and stripes, Capitol Building backdrop'),
    (_re.compile(r'\bchina\b|\bchinese\b|\bchino\b', _re.I),
        'place', 'China, Great Wall silhouette, red and gold tones, Beijing skyline at night'),
    (_re.compile(r'\bjap[oó]n\b|\bjapan\b|\bjapanese\b', _re.I),
        'place', 'Japan, Mount Fuji with cherry blossoms, rising sun, Tokyo skyline'),
    (_re.compile(r'\brussia\b|\brussian\b|\brusia\b', _re.I),
        'place', 'Russia, Kremlin towers, Red Square, cold dramatic atmosphere'),
    (_re.compile(r'\barabia\s*saudita\b|\bsaudi\b|\bopec\b', _re.I),
        'place', 'Saudi Arabia, desert oil fields, gleaming golden skyline, OPEC headquarters'),
    (_re.compile(r'\bir[aá]n\b|\birani\b|\biranian\b', _re.I),
        'place', 'Iran, Persian architecture, nuclear facility cooling towers, desert landscape'),
    (_re.compile(r'\bindia\b|\bindian\b', _re.I),
        'place', 'India, Taj Mahal silhouette, vibrant colors, Mumbai financial district'),
    (_re.compile(r'\bm[eé]xico\b|\bmexican\b|\bmexicano\b', _re.I),
        'place', 'Mexico, Aztec pyramid silhouette, vibrant market colors, Mexico City skyline'),
    (_re.compile(r'\bargentina\b|\bargentino\b', _re.I),
        'place', 'Argentina, Buenos Aires skyline, dramatic pampas landscape, blue and white flag'),
    (_re.compile(r'\bbrasil\b|\bbrazil\b|\bbrasile[ñn]o\b|\bbrazilian\b', _re.I),
        'place', 'Brazil, Rio de Janeiro Christ the Redeemer, financial district, green-yellow flag'),
    (_re.compile(r'\buk\b|\bbritain\b|\bingla?terra\b|\bbritish\b', _re.I),
        'place', 'United Kingdom, Big Ben, London financial district Canary Wharf, British flag'),
    (_re.compile(r'\baleman[ia]?\b|\bgerman[y]?\b|\bdeutsch', _re.I),
        'place', 'Germany, Frankfurt skyline, industrial precision, black-red-gold flag'),
    (_re.compile(r'\bfranci?a?\b|\bfrench\b', _re.I),
        'place', 'France, Paris skyline, Eiffel Tower, elegant blue-white-red tricolor'),
    (_re.compile(r'\bcorea\b|\bkorea\b|\bkorean\b', _re.I),
        'place', 'South Korea, Seoul modern skyline, technology and finance hub, K-pop neon'),
    (_re.compile(r'\btaiwan\b|\btaiwanese\b', _re.I),
        'place', 'Taiwan, Taipei 101 tower, semiconductor factory, dramatic mountain backdrop'),
    (_re.compile(r'\bcanad[áa]\b|\bcanadian\b|\bcanadiense\b', _re.I),
        'place', 'Canada, Toronto skyline, maple leaf flag, oil sands landscape'),
    (_re.compile(r'\baustralia\b|\baustralian\b', _re.I),
        'place', 'Australia, Sydney Opera House, mining industry, dramatic outback'),
    (_re.compile(r'\bsuiza\b|\bswitzerland\b|\bswiss\b', _re.I),
        'place', 'Switzerland, Alps mountain backdrop, Zurich financial district, Swiss flag'),
    (_re.compile(r'\bsingapur\b|\bsingapore\b|\bsingaporean\b', _re.I),
        'place', 'Singapore, Marina Bay Sands, futuristic skyline, financial hub neon'),
    (_re.compile(r'\bemiratos?\b|\buae\b|\bdubai\b|\babu\s*dhabi\b', _re.I),
        'place', 'Dubai skyline, Burj Khalifa, gold and glass towers, desert sunset'),
    (_re.compile(r'\bhong\s*kong\b', _re.I),
        'place', 'Hong Kong skyline, Victoria Harbour, dense neon towers, financial hub'),
    (_re.compile(r'\bturqu[ií]a\b|\bturkey\b|\bturkish\b|\bt[uú]nez\b', _re.I),
        'place', 'Turkey, Istanbul Bosphorus bridge, minarets, East meets West skyline'),
    (_re.compile(r'\bpoland\b|\bpolonia\b|\bpolish\b', _re.I),
        'place', 'Poland, Warsaw financial district, European growth, Polish flag'),
    (_re.compile(r'\bsudáfrica\b|\bsouth\s*africa\b|\bsudafricano\b|\bsarb\b', _re.I),
        'place', 'South Africa, Johannesburg skyline, gold mine, Table Mountain backdrop'),
    (_re.compile(r'\bnigeria\b|\bnigeriano\b|\bafrica\b|\bafricano\b', _re.I),
        'place', 'Africa, Lagos skyline, natural resources, continent silhouette at dusk'),
    (_re.compile(r'\bchile\b|\bchileno\b', _re.I),
        'place', 'Chile, Andes mountains, copper mine, Santiago modern skyline'),
    (_re.compile(r'\bcolom?bia\b|\bcolombian\b|\bcolombiano\b', _re.I),
        'place', 'Colombia, Bogotá skyline, coffee plantation, Andes mountains'),
    (_re.compile(r'\bper[uú]\b|\bperuano\b|\bperuvian\b', _re.I),
        'place', 'Peru, Machu Picchu, mining industry, Lima financial district'),
    (_re.compile(r'\bvenezuela\b|\bvenezolano\b', _re.I),
        'place', 'Venezuela, oil refinery, dramatic economic backdrop, Caracas skyline'),
    (_re.compile(r'\bvietnam\b|\bvietnamese\b', _re.I),
        'place', 'Vietnam, Ho Chi Minh City skyline, manufacturing hub, rapid growth'),
    (_re.compile(r'\bpakist[aá]n\b|\bpakistani\b', _re.I),
        'place', 'Pakistan, Karachi financial district, dramatic mountainous backdrop'),
    (_re.compile(r'\bespa[ñn]a\b|\bspain\b|\bspanish\b|\bespañol\b', _re.I),
        'place', 'Spain, Madrid financial district, Sagrada Familia, red-yellow flag'),
    (_re.compile(r'\bital[ia]\b|\bitalian\b|\bitaliano\b', _re.I),
        'place', 'Italy, Milan financial district, Colosseum backdrop, tricolor flag'),
    (_re.compile(r'\bpaises\s*bajos\b|\bnetherlands\b|\bdutch\b|\bholanda\b', _re.I),
        'place', 'Netherlands, Amsterdam canals, Port of Rotterdam, tulip fields at dusk'),
    (_re.compile(r'\bsuecia\b|\bsweden\b|\bswedish\b|\bnorway\b|\bnoruega\b', _re.I),
        'place', 'Scandinavia, Nordic financial hub, clean energy landscape, Nordic flags'),
    (_re.compile(r'\bucrania\b|\bukraine\b|\bukrainian\b', _re.I),
        'place', 'Ukraine, war-torn landscape, resilient Kyiv skyline, yellow-blue flag'),

    # ══ STRATEGIC LOCATIONS & INSTITUTIONS ════════════════════════════════════
    (_re.compile(r'\bhormuz\b', _re.I),
        'place', 'Strait of Hormuz aerial view, oil tankers on blue water, narrow rocky passage'),
    (_re.compile(r'\bpanama\b|\bcanal\b', _re.I),
        'place', 'Panama Canal, massive cargo ships, lock system, aerial view'),
    (_re.compile(r'\bsuez\b', _re.I),
        'place', 'Suez Canal, container ships queue, Egyptian desert, aerial view'),
    (_re.compile(r'\bmar\s*rojo\b|\bred\s*sea\b|\bhouthi\b', _re.I),
        'place', 'Red Sea, cargo ships under threat, dramatic naval scene, Yemen coastline'),
    (_re.compile(r'\bmar\s*del\s*sur\b|\bsouth\s*china\s*sea\b', _re.I),
        'place', 'South China Sea, naval vessels, disputed islands, tense aerial view'),
    (_re.compile(r'\bwall\s*street\b', _re.I),
        'place', 'Wall Street, NYSE facade, American flags, financial district hustle'),
    (_re.compile(r'\bnasdaq\b', _re.I),
        'place', 'Nasdaq MarketSite Times Square, glowing digital screens, New York night'),
    (_re.compile(r'\bfederal\s*reserve\b|\bfed\b', _re.I),
        'place', 'Federal Reserve building, neoclassical marble facade, Washington DC'),
    (_re.compile(r'\bbce\b|\becb\b', _re.I),
        'place', 'European Central Bank skyscraper, Frankfurt glass towers, euro symbol'),
    (_re.compile(r'\bfmi\b|\bimf\b|\bfondo\s*monetario\b', _re.I),
        'place', 'IMF headquarters, Washington DC, global financial institution, world map'),
    (_re.compile(r'\bbanco\s*mundial\b|\bworld\s*bank\b', _re.I),
        'place', 'World Bank headquarters, international development, global cooperation'),
    (_re.compile(r'\bfomc\b|\bfed\s*meeting\b|\breunion\s*fed\b', _re.I),
        'place', 'Federal Reserve FOMC meeting room, central bankers, rate decision drama'),
    (_re.compile(r'\bg7\b|\bg20\b|\bcumbre\b|\bsummit\b', _re.I),
        'place', 'G7 G20 summit, world leaders at round table, international diplomacy'),
    (_re.compile(r'\bdavos\b|\bwef\b|\bworld\s*economic\b', _re.I),
        'place', 'Davos World Economic Forum, snowy Alps, global elite gathering'),

    # ══ CRYPTOCURRENCIES ════════════════════════════════════════════════════════
    (_re.compile(r'\bbitcoin\b|\bbtc\b|\bsatoshi\b', _re.I),
        'crypto', 'glowing golden Bitcoin coin, metallic embossed B, dramatic reflections, dark background'),
    (_re.compile(r'\bethereum\b|\beth\b', _re.I),
        'crypto', 'glowing Ethereum diamond crystal, silver-blue shimmer, futuristic'),
    (_re.compile(r'\bsolana\b|\bsol\b', _re.I),
        'crypto', 'Solana coin, iridescent purple-teal gradient glow, high-speed blockchain'),
    (_re.compile(r'\bripple\b|\bxrp\b', _re.I),
        'crypto', 'XRP Ripple coin, sleek blue metallic, global payments network'),
    (_re.compile(r'\bbinance\b|\bbnb\b', _re.I),
        'crypto', 'Binance coin BNB, golden yellow glow, crypto exchange platform'),
    (_re.compile(r'\bdogecoin\b|\bdoge\b', _re.I),
        'crypto', 'Dogecoin with Shiba Inu dog face, golden coin, viral meme energy'),
    (_re.compile(r'\bcardano\b|\bada\b', _re.I),
        'crypto', 'Cardano ADA coin, navy blue metallic, blockchain network nodes'),
    (_re.compile(r'\bavalanche\b|\bavax\b', _re.I),
        'crypto', 'Avalanche AVAX coin, red glowing, high-speed blockchain'),
    (_re.compile(r'\bpolkadot\b|\bdot\b', _re.I),
        'crypto', 'Polkadot DOT coin, colorful interconnected dots network'),
    (_re.compile(r'\bchainlink\b|\blink\b', _re.I),
        'crypto', 'Chainlink LINK coin, blue hexagonal, oracle network visualization'),
    (_re.compile(r'\btether\b|\busdt\b|\busdc\b|\bstablecoin\b', _re.I),
        'crypto', 'stablecoin dollar-pegged coin, green stable glow, digital dollar'),
    (_re.compile(r'\bshiba\b|\bshib\b', _re.I),
        'crypto', 'Shiba Inu SHIB meme coin, cute dog face, explosive pink glow'),
    (_re.compile(r'\buniswap\b|\buni\b', _re.I),
        'crypto', 'Uniswap pink unicorn logo, DeFi liquidity pools, decentralized'),
    (_re.compile(r'\baave\b|\bdefi\b|\bdescentraliz\b|\bdecentraliz\b', _re.I),
        'crypto', 'DeFi protocol visualization, decentralized finance, blockchain nodes glow'),
    (_re.compile(r'\bnear\b|\bcosmos\b|\batom\b', _re.I),
        'crypto', 'blockchain interoperability visualization, multiple chains connecting'),
    (_re.compile(r'\bcoinbase\b', _re.I),
        'company', 'Coinbase headquarters, crypto exchange platform, blue corporate'),
    (_re.compile(r'\bmicrostrategy\b', _re.I),
        'company', 'MicroStrategy corporate office, Bitcoin treasury, bold financial bet'),
    (_re.compile(r'\bethf\b|\bbitcoin\s*etf\b|\bcrypto\s*etf\b', _re.I),
        'market', 'cryptocurrency ETF launch, Wall Street meets Bitcoin, golden opportunity'),

    # ══ FINANCE COMPANIES ════════════════════════════════════════════════════════
    (_re.compile(r'\bblackrock\b', _re.I),
        'company', 'BlackRock corporate skyscraper, glass tower, financial district skyline at dusk'),
    (_re.compile(r'\bberkshire\b', _re.I),
        'company', 'Berkshire Hathaway executive boardroom, classic American corporate'),
    (_re.compile(r'\bmorgan\s*stanley\b', _re.I),
        'company', 'Morgan Stanley glass skyscraper, Times Square, Wall Street power'),
    (_re.compile(r'\bciti\b|\bcitibank\b|\bcitigroup\b', _re.I),
        'company', 'Citigroup blue corporate tower, global bank headquarters'),
    (_re.compile(r'\bubs\b', _re.I),
        'company', 'UBS bank headquarters, Zurich precision, Swiss financial excellence'),
    (_re.compile(r'\bhsbc\b', _re.I),
        'company', 'HSBC bank tower, Hong Kong skyline, global banking network'),
    (_re.compile(r'\bdeutsche\s*bank\b', _re.I),
        'company', 'Deutsche Bank twin towers Frankfurt, European finance power'),
    (_re.compile(r'\bvanguard\b', _re.I),
        'company', 'Vanguard investment funds, global portfolio ETF visualization'),
    (_re.compile(r'\bfidelity\b', _re.I),
        'company', 'Fidelity Investments corporate campus, asset management giant'),
    (_re.compile(r'\bgoldman\b|\bgoldman\s*sachs\b', _re.I),
        'company', 'Goldman Sachs glass skyscraper, Manhattan skyline, finance power'),
    (_re.compile(r'\bjpmorgan\b|\bj\.?p\.?\s*morgan\b', _re.I),
        'company', 'JPMorgan Chase headquarters, Wall Street tower, banking giant'),
    (_re.compile(r'\bwells\s*fargo\b', _re.I),
        'company', 'Wells Fargo stagecoach logo, bank headquarters, American West heritage'),
    (_re.compile(r'\bbank\s*of\s*america\b|\bbofa\b', _re.I),
        'company', 'Bank of America tower, Charlotte skyline, corporate banking'),
    (_re.compile(r'\bbarclays\b', _re.I),
        'company', 'Barclays Bank headquarters, London Canary Wharf, British finance'),
    (_re.compile(r'\bsantander\b', _re.I),
        'company', 'Santander Bank red eagle logo, European headquarters, global banking'),
    (_re.compile(r'\bbbva\b', _re.I),
        'company', 'BBVA bank headquarters, Spain and Latin America banking giant'),
    (_re.compile(r'\bpimco\b', _re.I),
        'company', 'PIMCO bond market, fixed income visualization, Newport Beach campus'),
    (_re.compile(r'\bcitadel\b', _re.I),
        'company', 'Citadel hedge fund, Chicago skyline, high-frequency trading screens'),
    (_re.compile(r'\bbridgewater\b', _re.I),
        'company', 'Bridgewater Associates, macro hedge fund, global economic visualization'),
    (_re.compile(r'\boppenheimer\b', _re.I),
        'company', 'investment bank trading floor, financial analysts at multiple screens'),
    (_re.compile(r'\brobinhood\b', _re.I),
        'company', 'Robinhood app, retail investing revolution, green arrow on phone screen'),
    (_re.compile(r'\bcharles\s*schwab\b|\bschwab\b', _re.I),
        'company', 'Charles Schwab brokerage, retail investor platform, financial freedom'),
    (_re.compile(r'\bpaypal\b', _re.I),
        'company', 'PayPal headquarters, digital payments flow, blue corporate fintech'),
    (_re.compile(r'\bvisa\b', _re.I),
        'company', 'Visa payment network, global transactions visualization, blue brand'),
    (_re.compile(r'\bmastercard\b', _re.I),
        'company', 'Mastercard interlocking circles, global payments, financial network'),
    (_re.compile(r'\bstripe\b', _re.I),
        'company', 'Stripe fintech, payment infrastructure, developer-focused tech'),
    (_re.compile(r'\bklarna\b|\baffirm\b|\bbnpl\b', _re.I),
        'company', 'buy-now-pay-later fintech app, consumer credit, digital shopping'),

    # ══ TECH COMPANIES ═══════════════════════════════════════════════════════════
    (_re.compile(r'\btesla\b', _re.I),
        'company', 'Tesla electric car, sleek futuristic design, neon charging station'),
    (_re.compile(r'\bapple\b', _re.I),
        'company', 'Apple Park campus aerial, iconic bitten apple, minimalist design'),
    (_re.compile(r'\bnvidia\b', _re.I),
        'company', 'Nvidia GPU chip, glowing green circuits, AI data center server racks'),
    (_re.compile(r'\bmicrosoft\b', _re.I),
        'company', 'Microsoft campus Redmond, Windows logo, cloud computing visualization'),
    (_re.compile(r'\bamazon\b|\baws\b', _re.I),
        'company', 'Amazon fulfillment center, delivery drones, cloud server infrastructure'),
    (_re.compile(r'\bgoogle\b|\balphabet\b', _re.I),
        'company', 'Google Googleplex campus, colorful futuristic architecture, AI lab'),
    (_re.compile(r'\bmeta\b|\bfacebook\b', _re.I),
        'company', 'Meta headquarters, VR headsets, social network visualization, futuristic'),
    (_re.compile(r'\bopenai\b|\bchatgpt\b', _re.I),
        'company', 'OpenAI neural network visualization, AI brain, futuristic blue glow'),
    (_re.compile(r'\btsmc\b', _re.I),
        'company', 'TSMC semiconductor chip factory, Taiwan precision manufacturing'),
    (_re.compile(r'\bintel\b', _re.I),
        'company', 'Intel CPU chip, silicon wafer, semiconductor manufacturing'),
    (_re.compile(r'\bamd\b', _re.I),
        'company', 'AMD Ryzen processor chip, red glow, computing performance'),
    (_re.compile(r'\bpalantir\b', _re.I),
        'company', 'Palantir data analytics visualization, government intelligence, dark screens'),
    (_re.compile(r'\bnetflix\b', _re.I),
        'company', 'Netflix red logo, streaming platform, Hollywood content production'),
    (_re.compile(r'\bspotify\b', _re.I),
        'company', 'Spotify green soundwaves, music streaming, audio visualization'),
    (_re.compile(r'\bairbnb\b', _re.I),
        'company', 'Airbnb cozy homes worldwide, travel disruption, sharing economy'),
    (_re.compile(r'\buber\b|\blyft\b', _re.I),
        'company', 'ride-sharing app, city streets with autonomous vehicles, gig economy'),
    (_re.compile(r'\bsalesforce\b', _re.I),
        'company', 'Salesforce cloud CRM, San Francisco headquarters, SaaS enterprise'),
    (_re.compile(r'\boracle\b', _re.I),
        'company', 'Oracle database headquarters, Austin Texas, enterprise cloud'),
    (_re.compile(r'\bqualcomm\b', _re.I),
        'company', 'Qualcomm Snapdragon chip, mobile semiconductor, San Diego headquarters'),
    (_re.compile(r'\bbroadcom\b', _re.I),
        'company', 'Broadcom semiconductor chips, network infrastructure, tech supply chain'),
    (_re.compile(r'\barm\s*holdings\b|\barm\b', _re.I),
        'company', 'ARM processor architecture, chip design blueprint, mobile computing'),
    (_re.compile(r'\btiktok\b|\bbytedance\b', _re.I),
        'company', 'TikTok logo, viral short video, ByteDance Chinese tech, social media'),
    (_re.compile(r'\bsnap\b|\bsnapchat\b', _re.I),
        'company', 'Snapchat ghost logo, social media, augmented reality filters'),
    (_re.compile(r'\bshopify\b', _re.I),
        'company', 'Shopify e-commerce platform, merchant success, green brand'),
    (_re.compile(r'\bibm\b', _re.I),
        'company', 'IBM blue corporate headquarters, quantum computing, enterprise AI'),
    (_re.compile(r'\bcisco\b', _re.I),
        'company', 'Cisco network infrastructure, internet backbone, corporate tech'),
    (_re.compile(r'\bsamsung\b', _re.I),
        'company', 'Samsung Galaxy devices, semiconductor fab, Korean tech titan'),
    (_re.compile(r'\bsony\b', _re.I),
        'company', 'Sony PlayStation, entertainment empire, Tokyo headquarters'),
    (_re.compile(r'\balibaba\b', _re.I),
        'company', 'Alibaba e-commerce empire, Hangzhou headquarters, Chinese tech giant'),
    (_re.compile(r'\btencent\b', _re.I),
        'company', 'Tencent WeChat, gaming empire, Shenzhen headquarters, Chinese tech'),
    (_re.compile(r'\bbaidu\b', _re.I),
        'company', 'Baidu AI search engine, Chinese internet giant, autonomous driving'),

    # ══ AUTOMOTIVE ════════════════════════════════════════════════════════════════
    (_re.compile(r'\bvolkswagen\b|\bvw\b|\bvolkswagen\s*ag\b', _re.I),
        'company', 'Volkswagen factory floor, German automotive engineering, EV transformation'),
    (_re.compile(r'\brivian\b', _re.I),
        'company', 'Rivian electric truck, adventure-ready EV, forest trail backdrop'),
    (_re.compile(r'\bmercedes\b|\bbenz\b|\bdaimler\b', _re.I),
        'company', 'Mercedes-Benz three-pointed star, luxury automotive, German precision'),
    (_re.compile(r'\bbmw\b', _re.I),
        'company', 'BMW headquarters Munich, luxury vehicles, propeller logo roundel'),
    (_re.compile(r'\bford\b', _re.I),
        'company', 'Ford F-150 Lightning, American automotive, Dearborn Michigan factory'),
    (_re.compile(r'\bgeneral\s*motors\b|\bgm\b', _re.I),
        'company', 'General Motors headquarters Detroit, EV future, American auto giant'),
    (_re.compile(r'\btoyota\b', _re.I),
        'company', 'Toyota factory, hybrid vehicles, Japanese manufacturing excellence'),
    (_re.compile(r'\bhonda\b', _re.I),
        'company', 'Honda automotive and motorcycle, Japanese engineering, global brand'),
    (_re.compile(r'\bnio\b', _re.I),
        'company', 'NIO electric vehicle, Chinese EV challenger, sleek futuristic design'),
    (_re.compile(r'\bbyd\b', _re.I),
        'company', 'BYD electric vehicle, Chinese auto giant, green energy future'),
    (_re.compile(r'\blucid\b', _re.I),
        'company', 'Lucid Motors luxury EV, Air sedan, California desert highway'),
    (_re.compile(r'\bstell?antis\b', _re.I),
        'company', 'Stellantis multi-brand automotive group, European American merger'),
    (_re.compile(r'\bautomotr[iz]?\b|\bautomot[iv]+e?\b|\bindustria.*auto\b|\bcar\s*indust\b', _re.I),
        'market', 'automotive industry, car factory assembly line, global auto market'),

    # ══ HEALTHCARE & PHARMA ════════════════════════════════════════════════════
    (_re.compile(r'\bpfizer\b', _re.I),
        'company', 'Pfizer pharmaceutical laboratory, drug discovery, vaccine vials'),
    (_re.compile(r'\bmoderna\b', _re.I),
        'company', 'Moderna mRNA technology, vaccine research lab, biotech innovation'),
    (_re.compile(r'\bjohnson\s*&?\s*johnson\b|\bj&j\b|\bjnj\b', _re.I),
        'company', 'Johnson & Johnson healthcare products, medical research, trusted brand'),
    (_re.compile(r'\bmerck\b', _re.I),
        'company', 'Merck pharmaceutical research, cancer drug, laboratory innovation'),
    (_re.compile(r'\bastrazeneca\b', _re.I),
        'company', 'AstraZeneca pharmaceutical, UK-Sweden biotech, drug pipeline'),
    (_re.compile(r'\bnovartis\b', _re.I),
        'company', 'Novartis Swiss pharmaceutical, Basel headquarters, drug research'),
    (_re.compile(r'\broche\b', _re.I),
        'company', 'Roche diagnostics and pharmaceuticals, Swiss precision medicine'),
    (_re.compile(r'\bunitedhealth\b|\buhg\b', _re.I),
        'company', 'UnitedHealth headquarters, health insurance giant, medical network'),
    (_re.compile(r'\btenet\s*health\b|\bhca\b|\bhospital\b|\bhealthcare\b', _re.I),
        'company', 'healthcare hospital complex, medical professionals, patient care facility'),
    (_re.compile(r'\babbott\b', _re.I),
        'company', 'Abbott medical devices, diagnostic innovation, healthcare technology'),
    (_re.compile(r'\bgano\b|\bsupera\b.*expectativas\b|\bearnings\s*beat\b', _re.I),
        'market', 'earnings beat celebration, stock chart surging green, trading floor cheers'),

    # ══ ENERGY ═══════════════════════════════════════════════════════════════════
    (_re.compile(r'\bexxon\b|\bexxonmobil\b', _re.I),
        'company', 'ExxonMobil oil refinery, Texas headquarters, energy giant'),
    (_re.compile(r'\bchevron\b', _re.I),
        'company', 'Chevron offshore oil platform, energy production, California HQ'),
    (_re.compile(r'\bshell\b', _re.I),
        'company', 'Shell oil platform, global energy company, scallop shell logo'),
    (_re.compile(r'\bbp\b', _re.I),
        'company', 'BP energy transition, solar and oil, green flower logo'),
    (_re.compile(r'\btotalenergies\b|\btotal\b', _re.I),
        'company', 'TotalEnergies French oil major, energy transition, global operations'),
    (_re.compile(r'\bnextera\b|\bener[gj][ií]a\s*renovable\b|\brenewable\b|\bgreen\s*energy\b|\benerg[ií]a\s*solar\b', _re.I),
        'company', 'renewable energy solar farm and wind turbines, green future, clean power'),
    (_re.compile(r'\bnuclear\b|\bur[aá]nio\b|\buranium\b', _re.I),
        'commodity', 'nuclear power plant cooling towers, uranium fuel rods, atomic energy'),

    # ══ RETAIL & CONSUMER ═════════════════════════════════════════════════════
    (_re.compile(r'\bwalmart\b', _re.I),
        'company', 'Walmart supercenter, retail giant, American consumer economy'),
    (_re.compile(r'\btarget\b', _re.I),
        'company', 'Target store, red bullseye, American retail competition'),
    (_re.compile(r'\bcostco\b', _re.I),
        'company', 'Costco warehouse, bulk shopping, membership retail giant'),
    (_re.compile(r'\bhome\s*depot\b', _re.I),
        'company', 'Home Depot orange store, hardware retail, housing market link'),
    (_re.compile(r'\bnike\b', _re.I),
        'company', 'Nike swoosh, athletic brand, sports performance, global empire'),
    (_re.compile(r'\badidas\b', _re.I),
        'company', 'Adidas three stripes, sportswear brand, European athletic fashion'),
    (_re.compile(r'\blvmh\b|\blouisvuitton\b|\blujo\b|\bluxury\b', _re.I),
        'company', 'LVMH luxury fashion, Paris runway, Vuitton monogram, ultra-wealthy'),
    (_re.compile(r'\bmcdonald\b', _re.I),
        'company', 'McDonald\'s golden arches, fast food empire, global franchise'),
    (_re.compile(r'\bstarbucks\b', _re.I),
        'company', 'Starbucks coffee cup, green mermaid logo, cafe culture'),
    (_re.compile(r'\bcoca.?cola\b', _re.I),
        'company', 'Coca-Cola red can, iconic beverage brand, global consumer'),
    (_re.compile(r'\bpepsi\b', _re.I),
        'company', 'PepsiCo beverage and snacks, globe logo, consumer goods'),

    # ══ DEFENSE & AEROSPACE ═══════════════════════════════════════════════════
    (_re.compile(r'\blockheed\b', _re.I),
        'company', 'Lockheed Martin F-35 fighter jet, defense contractor, military tech'),
    (_re.compile(r'\braytheon\b|\brtx\b', _re.I),
        'company', 'Raytheon missile defense system, military technology, Pentagon contractor'),
    (_re.compile(r'\bboeing\b', _re.I),
        'company', 'Boeing aircraft manufacturing, Dreamliner, aerospace engineering'),
    (_re.compile(r'\bairbus\b', _re.I),
        'company', 'Airbus A380 aircraft, European aerospace, Toulouse headquarters'),
    (_re.compile(r'\bdefensa\b|\bdefense\b|\bmilitar\b|\barmamento\b|\bweapon\b', _re.I),
        'market', 'defense industry, military technology, armored vehicles, war preparation'),
    (_re.compile(r'\bfedex\b', _re.I),
        'company', 'FedEx delivery truck and plane, logistics giant, overnight shipping'),
    (_re.compile(r'\bups\b', _re.I),
        'company', 'UPS brown delivery trucks, global logistics, package delivery empire'),

    # ══ COMMODITIES ═══════════════════════════════════════════════════════════
    (_re.compile(r'\bpetróleo\b|\bpetrol[eo]\b|\bcrude\b|\bwti\b|\bbrent\b|\boil\b', _re.I),
        'commodity', 'oil barrels and industrial refinery, flames at sunset, energy industry'),
    (_re.compile(r'\bgas\s*natural\b|\bnatural\s*gas\b|\bgnl\b|\blng\b', _re.I),
        'commodity', 'natural gas pipeline, industrial facility, flames, energy infrastructure'),
    (_re.compile(r'\bor[oa]\b|\bgold\b', _re.I),
        'commodity', 'gold bars stacked in vault, gleaming warm light, safe haven metal'),
    (_re.compile(r'\bplata\b|\bsilver\b', _re.I),
        'commodity', 'silver bullion coins and bars, cool metallic sheen, precious metal'),
    (_re.compile(r'\bcobre\b|\bcopper\b', _re.I),
        'commodity', 'copper wire coils and ore, industrial orange-red metal, mining'),
    (_re.compile(r'\blitio\b|\blithium\b', _re.I),
        'commodity', 'lithium mine, electric battery cells, EV supply chain, white salt flat'),
    (_re.compile(r'\bpaladio\b|\bpalladium\b|\bplatino\b|\bplatinum\b', _re.I),
        'commodity', 'platinum and palladium precious metals, catalytic converter, rare mines'),
    (_re.compile(r'\bniq?uel\b|\bnickel\b', _re.I),
        'commodity', 'nickel ore and steel production, industrial metal, battery supply chain'),
    (_re.compile(r'\baluminio\b|\baluminum\b|\baluminium\b', _re.I),
        'commodity', 'aluminum smelter, industrial metal production, aerospace material'),
    (_re.compile(r'\btrigo\b|\bwheat\b|\bcorn\b|\bmaíz\b|\bsoja\b|\bsoybean\b|\bagricult\b', _re.I),
        'commodity', 'grain fields at golden hour, agricultural harvest, commodity market'),
    (_re.compile(r'\bcaf[eé]\b|\bcoffee\b|\bcacao\b|\bcocoa\b', _re.I),
        'commodity', 'coffee plantation, roasted beans, commodity trading, tropical farm'),
    (_re.compile(r'\baz[uú]car\b|\bsugar\b|\bcott?on\b|\balgodon\b', _re.I),
        'commodity', 'sugar cane fields and cotton harvest, soft commodity trading'),

    # ══ MACRO / MARKET EVENTS ══════════════════════════════════════════════════
    (_re.compile(r'\bwall\s*st\b|\bbolsa\b|\bstock\s*market\b|\bmercado\s*de\s*valores\b', _re.I),
        'market', 'stock market trading floor, screens with live charts, intense traders'),
    (_re.compile(r'\bcriptomoneda\b|\bcrypto\s*market\b|\bdigital\s*assets\b', _re.I),
        'market', 'cryptocurrency exchange, digital screens, blockchain network glow'),
    (_re.compile(r'\bnonfarm\b|\bpayroll\b|\bjobs\s*report\b|\bempleo\b|\bdesempleo\b', _re.I),
        'market', 'employment data charts, job market surge, business people working'),
    (_re.compile(r'\binflaci[oó]n\b|\binflation\b|\bipc\b|\bcpi\b', _re.I),
        'market', 'price tags rising, shopping cart, inflation graph spiking, economic pressure'),
    (_re.compile(r'\btasa\s*de\s*inter[eé]s\b|\binterest\s*rate\b|\bhike\b|\brate\s*cut\b', _re.I),
        'market', 'interest rate graph ascending, financial charts, central bank decision'),
    (_re.compile(r'\brecesi[oó]n\b|\brecession\b|\bcrash\b|\bcrisis\b', _re.I),
        'market', 'financial crisis, red falling stock charts, dramatic dark storm atmosphere'),
    (_re.compile(r'\baran?cel\b|\btariff\b|\btrade\s*war\b|\bguerra\s*comercial\b', _re.I),
        'market', 'trade war concept, shipping containers stacked, tariff barriers, tension'),
    (_re.compile(r'\bdeuda\b|\bdebt\b|\bbono\b|\bbond\b|\btesoro\b|\btreasury\b|\bendeudamiento\b', _re.I),
        'market', 'government bonds, treasury debt visualization, national debt chart rising'),
    (_re.compile(r'\bipo\b|\bsalida\s*a\s*bolsa\b|\boferta\s*p[uú]blica\b', _re.I),
        'market', 'IPO ringing the opening bell at stock exchange, celebration, confetti'),
    (_re.compile(r'\bfusi[oó]n\b|\badquisici[oó]n\b|\bmerger\b|\bacquisition\b|\bm&a\b', _re.I),
        'market', 'corporate merger handshake, two companies becoming one, boardroom drama'),
    (_re.compile(r'\bquiebra\b|\bbankruptcy\b|\bdefault\b|\binsolvencia\b', _re.I),
        'market', 'bankruptcy filing, company collapse, falling building concept, financial ruin'),
    (_re.compile(r'\betf\b|\bfondo\s*[ií]ndice\b|\bindex\s*fund\b', _re.I),
        'market', 'ETF index fund visualization, diversified portfolio, passive investing'),
    (_re.compile(r'\bhedge\s*fund\b|\bfondo\s*especulativo\b', _re.I),
        'market', 'hedge fund trading desk, sophisticated investors, quant algorithms'),
    (_re.compile(r'\bventure\s*capital\b|\bvc\b|\bstartup\b', _re.I),
        'market', 'startup pitch meeting, venture capital investment, Silicon Valley garage'),
    (_re.compile(r'\binteligencia\s*artificial\b|\bai\b|\bmachine\s*learning\b|\bllm\b', _re.I),
        'market', 'artificial intelligence neural network, glowing brain circuits, AI data center'),
    (_re.compile(r'\bsemiconductor\b|\bchip\b|\bwafer\b', _re.I),
        'market', 'semiconductor chip close-up, silicon wafer fabrication, nano-scale circuits'),
    (_re.compile(r'\bciberseguridad\b|\bcybersecurity\b|\bhack\b', _re.I),
        'market', 'cybersecurity shield, digital threat visualization, hacker dark screen'),
    (_re.compile(r'\bcadena\s*de\s*suministro\b|\bsupply\s*chain\b', _re.I),
        'market', 'global supply chain, container ships and logistics map, interconnected world'),
    (_re.compile(r'\bdxy\b|\bd[oó]lar\b|\bdollar\b', _re.I),
        'market', 'US dollar bills and coins, DXY index chart, global reserve currency power'),
    (_re.compile(r'\byen\b|\bjpy\b', _re.I),
        'market', 'Japanese yen currency, Bank of Japan, Tokyo financial district at night'),
    (_re.compile(r'\beuro\b|\beur\b', _re.I),
        'market', 'Euro currency coins and bills, European Central Bank, Frankfurt skyline'),
    (_re.compile(r'\bpib\b|\bgdp\b|\bcrecimiento\s*econ[oó]mico\b|\beconomic\s*growth\b', _re.I),
        'market', 'GDP economic growth chart, global economy rising, prosperity visualization'),
    (_re.compile(r'\brating\b|\bcalificaci[oó]n\b|\bmoody\b|\bfitch\b|\bs&p\b|\bstandard.*poor\b', _re.I),
        'market', 'credit rating agency scales, financial rating decision, bond market impact'),
]

# ─── Scene composition templates ────────────────────────────────────────────
_SCENE_TEMPLATES: dict[tuple, str] = {
    ('person', 'place'):     '{0} standing before the {1}, dramatic lighting, power pose',
    ('person', 'crypto'):    '{0}, powerful expression, holding a {1} coin in hand, dramatic glow',
    ('person', 'company'):   '{0} in front of {1} headquarters, leadership portrait',
    ('person', 'commodity'): '{0} with {1} in the dramatic background',
    ('person', 'market'):    '{0} observing financial screens showing market data',
    ('place', 'crypto'):     '{1} floating above {0} skyline, digital golden glow',
    ('place', 'company'):    '{1} tower rising from {0} cityscape at dusk',
    ('place', 'commodity'):  '{1} pipelines and tankers near {0} coastline',
    ('place', 'market'):     '{0} financial district at night, lit trading screens',
    ('place', 'place'):      'confrontation between {0} and {1}, dramatic split composition',
    ('crypto', 'company'):   '{0} coin hovering next to {1} skyscraper, neon glow',
    ('crypto', 'market'):    '{0} coin above a sea of financial data screens',
    ('company', 'market'):   '{0} headquarters overlooking a volatile stock market',
    ('commodity', 'market'): '{0} with financial market data screens in background',
    ('company', 'company'):  '{0} facing off against {1}, corporate rivalry composition',
    ('person', 'person'):    'split portrait of {0} and {1}, dramatic tension',
}

# Diverse fallback scenes — rotated by hash so no two posts share the same generic image.
# Reworked for flux-realism: each entry is now a cinematic scene with explicit
# camera/lighting cues. Grouped by mood (markets / money / macro / crisis /
# tech / global / digital) so the rotation pulls from varied visual registers
# instead of always landing on "trading floor red screens".
_GENERIC_FALLBACKS = [
    # ── Markets & trading ───────────────────────────────────────────────────
    "dramatic trading floor at golden hour, traders silhouetted against giant glowing market screens, intense focused atmosphere, cinematic wide shot",
    "Wall Street bull statue in dramatic morning fog, golden sunlight breaking through skyscrapers, low-angle hero composition",
    "stock exchange opening bell ceremony shot from behind the bell ringer, traders cheering in soft focus, motion blur and confetti, dramatic backlight",
    "abstract candlestick chart patterns floating in dark space, dramatic neon green and red glow, cinematic depth",

    # ── Money & currency ────────────────────────────────────────────────────
    "stacks of crisp banknotes arranged in artistic composition, dramatic side-lighting casting long shadows, dark velvet background, macro detail",
    "golden coins cascading in slow motion through a beam of dramatic light, soft bokeh background, luxurious finance aesthetic",
    "close-up of a banker's hands sliding documents across a polished mahogany desk, dramatic window light, cinematic shallow depth of field",

    # ── Macro / policy ──────────────────────────────────────────────────────
    "empty central bank boardroom shot from low angle, leather chairs around a long table, dramatic light streaming through tall windows, tension implied",
    "podium in an empty press conference hall, microphones lined up, dramatic spotlights, anticipation atmosphere, wide cinematic shot",
    "dramatic financial district skyscrapers shot from the ground at twilight, lit windows forming a vertical grid, vertiginous perspective",

    # ── Crisis / drama ──────────────────────────────────────────────────────
    "stormy sky over financial district at sunset, dramatic red clouds rolling in, foreboding atmosphere, wide cinematic shot",
    "rain-soaked Wall Street pavement at night, neon reflections, lone figure in trench coat, film noir mood",

    # ── Tech / AI / future ──────────────────────────────────────────────────
    "abstract neural network glowing in dark space, data nodes connected by light streams, blue-purple palette, hyper-detailed",
    "futuristic data center server racks vanishing into perspective, glowing LED indicators, dramatic backlight, cool tones",

    # ── Global trade ────────────────────────────────────────────────────────
    "massive cargo ships at international port at dramatic sunset, towering cranes silhouetted, golden hour palette, wide cinematic frame",
    "abstract globe with glowing trade routes connecting major cities, dark space backdrop, blue and amber light, cinematic",

    # ── Crypto / digital assets ─────────────────────────────────────────────
    "abstract digital coin floating in a beam of dramatic light, dark cyberpunk backdrop with blue and gold accents, slight motion blur",
    "blockchain network visualization, glowing hexagonal nodes connected by streams of light, futuristic abstract, dramatic depth",
]


# ─── Action / tone modifiers from angle_reasoning ───────────────────────────
#
# The LLM's `angle_reasoning` carries editorial tone that the headline alone
# rarely has — direction (rises/falls), emotion (panic/celebration), action
# (acquires/warns/regulates). Inject 1 short modifier into the scene when we
# match, otherwise omit silently. Order matters: stronger signals first so the
# first match wins.
_ACTION_MAP: list[tuple[_re.Pattern, str]] = [
    # ── Crash / fall / panic ────────────────────────────────────────────────
    (_re.compile(r'\b(crash(es|ed)?|plunge[sd]?|tumble[sd]?|collaps[a-z]+|sink[sd]?)\b'
                 r'|\b(desploma[a-z]*|hunde[a-z]*|colaps[a-z]*|caída[a-z]*|crisis)\b', _re.I),
     "panic atmosphere, red trading screens, traders in shock, dramatic crash mood"),
    (_re.compile(r'\b(fall[sing]*|drop[psing]*|slip[psing]*|losses?|bear[a-z]*)\b'
                 r'|\b(cae|baja[a-z]*|pierde|pérdid[a-z]*|bajista)\b', _re.I),
     "descending energy, downward motion, dim cool palette, somber mood"),

    # ── Rally / surge / celebration ─────────────────────────────────────────
    (_re.compile(r'\b(soar[sing]*|surge[sd]?|rally[a-z]*|jump[psing]*|skyrocket[a-z]*|breakthrough)\b'
                 r'|\b(salta[a-z]*|repunta[a-z]*|dispara[a-z]*|récord|máximo)\b', _re.I),
     "celebration atmosphere, green ascending screens, traders cheering, dramatic golden light"),
    (_re.compile(r'\b(rise[sn]?|gain[sing]*|climb[a-z]*|bullish|outperform[a-z]*|beat[sing]*)\b'
                 r'|\b(sube|gana|alza|supera|optimist[a-z]*|alcista)\b', _re.I),
     "rising energy, upward motion, warm bright palette, optimistic mood"),

    # ── M&A / partnership ───────────────────────────────────────────────────
    (_re.compile(r'\b(acqui[a-z]+|merge[a-z]*|buys?|bought|takeover|partnership|deal)\b'
                 r'|\b(compra|adquiere|fusiona|fusion|alianz[a-z]*|acuerdo)\b', _re.I),
     "boardroom handshake, signing documents, formal corporate atmosphere"),

    # ── Regulation / approval / ban ─────────────────────────────────────────
    (_re.compile(r'\b(approve[sd]?|approval|grant[sed]?|legalize[a-z]*|regulator[a-z]*)\b'
                 r'|\b(aprueba[a-z]*|aprobad[a-z]*|legaliza[a-z]*|regul[a-z]+)\b', _re.I),
     "official document being stamped, gavel and seal, institutional gravitas"),
    (_re.compile(r'\b(ban[sn]?ed|banning|outlaw|prohibit[a-z]*|reject[sed]?|deni[a-z]+)\b'
                 r'|\b(prohíbe[a-z]*|prohibici[a-z]*|veta[a-z]*|rechaza[a-z]*)\b', _re.I),
     "red prohibition seal, blocked path, foreboding institutional atmosphere"),

    # ── Lawsuit / investigation ─────────────────────────────────────────────
    (_re.compile(r'\b(sue[sd]?|lawsuit|charged?|indict[a-z]*|investigat[a-z]+|fraud)\b'
                 r'|\b(demand[a-z]+|denunci[a-z]+|investig[a-z]+|fraude)\b', _re.I),
     "courtroom drama, gavel mid-strike, stacks of legal documents, dramatic shadows"),

    # ── Launch / announcement ───────────────────────────────────────────────
    (_re.compile(r'\b(launch[sed]?|unveil[sed]?|introduc[ase]+|announce[sd]?|reveal[sed]?)\b'
                 r'|\b(lanza|presenta|anuncia|revela[a-z]*|estrena[a-z]*)\b', _re.I),
     "spotlight on stage, dramatic product reveal, anticipation in the air"),

    # ── Warning / threat ────────────────────────────────────────────────────
    (_re.compile(r'\b(warn[sing]*|threat[en]?[a-z]*|alert[sing]*|risk|danger)\b'
                 r'|\b(advierte[a-z]*|amenaza[a-z]*|alerta[a-z]*|riesgo|peligro)\b', _re.I),
     "ominous storm clouds gathering, tense atmosphere, cool dramatic palette"),

    # ── Cut / layoff ────────────────────────────────────────────────────────
    (_re.compile(r'\b(layoff[s]?|fire[sd]?|cut[s]? jobs|downsiz[a-z]+)\b'
                 r'|\b(despid[a-z]+|recort[a-z]+ empleo|reestructur[a-z]+)\b', _re.I),
     "empty corporate offices, abandoned workstations, somber dramatic lighting"),
]


# ─── Unknown-entity dynamic discovery (Frente 5) ────────────────────────────
#
# When _extract_subjects returns [] AND the editorial entity is also None,
# we try to find a capitalized noun phrase in the headline and ask the backend
# how to visualize it. The backend has a persistent Supabase cache, so each
# entity name only costs one LLM call across the system's lifetime.
#
# Why this matters: headlines often name companies we don't have in catalog
# (Concentrix, Dawn Labs, MARA, niche tickers). Without this, those headlines
# fall through to the generic _GENERIC_FALLBACKS and look anonymous.

# Words that are capitalized at the start of sentences but aren't entities.
_PROPER_NOUN_STOPWORDS = frozenset({
    "The", "A", "An", "This", "That", "These", "Those",
    "What", "Why", "How", "When", "Where", "Who", "Which",
    "After", "Before", "While", "During", "Since", "Until",
    "Wall", "Street",  # these come with " Street"/" St" follow-ons; handled in regex post-filter
    "BREAKING", "JUST", "ALERT", "ATTENTION", "URGENT",
    "I", "We", "You", "They", "He", "She", "It",
    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
    "January", "February", "March", "April", "May", "June", "July",
    "August", "September", "October", "November", "December",
    "Q1", "Q2", "Q3", "Q4",
})

# Matches capitalized phrases: word starting with uppercase letter + optional
# 1-3 additional capitalized words. Requires the first word to be >= 3 chars
# to avoid matching abbreviations like "S&P" (which is already in catalog).
_CAPITALIZED_PHRASE = _re.compile(
    r'\b([A-Z][a-zA-Z0-9]{2,}(?:\s+[A-Z][a-zA-Z0-9]+){0,3})\b'
)

# In-memory cache so the SAME process doesn't re-hit the backend for the same
# entity twice in a session. Backend also caches in Supabase, but this saves
# the HTTP round trip during the lifetime of one Railway deploy.
_visual_subject_local_cache: dict[str, Optional[dict]] = {}


def _extract_unknown_entity_candidate(headline: str) -> Optional[str]:
    """Pull the most prominent capitalized noun phrase from `headline`.
    Returns the candidate string, or None if nothing usable was found."""
    if not headline:
        return None
    matches = _CAPITALIZED_PHRASE.findall(headline)
    for raw in matches:
        first_word = raw.split()[0]
        if first_word in _PROPER_NOUN_STOPWORDS:
            continue
        # Skip if already matched by catalog — caller should not reach here
        # in that case, but belt-and-suspenders.
        if _SUBJECTS and any(p.search(raw) for p, _, _ in _SUBJECTS):
            continue
        return raw.strip()
    return None


def _lookup_visual_subject(name: str) -> Optional[dict]:
    """In-memory cached wrapper around wastake_client.get_visual_subject."""
    key = name.strip().lower()
    if key in _visual_subject_local_cache:
        return _visual_subject_local_cache[key]
    # Lazy import to avoid a hard dependency for tests / scripts that import
    # ai_image_generator standalone.
    try:
        from . import wastake_client
    except Exception:
        _visual_subject_local_cache[key] = None
        return None
    result = wastake_client.get_visual_subject(name)
    # Persist None too, so a transient backend hiccup doesn't get retried in
    # the same process for the same name (next cycle's tick is enough latency).
    _visual_subject_local_cache[key] = result
    return result


def _action_modifiers(angle_reasoning: str) -> str:
    """Return the first matching action modifier from the angle text, or ''.

    Kept conservative: at most ONE modifier per scene to avoid prompt bloat
    and conflicting tone signals (e.g. crash + celebration).
    """
    if not angle_reasoning:
        return ""
    for pattern, modifier in _ACTION_MAP:
        if pattern.search(angle_reasoning):
            return modifier
    return ""


def _extract_subjects(text: str) -> list[tuple[str, str]]:
    seen_cats: list[str] = []
    results: list[tuple[str, str]] = []
    for pattern, cat, visual in _SUBJECTS:
        if pattern.search(text):
            if len(results) < 2:
                results.append((cat, visual))
                seen_cats.append(cat)
            if len(results) == 2:
                break
    return results


def _build_scene(subjects: list[tuple[str, str]], entity, headline: str) -> str:
    if len(subjects) == 0:
        if entity:
            e_type = entity.get("type")
            name   = entity.get("display") or entity.get("id") or "subject"
            if e_type == "crypto":
                return f"glowing {name} cryptocurrency coin, metallic, floating mid-air"
            if e_type == "company":
                return f"{name} corporate headquarters, glass skyscraper, financial district"
            if e_type == "person":
                return f"dramatic portrait of {name}, intense expression, cinematic lighting"
            if e_type in ("index", "commodity"):
                return f"dramatic visualization of {name} market movement, charts, data"

        # Frente 5 — dynamic discovery: try to identify an unknown entity
        # from the headline and ask the backend how to visualize it before
        # falling back to a generic scene.
        candidate = _extract_unknown_entity_candidate(headline)
        if candidate:
            looked_up = _lookup_visual_subject(candidate)
            if looked_up:
                desc = (looked_up.get("description") or {})
                visual = (desc.get("visual_description") or "").strip()
                category = desc.get("category") or "unknown"
                if visual and category != "unknown":
                    palette = (desc.get("color_palette") or "").strip()
                    scene = f"{visual}, dramatic composition"
                    if palette:
                        scene = f"{scene}, {palette} palette"
                    return scene

        # Rotate through diverse fallbacks based on headline hash to avoid repetition
        idx = int(hashlib.md5(headline.encode()).hexdigest(), 16) % len(_GENERIC_FALLBACKS)
        return _GENERIC_FALLBACKS[idx]

    if len(subjects) == 1:
        cat, vis = subjects[0]
        return f"{vis}, dramatic backdrop, powerful composition"

    cat1, vis1 = subjects[0]
    cat2, vis2 = subjects[1]
    template = _SCENE_TEMPLATES.get((cat1, cat2)) or _SCENE_TEMPLATES.get((cat2, cat1))
    if template:
        if (cat2, cat1) in _SCENE_TEMPLATES and (cat1, cat2) not in _SCENE_TEMPLATES:
            return template.format(vis2, vis1)
        return template.format(vis1, vis2)
    return f"{vis1} juxtaposed with {vis2}, dramatic cinematic split composition"


def craft_prompt(
    headline: str,
    hook: str = "",
    entity=None,
    angle_reasoning: str = "",
    strength: int = 3,
) -> str:
    """
    Build the image prompt from a news post.

    Backwards-compatible: existing callers that only pass headline/hook/entity
    keep working because angle_reasoning and strength are optional with sane
    defaults.

    angle_reasoning:
      The LLM's analytical reasoning for this post. Used to extract ONE action
      modifier (panic / celebration / handshake / etc.) that shapes scene tone.
      Optional — if empty, no modifier is added.

    strength:
      Editorial strength 1-5 (from the news-angle LLM rating).
      >=4 → use the richer style tail (Bloomberg/Vanity Fair aesthetic).
      <4  → use the standard tail. Saves token budget on the long tail of
            mid-impact posts that don't justify maximum descriptors.
    """
    combined_text = f"{headline} {hook}".strip()
    subjects = _extract_subjects(combined_text)
    scene    = _build_scene(subjects, entity, headline)

    # Inject ONE action modifier from angle_reasoning if matched.
    modifier = _action_modifiers(angle_reasoning)
    if modifier:
        scene = f"{scene}, {modifier}"

    tail   = _STYLE_TAIL_RICH if strength >= 4 else _STYLE_TAIL
    prompt = ", ".join([scene, tail])
    return prompt[:PROMPT_MAX]
