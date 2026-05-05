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
# AI gen can be slow but we cap so a hung request doesn't block the cycle.
# Pollinations Flux observed at ~85-90s typical; 120s gives headroom.
# Imagen 3 is much faster (~3-8s), same timeout is fine.
HTTP_TIMEOUT = 120
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
    """Pollinations returns the PNG bytes directly via GET.

    Using `turbo` model. Removed `enhance=true` because it added a slow
    LLM-prompt-enhancement step (~10s extra). With enhance off and turbo,
    requests should land closer to ~5-15s.
    """
    safe = quote(prompt[:PROMPT_MAX])
    url = (
        f"https://image.pollinations.ai/prompt/{safe}"
        "?width=1080&height=1350&model=turbo&nologo=true"
    )
    resp = requests.get(url, timeout=HTTP_TIMEOUT, headers={
        "User-Agent": "WaCapital-PulseEngine/1.0",
    })
    resp.raise_for_status()
    return resp.content


# ─── Public entry point ─────────────────────────────────────────────────────

def generate(prompt: str, *, try_imagen: bool = True) -> Optional[Image.Image]:
    """
    Generate a portrait image from `prompt`. Returns PIL.Image (RGB) or None
    if every available provider failed.

    try_imagen:
      True  → primary: Imagen 3 (multi-key rotation), fallback: Pollinations.
              For premium/fresh posts where we want best quality.
      False → skip Imagen entirely, use Pollinations only.
              For backfill of historical posts — saves Imagen 3 daily quota.

    Caches in /tmp/ keyed by sha256(prompt) — same prompt = no re-billing.
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

    if try_imagen:
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
                # Pull a useful body snippet so we can diagnose 404s, scope errors, etc.
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
                    # 404 = model not found / not enabled for this key.
                    # Mark the key cooldown so we don't hammer it; logged so user can debug.
                    _mark_key_exhausted(key)
                    log.warning("[ai-image] Imagen 3 NOT available for key %s (status=404). "
                                "Key likely lacks Vertex AI / Imagen access. Body: %s",
                                _key_label(key), body_snippet)
                    continue
                log.warning("[ai-image] Imagen 3 failed on key %s: status=%s body=%s",
                            _key_label(key), status, body_snippet)
                continue
            except Exception as e:
                log.warning("[ai-image] Imagen 3 error on key %s: %s", _key_label(key), e)
                continue

    # Pollinations fallback (or primary when try_imagen=False).
    try:
        if try_imagen:
            log.info("[ai-image] falling back to Pollinations (Flux)")
        else:
            log.info("[ai-image] using Pollinations (Flux) — Imagen skipped")
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
#
# Strong negative-text directives: diffusion models LOVE to scribble fake
# letters/logos inside images (we saw "BLENA TIGOP"-style gibberish). The
# repetition of "no text / no letters / no signs" is the only reliable way
# to suppress it on Flux; Imagen 3 respects it more reliably.
_STYLE_TAIL = (
    "editorial photography, cinematic dramatic lighting, photorealistic, hyper-detailed, "
    "8k, sharp focus, vertical 9:16 composition, "
    "ABSOLUTELY no text, no letters, no readable writing, no captions, no labels, "
    "no logos with text, no signs, no numbers, no watermark"
)

# ─── Dual-subject catalog ───────────────────────────────────────────────────
# Each entry: (regex_pattern, category, english_visual_description)
# Regex is matched case-insensitively against the combined headline+hook text.
# The 'category' drives scene composition logic below.
import re as _re

_SUBJECTS = [
    # ── Politicians / public figures — global
    (_re.compile(r'\btrump\b', _re.I),
        'person', 'Donald Trump, intense expression, dark suit, power pose'),
    (_re.compile(r'\bpowell\b', _re.I),
        'person', 'Jerome Powell, stern expression, Federal Reserve formal attire'),
    (_re.compile(r'\bbiden\b', _re.I),
        'person', 'Joe Biden, formal presidential portrait, American flag background'),
    (_re.compile(r'\blagarde\b', _re.I),
        'person', 'Christine Lagarde, ECB president, elegant formal attire'),
    (_re.compile(r'\bmusk\b', _re.I),
        'person', 'Elon Musk, tech visionary, intense focused look'),
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
    (_re.compile(r'\bbuffett\b|\bwarren\s*buffett\b', _re.I),
        'person', 'Warren Buffett, legendary investor, warm smile, Berkshire office'),
    (_re.compile(r'\bdimon\b|\bjamie\s*dimon\b', _re.I),
        'person', 'Jamie Dimon, JPMorgan CEO, confident executive portrait'),
    (_re.compile(r'\baltman\b|\bsam\s*altman\b', _re.I),
        'person', 'Sam Altman, OpenAI CEO, tech leader, futuristic backdrop'),
    (_re.compile(r'\bzuckerberg\b', _re.I),
        'person', 'Mark Zuckerberg, Meta CEO, casual tech style, intense focus'),
    (_re.compile(r'\bjensen\b|\bhuang\b', _re.I),
        'person', 'Jensen Huang, Nvidia CEO, signature leather jacket, GPU chip backdrop'),
    # ── Countries / regions (Spanish + English)
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
        'place', 'South Korea, Seoul modern skyline, technology and finance hub'),
    (_re.compile(r'\bt[uú]nez\b|\bturqu[ií]a\b|\bturkey\b|\bturkish\b', _re.I),
        'place', 'Turkey, Istanbul Bosphorus bridge, East meets West skyline'),
    # ── Strategic locations / institutions
    (_re.compile(r'\bhormuz\b', _re.I),
        'place', 'Strait of Hormuz aerial view, oil tankers on blue water, narrow rocky passage'),
    (_re.compile(r'\bpanama\b|\bcanal\b', _re.I),
        'place', 'Panama Canal, massive cargo ships, lock system, aerial view'),
    (_re.compile(r'\bsuez\b', _re.I),
        'place', 'Suez Canal, container ships queue, Egyptian desert, aerial view'),
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
    # ── Crypto assets
    (_re.compile(r'\bbitcoin\b|\bbtc\b', _re.I),
        'crypto', 'glowing golden Bitcoin coin, metallic embossed B, dramatic reflections, dark background'),
    (_re.compile(r'\bethereum\b|\beth\b', _re.I),
        'crypto', 'glowing Ethereum diamond crystal, silver-blue shimmer, futuristic'),
    (_re.compile(r'\bsolana\b|\bsol\b', _re.I),
        'crypto', 'Solana coin, iridescent purple-teal gradient glow'),
    (_re.compile(r'\bripple\b|\bxrp\b', _re.I),
        'crypto', 'XRP Ripple coin, sleek blue metallic, global payments network visualization'),
    (_re.compile(r'\bbinance\b|\bbnb\b', _re.I),
        'crypto', 'Binance coin BNB, golden yellow glow, exchange platform'),
    (_re.compile(r'\bdogecoin\b|\bdoge\b', _re.I),
        'crypto', 'Dogecoin with Shiba Inu dog face, golden coin, meme energy'),
    (_re.compile(r'\bcardano\b|\bada\b', _re.I),
        'crypto', 'Cardano ADA coin, navy blue metallic, blockchain network nodes'),
    (_re.compile(r'\bavalanche\b|\bavax\b', _re.I),
        'crypto', 'Avalanche AVAX coin, red glowing, high-speed blockchain'),
    (_re.compile(r'\bpolkadot\b|\bdot\b', _re.I),
        'crypto', 'Polkadot DOT coin, colorful interconnected dots network'),
    (_re.compile(r'\bchainlink\b|\blink\b', _re.I),
        'crypto', 'Chainlink LINK coin, blue hexagonal, oracle network'),
    # ── Major companies — finance
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
        'company', 'HSBC bank tower, Hong Kong skyline, global banking'),
    (_re.compile(r'\bdeutsche\s*bank\b', _re.I),
        'company', 'Deutsche Bank twin towers Frankfurt, European finance'),
    (_re.compile(r'\bvanguard\b', _re.I),
        'company', 'Vanguard investment funds, global portfolio visualization'),
    (_re.compile(r'\bfidelity\b', _re.I),
        'company', 'Fidelity Investments corporate campus, asset management'),
    # ── Major companies — tech
    (_re.compile(r'\btesla\b', _re.I),
        'company', 'Tesla electric car, sleek futuristic design, neon charging station'),
    (_re.compile(r'\bapple\b', _re.I),
        'company', 'Apple Park campus aerial, iconic bitten apple logo, minimalist design'),
    (_re.compile(r'\bnvidia\b', _re.I),
        'company', 'Nvidia GPU chip, glowing green circuits, AI data center server racks'),
    (_re.compile(r'\bgoldman\b|\bgoldman\s*sachs\b', _re.I),
        'company', 'Goldman Sachs glass skyscraper, Manhattan skyline, finance power'),
    (_re.compile(r'\bjpmorgan\b|\bj\.?p\.?\s*morgan\b', _re.I),
        'company', 'JPMorgan Chase headquarters, Wall Street tower, banking giant'),
    (_re.compile(r'\bmicrosoft\b', _re.I),
        'company', 'Microsoft campus Redmond, Windows logo, cloud computing visualization'),
    (_re.compile(r'\bamazon\b|\baws\b', _re.I),
        'company', 'Amazon fulfillment center, delivery drones, cloud server infrastructure'),
    (_re.compile(r'\bgoogle\b|\balphabet\b', _re.I),
        'company', 'Google Googleplex campus, colorful futuristic architecture, AI lab'),
    (_re.compile(r'\bmeta\b|\bfacebook\b|\binstagram\b', _re.I),
        'company', 'Meta headquarters, VR headsets, social network visualization, futuristic'),
    (_re.compile(r'\bopenai\b', _re.I),
        'company', 'OpenAI neural network visualization, AI brain, futuristic blue glow'),
    (_re.compile(r'\btsmc\b', _re.I),
        'company', 'TSMC semiconductor chip factory, Taiwan precision manufacturing'),
    (_re.compile(r'\bintel\b', _re.I),
        'company', 'Intel CPU chip, silicon wafer, semiconductor manufacturing'),
    (_re.compile(r'\bamd\b', _re.I),
        'company', 'AMD processor chip, red glow, computing power'),
    (_re.compile(r'\bpalantir\b', _re.I),
        'company', 'Palantir data analytics visualization, government intelligence, dark screens'),
    (_re.compile(r'\boppenheimer\b', _re.I),
        'company', 'investment bank trading floor, financial analysts at screens'),
    # ── Commodities
    (_re.compile(r'\bpetróleo\b|\bpetrol[eo]\b|\bcrude\b|\bwti\b|\bbrent\b|\boil\b', _re.I),
        'commodity', 'oil barrels and industrial refinery, flames at sunset, energy industry'),
    (_re.compile(r'\bgas\s*natural\b|\bnatural\s*gas\b|\bgnl\b|\blng\b', _re.I),
        'commodity', 'natural gas pipeline, industrial facility, flames, energy infrastructure'),
    (_re.compile(r'\bor[oa]\b|\bgold\b', _re.I),
        'commodity', 'gold bars stacked in vault, gleaming warm light, safe haven'),
    (_re.compile(r'\bplata\b|\bsilver\b', _re.I),
        'commodity', 'silver bullion coins and bars, cool metallic sheen, precious metal'),
    (_re.compile(r'\bcobre\b|\bcopper\b', _re.I),
        'commodity', 'copper wire coils and ore, industrial orange-red metal'),
    (_re.compile(r'\blitio\b|\blithium\b', _re.I),
        'commodity', 'lithium mine, electric battery cells, EV supply chain'),
    (_re.compile(r'\btrigo\b|\bwheat\b|\bcorn\b|\bmaíz\b|\bsoja\b|\bsoybean\b', _re.I),
        'commodity', 'grain fields at golden hour, agricultural harvest, commodity market'),
    # ── Macro events / institutions
    (_re.compile(r'\bwall\s*st\b|\bbolsa\b|\bstock\s*market\b|\bmercado\s*de\s*valores\b', _re.I),
        'market', 'stock market trading floor, screens with live charts, intense traders'),
    (_re.compile(r'\bcriptomoneda\b|\bcrypto\s*market\b|\bdigital\s*assets\b', _re.I),
        'market', 'cryptocurrency exchange, digital screens, blockchain network visualization'),
    (_re.compile(r'\bnonfarm\b|\bpayroll\b|\bjobs\s*report\b|\bempleo\b|\bdesempleo\b', _re.I),
        'market', 'employment data, business people working, economic growth visualization'),
    (_re.compile(r'\binflaci[oó]n\b|\binflation\b|\bipc\b|\bcpi\b', _re.I),
        'market', 'price tags rising, shopping cart, inflation graph, economic pressure'),
    (_re.compile(r'\btasa\s*de\s*inter[eé]s\b|\binterest\s*rate\b|\bhike\b|\brate\s*cut\b', _re.I),
        'market', 'interest rate graph ascending, financial charts, central bank concept'),
    (_re.compile(r'\brecesi[oó]n\b|\brecession\b|\bcrash\b|\bcrisis\b', _re.I),
        'market', 'financial crisis, red falling stock charts, dramatic dark atmosphere'),
    (_re.compile(r'\baran?cel\b|\btariff\b|\btrade\s*war\b|\bguerra\s*comercial\b', _re.I),
        'market', 'trade war concept, shipping containers, tariff barriers, global trade tension'),
    (_re.compile(r'\bdeuda\b|\bdebt\b|\bbono\b|\bbond\b|\btesoro\b|\btreasury\b', _re.I),
        'market', 'government bonds, treasury notes, national debt visualization, finance'),
    (_re.compile(r'\bipo\b|\bsalida\s*a\s*bolsa\b|\boferta\s*p[uú]blica\b', _re.I),
        'market', 'IPO ringing the opening bell at stock exchange, celebration, confetti'),
]

# Scene composition templates — indexed by (category1, category2) of the two subjects.
# Placeholders: {0} = first subject description, {1} = second subject description.
_SCENE_TEMPLATES: dict[tuple, str] = {
    ('person', 'place'):    '{0} standing before the {1}, dramatic lighting, power pose',
    ('person', 'crypto'):   '{0}, powerful expression, holding a {1} coin in hand, dramatic glow',
    ('person', 'company'):  '{0} in front of {1} headquarters, leadership portrait',
    ('person', 'commodity'): '{0} with {1} in the dramatic background',
    ('person', 'market'):   '{0} observing financial screens showing market data',
    ('place', 'crypto'):    '{1} floating above {0} skyline, digital golden glow',
    ('place', 'company'):   '{1} tower rising from {0} cityscape at dusk',
    ('place', 'commodity'): '{1} pipelines and tankers near {0} coastline',
    ('place', 'market'):    '{0} financial district at night, lit trading screens',
    ('place', 'place'):     'confrontation between {0} and {1}, dramatic split composition',
    ('crypto', 'company'):  '{0} coin hovering next to {1} skyscraper, neon glow',
    ('crypto', 'market'):   '{0} coin above a sea of financial data screens',
    ('company', 'market'):  '{0} headquarters overlooking a volatile stock market',
    ('commodity', 'market'): '{0} with financial market data screens in background',
    ('company',  'company'): '{0} facing off against {1}, corporate rivalry composition',
    ('person',   'person'):  'split portrait of {0} and {1}, dramatic tension',
}


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
        return "dramatic financial market scene, trading floor, global economy visualization"

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


def craft_prompt(headline: str, hook: str = "", entity=None) -> str:
    combined_text = f"{headline} {hook}".strip()
    subjects = _extract_subjects(combined_text)
    scene    = _build_scene(subjects, entity, headline)
    parts    = [scene, _STYLE_TAIL]
    prompt   = ", ".join(parts)
    return prompt[:PROMPT_MAX]
