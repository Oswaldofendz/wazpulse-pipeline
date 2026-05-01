"""
Image fetcher with on-disk cache — Bloque 6c-V2.

Fetches PNG/JPG/SVG logos and photos from public CDNs (Clearbit for company
logos, cryptologos.cc for crypto). Caches to /tmp/wacapital_images/ keyed by
SHA256(url) so subsequent renders within the same container reuse the file.

Cache survives within a Railway deploy but resets on container restart — that
is fine, the cache only exists to avoid slamming the CDN inside a tight loop.

Returns PIL.Image instances ready to paste into a card. Failures (network,
404, parse error) return None so the card generator can fall back to text-only.
"""
import hashlib
import logging
import os
from io import BytesIO
from typing import Optional

import requests
from PIL import Image

log = logging.getLogger("image-fetcher")

CACHE_DIR = "/tmp/wacapital_images"
HTTP_TIMEOUT = 10
USER_AGENT = "WaCapital-PulseEngine/1.0 (+https://wastake.vercel.app)"

try:
    os.makedirs(CACHE_DIR, exist_ok=True)
except Exception as e:
    log.warning("could not create cache dir %s: %s", CACHE_DIR, e)


def _cache_path_for(url: str) -> str:
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
    return os.path.join(CACHE_DIR, f"{digest}.png")


def fetch(url: Optional[str]) -> Optional[Image.Image]:
    """Fetch + cache + return as PIL.Image. Returns None on any failure."""
    if not url:
        return None

    cache_path = _cache_path_for(url)

    # Hit cache first.
    if os.path.exists(cache_path):
        try:
            return Image.open(cache_path).convert("RGBA")
        except Exception as e:
            log.warning("cache file unreadable, refetching: %s", e)
            try:
                os.remove(cache_path)
            except Exception:
                pass

    # Fetch fresh.
    try:
        resp = requests.get(
            url,
            timeout=HTTP_TIMEOUT,
            headers={"User-Agent": USER_AGENT, "Accept": "image/*"},
        )
        resp.raise_for_status()

        img = Image.open(BytesIO(resp.content))
        # Always normalize to RGBA so paste() with mask works downstream.
        img = img.convert("RGBA")

        # Save normalized version to cache.
        try:
            img.save(cache_path, format="PNG")
        except Exception as e:
            log.warning("could not write cache for %s: %s", url, e)

        return img

    except Exception as e:
        log.warning("image fetch failed url=%s err=%s", url, e)
        return None


def fit_into(image: Image.Image, max_w: int, max_h: int) -> Image.Image:
    """
    Resize `image` to fit inside max_w × max_h, preserving aspect ratio.
    Returns a new RGBA image of the scaled size (no padding/canvas).
    """
    iw, ih = image.size
    if iw == 0 or ih == 0:
        return image
    scale = min(max_w / iw, max_h / ih)
    new_w = max(1, int(iw * scale))
    new_h = max(1, int(ih * scale))
    return image.resize((new_w, new_h), Image.LANCZOS)
