"""HTTP client for the WaStake backend.

Wraps the two endpoints the pipeline consumes:
  - POST /api/analysis/news-angle  — Groq JSON editorial angle per candidate
  - GET  /api/wapulse/snapshot      — market context aggregator (used in Bloque 6b)
"""
import logging
from typing import Optional

import requests

from . import config

log = logging.getLogger("wastake-client")

# news-angle has a server-side 15s Groq timeout; we add headroom for network.
HTTP_TIMEOUT_SEC = 25


def _base() -> str:
    return config.WASTAKE_API_URL.rstrip("/")


def get_news_angle(
    title: str,
    summary: str = "",
    link: str = "",
    tickers: Optional[list[str]] = None,
    lang: str = "es",
) -> dict:
    """POST /api/analysis/news-angle. Raises on non-2xx."""
    body = {
        "title":   title,
        "summary": summary,
        "link":    link,
        "tickers": tickers or [],
        "lang":    lang,
    }
    resp = requests.post(
        f"{_base()}/api/analysis/news-angle",
        json=body,
        timeout=HTTP_TIMEOUT_SEC,
        headers={"Accept": "application/json"},
    )
    resp.raise_for_status()
    return resp.json()


def get_snapshot() -> dict:
    """GET /api/wapulse/snapshot. Cached server-side 2 min."""
    resp = requests.get(
        f"{_base()}/api/wapulse/snapshot",
        timeout=HTTP_TIMEOUT_SEC,
        headers={"Accept": "application/json"},
    )
    resp.raise_for_status()
    return resp.json()


def get_visual_subject(name: str) -> Optional[dict]:
    """POST /api/analysis/visual-subject.

    Asks the backend (LLM-backed, with Supabase cache) how to visualize an
    unknown entity that doesn't match anything in the local _SUBJECTS catalog.
    Returns the parsed JSON or None on any failure -- callers should fall
    back to generic visuals when None.

    Example response:
      {
        "name": "concentrix",
        "display": "Concentrix",
        "description": {
          "industry": "BPO services",
          "category": "company",
          "visual_description": "modern open-plan customer service center, ...",
          "color_palette": "deep blue and silver"
        },
        "cached": true,
        "provider": "cache"
      }
    """
    if not name or not name.strip():
        return None
    try:
        resp = requests.post(
            f"{_base()}/api/analysis/visual-subject",
            json={"name": name.strip()},
            timeout=HTTP_TIMEOUT_SEC,
            headers={"Accept": "application/json"},
        )
        if resp.status_code != 200:
            log.debug("visual-subject HTTP %s for %r", resp.status_code, name)
            return None
        return resp.json()
    except Exception as e:
        log.debug("visual-subject call failed for %r: %s", name, e)
        return None
