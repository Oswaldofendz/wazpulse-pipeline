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
