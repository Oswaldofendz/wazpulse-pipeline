"""Thin wrapper around supabase-py. Lazy singleton."""
from supabase import create_client, Client

from . import config

_client: Client | None = None


def get_client() -> Client:
    global _client
    if _client is None:
        _client = create_client(config.SUPABASE_URL, config.SUPABASE_SERVICE_KEY)
    return _client


def count_candidates() -> int:
    """Sanity check: total rows in pulse_candidates (any status).

    Uses .limit(1) instead of head=True (head was added in supabase-py 2.10+,
    but requirements.txt pins 2.7.4). Count comes from the Content-Range header
    that Supabase populates when count="exact".
    """
    client = get_client()
    res = client.table("pulse_candidates").select("id", count="exact").limit(1).execute()
    return res.count or 0


def count_sources_active() -> int:
    """Sanity check: active RSS sources in pulse_sources_config."""
    client = get_client()
    res = (
        client.table("pulse_sources_config")
        .select("id", count="exact")
        .eq("is_active", True)
        .limit(1)
        .execute()
    )
    return res.count or 0
