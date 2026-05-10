"""Environment + configuration loader for WazPulse PulseEngine."""
import os
from dotenv import load_dotenv

load_dotenv()

# --- Supabase (required from Bloque 4) ---
SUPABASE_URL         = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

# --- WaStake backend (required from Bloque 6) ---
WASTAKE_API_URL = os.getenv(
    "WASTAKE_API_URL",
    "https://wastake-backend-production.up.railway.app",
)

# --- Groq (optional fallback) ---
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

# --- Telegram (required from Bloque 7) ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

# --- TikTok Content Posting API (Bloque 8c) ---
# Optional: publisher skips gracefully if not set.
TIKTOK_CLIENT_KEY    = os.getenv("TIKTOK_CLIENT_KEY")
TIKTOK_CLIENT_SECRET = os.getenv("TIKTOK_CLIENT_SECRET")
TIKTOK_REFRESH_TOKEN = os.getenv("TIKTOK_REFRESH_TOKEN")
TIKTOK_OPEN_ID       = os.getenv("TIKTOK_OPEN_ID")

# --- Make.com webhook (Bloque 8a - Twitter via Make.com) ---
# Optional: publisher skips gracefully if not set (Twitter paused).
MAKE_WEBHOOK_URL = os.getenv("MAKE_WEBHOOK_URL")

# --- Tunables ---
CYCLE_INTERVAL_SECONDS = int(os.getenv("CYCLE_INTERVAL_SECONDS", "300"))
LOG_LEVEL              = os.getenv("LOG_LEVEL", "INFO").upper()
BLOQUE_ACTUAL          = int(os.getenv("BLOQUE_ACTUAL", "4"))


def assert_required_for_bloque(bloque: int) -> None:
    """Fail fast if a hard-required env var is missing at startup.

    MAKE_WEBHOOK_URL and TIKTOK_* are intentionally excluded:
    both publishers check for their own vars at cycle time and skip
    gracefully -- no reason to block startup if only one platform is live.
    """
    required: dict[str, str | None] = {}

    if bloque >= 4:
        required["SUPABASE_URL"]         = SUPABASE_URL
        required["SUPABASE_SERVICE_KEY"] = SUPABASE_SERVICE_KEY

    if bloque >= 6:
        required["WASTAKE_API_URL"] = WASTAKE_API_URL

    if bloque >= 7:
        required["TELEGRAM_BOT_TOKEN"] = TELEGRAM_BOT_TOKEN
        required["TELEGRAM_CHAT_ID"]   = TELEGRAM_CHAT_ID

    # Bloque 8 publishers (Twitter, TikTok) self-guard -- not checked here.

    missing = [k for k, v in required.items() if not v]
    if missing:
        raise RuntimeError(
            f"Missing required env vars for Bloque {bloque}: {', '.join(missing)}"
        )
