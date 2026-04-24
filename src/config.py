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

# --- Groq (required from Bloque 6, for fallbacks; primary calls go through WaStake backend) ---
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

# --- Telegram (required from Bloque 7) ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

# --- Tunables ---
CYCLE_INTERVAL_SECONDS = int(os.getenv("CYCLE_INTERVAL_SECONDS", "300"))
LOG_LEVEL              = os.getenv("LOG_LEVEL", "INFO").upper()


def assert_required_for_bloque(bloque: int) -> None:
    """Fail fast if an env var required for the current bloque is missing."""
    required: dict[str, str | None] = {}

    # Bloque 4 — scaffold + Supabase connection
    if bloque >= 4:
        required["SUPABASE_URL"]         = SUPABASE_URL
        required["SUPABASE_SERVICE_KEY"] = SUPABASE_SERVICE_KEY

    # Bloque 6 — editorial generator
    if bloque >= 6:
        required["WASTAKE_API_URL"] = WASTAKE_API_URL

    # Bloque 7 — Telegram bot
    if bloque >= 7:
        required["TELEGRAM_BOT_TOKEN"] = TELEGRAM_BOT_TOKEN
        required["TELEGRAM_CHAT_ID"]   = TELEGRAM_CHAT_ID

    missing = [k for k, v in required.items() if not v]
    if missing:
        raise RuntimeError(
            f"Missing required env vars for Bloque {bloque}: {', '.join(missing)}"
        )
