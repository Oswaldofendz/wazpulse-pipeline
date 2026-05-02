"""
Entity detector — Bloque 6c-V2.

Scans a headline and returns the first matching entity (crypto coin, company,
person, or topic). Each entity carries enough metadata for the card generator
to fetch a contextual image and label the card properly.

The detector is conservative: false positives (matching the wrong entity) are
worse than no match, since the generic fallback card is always available.

Adding a new entity = appending one dict to ENTITIES below.
"""
import logging
import re
from typing import Optional

log = logging.getLogger("entity-detector")


# Order matters: longer/more specific names first to avoid sub-string false
# positives (e.g. "metaverse" should NOT match before "Meta Platforms").
ENTITIES: list[dict] = [
    # ─── Crypto ────────────────────────────────────────────────────────────
    {
        "id": "bitcoin",
        "type": "crypto",
        "display": "BTC",
        "patterns": [r"\bbitcoin\b", r"\bbtc\b", r"satoshi"],
        "logo_url": "https://cryptologos.cc/logos/bitcoin-btc-logo.png",
    },
    {
        "id": "ethereum",
        "type": "crypto",
        "display": "ETH",
        "patterns": [r"\bethereum\b", r"\beth\b", r"vitalik"],
        "logo_url": "https://cryptologos.cc/logos/ethereum-eth-logo.png",
    },
    {
        "id": "solana",
        "type": "crypto",
        "display": "SOL",
        "patterns": [r"\bsolana\b", r"\bsol\b"],
        "logo_url": "https://cryptologos.cc/logos/solana-sol-logo.png",
    },
    {
        "id": "ripple",
        "type": "crypto",
        "display": "XRP",
        "patterns": [r"\bripple\b", r"\bxrp\b"],
        "logo_url": "https://cryptologos.cc/logos/xrp-xrp-logo.png",
    },
    {
        "id": "cardano",
        "type": "crypto",
        "display": "ADA",
        "patterns": [r"\bcardano\b", r"\bada\b"],
        "logo_url": "https://cryptologos.cc/logos/cardano-ada-logo.png",
    },
    {
        "id": "dogecoin",
        "type": "crypto",
        "display": "DOGE",
        "patterns": [r"\bdogecoin\b", r"\bdoge\b"],
        "logo_url": "https://cryptologos.cc/logos/dogecoin-doge-logo.png",
    },
    {
        "id": "binance-coin",
        "type": "crypto",
        "display": "BNB",
        "patterns": [r"\bbnb\b", r"\bbinance coin\b"],
        "logo_url": "https://cryptologos.cc/logos/bnb-bnb-logo.png",
    },
    {
        "id": "polkadot",
        "type": "crypto",
        "display": "DOT",
        "patterns": [r"\bpolkadot\b", r"\bdot\b"],
        "logo_url": "https://cryptologos.cc/logos/polkadot-new-dot-logo.png",
    },
    {
        "id": "avalanche",
        "type": "crypto",
        "display": "AVAX",
        "patterns": [r"\bavalanche\b", r"\bavax\b"],
        "logo_url": "https://cryptologos.cc/logos/avalanche-avax-logo.png",
    },
    {
        "id": "chainlink",
        "type": "crypto",
        "display": "LINK",
        "patterns": [r"\bchainlink\b", r"\blink token\b"],
        "logo_url": "https://cryptologos.cc/logos/chainlink-link-logo.png",
    },

    # ─── Big tech / Magnificent 7 ──────────────────────────────────────────
    {
        "id": "tesla",
        "type": "company",
        "display": "Tesla",
        "patterns": [r"\btesla\b", r"\btsla\b", r"\bcybertruck\b"],
        "logo_url": "https://logo.clearbit.com/tesla.com",
    },
    {
        "id": "apple",
        "type": "company",
        "display": "Apple",
        "patterns": [r"\bapple\b", r"\baapl\b", r"\biphone\b", r"\bipad\b"],
        "logo_url": "https://logo.clearbit.com/apple.com",
    },
    {
        "id": "alphabet",
        "type": "company",
        "display": "Google",
        "patterns": [r"\balphabet\b", r"\bgoogle\b", r"\bgoogl\b", r"\bgoog\b", r"\byoutube\b"],
        "logo_url": "https://logo.clearbit.com/google.com",
    },
    {
        "id": "microsoft",
        "type": "company",
        "display": "Microsoft",
        "patterns": [r"\bmicrosoft\b", r"\bmsft\b", r"\bazure\b", r"\bcopilot\b"],
        "logo_url": "https://logo.clearbit.com/microsoft.com",
    },
    {
        "id": "amazon",
        "type": "company",
        "display": "Amazon",
        "patterns": [r"\bamazon\b", r"\bamzn\b", r"\baws\b"],
        "logo_url": "https://logo.clearbit.com/amazon.com",
    },
    {
        "id": "meta",
        "type": "company",
        "display": "Meta",
        "patterns": [r"\bmeta platforms\b", r"\bmeta\b(?! data)", r"\bfacebook\b", r"\binstagram\b", r"\bwhatsapp\b"],
        "logo_url": "https://logo.clearbit.com/meta.com",
    },
    {
        "id": "nvidia",
        "type": "company",
        "display": "Nvidia",
        "patterns": [r"\bnvidia\b", r"\bnvda\b"],
        "logo_url": "https://logo.clearbit.com/nvidia.com",
    },

    # ─── Other major US listed ─────────────────────────────────────────────
    {
        "id": "netflix",
        "type": "company",
        "display": "Netflix",
        "patterns": [r"\bnetflix\b", r"\bnflx\b"],
        "logo_url": "https://logo.clearbit.com/netflix.com",
    },
    {
        "id": "boeing",
        "type": "company",
        "display": "Boeing",
        "patterns": [r"\bboeing\b", r"\bba\b\s+(stock|shares|earnings)"],
        "logo_url": "https://logo.clearbit.com/boeing.com",
    },
    {
        "id": "jpmorgan",
        "type": "company",
        "display": "JPMorgan",
        "patterns": [r"\bjpmorgan\b", r"\bjpm\b", r"\bjp morgan\b"],
        "logo_url": "https://logo.clearbit.com/jpmorgan.com",
    },
    {
        "id": "goldman",
        "type": "company",
        "display": "Goldman Sachs",
        "patterns": [r"\bgoldman sachs\b", r"\bgoldman\b", r"\bgs\b\s+(stock|shares|earnings)"],
        "logo_url": "https://logo.clearbit.com/goldmansachs.com",
    },
    {
        "id": "robinhood",
        "type": "company",
        "display": "Robinhood",
        "patterns": [r"\brobinhood\b", r"\bhood\b\s+(stock|shares|earnings)"],
        "logo_url": "https://logo.clearbit.com/robinhood.com",
    },
    {
        "id": "coinbase",
        "type": "company",
        "display": "Coinbase",
        "patterns": [r"\bcoinbase\b", r"\bcoin\b\s+(stock|shares|earnings)"],
        "logo_url": "https://logo.clearbit.com/coinbase.com",
    },
    {
        "id": "openai",
        "type": "company",
        "display": "OpenAI",
        "patterns": [r"\bopenai\b", r"\bchatgpt\b", r"\bsam altman\b"],
        "logo_url": "https://logo.clearbit.com/openai.com",
    },
    {
        "id": "anthropic",
        "type": "company",
        "display": "Anthropic",
        "patterns": [r"\banthropic\b", r"\bclaude\b\s+(ai|2|3)"],
        "logo_url": "https://logo.clearbit.com/anthropic.com",
    },
    {
        "id": "uber",
        "type": "company",
        "display": "Uber",
        "patterns": [r"\buber\b", r"\bubereats\b"],
        "logo_url": "https://logo.clearbit.com/uber.com",
    },
    {
        "id": "spotify",
        "type": "company",
        "display": "Spotify",
        "patterns": [r"\bspotify\b", r"\bspot\b\s+(stock|shares|earnings)"],
        "logo_url": "https://logo.clearbit.com/spotify.com",
    },

    # ─── Macro / indices / commodities ─────────────────────────────────────
    {
        "id": "spy",
        "type": "index",
        "display": "S&P 500",
        "patterns": [r"\bs&p ?500\b", r"\bspx\b", r"\bspy\b"],
        "logo_url": None,  # no clean logo for SP500; card uses default visual
    },
    {
        "id": "gc=f",
        "type": "commodity",
        "display": "Oro",
        "patterns": [r"\bgold price\b", r"\bprecio del oro\b", r"\boro\b"],
        "logo_url": None,
    },
]

# Pre-compile patterns once at module load.
for _e in ENTITIES:
    _e["_compiled"] = [re.compile(p, re.IGNORECASE) for p in _e["patterns"]]


def detect_entity(headline: Optional[str]) -> Optional[dict]:
    """
    Returns the first matching entity dict (with id, type, display, logo_url),
    or None if nothing matched.
    """
    if not headline:
        return None
    for e in ENTITIES:
        for rx in e["_compiled"]:
            if rx.search(headline):
                return {
                    "id":       e["id"],
                    "type":     e["type"],
                    "display":  e["display"],
                    "logo_url": e.get("logo_url"),
                }
    return None


def find_by_id(entity_id: Optional[str]) -> Optional[dict]:
    """
    Look up an entity by its ID directly. The card generator uses this when
    pulse_posts.asset_affected was already populated at editorial time — that
    field comes from the ORIGINAL English RSS headline, which preserves the
    entity name even when Groq later rewrites the headline in Spanish.
    """
    if not entity_id:
        return None
    for e in ENTITIES:
        if e["id"] == entity_id:
            return {
                "id":       e["id"],
                "type":     e["type"],
                "display":  e["display"],
                "logo_url": e.get("logo_url"),
            }
    return None
