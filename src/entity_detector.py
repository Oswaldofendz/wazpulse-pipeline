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
    # ─── Semiconductors ────────────────────────────────────────────────────
    {
        "id": "intel",
        "type": "company",
        "display": "Intel",
        "patterns": [r"\bintel\b", r"\bintc\b"],
        "logo_url": "https://logo.clearbit.com/intel.com",
    },
    {
        "id": "amd",
        "type": "company",
        "display": "AMD",
        "patterns": [r"\bamd\b", r"\badvanced\s+micro\s+devices\b"],
        "logo_url": "https://logo.clearbit.com/amd.com",
    },
    {
        "id": "qualcomm",
        "type": "company",
        "display": "Qualcomm",
        "patterns": [r"\bqualcomm\b", r"\bqcom\b"],
        "logo_url": "https://logo.clearbit.com/qualcomm.com",
    },
    {
        "id": "tsmc",
        "type": "company",
        "display": "TSMC",
        "patterns": [r"\btsmc\b", r"\btaiwan\s+semi", r"\btsm\b"],
        "logo_url": "https://logo.clearbit.com/tsmc.com",
    },
    {
        "id": "asml",
        "type": "company",
        "display": "ASML",
        "patterns": [r"\basml\b"],
        "logo_url": "https://logo.clearbit.com/asml.com",
    },
    {
        "id": "micron",
        "type": "company",
        "display": "Micron",
        "patterns": [r"\bmicron\b", r"\bmu\b\s+(stock|shares|earnings)"],
        "logo_url": "https://logo.clearbit.com/micron.com",
    },
    {
        "id": "broadcom",
        "type": "company",
        "display": "Broadcom",
        "patterns": [r"\bbroadcom\b", r"\bavgo\b"],
        "logo_url": "https://logo.clearbit.com/broadcom.com",
    },
    # ─── EV / automotive ───────────────────────────────────────────────────
    {
        "id": "rivian",
        "type": "company",
        "display": "Rivian",
        "patterns": [r"\brivian\b", r"\brivn\b"],
        "logo_url": "https://logo.clearbit.com/rivian.com",
    },
    {
        "id": "lucid",
        "type": "company",
        "display": "Lucid",
        "patterns": [r"\blucid\s+motors?\b", r"\blcid\b"],
        "logo_url": "https://logo.clearbit.com/lucidmotors.com",
    },
    {
        "id": "ford",
        "type": "company",
        "display": "Ford",
        "patterns": [r"\bford\b(?!\s+focus)", r"\bf\b\s+(stock|shares|earnings)"],
        "logo_url": "https://logo.clearbit.com/ford.com",
    },
    {
        "id": "gm",
        "type": "company",
        "display": "General Motors",
        "patterns": [r"\bgeneral\s+motors\b", r"\bgm\b\s+(stock|shares|earnings)"],
        "logo_url": "https://logo.clearbit.com/gm.com",
    },
    {
        "id": "byd",
        "type": "company",
        "display": "BYD",
        "patterns": [r"\bbyd\b"],
        "logo_url": "https://logo.clearbit.com/byd.com",
    },
    {
        "id": "nio",
        "type": "company",
        "display": "Nio",
        "patterns": [r"\bnio\b"],
        "logo_url": "https://logo.clearbit.com/nio.com",
    },
    {
        "id": "toyota",
        "type": "company",
        "display": "Toyota",
        "patterns": [r"\btoyota\b", r"\btm\b\s+(stock|shares|earnings)"],
        "logo_url": "https://logo.clearbit.com/toyota.com",
    },
    # ─── Crypto / Fintech / Payments ───────────────────────────────────────
    {
        "id": "moonpay",
        "type": "company",
        "display": "MoonPay",
        "patterns": [r"\bmoonpay\b"],
        "logo_url": "https://logo.clearbit.com/moonpay.com",
    },
    {
        "id": "stripe",
        "type": "company",
        "display": "Stripe",
        "patterns": [r"\bstripe\b"],
        "logo_url": "https://logo.clearbit.com/stripe.com",
    },
    {
        "id": "paypal",
        "type": "company",
        "display": "PayPal",
        "patterns": [r"\bpaypal\b", r"\bpypl\b"],
        "logo_url": "https://logo.clearbit.com/paypal.com",
    },
    {
        "id": "square-block",
        "type": "company",
        "display": "Block",
        "patterns": [r"\bblock\s+inc\b", r"\bsquare\b\s+(?:inc|payments)", r"\bsq\b\s+(stock|shares|earnings)"],
        "logo_url": "https://logo.clearbit.com/block.xyz",
    },
    {
        "id": "visa",
        "type": "company",
        "display": "Visa",
        "patterns": [r"\bvisa\b(?!\s*(?:gift|prepaid))", r"\bv\b\s+(stock|shares|earnings)"],
        "logo_url": "https://logo.clearbit.com/visa.com",
    },
    {
        "id": "mastercard",
        "type": "company",
        "display": "Mastercard",
        "patterns": [r"\bmastercard\b", r"\bma\b\s+(stock|shares|earnings)"],
        "logo_url": "https://logo.clearbit.com/mastercard.com",
    },
    {
        "id": "kraken",
        "type": "company",
        "display": "Kraken",
        "patterns": [r"\bkraken\b"],
        "logo_url": "https://logo.clearbit.com/kraken.com",
    },
    {
        "id": "binance",
        "type": "company",
        "display": "Binance",
        "patterns": [r"\bbinance\b(?!\s+coin)"],
        "logo_url": "https://logo.clearbit.com/binance.com",
    },
    # ─── Banks ─────────────────────────────────────────────────────────────
    {
        "id": "bofa",
        "type": "company",
        "display": "Bank of America",
        "patterns": [r"\bbank\s+of\s+america\b", r"\bbac\b\s+(stock|shares|earnings)"],
        "logo_url": "https://logo.clearbit.com/bankofamerica.com",
    },
    {
        "id": "wells-fargo",
        "type": "company",
        "display": "Wells Fargo",
        "patterns": [r"\bwells\s+fargo\b", r"\bwfc\b"],
        "logo_url": "https://logo.clearbit.com/wellsfargo.com",
    },
    {
        "id": "citi",
        "type": "company",
        "display": "Citigroup",
        "patterns": [r"\bcitigroup\b", r"\bciti\b(?!\s+bike)"],
        "logo_url": "https://logo.clearbit.com/citigroup.com",
    },
    {
        "id": "morgan-stanley",
        "type": "company",
        "display": "Morgan Stanley",
        "patterns": [r"\bmorgan\s+stanley\b"],
        "logo_url": "https://logo.clearbit.com/morganstanley.com",
    },
    {
        "id": "blackrock",
        "type": "company",
        "display": "BlackRock",
        "patterns": [r"\bblackrock\b", r"\bblk\b\s+(stock|shares|earnings)"],
        "logo_url": "https://logo.clearbit.com/blackrock.com",
    },
    {
        "id": "berkshire",
        "type": "company",
        "display": "Berkshire Hathaway",
        "patterns": [r"\bberkshire\b", r"\bbrk\.?[ab]?\b", r"\bbuffett\b"],
        "logo_url": "https://logo.clearbit.com/berkshirehathaway.com",
    },
    {
        "id": "santander",
        "type": "company",
        "display": "Santander",
        "patterns": [r"\bsantander\b", r"\bsan\b\s+(stock|shares|earnings)"],
        "logo_url": "https://logo.clearbit.com/santander.com",
    },
    # ─── Energy / commodities companies ────────────────────────────────────
    {
        "id": "exxon",
        "type": "company",
        "display": "ExxonMobil",
        "patterns": [r"\bexxon\b", r"\bxom\b"],
        "logo_url": "https://logo.clearbit.com/exxonmobil.com",
    },
    {
        "id": "chevron",
        "type": "company",
        "display": "Chevron",
        "patterns": [r"\bchevron\b", r"\bcvx\b"],
        "logo_url": "https://logo.clearbit.com/chevron.com",
    },
    {
        "id": "shell",
        "type": "company",
        "display": "Shell",
        "patterns": [r"\broyal\s+dutch\s+shell\b", r"\bshell\b(?!\s+game)"],
        "logo_url": "https://logo.clearbit.com/shell.com",
    },
    # ─── Aerospace / Defense ───────────────────────────────────────────────
    {
        "id": "lockheed",
        "type": "company",
        "display": "Lockheed Martin",
        "patterns": [r"\block?heed\b", r"\blmt\b"],
        "logo_url": "https://logo.clearbit.com/lockheedmartin.com",
    },
    {
        "id": "rtx",
        "type": "company",
        "display": "RTX",
        "patterns": [r"\braytheon\b", r"\brtx\b"],
        "logo_url": "https://logo.clearbit.com/rtx.com",
    },
    {
        "id": "airbus",
        "type": "company",
        "display": "Airbus",
        "patterns": [r"\bairbus\b"],
        "logo_url": "https://logo.clearbit.com/airbus.com",
    },
    # ─── Major retailers / consumer ────────────────────────────────────────
    {
        "id": "walmart",
        "type": "company",
        "display": "Walmart",
        "patterns": [r"\bwalmart\b", r"\bwmt\b"],
        "logo_url": "https://logo.clearbit.com/walmart.com",
    },
    {
        "id": "costco",
        "type": "company",
        "display": "Costco",
        "patterns": [r"\bcostco\b", r"\bcost\b\s+(stock|shares|earnings)"],
        "logo_url": "https://logo.clearbit.com/costco.com",
    },
    {
        "id": "disney",
        "type": "company",
        "display": "Disney",
        "patterns": [r"\bdisney\b", r"\bdis\b\s+(stock|shares|earnings)"],
        "logo_url": "https://logo.clearbit.com/disney.com",
    },
    {
        "id": "nike",
        "type": "company",
        "display": "Nike",
        "patterns": [r"\bnike\b", r"\bnke\b\s+(stock|shares|earnings)"],
        "logo_url": "https://logo.clearbit.com/nike.com",
    },
    # ─── Healthcare / pharma ───────────────────────────────────────────────
    {
        "id": "pfizer",
        "type": "company",
        "display": "Pfizer",
        "patterns": [r"\bpfizer\b", r"\bpfe\b"],
        "logo_url": "https://logo.clearbit.com/pfizer.com",
    },
    {
        "id": "moderna",
        "type": "company",
        "display": "Moderna",
        "patterns": [r"\bmoderna\b", r"\bmrna\b"],
        "logo_url": "https://logo.clearbit.com/modernatx.com",
    },
    {
        "id": "novo-nordisk",
        "type": "company",
        "display": "Novo Nordisk",
        "patterns": [r"\bnovo\s+nordisk\b", r"\bozempic\b", r"\bwegovy\b"],
        "logo_url": "https://logo.clearbit.com/novonordisk.com",
    },
    # ─── Industrials / heavy ───────────────────────────────────────────────
    {
        "id": "caterpillar",
        "type": "company",
        "display": "Caterpillar",
        "patterns": [r"\bcaterpillar\b", r"\bcat\b\s+(stock|shares|earnings)"],
        "logo_url": "https://logo.clearbit.com/caterpillar.com",
    },
    {
        "id": "deere",
        "type": "company",
        "display": "John Deere",
        "patterns": [r"\bjohn\s+deere\b", r"\bdeere\b", r"\bde\b\s+(stock|shares|earnings)"],
        "logo_url": "https://logo.clearbit.com/deere.com",
    },
    # ─── AI / cloud ────────────────────────────────────────────────────────
    {
        "id": "palantir",
        "type": "company",
        "display": "Palantir",
        "patterns": [r"\bpalantir\b", r"\bpltr\b"],
        "logo_url": "https://logo.clearbit.com/palantir.com",
    },
    {
        "id": "oracle",
        "type": "company",
        "display": "Oracle",
        "patterns": [r"\boracle\b", r"\borcl\b"],
        "logo_url": "https://logo.clearbit.com/oracle.com",
    },
    {
        "id": "salesforce",
        "type": "company",
        "display": "Salesforce",
        "patterns": [r"\bsalesforce\b", r"\bcrm\b\s+(stock|shares|earnings)"],
        "logo_url": "https://logo.clearbit.com/salesforce.com",
    },
    {
        "id": "ibm",
        "type": "company",
        "display": "IBM",
        "patterns": [r"\bibm\b", r"\binternational\s+business\s+machines\b"],
        "logo_url": "https://logo.clearbit.com/ibm.com",
    },
    {
        "id": "adobe",
        "type": "company",
        "display": "Adobe",
        "patterns": [r"\badobe\b", r"\badbe\b"],
        "logo_url": "https://logo.clearbit.com/adobe.com",
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


def detect_entities(headline: Optional[str], limit: int = 3) -> list[dict]:
    """
    Returns ALL matching entities in headline order (left to right based on the
    earliest pattern match position). Deduplicates by entity id. Capped at
    `limit` entities to avoid pathological matches.

    Used by card_generator to detect comparison/acquisition patterns like
    "Alphabet vs Nvidia" or "MoonPay compra Dawn Labs" — situations where
    we want to render two brands side by side instead of trying (and failing)
    to make an AI model draw both logos.
    """
    if not headline:
        return []
    matches: list[tuple[int, dict]] = []
    seen_ids: set[str] = set()
    for e in ENTITIES:
        if e["id"] in seen_ids:
            continue
        for rx in e["_compiled"]:
            m = rx.search(headline)
            if m:
                matches.append((m.start(), {
                    "id":       e["id"],
                    "type":     e["type"],
                    "display":  e["display"],
                    "logo_url": e.get("logo_url"),
                }))
                seen_ids.add(e["id"])
                break
    matches.sort(key=lambda t: t[0])     # left-to-right order in headline
    return [ent for _, ent in matches[:limit]]


# ─── Intent detection ────────────────────────────────────────────────────────

_INTENT_PATTERNS: list[tuple[str, re.Pattern]] = [
    # comparison — explicit "vs" or "supera a"
    ("comparison", re.compile(
        r"\b(?:vs\.?|versus|frente\s+a|contra)\b"
        r"|\bsupera\s+a\b|\boutperform(?:s|ed)?\b"
        r"|\bcompite\s+con\b|\brivalry?\b"
        r"|\bcould\s+pass\b|\bpodr[ií]a\s+superar\b",
        re.IGNORECASE,
    )),
    # acquisition / M&A
    ("acquisition", re.compile(
        r"\b(?:compra|adquiere|adquiri[oó]|absorbe|absorbi[oó])\b"
        r"|\b(?:acquires?|buys?|bought|takes?\s+over|takeover|merger\s+with)\b"
        r"|\bfusi[oó]n\s+(?:con|de)\b",
        re.IGNORECASE,
    )),
    # personnel — hire/name/fire executives
    ("personnel", re.compile(
        r"\b(?:contrata|contrat[oó]|nombra|nombr[oó]|despide|despidi[oó]|reemplaza|reemplaz[oó])\b"
        r"|\b(?:hires?|hired|names?|named|fires?|fired|appoint(?:s|ed)?|replaces?)\b"
        r"|\bejecutiv[oa]\s+de\b|\bexecutive\s+from\b|\bnew\s+ceo\b|\bnuev[oa]\s+ceo\b",
        re.IGNORECASE,
    )),
    # partnership / alliance
    ("partnership", re.compile(
        r"\b(?:alianza\s+con|se\s+une\s+a|asocia|asociaci[oó]n)\b"
        r"|\b(?:partners?\s+with|partnership|joins\s+forces|teams?\s+up)\b",
        re.IGNORECASE,
    )),
    # product launch
    ("product_launch", re.compile(
        r"\b(?:lanza|presenta|anuncia)\b"
        r"|\b(?:launches?|unveils?|announces?|introduces?|debuts?)\b",
        re.IGNORECASE,
    )),
]


def detect_intent(headline: Optional[str]) -> Optional[str]:
    """
    Returns the FIRST matching intent label, or None.

    Order matters — more specific intents first (comparison/acquisition before
    product_launch) so "Alphabet supera a Nvidia" picks comparison, not the
    weaker "anuncia" pattern that doesn't even exist in this headline anyway.
    """
    if not headline:
        return None
    for label, rx in _INTENT_PATTERNS:
        if rx.search(headline):
            return label
    return None


def detect_news_shape(headline: Optional[str]) -> dict:
    """
    Convenience: returns a structured description of the headline shape.

    Output:
      {
        "entities": [list of detected entity dicts, in headline order],
        "intent":   "comparison" | "acquisition" | "personnel" | ... | None,
        "shape":    "comparison_2e" | "acquisition_2e" | "personnel_1e" |
                    "single_entity" | "macro_no_entity",
      }

    `shape` is the high-level routing key card_generator uses to pick a
    composition template without re-running detection itself.
    """
    entities = detect_entities(headline)
    intent   = detect_intent(headline)

    if intent == "comparison" and len(entities) >= 2:
        shape = "comparison_2e"
    elif intent == "acquisition" and len(entities) >= 2:
        shape = "acquisition_2e"
    elif intent == "partnership" and len(entities) >= 2:
        shape = "partnership_2e"
    elif intent == "personnel" and len(entities) >= 2:
        # "Intel contrata ejecutivo de Qualcomm" — talent flowing between
        # two named companies. Treat as a dual-entity composition.
        shape = "personnel_2e"
    elif intent == "personnel" and len(entities) >= 1:
        shape = "personnel_1e"
    elif intent == "product_launch" and len(entities) >= 1:
        shape = "product_launch_1e"
    elif len(entities) >= 1:
        shape = "single_entity"
    else:
        shape = "macro_no_entity"

    return {"entities": entities, "intent": intent, "shape": shape}


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
