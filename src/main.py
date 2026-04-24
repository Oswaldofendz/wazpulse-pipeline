"""
WazPulse PulseEngine — entry point.

Bloque 4 scope:
  - Validate env
  - Connect to Supabase with service_role key
  - Every CYCLE_INTERVAL_SECONDS, log a sanity-check cycle ("tick")
  - No RSS, no editorial generation, no Telegram yet

Later bloques wire in the real work inside `tick()`.
"""
import logging
import signal
import sys
import time

from . import config
from .supabase_client import count_candidates, count_sources_active

logging.basicConfig(
    level=config.LOG_LEVEL,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("pulse-engine")

_shutdown = False


def _handle_sigterm(signum, _frame):
    global _shutdown
    log.info("received signal %d — draining current cycle then exiting", signum)
    _shutdown = True


def tick(cycle_n: int) -> None:
    """One cycle of work. For now: log DB sanity."""
    try:
        candidates = count_candidates()
        sources    = count_sources_active()
        log.info(
            "cycle %d tick — pulse_candidates=%d, active_sources=%d",
            cycle_n, candidates, sources,
        )
    except Exception as e:
        # Never crash the loop on a single bad cycle — log and continue.
        log.exception("cycle %d failed: %s", cycle_n, e)


def main() -> None:
    config.assert_required_for_bloque(4)
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT,  _handle_sigterm)

    log.info(
        "WazPulse PulseEngine starting — interval=%ds, wastake_api=%s",
        config.CYCLE_INTERVAL_SECONDS,
        config.WASTAKE_API_URL,
    )

    cycle_n = 0
    while not _shutdown:
        cycle_n += 1
        tick(cycle_n)
        # Sleep in 1s slices so SIGTERM is respected quickly
        for _ in range(config.CYCLE_INTERVAL_SECONDS):
            if _shutdown:
                break
            time.sleep(1)

    log.info("PulseEngine shut down cleanly after %d cycles", cycle_n)


if __name__ == "__main__":
    main()
