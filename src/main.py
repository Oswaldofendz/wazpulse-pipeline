"""
WazPulse PulseEngine — entry point.

Behaviour per BLOQUE_ACTUAL env var:
  - 4: just connect to Supabase and log row counts (sanity check)
  - 5+: run the RSS fetcher, write new candidates to pulse_candidates

The tick() loop catches all exceptions so a single bad cycle never crashes
the container. Watch Deploy Logs for ERROR lines — Railway "Active" badge
does NOT mean cycles are succeeding.
"""
import logging
import signal
import sys
import time

from . import config, rss_fetcher
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


def _tick_bloque4(cycle_n: int) -> None:
    candidates = count_candidates()
    sources    = count_sources_active()
    log.info(
        "cycle %d sanity — pulse_candidates=%d, active_sources=%d",
        cycle_n, candidates, sources,
    )


def _tick_bloque5(cycle_n: int) -> None:
    log.info("cycle %d starting RSS fetch", cycle_n)
    stats = rss_fetcher.run_one_cycle()
    log.info(
        "cycle %d done — sources=%d ok=%d errors=%d fetched=%d NEW=%d dup=%d old=%d",
        cycle_n,
        stats["sources"], stats["source_ok"], stats["source_errors"],
        stats["fetched"], stats["new"], stats["skipped_dup"], stats["skipped_old"],
    )


def tick(cycle_n: int) -> None:
    """Dispatch one cycle of work based on BLOQUE_ACTUAL."""
    try:
        if config.BLOQUE_ACTUAL >= 5:
            _tick_bloque5(cycle_n)
        else:
            _tick_bloque4(cycle_n)
    except Exception as e:
        log.exception("cycle %d failed: %s", cycle_n, e)


def main() -> None:
    config.assert_required_for_bloque(config.BLOQUE_ACTUAL)
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT,  _handle_sigterm)

    log.info(
        "WazPulse PulseEngine starting — bloque=%d, interval=%ds, wastake_api=%s",
        config.BLOQUE_ACTUAL, config.CYCLE_INTERVAL_SECONDS, config.WASTAKE_API_URL,
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
