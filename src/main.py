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

from . import config, rss_fetcher, editorial_generator, telegram_bot, twitter_publisher
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
        "cycle %d RSS done — sources=%d ok=%d errors=%d fetched=%d NEW=%d dup=%d old=%d",
        cycle_n,
        stats["sources"], stats["source_ok"], stats["source_errors"],
        stats["fetched"], stats["new"], stats["skipped_dup"], stats["skipped_old"],
    )


def _tick_bloque6(cycle_n: int) -> None:
    # 1) RSS fetch first — keeps the candidate inbox fresh.
    _tick_bloque5(cycle_n)
    # 2) Editorial generation on pending candidates.
    log.info("cycle %d starting editorial generation", cycle_n)
    stats = editorial_generator.run_one_cycle()
    log.info(
        "cycle %d editorial done — picked=%d generated=%d errors=%d",
        cycle_n, stats["pending_picked"], stats["generated"], stats["errors"],
    )
    # 3) Card backfill — generate images for posts that still don't have one.
    #    LLM-independent: drains the historical backlog while news-angle is rate-limited.
    log.info("cycle %d card backfill", cycle_n)
    bf = editorial_generator.backfill_cards()
    log.info(
        "cycle %d backfill done — picked=%d generated=%d errors=%d",
        cycle_n, bf["picked"], bf["generated"], bf["errors"],
    )


def _tick_bloque7(cycle_n: int) -> None:
    # 1+2: RSS + editorial (existing).
    _tick_bloque6(cycle_n)
    # 3: Telegram approval bot (send pending + process callbacks).
    log.info("cycle %d starting Telegram bot step", cycle_n)
    stats = telegram_bot.run_one_cycle()
    log.info(
        "cycle %d Telegram done — sent=%d (eligible=%d) | callbacks: approved=%d rejected=%d skipped=%d",
        cycle_n,
        stats["send"]["sent"], stats["send"]["eligible"],
        stats["callbacks"]["approved"], stats["callbacks"]["rejected"], stats["callbacks"]["skipped"],
    )


def _tick_bloque8(cycle_n: int) -> None:
    # 1+2+3: RSS + editorial + Telegram (existing).
    _tick_bloque7(cycle_n)
    # 4: Twitter publisher — post approved cards.
    log.info("cycle %d starting Twitter publisher step", cycle_n)
    tw = twitter_publisher.run_one_cycle()
    log.info(
        "cycle %d Twitter done — eligible=%d published=%d skipped_no_card=%d errors=%d",
        cycle_n,
        tw["eligible"], tw["published"], tw["skipped_no_card"], tw["errors"],
    )


def tick(cycle_n: int) -> None:
    """Dispatch one cycle of work based on BLOQUE_ACTUAL."""
    try:
        if config.BLOQUE_ACTUAL >= 8:
            _tick_bloque8(cycle_n)
        elif config.BLOQUE_ACTUAL >= 7:
            _tick_bloque7(cycle_n)
        elif config.BLOQUE_ACTUAL >= 6:
            _tick_bloque6(cycle_n)
        elif config.BLOQUE_ACTUAL >= 5:
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
