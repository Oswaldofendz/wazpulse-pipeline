"""
WazPulse PulseEngine -- entry point.

Behaviour per BLOQUE_ACTUAL env var:
  - 4: just connect to Supabase and log row counts (sanity check)
  - 5+: run the RSS fetcher, write new candidates to pulse_candidates

The tick() loop catches all exceptions so a single bad cycle never crashes
the container. Watch Deploy Logs for ERROR lines -- Railway "Active" badge
does NOT mean cycles are succeeding.

Telegram callbacks (button presses) are processed every CALLBACK_POLL_SEC
seconds during the inter-cycle sleep so the bot responds within ~10s of a
button press -- well inside Telegram's 30s answerCallbackQuery window. The
full pipeline tick still runs every CYCLE_INTERVAL_SECONDS.
"""
import logging
import signal
import sys
import time

from . import config, rss_fetcher, editorial_generator, telegram_bot, twitter_publisher, tiktok_publisher, storage_cleanup
from .supabase_client import count_candidates, count_sources_active

logging.basicConfig(
    level=config.LOG_LEVEL,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("pulse-engine")

_shutdown = False

# How often to poll Telegram for button callbacks during the inter-cycle sleep.
# Must be < 30s (Telegram's answerCallbackQuery timeout).
CALLBACK_POLL_SEC = 10


def _handle_sigterm(signum, _frame):
    global _shutdown
    log.info("received signal %d -- draining current cycle then exiting", signum)
    _shutdown = True


def _tick_bloque4(cycle_n):
    candidates = count_candidates()
    sources    = count_sources_active()
    log.info(
        "cycle %d sanity -- pulse_candidates=%d, active_sources=%d",
        cycle_n, candidates, sources,
    )


def _tick_bloque5(cycle_n):
    log.info("cycle %d starting RSS fetch", cycle_n)
    stats = rss_fetcher.run_one_cycle()
    log.info(
        "cycle %d RSS done -- sources=%d ok=%d errors=%d fetched=%d NEW=%d dup=%d old=%d",
        cycle_n,
        stats["sources"], stats["source_ok"], stats["source_errors"],
        stats["fetched"], stats["new"], stats["skipped_dup"], stats["skipped_old"],
    )


def _tick_bloque6(cycle_n):
    _tick_bloque5(cycle_n)
    log.info("cycle %d starting editorial generation", cycle_n)
    stats = editorial_generator.run_one_cycle()
    log.info(
        "cycle %d editorial done -- picked=%d generated=%d errors=%d",
        cycle_n, stats["pending_picked"], stats["generated"], stats["errors"],
    )
    log.info("cycle %d card backfill", cycle_n)
    bf = editorial_generator.backfill_cards()
    log.info(
        "cycle %d backfill done -- picked=%d generated=%d errors=%d",
        cycle_n, bf["picked"], bf["generated"], bf["errors"],
    )


def _tick_bloque7(cycle_n):
    _tick_bloque6(cycle_n)
    log.info("cycle %d starting Telegram send step", cycle_n)
    stats = telegram_bot.send_pending()
    log.info(
        "cycle %d Telegram send done -- pool=%d eligible=%d sent=%d errors=%d",
        cycle_n,
        stats["pool"], stats["eligible"], stats["sent"], stats["send_errors"],
    )


def _tick_bloque8(cycle_n):
    _tick_bloque7(cycle_n)
    # 4a: Twitter publisher (paused if MAKE_WEBHOOK_URL unset)
    log.info("cycle %d starting Twitter publisher step", cycle_n)
    tw = twitter_publisher.run_one_cycle()
    log.info(
        "cycle %d Twitter done -- eligible=%d published=%d skipped_no_card=%d errors=%d",
        cycle_n,
        tw["eligible"], tw["published"], tw["skipped_no_card"], tw["errors"],
    )
    # 4c: TikTok publisher -- photo carousel
    log.info("cycle %d starting TikTok publisher step", cycle_n)
    tt = tiktok_publisher.run_one_cycle()
    log.info(
        "cycle %d TikTok done -- eligible=%d published=%d generated_carousel=%d "
        "skipped_no_card=%d errors=%d",
        cycle_n,
        tt["eligible"], tt["published"], tt["generated_carousel"],
        tt["skipped_no_card"], tt["errors"],
    )


def _poll_callbacks():
    """Quick callback poll -- runs every CALLBACK_POLL_SEC during inter-cycle sleep."""
    if config.BLOQUE_ACTUAL < 7:
        return
    try:
        stats = telegram_bot.process_callbacks()
        if stats.get("approved") or stats.get("rejected") or stats.get("skipped"):
            log.info(
                "callback-poll: approved=%d rejected=%d skipped=%d",
                stats["approved"], stats["rejected"], stats["skipped"],
            )
    except Exception as e:
        log.debug("callback-poll error (non-fatal): %s", e)


def tick(cycle_n):
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

    # Daily maintenance — gated internally by pulse_state.storage_cleanup_last_run
    # so it actually runs at most once every 24h regardless of cycle count.
    # Safe to call every tick; idempotent and error-swallowing.
    if config.BLOQUE_ACTUAL >= 5:
        storage_cleanup.run_if_due()


def main():
    config.assert_required_for_bloque(config.BLOQUE_ACTUAL)
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT,  _handle_sigterm)

    log.info(
        "WazPulse PulseEngine starting -- bloque=%d, interval=%ds, wastake_api=%s",
        config.BLOQUE_ACTUAL, config.CYCLE_INTERVAL_SECONDS, config.WASTAKE_API_URL,
    )

    cycle_n = 0
    while not _shutdown:
        cycle_n += 1
        tick(cycle_n)
        for i in range(config.CYCLE_INTERVAL_SECONDS):
            if _shutdown:
                break
            if i > 0 and i % CALLBACK_POLL_SEC == 0:
                _poll_callbacks()
            time.sleep(1)

    log.info("PulseEngine shut down cleanly after %d cycles", cycle_n)


if __name__ == "__main__":
    main()
