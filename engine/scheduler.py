"""
Scheduler: periodic tick that decides what to crawl next.

Week 7 additions:
- Detect interrupted/stale crawl jobs.
- Allow those sources to resume automatically.
- Keep Redis locking to prevent duplicate jobs.
- Preserve existing source health and backoff behaviour.
"""

import logging
import redis

from celery import shared_task

from engine.runner import lock_ttl_for
from engine.sources import load_sources
from engine.state import CrawlState
from engine.tasks import crawl_source


logger = logging.getLogger(__name__)

state = CrawlState()


@shared_task(ignore_result=True)
def check_and_schedule():
    scheduled = []

    try:
        sources = [
            source
            for source in load_sources(strict=False)
            if source.get("enabled", True)
        ]
    except Exception:
        logger.exception(
            "Could not load sources.yaml; nothing scheduled this tick"
        )
        return {"scheduled": scheduled}

    source_names = [source["name"] for source in sources]
    try:
        snapshot = state.snapshot(source_names)
    except redis.RedisError:
        logger.exception(
            "Redis unavailable while reading source health; "
            "no recovery jobs scheduled this tick"
        )
        return {"scheduled": scheduled}

    for source in sources:
        name = source["name"]
        priority = source["priority"]
        interval_minutes = source["interval_minutes"]

        source_state = snapshot.get(name, {})
        ttl = lock_ttl_for(source)

        # -------------------------------------------------------------- #
        # Week 7 recovery:
        # If the previous worker disappeared, its in_progress flag may
        # still exist. Once the flag becomes stale, allow the crawl to
        # resume normally.
        # -------------------------------------------------------------- #
        was_in_progress = source_state.get("in_progress") == "1"
        stale_in_progress = state.stale_in_progress_from(
            source_state,
            ttl,
        )

        if was_in_progress:
            if not stale_in_progress:
                logger.debug(
                    "Skipping '%s': crawl still in progress",
                    name,
                )
                continue

            recovery_decision = state.recovery_allowed_from(
                source_state,
            )
            if (
                isinstance(recovery_decision, tuple)
                and len(recovery_decision) == 2
            ):
                recovery_allowed, recovery_reason = recovery_decision
            else:
                recovery_allowed = True
                recovery_reason = "eligible"
            if not recovery_allowed:
                logger.warning(
                    "Skipping recovery for stale crawl '%s': %s",
                    name,
                    recovery_reason,
                )
                continue

            logger.warning(
                "Detected interrupted/stale crawl for '%s'; "
                "allowing automatic recovery",
                name,
            )

        # -------------------------------------------------------------- #
        # Normal due check.
        # Existing failure backoff remains active.
        # -------------------------------------------------------------- #
        # A stale crawl must be resumed immediately rather than waiting
        # for the next regular interval. Leave the marker in Redis so
        # the worker can identify this as a recovery when it starts.
        if not stale_in_progress and not state.due_from(
            source_state,
            interval_minutes,
        ):
            continue

        # -------------------------------------------------------------- #
        # Redis lock prevents two scheduler ticks from creating
        # duplicate crawl jobs for the same source.
        # -------------------------------------------------------------- #
        if not state.acquire_lock(
            name,
            ttl=ttl
        ):
            logger.debug(
                "Skipping '%s': already claimed",
                name,
            )
            continue

        try:
            crawl_source.apply_async(
                args=[source],
                queue=priority,
            )

        except Exception:
            state.release_lock(name)

            logger.exception(
                "Failed to enqueue '%s'; lock released",
                name,
            )

            continue

        scheduled.append(name)

        logger.info(
            "Scheduled crawl for '%s' on queue '%s'",
            name,
            priority,
        )

    return {
        "scheduled": scheduled
    }