"""
Worker task: fetch one source.

Week 6 functionality preserved:
    - Clean Scrapy subprocess through engine.runner
    - ETag / Last-Modified incremental crawling
    - Whole-job Celery retries
    - Exponential retry delays
    - Permanent failure handling
    - Dead-letter JSONL logging
    - Redis source state
    - Scheduler lock release
    - 304 / unchanged reporting

Week 7 Stage 3:
    - Interrupted crawl detection
    - Recovery-aware crawl starts
    - Celery retries are NOT incorrectly counted as crash recovery

Week 8:
    - Resume unfinished URLs from the durable Redis crawl checkpoint

Important recovery distinction:

    Celery retry:
        self.request.retries > 0
        The same logical task is still active.
        Do NOT create another recovery attempt.

    Worker/process interruption:
        Celery redelivers the task with retries == 0.
        Redis still contains in_progress=1.
        This is treated as a recovery.
"""

import json
import logging
import os
import time
import redis

from engine.celery_app import app
from engine.runner import (
    JOB_RETRY_BASE_DELAY,
    MAX_JOB_RETRIES,
    PROJECT_ROOT,
    PermanentCrawlError,
    run_crawl,
)
from engine.state import CrawlState


logger = logging.getLogger(__name__)

state = CrawlState()

DEAD_LETTER_LOG = os.path.join(
    PROJECT_ROOT,
    "crawl_output",
    "dead_letter.jsonl",
)

RECOVERY_EVENT_LOG = os.path.join(
    PROJECT_ROOT,
    "crawl_output",
    "recovery_events.jsonl",
)
MAX_RECOVERY_ATTEMPTS = 3


@app.task(
    bind=True,
    max_retries=MAX_JOB_RETRIES,
    ignore_result=False,
)
def crawl_source(self, source: dict):

    name = source["name"]
    redis_available = True

    # ==============================================================
    # WEEK 7 RECOVERY-AWARE START
    # ==============================================================

    celery_retry_attempt = self.request.retries

    try:
        # A fresh Celery task with an existing in_progress state means
        # the previous worker/process may have been interrupted.
        #
        # A Celery retry (retries > 0) is NOT considered a new recovery.
        recovering = (
            celery_retry_attempt == 0
            and state.needs_recovery(name)
        )
    except redis.RedisError:
        redis_available = False
        recovering = False
        logger.error(
            "Redis unavailable for source '%s'; "
            "running a clean crawl without checkpoint recovery",
            name,
        )

    resume_progress = (
        recovering
        or celery_retry_attempt > 0
    )

    if not resume_progress and redis_available:
        try:
            state.clear_crawl_checkpoint(name)
        except redis.RedisError:
            redis_available = False

    try:
        run_id = (
            state.mark_started(
                name,
                recovered=recovering,
            )
            if redis_available
            else None
        )
    except redis.RedisError:
        redis_available = False
        run_id = None
        logger.error(
            "Redis unavailable while starting source '%s'; "
            "checkpoint recovery is disabled",
            name,
        )

    if recovering:
        try:
            recovery_decision = state.recovery_allowed(
                name,
                MAX_RECOVERY_ATTEMPTS,
            )
            if (
                isinstance(recovery_decision, tuple)
                and len(recovery_decision) == 2
            ):
                recovery_allowed, recovery_reason = recovery_decision
            else:
                recovery_allowed = True
                recovery_reason = "eligible"
        except redis.RedisError:
            recovery_allowed = False
            recovery_reason = "redis unavailable"

        if not recovery_allowed:
            _append_recovery_event(
                source,
                run_id,
                "skipped",
                recovery_reason,
            )
            logger.warning(
                "RECOVERY SKIPPED: source '%s' (%s)",
                name,
                recovery_reason,
            )
            try:
                state.mark_finished(name)
                state.release_lock(name)
            except redis.RedisError:
                pass
            return {
                "source": name,
                "status": "recovery_skipped",
                "reason": recovery_reason,
                "crawl_run_id": run_id,
                "recovered": False,
            }

        logger.warning(
            "RECOVERY: resuming interrupted crawl for source "
            "'%s' (crawl_run_id=%s)",
            name,
            run_id,
        )

        try:
            state.mark_recovery_pending(
                name,
                "worker/process interruption; crawl redelivered",
            )
        except redis.RedisError:
            redis_available = False
            resume_progress = False

        _append_recovery_event(
            source,
            run_id,
            "started",
            "worker/process interruption; resuming valid checkpoint",
        )

        try:
            state.get_crawl_checkpoint(name)
        except (ValueError, json.JSONDecodeError) as exc:
            logger.error(
                "RECOVERY RESET: source '%s' has an invalid checkpoint "
                "(%s); clearing it and restarting a clean crawl",
                name,
                exc,
            )
            try:
                state.clear_crawl_checkpoint(name)
            except redis.RedisError:
                logger.error(
                    "Redis unavailable while clearing malformed "
                    "checkpoint for '%s'",
                    name,
                )
            _append_recovery_event(
                source,
                run_id,
                "reset",
                f"invalid checkpoint cleared and clean crawl started: {exc}",
            )
            recovering = False
            resume_progress = False

    else:

        logger.info(
            "Crawl started for source '%s' "
            "(crawl_run_id=%s, celery_retry=%d)",
            name,
            run_id,
            celery_retry_attempt,
        )

    try:

        # ==========================================================
        # INCREMENTAL VALIDATORS
        # ==========================================================

        # Validators from the last successful crawl:
        #
        #   ETag
        #   Last-Modified
        #   previous content hash
        #
        if redis_available:
            try:
                validators = state.get_validators(name)
            except redis.RedisError:
                redis_available = False
                validators = {}
        else:
            validators = {}

        if recovering:
            # A conditional 304 would stop at the seed and prevent the
            # interrupted crawl from rediscovering its child pages.
            # Re-fetch the seed during recovery; retain saved validators
            # in Redis for the next regular incremental crawl.
            validators = {}

        record = run_crawl(
            source,
            validators,
            checkpoint_enabled=redis_available,
            resume_progress=resume_progress,
        )

        # ==========================================================
        # SUCCESS
        # ==========================================================

        if redis_available:
            try:
                state.mark_success(
                    name,
                    record,
                )

                state.mark_finished(
                    name,
                )

                state.release_lock(
                    name,
                )
            except redis.RedisError:
                redis_available = False
                logger.error(
                    "Redis became unavailable after source '%s' completed; "
                    "crawl result remains usable",
                    name,
                )

        unchanged = bool(
            record.get("unchanged")
        )

        logger.info(
            "Crawl succeeded for source '%s'%s",
            name,
            " (unchanged, 304)" if unchanged else "",
        )
        if recovering:
            _append_recovery_event(
                source,
                run_id,
                "succeeded",
                "interrupted crawl resumed successfully",
            )

        return {
            "source": name,
            "status": "success",
            "unchanged": unchanged,
            "http_status": record.get(
                "http_status"
            ),
            "crawl_run_id": run_id,
            "recovered": recovering,
        }

    # ==============================================================
    # PERMANENT FAILURE
    # ==============================================================

    except PermanentCrawlError as exc:

        logger.error(
            "Permanent failure for '%s' "
            "(not retrying): %s",
            name,
            exc,
        )

        return _dead_letter(
            source,
            exc,
            crawl_run_id=run_id,
            recovered=recovering,
        )

    # ==============================================================
    # TEMPORARY / UNKNOWN FAILURE
    # ==============================================================

    except Exception as exc:

        attempt = self.request.retries

        # ----------------------------------------------------------
        # CELERY RETRY
        # ----------------------------------------------------------

        if attempt < MAX_JOB_RETRIES:

            delay = (
                JOB_RETRY_BASE_DELAY
                * (2 ** attempt)
            )

            try:
                state.mark_retry(
                    name,
                    str(exc),
                )
            except redis.RedisError:
                logger.error(
                    "Redis unavailable while recording retry for '%s'",
                    name,
                )

            if recovering:
                _append_recovery_event(
                    source,
                    run_id,
                    "retrying",
                    f"celery retry {attempt + 1}/{MAX_JOB_RETRIES}: {exc}",
                )

            logger.warning(
                "Crawl job failed for '%s' "
                "(attempt %d/%d), "
                "retrying in %ds: %s",
                name,
                attempt + 1,
                MAX_JOB_RETRIES,
                delay,
                exc,
            )

            # IMPORTANT:
            #
            # Do NOT mark the crawl finished here.
            # Do NOT release the scheduler lock here.
            #
            # Celery is going to retry the same logical job.
            #
            # This also prevents the retry from being incorrectly
            # interpreted as a worker-crash recovery.
            raise self.retry(
                exc=exc,
                countdown=delay,
            )

        # ----------------------------------------------------------
        # RETRIES EXHAUSTED
        # ----------------------------------------------------------

        return _dead_letter(
            source,
            exc,
            crawl_run_id=run_id,
            recovered=recovering,
        )


def _dead_letter(
    source: dict,
    exc: Exception,
    crawl_run_id: str | None = None,
    recovered: bool = False,
) -> dict:

    name = source["name"]

    _append_recovery_event(
        source,
        crawl_run_id,
        "dead_letter",
        str(exc),
    ) if recovered else None

    # ==============================================================
    # REDIS DEAD-LETTER STATE
    # ==============================================================

    try:
        state.mark_dead_letter(
            name,
            str(exc),
        )
    except redis.RedisError:
        logger.error(
            "Redis unavailable while recording dead letter for '%s'",
            name,
        )

    # ==============================================================
    # JSONL DEAD-LETTER RECORD
    # ==============================================================

    _append_dead_letter(
        source,
        str(exc),
        crawl_run_id=crawl_run_id,
        recovered=recovered,
    )

    # ==============================================================
    # FINISH WORKER EXECUTION
    # ==============================================================

    try:
        state.mark_finished(
            name,
        )

        state.release_lock(
            name,
        )
    except redis.RedisError:
        logger.error(
            "Redis unavailable while finishing source '%s'",
            name,
        )

    logger.error(
        "DEAD-LETTER: source '%s' failed: %s",
        name,
        exc,
    )

    return {
        "source": name,
        "status": "dead_letter",
        "error": str(exc),
        "crawl_run_id": crawl_run_id,
        "recovered": recovered,
    }


def _append_recovery_event(
    source: dict,
    crawl_run_id: str | None,
    outcome: str,
    reason: str,
) -> None:
    try:
        os.makedirs(
            os.path.dirname(RECOVERY_EVENT_LOG),
            exist_ok=True,
        )

        record = {
            "source_name": source["name"],
            "job_id": crawl_run_id,
            "timestamp": time.time(),
            "outcome": outcome,
            "reason": str(reason)[:1500],
        }

        with open(
            RECOVERY_EVENT_LOG,
            "a",
            encoding="utf-8",
        ) as f:
            f.write(
                json.dumps(
                    record,
                    ensure_ascii=False,
                )
                + "\n"
            )
    except OSError as exc:
        logger.error(
            "Could not append recovery event for source '%s': %s",
            source["name"],
            exc,
        )


def _append_dead_letter(
    source: dict,
    reason: str,
    crawl_run_id: str | None = None,
    recovered: bool = False,
) -> None:

    os.makedirs(
        os.path.dirname(
            DEAD_LETTER_LOG
        ),
        exist_ok=True,
    )

    record = {
        "source_name": source["name"],
        "url": source["url"],
        "source_type": source["source_type"],
        "reason": reason[:1500],
        "timestamp": time.time(),
    }

    # Week 7 recovery metadata.
    #
    # These fields are additive; the existing Week 6 fields remain
    # unchanged.
    if crawl_run_id is not None:
        record["crawl_run_id"] = crawl_run_id

    record["recovered"] = recovered

    with open(
        DEAD_LETTER_LOG,
        "a",
        encoding="utf-8",
    ) as f:

        f.write(
            json.dumps(
                record,
                ensure_ascii=False,
            )
            + "\n"
        )