import asyncio
import json
import os
import subprocess
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

import redis

from Crawler.spiders.single_source import SingleSourceSpider
from engine.scheduler import check_and_schedule
from engine.runner import CrawlError, run_crawl
from engine.state import CrawlState
from engine.tasks import crawl_source


class MemoryPipeline:
    def __init__(self, redis_client):
        self.redis_client = redis_client
        self.commands = []

    def hset(self, key, field=None, value=None, mapping=None):
        if mapping is not None:
            self.commands.append(
                lambda: self.redis_client.hset(
                    key,
                    mapping=mapping,
                )
            )
        else:
            self.commands.append(
                lambda: self.redis_client.hset(
                    key,
                    field,
                    value,
                )
            )
        return self

    def hdel(self, key, *fields):
        self.commands.append(
            lambda: self.redis_client.hdel(
                key,
                *fields,
            )
        )
        return self

    def hincrby(self, key, field, amount):
        self.commands.append(
            lambda: self.redis_client.hincrby(
                key,
                field,
                amount,
            )
        )
        return self

    def execute(self):
        for command in self.commands:
            command()


class MemoryRedis:
    def __init__(self):
        self.hashes = {}

    def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)

    def hset(self, key, field=None, value=None, mapping=None):
        values = self.hashes.setdefault(key, {})
        if mapping is not None:
            values.update(mapping)
        else:
            values[field] = value

    def hdel(self, key, *fields):
        values = self.hashes.setdefault(key, {})
        for field in fields:
            values.pop(field, None)

    def hmget(self, key, *fields):
        values = self.hashes.get(key, {})
        return [values.get(field) for field in fields]

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def hincrby(self, key, field, amount):
        values = self.hashes.setdefault(key, {})
        values[field] = str(
            int(values.get(field, 0) or 0)
            + amount
        )

    def pipeline(self, transaction=True):
        return MemoryPipeline(self)


class RecoveryStateTests(unittest.TestCase):
    def test_malformed_checkpoint_raises_clear_source_error(self):
        state = CrawlState.__new__(CrawlState)
        state.r = MemoryRedis()
        state.r.hset(
            "crawlstate:source",
            "last_checkpoint",
            "{not-json",
        )

        with self.assertRaisesRegex(
            ValueError,
            "Malformed crawl checkpoint",
        ):
            state.get_crawl_checkpoint("source")

    def test_recovery_event_log_failure_is_nonfatal(self):
        from engine import tasks

        source = {
            "name": "source",
            "url": "https://example.test/",
            "source_type": "validation",
        }

        with patch(
            "builtins.open",
            side_effect=PermissionError(
                "simulated recovery log unwritable"
            ),
        ):
            tasks._append_recovery_event(
                source,
                "job-id",
                "succeeded",
                "test",
            )

    def test_malformed_checkpoint_resets_to_clean_crawl(self):
        source = {
            "name": "source",
            "url": "https://example.test/",
            "source_type": "validation",
        }
        state = Mock()
        state.needs_recovery.return_value = True
        state.mark_started.return_value = "run-id"
        state.recovery_allowed.return_value = (True, "eligible")
        state.get_validators.return_value = {}
        state.get_crawl_checkpoint.side_effect = ValueError(
            "Malformed crawl checkpoint"
        )

        with (
            patch("engine.tasks.state", state),
            patch(
                "engine.tasks.run_crawl",
                return_value={
                    "unchanged": False,
                    "http_status": 200,
                },
            ) as run_crawl,
        ):
            result = crawl_source.apply(args=[source]).get()

        self.assertEqual(result["status"], "success")
        self.assertFalse(result["recovered"])
        state.clear_crawl_checkpoint.assert_called_once_with(
            "source"
        )
        run_crawl.assert_called_once_with(
            source,
            {},
            checkpoint_enabled=True,
            resume_progress=False,
        )

    def test_p1_unhealthy_state_prevents_recovery(self):
        state = CrawlState.__new__(CrawlState)
        state.r = MemoryRedis()
        state.set_p1_health_status("source", "unhealthy")

        allowed, reason = state.recovery_allowed("source")

        self.assertFalse(allowed)
        self.assertIn("p1 health", reason)

    def test_p1_healthy_state_allows_recovery(self):
        state = CrawlState.__new__(CrawlState)
        state.r = MemoryRedis()
        state.set_p1_health_status("source", "healthy")

        allowed, reason = state.recovery_allowed("source")

        self.assertTrue(allowed)
        self.assertEqual(reason, "eligible")

    def test_recovery_limit_is_respected(self):
        allowed, reason = CrawlState.recovery_allowed_from(
            {"recovery_count": "3"},
            max_attempts=3,
        )

        self.assertFalse(allowed)
        self.assertIn("recovery limit reached", reason)

    def test_recovery_event_is_logged_with_required_fields(self):
        from engine import tasks

        with tempfile.TemporaryDirectory() as tmp:
            event_log = os.path.join(tmp, "recovery_events.jsonl")

            with patch.object(tasks, "RECOVERY_EVENT_LOG", event_log):
                tasks._append_recovery_event(
                    {
                        "name": "source",
                        "url": "https://example.test/",
                        "source_type": "validation",
                    },
                    "job-id",
                    "started",
                    "worker/process interruption",
                )

            with open(event_log, "r", encoding="utf-8") as f:
                event = json.loads(f.readline())

        self.assertEqual(event["source_name"], "source")
        self.assertEqual(event["job_id"], "job-id")
        self.assertEqual(event["outcome"], "started")
        self.assertIn("timestamp", event)
        self.assertIn("reason", event)

    def test_temporary_redis_unavailability_runs_clean_crawl(self):
        source = {
            "name": "source",
            "url": "https://example.test/",
            "source_type": "validation",
        }
        state = Mock()
        state.needs_recovery.side_effect = redis.RedisError("redis unavailable")

        with (
            patch("engine.tasks.state", state),
            patch(
                "engine.tasks.run_crawl",
                return_value={
                    "unchanged": False,
                    "http_status": 200,
                },
            ) as run_crawl,
        ):
            result = crawl_source.apply(args=[source]).get()

        self.assertEqual(result["status"], "success")
        run_crawl.assert_called_once_with(
            source,
            {},
            checkpoint_enabled=False,
            resume_progress=False,
        )

    def test_scheduler_redis_outage_schedules_nothing(self):
        state = Mock()
        state.snapshot.side_effect = redis.RedisError("redis unavailable")

        source = {
            "name": "source",
            "url": "https://example.test/",
            "source_type": "validation",
            "priority": "standard",
            "interval_minutes": 30,
            "timeout_seconds": 120,
        }

        with (
            patch("engine.scheduler.state", state),
            patch(
                "engine.scheduler.load_sources",
                return_value=[source],
            ),
        ):
            result = check_and_schedule.run()

        self.assertEqual(result["scheduled"], [])

    def test_malformed_last_crawled_timestamp_is_due(self):
        self.assertTrue(
            CrawlState.due_from(
                {"last_crawled_ts": "not-a-timestamp"},
                interval_minutes=30,
            )
        )

    def test_nonfinite_last_crawled_timestamp_is_due(self):
        self.assertTrue(
            CrawlState.due_from(
                {"last_crawled_ts": "nan"},
                interval_minutes=30,
            )
        )

    def test_hash_is_preserved_without_http_validators(self):
        state = CrawlState.__new__(CrawlState)
        state.r = MemoryRedis()
        state.mark_success(
            "source",
            {
                "etag": None,
                "last_modified": None,
                "content_hash": "abc",
            },
        )

        self.assertEqual(
            state.get_validators("source"),
            {
                "etag": None,
                "last_modified": None,
                "prev_hash": "abc",
            },
        )

    def test_missing_or_invalid_start_time_is_stale(self):
        self.assertTrue(
            CrawlState.stale_in_progress_from(
                {"in_progress": "1"},
                stale_after=60,
            )
        )
        self.assertTrue(
            CrawlState.stale_in_progress_from(
                {
                    "in_progress": "1",
                    "started_ts": "not-a-timestamp",
                },
                stale_after=60,
            )
        )

    def test_fresh_start_is_active_and_old_start_is_stale(self):
        now = time.time()
        self.assertFalse(
            CrawlState.stale_in_progress_from(
                {
                    "in_progress": "1",
                    "started_ts": now - 5,
                },
                stale_after=60,
                now=now,
            )
        )
        self.assertTrue(
            CrawlState.stale_in_progress_from(
                {
                    "in_progress": "1",
                    "started_ts": now - 61,
                },
                stale_after=60,
                now=now,
            )
        )

    def test_only_crash_recovery_increments_recovery_count(self):
        state = CrawlState.__new__(CrawlState)
        state.r = MemoryRedis()
        state.r.hashes["crawlstate:source"] = {
            "in_progress": "1",
            "recovery_count": "2",
            "recovery_pending": "1",
        }

        state.mark_started("source", recovered=False)
        self.assertEqual(
            state.r.hget("crawlstate:source", "recovery_count"),
            "2",
        )
        self.assertEqual(
            state.r.hget("crawlstate:source", "recovery_pending"),
            "1",
        )

        state.mark_started("source", recovered=True)
        self.assertEqual(
            int(
                state.r.hget(
                    "crawlstate:source",
                    "recovery_count",
                )
            ),
            3,
        )

    def test_stale_crawl_is_scheduled_even_when_interval_is_not_due(self):
        source = {
            "name": "source",
            "priority": "standard",
            "interval_minutes": 60,
            "timeout_seconds": 120,
        }
        source_state = {
            "in_progress": "1",
            "started_ts": time.time() - 10_000,
            "last_crawled_ts": time.time(),
        }
        state = Mock()
        state.snapshot.return_value = {"source": source_state}
        state.stale_in_progress_from.side_effect = (
            CrawlState.stale_in_progress_from
        )
        state.due_from.return_value = False
        state.acquire_lock.return_value = True
        task = Mock()

        with (
            patch("engine.scheduler.state", state),
            patch("engine.scheduler.load_sources", return_value=[source]),
            patch("engine.scheduler.crawl_source", task),
        ):
            result = check_and_schedule.run()

        self.assertEqual(result["scheduled"], ["source"])
        state.due_from.assert_not_called()
        state.mark_finished.assert_not_called()
        task.apply_async.assert_called_once_with(
            args=[source],
            queue="standard",
        )

    def test_fresh_crawl_is_not_duplicated(self):
        source = {
            "name": "source",
            "priority": "standard",
            "interval_minutes": 60,
            "timeout_seconds": 120,
        }
        state = Mock()
        state.snapshot.return_value = {
            "source": {
                "in_progress": "1",
                "started_ts": time.time(),
            }
        }
        state.stale_in_progress_from.side_effect = (
            CrawlState.stale_in_progress_from
        )
        task = Mock()

        with (
            patch("engine.scheduler.state", state),
            patch("engine.scheduler.load_sources", return_value=[source]),
            patch("engine.scheduler.crawl_source", task),
        ):
            result = check_and_schedule.run()

        self.assertEqual(result["scheduled"], [])
        state.acquire_lock.assert_not_called()
        task.apply_async.assert_not_called()

    def test_recovery_restarts_unconditionally_with_saved_validators(self):
        source = {
            "name": "source",
            "url": "https://example.test/",
            "source_type": "validation",
        }
        state = Mock()
        state.needs_recovery.return_value = True
        state.mark_started.return_value = "run-id"
        state.get_validators.return_value = {
            "etag": '"old"',
            "last_modified": "yesterday",
            "prev_hash": "old-hash",
        }
        record = {
            "unchanged": False,
            "http_status": 200,
        }

        with (
            patch("engine.tasks.state", state),
            patch("engine.tasks.run_crawl", return_value=record) as run_crawl,
        ):
            result = crawl_source.apply(args=[source]).get()

        self.assertEqual(result["status"], "success")
        self.assertTrue(result["recovered"])
        state.mark_started.assert_called_once_with(
            "source",
            recovered=True,
        )
        run_crawl.assert_called_once_with(
            source,
            {},
            checkpoint_enabled=True,
            resume_progress=True,
        )

    def test_checkpoint_tracks_frontier_and_completes_url_once(self):
        state = CrawlState.__new__(CrawlState)
        state.r = MemoryRedis()
        state.save_crawl_checkpoint(
            "source",
            [
                "https://example.test/",
                "https://example.test/a",
            ],
            [
                "https://example.test/",
                "https://example.test/a",
            ],
            0,
            "https://example.test/",
        )

        seed_record = {
            "is_seed": True,
            "url": "https://example.test/",
            "status": "success",
        }
        state.complete_checkpoint_url(
            "source",
            "https://example.test/",
            seed_record=seed_record,
        )
        state.complete_checkpoint_url(
            "source",
            "https://example.test/",
            seed_record=seed_record,
        )

        checkpoint = state.get_crawl_checkpoint("source")
        self.assertIsNotNone(checkpoint)
        assert checkpoint is not None
        self.assertEqual(
            checkpoint["pending_urls"],
            ["https://example.test/a"],
        )
        self.assertEqual(checkpoint["processed_count"], 1)
        self.assertEqual(checkpoint["seed_record"], seed_record)

    def test_spider_restores_pending_frontier_without_restarting_seed(self):
        checkpoint = {
            "seed_url": "https://example.test/",
            "pending_urls": [
                "https://example.test/unfinished",
            ],
            "visited_urls": [
                "https://example.test/",
                "https://example.test/done",
                "https://example.test/unfinished",
            ],
            "processed_count": 2,
        }
        state = Mock()
        state.get_crawl_checkpoint.return_value = checkpoint
        requests = []

        async def collect_requests():
            spider = SingleSourceSpider(
                url="https://example.test/",
                checkpoint="1",
                resume_progress="1",
            )
            spider.crawl_state = state
            async for request in spider.start():
                requests.append(request)

        with patch(
            "Crawler.spiders.single_source.CrawlState",
            return_value=state,
        ):
            asyncio.run(collect_requests())

        self.assertEqual(
            [request.url for request in requests],
            ["https://example.test/unfinished"],
        )
        self.assertFalse(
            requests[0].headers.get("If-None-Match")
        )
        state.save_crawl_checkpoint.assert_called_once_with(
            "source_1",
            ["https://example.test/unfinished"],
            [
                "https://example.test/",
                "https://example.test/done",
                "https://example.test/unfinished",
            ],
            2,
            "https://example.test/",
        )

    def test_resumed_child_only_run_uses_saved_seed_result(self):
        source = {
            "name": "source",
            "url": "https://example.test/",
            "source_type": "validation",
            "timeout_seconds": 120,
        }
        saved_seed = {
            "is_seed": True,
            "source_name": "source",
            "url": "https://example.test/",
            "status": "success",
            "http_status": 200,
            "unchanged": False,
            "content_hash": "seed-hash",
        }
        state = Mock()
        state.get_crawl_checkpoint.return_value = {
            "seed_url": source["url"],
            "pending_urls": [],
            "seed_record": saved_seed,
        }

        with (
            patch("engine.runner._log_size", return_value=0),
            patch(
                "engine.runner.subprocess.run",
                return_value=subprocess.CompletedProcess(
                    args=[],
                    returncode=0,
                    stdout="",
                    stderr="",
                ),
            ),
            patch(
                "engine.runner._find_records",
                return_value=[
                    {
                        "is_seed": False,
                        "status": "success",
                        "source_name": "source",
                        "url": "https://example.test/child",
                    },
                ],
            ),
            patch("engine.state.CrawlState", return_value=state),
        ):
            result = run_crawl(
                source,
                checkpoint_enabled=True,
                resume_progress=True,
            )

        self.assertEqual(result["source_name"], "source")
        self.assertTrue(result["is_seed"])
        self.assertEqual(result["content_hash"], "seed-hash")

    def test_runner_rejects_success_with_pending_checkpoint_urls(self):
        source = {
            "name": "source",
            "url": "https://example.test/",
            "source_type": "validation",
            "timeout_seconds": 120,
        }
        state = Mock()
        state.get_crawl_checkpoint.return_value = {
            "seed_url": source["url"],
            "pending_urls": ["https://example.test/unfinished"],
            "seed_record": None,
        }

        with (
            patch("engine.runner._log_size", return_value=0),
            patch(
                "engine.runner.subprocess.run",
                return_value=subprocess.CompletedProcess(
                    args=[],
                    returncode=0,
                    stdout="",
                    stderr="",
                ),
            ),
            patch(
                "engine.runner._find_records",
                return_value=[
                    {
                        "is_seed": True,
                        "status": "success",
                        "source_name": "source",
                        "url": source["url"],
                    },
                ],
            ),
            patch("engine.state.CrawlState", return_value=state),
        ):
            with self.assertRaisesRegex(
                CrawlError,
                "1 URL\\(s\\) still pending",
            ):
                run_crawl(
                    source,
                    checkpoint_enabled=True,
                    resume_progress=True,
                )

    def test_fresh_crawl_accepts_bounded_pending_frontier(self):
        source = {
            "name": "source",
            "url": "https://example.test/",
            "source_type": "validation",
            "timeout_seconds": 120,
        }
        state = Mock()
        state.get_crawl_checkpoint.return_value = {
            "seed_url": source["url"],
            "pending_urls": ["https://example.test/unfinished"],
            "seed_record": None,
        }

        with (
            patch("engine.runner._log_size", return_value=0),
            patch(
                "engine.runner.subprocess.run",
                return_value=subprocess.CompletedProcess(
                    args=[],
                    returncode=0,
                    stdout="",
                    stderr="",
                ),
            ),
            patch(
                "engine.runner._find_records",
                return_value=[{
                    "is_seed": True,
                    "status": "success",
                    "source_name": "source",
                    "url": source["url"],
                    "http_status": 200,
                }],
            ),
            patch("engine.state.CrawlState", return_value=state),
        ):
            result = run_crawl(
                source,
                checkpoint_enabled=True,
                resume_progress=False,
            )

        self.assertEqual(result["status"], "success")

    def test_runner_fails_job_when_child_record_failed(self):
        source = {
            "name": "source",
            "url": "https://example.test/",
            "source_type": "validation",
            "timeout_seconds": 120,
        }

        with (
            patch("engine.runner._log_size", return_value=0),
            patch(
                "engine.runner.subprocess.run",
                return_value=subprocess.CompletedProcess(
                    args=[],
                    returncode=0,
                    stdout="",
                    stderr="",
                ),
            ),
            patch(
                "engine.runner._find_records",
                return_value=[
                    {
                        "is_seed": True,
                        "status": "success",
                        "source_name": "source",
                        "url": source["url"],
                    },
                    {
                        "is_seed": False,
                        "status": "failed",
                        "source_name": "source",
                        "url": "https://example.test/child",
                        "error": "HTTP 503",
                        "http_status": 503,
                    },
                ],
            ),
        ):
            with self.assertRaisesRegex(
                CrawlError,
                "https://example.test/child",
            ):
                run_crawl(source)

if __name__ == "__main__":
    unittest.main()
