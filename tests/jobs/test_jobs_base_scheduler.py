"""Tests for jobs/base.py and jobs/scheduler.py.

Covers BaseJob.update_settings safety, Scheduler orchestration,
idle time calculation, pending checks, and job execution paths.
"""

import asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from jobs.base import BaseJob, JobContext, JobResult
from jobs.scheduler import Scheduler


# ── Concrete job for testing ────────────────────────────

class StubJob(BaseJob):
    """Minimal concrete job for testing base class and scheduler."""

    def __init__(self, should_run_val=False, execute_result=None, execute_delay=0):
        self.threshold = 10
        self.interval = 60
        self._should_run_val = should_run_val
        self._execute_result = execute_result or JobResult(success=True, summary="done")
        self._execute_delay = execute_delay
        self.enabled = True
        self.shutdown_called = False

    @property
    def name(self) -> str:
        return "stub_job"

    async def should_run(self, ctx: JobContext) -> bool:
        return self._should_run_val

    async def execute(self, ctx: JobContext) -> JobResult:
        if self._execute_delay:
            await asyncio.sleep(self._execute_delay)
        return self._execute_result

    async def on_shutdown(self, ctx: JobContext) -> None:
        self.shutdown_called = True


def make_job_context(redis=None, idle_seconds=0.0):
    return JobContext(
        user_name="Yinka",
        session_id="test-session",
        redis=redis or MagicMock(),
        idle_seconds=idle_seconds,
    )


def make_scheduler(redis=None):
    if redis is None:
        redis = MagicMock()
        redis.get = AsyncMock(return_value=None)
        redis.set = AsyncMock()
        redis.delete = AsyncMock()
    return Scheduler(
        user_name="Yinka",
        session_id="test-session",
        redis=redis,
    )


# ════════════════════════════════════════════════════════
#  BaseJob.update_settings
# ════════════════════════════════════════════════════════

class TestBaseJobUpdateSettings:

    def test_updates_existing_attribute(self):
        job = StubJob()
        job.update_settings(threshold=20)
        assert job.threshold == 20

    def test_skips_none_values(self):
        job = StubJob()
        job.update_settings(threshold=None)
        assert job.threshold == 10  # unchanged

    def test_rejects_unknown_attribute(self):
        job = StubJob()
        job.update_settings(nonexistent_field=42)
        assert not hasattr(job, "nonexistent_field")

    def test_multiple_updates(self):
        job = StubJob()
        job.update_settings(threshold=20, interval=120)
        assert job.threshold == 20
        assert job.interval == 120

    def test_empty_kwargs_noop(self):
        job = StubJob()
        job.update_settings()
        assert job.threshold == 10
        assert job.interval == 60

    def test_mixed_valid_and_invalid(self):
        """Valid keys update, invalid keys are rejected."""
        job = StubJob()
        job.update_settings(threshold=50, garbage=True)
        assert job.threshold == 50
        assert not hasattr(job, "garbage")


# ════════════════════════════════════════════════════════
#  JobContext / JobResult dataclasses
# ════════════════════════════════════════════════════════

class TestDataclasses:

    def test_job_context_defaults(self):
        ctx = JobContext(user_name="test", session_id="s1", redis=MagicMock())
        assert ctx.idle_seconds == 0.0
        assert ctx.last_run is None
        assert ctx.resources is None

    def test_job_result_defaults(self):
        result = JobResult()
        assert result.success is True
        assert result.summary == ""
        assert result.reschedule_seconds is None

    def test_job_result_custom(self):
        result = JobResult(success=False, summary="failed", reschedule_seconds=30.0)
        assert result.success is False
        assert result.reschedule_seconds == 30.0


class TestScheduler:

    # ════════════════════════════════════════════════════════
    #  Scheduler — register
    # ════════════════════════════════════════════════════════

    def test_register_adds_job(self):
        scheduler = make_scheduler()
        job = StubJob()
        scheduler.register(job)
        assert "stub_job" in scheduler._jobs

    def test_register_returns_self_for_chaining(self):
        scheduler = make_scheduler()
        result = scheduler.register(StubJob())
        assert result is scheduler

    def test_register_multiple_jobs(self):
        scheduler = make_scheduler()
        job_a = StubJob()
        job_a.name  # "stub_job"

        # Create a second job with different name
        job_b = StubJob()
        job_b.__class__ = type("OtherJob", (StubJob,), {"name": property(lambda self: "other_job")})

        scheduler.register(job_a).register(job_b)
        assert len(scheduler._jobs) == 2


    # ════════════════════════════════════════════════════════
    #  Scheduler — _get_idle_seconds
    # ════════════════════════════════════════════════════════

    @pytest.mark.asyncio
    async def test_no_activity_key_returns_zero(self):
        redis = MagicMock()
        redis.get = AsyncMock(return_value=None)
        scheduler = make_scheduler(redis)

        idle = await scheduler._get_idle_seconds()
        assert idle == 0.0

    @pytest.mark.asyncio
    async def test_recent_activity(self):
        redis = MagicMock()
        recent = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
        redis.get = AsyncMock(return_value=recent)
        scheduler = make_scheduler(redis)

        idle = await scheduler._get_idle_seconds()
        assert 25 <= idle <= 35  # allow small timing variance

    @pytest.mark.asyncio
    async def test_old_activity(self):
        redis = MagicMock()
        old = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        redis.get = AsyncMock(return_value=old)
        scheduler = make_scheduler(redis)

        idle = await scheduler._get_idle_seconds()
        assert idle >= 3500  # ~1 hour


    # ════════════════════════════════════════════════════════
    #  Scheduler — record_activity
    # ════════════════════════════════════════════════════════

    @pytest.mark.asyncio
    async def test_writes_timestamp_to_redis(self):
        redis = MagicMock()
        redis.set = AsyncMock()
        scheduler = make_scheduler(redis)

        await scheduler.record_activity()

        redis.set.assert_called_once()
        call_args = redis.set.call_args[0]
        assert "last_activity" in call_args[0]
        # Value should be a valid ISO timestamp
        datetime.fromisoformat(call_args[1])


    # ════════════════════════════════════════════════════════
    #  Scheduler — _build_context
    # ════════════════════════════════════════════════════════

    @pytest.mark.asyncio
    async def test_populates_all_fields(self):
        redis = MagicMock()
        redis.get = AsyncMock(return_value=None)
        scheduler = make_scheduler(redis)
        scheduler.resources = MagicMock()

        ctx = await scheduler._build_context()

        assert ctx.user_name == "Yinka"
        assert ctx.session_id == "test-session"
        assert ctx.redis is redis
        assert ctx.idle_seconds == 0.0
        assert ctx.resources is scheduler.resources


    # ════════════════════════════════════════════════════════
    #  Scheduler — _run_pending_checks
    # ════════════════════════════════════════════════════════

    @pytest.mark.asyncio
    async def test_pending_key_triggers_execute(self):
        redis = MagicMock()
        redis.get = AsyncMock(side_effect=lambda key: "1" if "pending" in key else None)
        redis.delete = AsyncMock()
        redis.set = AsyncMock()
        scheduler = make_scheduler(redis)

        job = StubJob(execute_result=JobResult(success=True, summary="ran"))
        scheduler.register(job)

        with patch.object(scheduler, "_execute_job", new_callable=AsyncMock) as mock_exec:
            await scheduler._run_pending_checks()

        mock_exec.assert_called_once()
        redis.delete.assert_called()

    @pytest.mark.asyncio
    async def test_no_pending_key_skips(self):
        redis = MagicMock()
        redis.get = AsyncMock(return_value=None)
        scheduler = make_scheduler(redis)

        job = StubJob()
        scheduler.register(job)

        with patch.object(scheduler, "_execute_job", new_callable=AsyncMock) as mock_exec:
            await scheduler._run_pending_checks()

        mock_exec.assert_not_called()

    @pytest.mark.asyncio
    async def test_disabled_job_skipped(self):
        redis = MagicMock()
        redis.set = AsyncMock()
        redis.delete = AsyncMock()

        async def key_aware_get(key):
            if "pending" in key:
                return "1"
            return None  # last_activity and others

        redis.get = AsyncMock(side_effect=key_aware_get)
        scheduler = make_scheduler(redis)

        job = StubJob()
        job.enabled = False
        scheduler.register(job)

        with patch.object(scheduler, "_execute_job", new_callable=AsyncMock) as mock_exec:
            await scheduler._run_pending_checks()

        mock_exec.assert_not_called()


    # ════════════════════════════════════════════════════════
    #  Scheduler — _execute_job
    # ════════════════════════════════════════════════════════

    @pytest.mark.asyncio
    async def test_successful_execution(self):
        redis = MagicMock()
        redis.get = AsyncMock(return_value=None)
        redis.set = AsyncMock()
        scheduler = make_scheduler(redis)

        job = StubJob(execute_result=JobResult(success=True, summary="cleaned 5 entities"))
        ctx = make_job_context(redis)

        with patch("jobs.scheduler.emit", new_callable=AsyncMock):
            await scheduler._execute_job(job, ctx)

        assert "stub_job" in scheduler._last_runs

    @pytest.mark.asyncio
    async def test_timeout(self):
        redis = MagicMock()
        redis.get = AsyncMock(return_value=None)
        scheduler = make_scheduler(redis)
        scheduler.JOB_EXECUTION_TIMEOUT = 0.1  # very short

        job = StubJob(execute_delay=10)  # will exceed timeout
        ctx = make_job_context(redis)

        with patch("jobs.scheduler.emit", new_callable=AsyncMock):
            await scheduler._execute_job(job, ctx)

        # Should not have recorded a last_run (timed out)
        assert "stub_job" not in scheduler._last_runs

    @pytest.mark.asyncio
    async def test_exception_handled(self):
        redis = MagicMock()
        redis.get = AsyncMock(return_value=None)
        scheduler = make_scheduler(redis)

        job = StubJob()
        job.execute = AsyncMock(side_effect=RuntimeError("job exploded"))
        ctx = make_job_context(redis)

        with patch("jobs.scheduler.emit", new_callable=AsyncMock):
            # Should not raise
            await scheduler._execute_job(job, ctx)

        assert "stub_job" not in scheduler._last_runs

    @pytest.mark.asyncio
    async def test_reschedule(self):
        redis = MagicMock()
        redis.get = AsyncMock(return_value=None)
        scheduler = make_scheduler(redis)
        scheduler._is_running = True

        job = StubJob(execute_result=JobResult(success=True, reschedule_seconds=0.1))
        ctx = make_job_context(redis)

        with patch("jobs.scheduler.emit", new_callable=AsyncMock):
            await scheduler._execute_job(job, ctx)

        # Should have created a delayed task
        assert "stub_job" in scheduler._running_tasks

        # Cleanup
        task = scheduler._running_tasks.get("stub_job")
        if task:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass


    # ════════════════════════════════════════════════════════
    #  Scheduler — _cleanup_task
    # ════════════════════════════════════════════════════════

    def test_removes_finished_task(self):
        scheduler = make_scheduler()
        mock_task = MagicMock()
        scheduler._running_tasks["stub_job"] = mock_task

        scheduler._cleanup_task("stub_job", mock_task)
        assert "stub_job" not in scheduler._running_tasks

    def test_ignores_replaced_task(self):
        """If the task was replaced by a new one, don't remove."""
        scheduler = make_scheduler()
        old_task = MagicMock()
        new_task = MagicMock()
        scheduler._running_tasks["stub_job"] = new_task

        scheduler._cleanup_task("stub_job", old_task)
        # new_task should still be there
        assert scheduler._running_tasks["stub_job"] is new_task


    # ════════════════════════════════════════════════════════
    #  Scheduler — stop lifecycle
    # ════════════════════════════════════════════════════════

    @pytest.mark.asyncio
    async def test_stop_calls_on_shutdown(self):
        redis = MagicMock()
        redis.get = AsyncMock(return_value=None)
        redis.set = AsyncMock()
        scheduler = make_scheduler(redis)

        job = StubJob()
        scheduler.register(job)
        scheduler._is_running = True

        with patch("jobs.scheduler.emit", new_callable=AsyncMock):
            await scheduler.stop()

        assert scheduler._is_running is False
        assert job.shutdown_called is True

    @pytest.mark.asyncio
    async def test_stop_waits_for_running_tasks(self):
        redis = MagicMock()
        redis.get = AsyncMock(return_value=None)
        redis.set = AsyncMock()
        scheduler = make_scheduler(redis)
        scheduler._is_running = True

        completed = False

        async def slow_job():
            nonlocal completed
            await asyncio.sleep(0.1)
            completed = True

        task = asyncio.create_task(slow_job())
        scheduler._running_tasks["slow"] = task

        with patch("jobs.scheduler.emit", new_callable=AsyncMock):
            await scheduler.stop()

        assert completed is True

    @pytest.mark.asyncio
    async def test_stop_handles_shutdown_exception(self):
        """If a job's on_shutdown raises, other jobs should still get called."""
        redis = MagicMock()
        redis.get = AsyncMock(return_value=None)
        redis.set = AsyncMock()
        scheduler = make_scheduler(redis)
        scheduler._is_running = True

        bad_job = StubJob()
        bad_job.on_shutdown = AsyncMock(side_effect=RuntimeError("cleanup crashed"))
        scheduler.register(bad_job)

        with patch("jobs.scheduler.emit", new_callable=AsyncMock):
            # Should not raise
            await scheduler.stop()

        assert scheduler._is_running is False