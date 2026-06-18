import asyncio

import pytest

from infrastructure.job.base import JobContext, JobResult
from infrastructure.job.scheduler import Scheduler
from infrastructure.redis_client import RedisKeys
from knoggin_server.knowledge.jobs.merge_job import MergeDetectionJob
from tests.fixtures.fakes import FakeRedis


class ControlledJob:
    enabled = True

    def __init__(
        self,
        name="controlled",
        *,
        due=True,
        result=None,
        error=None,
        blocker=None,
        check_blocker=None,
        cadence_seconds=None,
        run_immediately_on_first_check=False,
    ):
        self._name = name
        self.due = due
        self.result = result or JobResult(success=True, summary="done")
        self.error = error
        self.blocker = blocker
        self.check_blocker = check_blocker
        self.cadence_seconds = cadence_seconds
        self.run_immediately_on_first_check = run_immediately_on_first_check
        self.should_run_calls = 0
        self.execute_calls = 0
        self.check_started = asyncio.Event()
        self.started = asyncio.Event()
        self.finished = asyncio.Event()

    @property
    def name(self):
        return self._name

    async def should_run(self, ctx):
        self.should_run_calls += 1
        self.check_started.set()
        if self.check_blocker is not None:
            await self.check_blocker.wait()
        return self.due

    async def execute(self, ctx):
        self.execute_calls += 1
        self.started.set()
        try:
            if self.blocker is not None:
                await self.blocker.wait()
            if self.error is not None:
                raise self.error
            return self.result
        finally:
            self.finished.set()


def context():
    return JobContext(user_name="ada", project_id="project-1")


def capture_events(monkeypatch):
    events = []

    async def fake_emit(scope_id, component, event, data=None, verbose_only=False):
        events.append((scope_id, component, event, data or {}, verbose_only))

    monkeypatch.setattr("infrastructure.job.scheduler.emit", fake_emit)
    return events


@pytest.mark.runtime
@pytest.mark.no_network
async def test_start_checks_jobs_immediately_and_start_stop_are_idempotent(monkeypatch):
    capture_events(monkeypatch)
    scheduler = Scheduler("ada", "project-1", FakeRedis())
    job = ControlledJob()
    scheduler.register(job)

    await scheduler.start()
    monitor_task = scheduler._monitor_task
    await asyncio.wait_for(job.started.wait(), timeout=1)
    await scheduler.start()

    assert scheduler.running is True
    assert scheduler._monitor_task is monitor_task
    assert job.should_run_calls == 1
    assert job.execute_calls == 1

    await scheduler.stop()
    await scheduler.stop()
    assert scheduler.running is False
    assert scheduler._monitor_task is None


@pytest.mark.runtime
@pytest.mark.no_network
async def test_start_rolls_back_running_state_when_initial_check_fails():
    class FailingRedis(FakeRedis):
        async def get(self, key):
            raise RuntimeError("redis unavailable")

    scheduler = Scheduler("ada", "project-1", FailingRedis())
    scheduler.register(ControlledJob())

    with pytest.raises(RuntimeError, match="redis unavailable"):
        await scheduler.start()

    assert scheduler.running is False
    assert scheduler._monitor_task is None


@pytest.mark.runtime
@pytest.mark.no_network
def test_register_rejects_duplicate_job_names():
    scheduler = Scheduler("ada", "project-1", FakeRedis())
    scheduler.register(ControlledJob(name="duplicate"))

    with pytest.raises(ValueError, match="already registered"):
        scheduler.register(ControlledJob(name="duplicate"))


@pytest.mark.runtime
@pytest.mark.no_network
async def test_startup_check_recovers_merge_work_from_durable_queue(monkeypatch):
    capture_events(monkeypatch)
    redis = FakeRedis()
    await redis.sadd(RedisKeys.merge_queue("ada", "project-1"), "2")
    scheduler = Scheduler("ada", "project-1", redis)
    job = MergeDetectionJob(
        user_name="ada",
        entities=object(),
        graph_client=object(),
        llm_client=object(),
        topic_config=object(),
        redis_client=redis,
    )
    executed = asyncio.Event()

    async def execute(ctx):
        executed.set()
        return JobResult(success=True, summary="recovered")

    job.execute = execute
    scheduler.register(job)

    await scheduler.start()
    await asyncio.wait_for(executed.wait(), timeout=1)
    await scheduler.stop()


@pytest.mark.runtime
@pytest.mark.no_network
async def test_scheduler_owns_delayed_cadence_and_records_success(monkeypatch):
    capture_events(monkeypatch)
    redis = FakeRedis()
    scheduler = Scheduler("ada", "project-1", redis)
    job = ControlledJob(due=False, cadence_seconds=60)
    last_run_key = RedisKeys.job_last_run(job.name, "ada", "project-1")

    monkeypatch.setattr(
        "infrastructure.job.scheduler.get_now_unix",
        lambda: 1000,
    )
    await scheduler._schedule_if_due(job.name, job, context())

    assert job.execute_calls == 0
    assert await redis.get(last_run_key) == "1000"

    monkeypatch.setattr(
        "infrastructure.job.scheduler.get_now_unix",
        lambda: 1060,
    )
    await scheduler._schedule_if_due(job.name, job, context())
    await asyncio.wait_for(job.finished.wait(), timeout=1)

    assert job.execute_calls == 1
    assert await redis.get(last_run_key) == "1060"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_immediate_cadence_runs_without_an_existing_timestamp(monkeypatch):
    capture_events(monkeypatch)
    redis = FakeRedis()
    scheduler = Scheduler("ada", "project-1", redis)
    job = ControlledJob(
        due=False,
        cadence_seconds=60,
        run_immediately_on_first_check=True,
    )

    await scheduler._schedule_if_due(job.name, job, context())
    await asyncio.wait_for(job.finished.wait(), timeout=1)

    assert job.execute_calls == 1


@pytest.mark.runtime
@pytest.mark.no_network
async def test_invalid_cadence_timestamp_runs_and_repairs_on_success(monkeypatch):
    capture_events(monkeypatch)
    redis = FakeRedis()
    scheduler = Scheduler("ada", "project-1", redis)
    job = ControlledJob(due=False, cadence_seconds=60)
    last_run_key = RedisKeys.job_last_run(job.name, "ada", "project-1")
    await redis.set(last_run_key, "invalid")
    monkeypatch.setattr(
        "infrastructure.job.scheduler.get_now_unix",
        lambda: 2000,
    )

    await scheduler._schedule_if_due(job.name, job, context())
    await asyncio.wait_for(job.finished.wait(), timeout=1)

    assert job.execute_calls == 1
    assert await redis.get(last_run_key) == "2000"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_false_result_emits_failed_not_completed(monkeypatch):
    events = capture_events(monkeypatch)
    scheduler = Scheduler("ada", "project-1", FakeRedis())
    job = ControlledJob(
        result=JobResult(success=False, summary="rejected"),
    )

    result = await scheduler._execute_job(job, context())

    assert result.success is False
    assert [event[2] for event in events] == ["started", "failed"]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_failed_result_does_not_advance_cadence(monkeypatch):
    capture_events(monkeypatch)
    redis = FakeRedis()
    scheduler = Scheduler("ada", "project-1", redis)
    job = ControlledJob(
        cadence_seconds=60,
        result=JobResult(success=False, summary="retry"),
    )
    last_run_key = RedisKeys.job_last_run(job.name, "ada", "project-1")
    await redis.set(last_run_key, "1000")
    monkeypatch.setattr(
        "infrastructure.job.scheduler.get_now_unix",
        lambda: 2000,
    )

    await scheduler._execute_job(job, context())

    assert await redis.get(last_run_key) == "1000"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_exception_emits_failed_and_releases_lease(monkeypatch):
    events = capture_events(monkeypatch)
    redis = FakeRedis()
    scheduler = Scheduler("ada", "project-1", redis)
    job = ControlledJob(error=RuntimeError("boom"))

    assert await scheduler._execute_job(job, context()) is None

    assert [event[2] for event in events] == ["started", "failed"]
    assert events[-1][3]["error"] == "boom"
    assert await redis.get(
        RedisKeys.job_lease("ada", "project-1", job.name)
    ) is None


@pytest.mark.runtime
@pytest.mark.no_network
async def test_timeout_emits_timeout_and_releases_lease(monkeypatch):
    events = capture_events(monkeypatch)
    redis = FakeRedis()
    scheduler = Scheduler("ada", "project-1", redis)
    scheduler.JOB_EXECUTION_TIMEOUT = 0.01
    job = ControlledJob(blocker=asyncio.Event())

    assert await scheduler._execute_job(job, context()) is None

    assert [event[2] for event in events] == ["started", "timeout"]
    assert await redis.get(
        RedisKeys.job_lease("ada", "project-1", job.name)
    ) is None


@pytest.mark.runtime
@pytest.mark.no_network
async def test_event_failures_do_not_abort_job_execution(monkeypatch):
    async def failing_emit(*args, **kwargs):
        raise RuntimeError("observer unavailable")

    monkeypatch.setattr("infrastructure.job.scheduler.emit", failing_emit)
    scheduler = Scheduler("ada", "project-1", FakeRedis())
    job = ControlledJob()

    result = await scheduler._execute_job(job, context())

    assert result.success is True
    assert job.execute_calls == 1


@pytest.mark.runtime
@pytest.mark.no_network
async def test_two_schedulers_compete_for_one_execution_lease(monkeypatch):
    capture_events(monkeypatch)
    redis = FakeRedis()
    blocker = asyncio.Event()
    first = ControlledJob(name="shared", blocker=blocker)
    second = ControlledJob(name="shared")
    scheduler_a = Scheduler("ada", "project-1", redis)
    scheduler_b = Scheduler("ada", "project-1", redis)

    first_task = asyncio.create_task(
        scheduler_a._execute_job(first, context())
    )
    await asyncio.wait_for(first.started.wait(), timeout=1)
    second_result = await scheduler_b._execute_job(second, context())

    assert second_result is None
    assert second.execute_calls == 0

    blocker.set()
    await first_task
    assert await redis.get(
        RedisKeys.job_lease("ada", "project-1", first.name)
    ) is None


@pytest.mark.runtime
@pytest.mark.no_network
async def test_execution_lease_also_protects_trigger_evaluation(monkeypatch):
    capture_events(monkeypatch)
    redis = FakeRedis()
    check_blocker = asyncio.Event()
    first = ControlledJob(name="shared", check_blocker=check_blocker)
    second = ControlledJob(name="shared")
    scheduler_a = Scheduler("ada", "project-1", redis)
    scheduler_b = Scheduler("ada", "project-1", redis)
    scheduler_a.register(first)
    scheduler_b.register(second)

    first_check = asyncio.create_task(scheduler_a._check_jobs())
    await asyncio.wait_for(first.check_started.wait(), timeout=1)
    await scheduler_b._check_jobs()

    assert second.should_run_calls == 0
    assert second.execute_calls == 0

    check_blocker.set()
    await first_check
    await asyncio.wait_for(first.finished.wait(), timeout=1)


@pytest.mark.runtime
@pytest.mark.no_network
async def test_stale_lease_expiry_allows_execution(monkeypatch):
    capture_events(monkeypatch)
    redis = FakeRedis()
    scheduler = Scheduler("ada", "project-1", redis)
    job = ControlledJob()
    lease_key = RedisKeys.job_lease("ada", "project-1", job.name)
    await redis.set(lease_key, "stale-owner", ex=360, nx=True)
    redis.string_expirations[lease_key] = 0

    result = await scheduler._execute_job(job, context())

    assert result.success is True
    assert job.execute_calls == 1


@pytest.mark.runtime
@pytest.mark.no_network
async def test_lease_release_requires_matching_owner_token():
    redis = FakeRedis()
    scheduler = Scheduler("ada", "project-1", redis)
    lease_key = RedisKeys.job_lease("ada", "project-1", "owned")
    await redis.set(lease_key, "owner-a", ex=360, nx=True)

    await scheduler._release_lease(lease_key, "owner-b")
    assert await redis.get(lease_key) == "owner-a"

    await scheduler._release_lease(lease_key, "owner-a")
    assert await redis.get(lease_key) is None


@pytest.mark.runtime
@pytest.mark.no_network
async def test_job_cancellation_releases_lease(monkeypatch):
    capture_events(monkeypatch)
    redis = FakeRedis()
    scheduler = Scheduler("ada", "project-1", redis)
    job = ControlledJob(blocker=asyncio.Event())
    task = asyncio.create_task(scheduler._execute_job(job, context()))
    await asyncio.wait_for(job.started.wait(), timeout=1)

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert await redis.get(
        RedisKeys.job_lease("ada", "project-1", job.name)
    ) is None


@pytest.mark.runtime
@pytest.mark.no_network
async def test_stop_uses_one_deadline_then_cancels_all_jobs(monkeypatch):
    capture_events(monkeypatch)
    redis = FakeRedis()
    scheduler = Scheduler("ada", "project-1", redis)
    scheduler.SHUTDOWN_TIMEOUT = 0.01
    scheduler._is_running = True
    jobs = [
        ControlledJob(name="one", blocker=asyncio.Event()),
        ControlledJob(name="two", blocker=asyncio.Event()),
    ]
    for job in jobs:
        task = asyncio.create_task(scheduler._execute_job(job, context()))
        scheduler._running_tasks[job.name] = task
        task.add_done_callback(
            lambda completed, name=job.name: scheduler._cleanup_task(
                name, completed
            )
        )

    await asyncio.gather(*(job.started.wait() for job in jobs))
    await scheduler.stop()

    assert all(job.finished.is_set() for job in jobs)
    assert scheduler._running_tasks == {}
    for job in jobs:
        assert await redis.get(
            RedisKeys.job_lease("ada", "project-1", job.name)
        ) is None


@pytest.mark.runtime
@pytest.mark.no_network
async def test_cleanup_observes_unhandled_task_exception(monkeypatch):
    errors = []
    monkeypatch.setattr(
        "infrastructure.job.scheduler.logger.error",
        lambda message: errors.append(str(message)),
    )

    async def fail():
        raise RuntimeError("unobserved")

    scheduler = Scheduler("ada", "project-1", FakeRedis())
    task = asyncio.create_task(fail())
    await asyncio.sleep(0)
    scheduler._running_tasks["broken"] = task

    scheduler._cleanup_task("broken", task)

    assert any("unobserved" in message for message in errors)
    assert "broken" not in scheduler._running_tasks
