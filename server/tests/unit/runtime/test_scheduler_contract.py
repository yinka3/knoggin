import asyncio

import pytest

from infrastructure.background_work import BackgroundWorkRejected
from infrastructure.job.base import JobContext, JobResult
from infrastructure.job.scheduler import Scheduler


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

    async def should_run(self, _ctx):
        self.should_run_calls += 1
        self.check_started.set()
        if self.check_blocker is not None:
            await self.check_blocker.wait()
        return self.due

    async def execute(self, _ctx):
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
async def test_start_checks_jobs_immediately_and_stop_is_idempotent(monkeypatch):
    capture_events(monkeypatch)
    scheduler = Scheduler("ada", "project-1")
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
def test_register_rejects_duplicate_names_and_stopped_scheduler():
    scheduler = Scheduler("ada", "project-1")
    scheduler.register(ControlledJob(name="duplicate"))
    with pytest.raises(ValueError, match="already registered"):
        scheduler.register(ControlledJob(name="duplicate"))


@pytest.mark.runtime
@pytest.mark.no_network
async def test_scheduler_keeps_cadence_locally_and_records_success(monkeypatch):
    capture_events(monkeypatch)
    scheduler = Scheduler("ada", "project-1")
    job = ControlledJob(due=False, cadence_seconds=60)

    monkeypatch.setattr("infrastructure.job.scheduler.get_now_unix", lambda: 1000)
    await scheduler._schedule_if_due(job.name, job, context())
    assert job.execute_calls == 0
    assert scheduler._last_successful_runs == {job.name: 1000}

    monkeypatch.setattr("infrastructure.job.scheduler.get_now_unix", lambda: 1060)
    await scheduler._schedule_if_due(job.name, job, context())
    await asyncio.wait_for(job.finished.wait(), timeout=1)

    assert job.execute_calls == 1
    assert scheduler._last_successful_runs == {job.name: 1060}


@pytest.mark.runtime
@pytest.mark.no_network
async def test_immediate_cadence_runs_without_prior_local_timestamp(monkeypatch):
    capture_events(monkeypatch)
    scheduler = Scheduler("ada", "project-1")
    job = ControlledJob(
        due=False,
        cadence_seconds=60,
        run_immediately_on_first_check=True,
    )
    monkeypatch.setattr("infrastructure.job.scheduler.get_now_unix", lambda: 2000)

    await scheduler._schedule_if_due(job.name, job, context())
    await asyncio.wait_for(job.finished.wait(), timeout=1)

    assert job.execute_calls == 1
    assert scheduler._last_successful_runs == {job.name: 2000}


@pytest.mark.runtime
@pytest.mark.no_network
async def test_failed_job_does_not_advance_local_cadence(monkeypatch):
    capture_events(monkeypatch)
    scheduler = Scheduler("ada", "project-1")
    scheduler._last_successful_runs["controlled"] = 1000
    job = ControlledJob(
        cadence_seconds=60,
        result=JobResult(success=False, summary="retry"),
    )
    monkeypatch.setattr("infrastructure.job.scheduler.get_now_unix", lambda: 2000)

    result = await scheduler._execute_job(job, context())

    assert result.success is False
    assert scheduler._last_successful_runs == {"controlled": 1000}


@pytest.mark.runtime
@pytest.mark.no_network
async def test_scheduler_rejects_background_admission_without_leases(monkeypatch):
    events = capture_events(monkeypatch)

    class RejectingBackgroundWork:
        async def submit(self, project_id, operation, *, name, coalesce_key=None):
            raise BackgroundWorkRejected(
                project_id=project_id,
                name=name,
                reason="global_queue_full",
                limit=1,
                queued=1,
            )

    scheduler = Scheduler(
        "ada",
        "project-1",
        background_work=RejectingBackgroundWork(),
    )
    job = ControlledJob(name="profile_refinement")

    rejected = await scheduler._execute_job(job, context())

    assert rejected.success is False
    assert job.execute_calls == 0
    assert events[-1][2] == "admission_rejected"
    assert events[-1][3]["reason"] == "global_queue_full"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_timeout_and_cancellation_are_reported_locally(monkeypatch):
    events = capture_events(monkeypatch)
    scheduler = Scheduler("ada", "project-1")
    scheduler.JOB_EXECUTION_TIMEOUT = 0.01

    assert await scheduler._execute_job(
        ControlledJob(blocker=asyncio.Event()),
        context(),
    ) is None
    assert [event[2] for event in events] == ["started", "timeout"]

    events.clear()
    scheduler.JOB_EXECUTION_TIMEOUT = 300
    task = asyncio.create_task(
        scheduler._execute_job(ControlledJob(blocker=asyncio.Event()), context())
    )
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert [event[2] for event in events] == ["started", "cancelled"]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_local_admission_lock_prevents_duplicate_execution():
    scheduler = Scheduler("ada", "project-1")
    allow_check = asyncio.Event()
    allow_execution = asyncio.Event()
    job = ControlledJob(
        name="shared",
        check_blocker=allow_check,
        blocker=allow_execution,
    )

    first = asyncio.create_task(scheduler._schedule_if_due(job.name, job, context()))
    await asyncio.wait_for(job.check_started.wait(), timeout=1)
    second = asyncio.create_task(scheduler._schedule_if_due(job.name, job, context()))
    allow_check.set()
    await first
    await asyncio.wait_for(job.started.wait(), timeout=1)
    await second

    assert job.should_run_calls == 1
    assert job.execute_calls == 1
    allow_execution.set()
    await asyncio.wait_for(job.finished.wait(), timeout=1)


@pytest.mark.runtime
@pytest.mark.no_network
async def test_stop_uses_one_deadline_then_cancels_all_jobs(monkeypatch):
    capture_events(monkeypatch)
    scheduler = Scheduler("ada", "project-1")
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
            lambda completed, name=job.name: scheduler._cleanup_task(name, completed)
        )

    await asyncio.gather(*(job.started.wait() for job in jobs))
    await scheduler.stop()

    assert all(job.finished.is_set() for job in jobs)
    assert scheduler._running_tasks == {}


@pytest.mark.runtime
@pytest.mark.no_network
async def test_scheduler_health_snapshot_has_no_lease_state(monkeypatch):
    scheduler = Scheduler("ada", "project-1")
    scheduler._is_running = True
    job = ControlledJob(blocker=asyncio.Event())
    task = asyncio.create_task(scheduler._execute_job(job, context()))
    scheduler._running_tasks[job.name] = task
    await asyncio.wait_for(job.started.wait(), timeout=1)

    snapshot = scheduler.snapshot_for_health()

    assert snapshot["running_jobs"] == 1
    assert "lease_seconds" not in snapshot["active_jobs"][0]
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
