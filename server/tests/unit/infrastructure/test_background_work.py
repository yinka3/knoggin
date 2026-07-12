import asyncio

import pytest

from infrastructure.background_work import (
    BackgroundWorkCoordinator,
    BackgroundWorkRejected,
)
from infrastructure.job.base import JobContext, JobResult
from infrastructure.job.scheduler import Scheduler
from tests.fixtures.fakes import FakeRedis


@pytest.mark.no_network
async def test_background_work_round_robins_projects_between_turns():
    coordinator = BackgroundWorkCoordinator(max_concurrency=1)
    order = []
    release_first = asyncio.Event()

    async def first_project_a_work():
        order.append("a-1")
        await release_first.wait()
        return "a-1"

    async def second_project_a_work():
        order.append("a-2")
        return "a-2"

    async def project_b_work():
        order.append("b-1")
        return "b-1"

    first = asyncio.create_task(
        coordinator.submit("project-a", first_project_a_work, name="profile")
    )
    await asyncio.sleep(0)
    second = asyncio.create_task(
        coordinator.submit("project-a", second_project_a_work, name="profile")
    )
    other_project = asyncio.create_task(
        coordinator.submit("project-b", project_b_work, name="aac")
    )
    await asyncio.sleep(0)

    assert coordinator.snapshot()["queued_by_project"] == {
        "project-a": 1,
        "project-b": 1,
    }
    release_first.set()

    assert await first == "a-1"
    assert await other_project == "b-1"
    assert await second == "a-2"
    assert order == ["a-1", "b-1", "a-2"]

    await coordinator.shutdown()


@pytest.mark.no_network
async def test_background_work_never_runs_two_operations_for_one_project_together():
    coordinator = BackgroundWorkCoordinator(max_concurrency=2)
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    second_started = asyncio.Event()
    other_project_started = asyncio.Event()

    async def first_project_a_work():
        first_started.set()
        await release_first.wait()

    async def second_project_a_work():
        second_started.set()

    async def project_b_work():
        other_project_started.set()

    first = asyncio.create_task(
        coordinator.submit("project-a", first_project_a_work, name="profile")
    )
    await first_started.wait()
    second = asyncio.create_task(
        coordinator.submit("project-a", second_project_a_work, name="profile")
    )
    other_project = asyncio.create_task(
        coordinator.submit("project-b", project_b_work, name="aac")
    )

    await other_project_started.wait()
    assert not second_started.is_set()
    release_first.set()
    await asyncio.gather(first, second, other_project)

    await coordinator.shutdown()


@pytest.mark.no_network
async def test_background_work_shutdown_cancels_queued_operations():
    coordinator = BackgroundWorkCoordinator(max_concurrency=1)
    release_first = asyncio.Event()

    async def active_work():
        await release_first.wait()

    async def queued_work():
        raise AssertionError("queued work should be cancelled")

    active = asyncio.create_task(
        coordinator.submit("project-a", active_work, name="profile")
    )
    await asyncio.sleep(0)
    queued = asyncio.create_task(
        coordinator.submit("project-b", queued_work, name="aac")
    )
    await asyncio.sleep(0)

    shutdown = asyncio.create_task(coordinator.shutdown())
    release_first.set()
    await shutdown

    await active
    with pytest.raises(asyncio.CancelledError):
        await queued


@pytest.mark.no_network
async def test_background_work_rejects_a_project_that_reaches_its_queue_limit():
    coordinator = BackgroundWorkCoordinator(
        max_queued_per_project=1,
        max_queued_global=4,
    )
    release_active = asyncio.Event()

    async def active_work():
        await release_active.wait()

    active = asyncio.create_task(
        coordinator.submit("project-a", active_work, name="profile")
    )
    await asyncio.sleep(0)
    queued = asyncio.create_task(
        coordinator.submit("project-a", active_work, name="profile")
    )
    await asyncio.sleep(0)

    with pytest.raises(BackgroundWorkRejected) as error:
        await coordinator.submit("project-a", active_work, name="profile")

    assert error.value.details == {
        "project_id": "project-a",
        "name": "profile",
        "reason": "project_queue_full",
        "limit": 1,
        "queued": 1,
    }
    snapshot = coordinator.snapshot()
    assert snapshot["rejected_by_reason"] == {
        "global_queue_full": 0,
        "project_queue_full": 1,
    }

    release_active.set()
    await asyncio.gather(active, queued)
    await coordinator.shutdown()


@pytest.mark.no_network
async def test_background_work_rejects_when_the_global_queue_is_full():
    coordinator = BackgroundWorkCoordinator(
        max_queued_per_project=3,
        max_queued_global=1,
    )
    active_started = asyncio.Event()
    release_active = asyncio.Event()

    async def active_work():
        active_started.set()
        await release_active.wait()

    active = asyncio.create_task(
        coordinator.submit("project-a", active_work, name="profile")
    )
    await active_started.wait()
    queued = asyncio.create_task(
        coordinator.submit("project-b", active_work, name="aac")
    )
    await asyncio.sleep(0)

    with pytest.raises(BackgroundWorkRejected) as error:
        await coordinator.submit("project-c", active_work, name="document-index")

    assert error.value.details["reason"] == "global_queue_full"
    assert error.value.details["limit"] == 1
    assert error.value.details["queued"] == 1

    release_active.set()
    await asyncio.gather(active, queued)
    await coordinator.shutdown()


@pytest.mark.no_network
async def test_background_work_coalesces_duplicate_project_operations():
    coordinator = BackgroundWorkCoordinator()
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def profile_work():
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()
        return "profile-complete"

    first = asyncio.create_task(
        coordinator.submit(
            "project-a",
            profile_work,
            name="profile",
            coalesce_key="job:profile_refinement",
        )
    )
    await started.wait()
    duplicate = asyncio.create_task(
        coordinator.submit(
            "project-a",
            profile_work,
            name="profile",
            coalesce_key="job:profile_refinement",
        )
    )
    await asyncio.sleep(0)

    assert coordinator.snapshot()["coalesced"] == 1
    assert calls == 1
    release.set()
    assert await asyncio.gather(first, duplicate) == [
        "profile-complete",
        "profile-complete",
    ]
    assert calls == 1

    await coordinator.shutdown()


@pytest.mark.no_network
async def test_cancelling_a_coalesced_waiter_does_not_cancel_shared_work():
    coordinator = BackgroundWorkCoordinator()
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def profile_work():
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()
        return "profile-complete"

    primary = asyncio.create_task(
        coordinator.submit(
            "project-a",
            profile_work,
            name="profile",
            coalesce_key="job:profile_refinement",
        )
    )
    await started.wait()
    cancelled_waiter = asyncio.create_task(
        coordinator.submit(
            "project-a",
            profile_work,
            name="profile",
            coalesce_key="job:profile_refinement",
        )
    )
    await asyncio.sleep(0)
    cancelled_waiter.cancel()

    with pytest.raises(asyncio.CancelledError):
        await cancelled_waiter
    release.set()

    assert await primary == "profile-complete"
    assert calls == 1
    assert coordinator.snapshot()["cancelled"] == 0

    await coordinator.shutdown()


@pytest.mark.no_network
async def test_scheduler_uses_a_stable_coalesce_key_for_profile_jobs(monkeypatch):
    async def ignore_events(*args, **kwargs):
        pass

    class RecordingBackgroundWork:
        def __init__(self):
            self.calls = []

        async def submit(self, project_id, operation, *, name, coalesce_key=None):
            self.calls.append((project_id, name, coalesce_key))
            return await operation()

    monkeypatch.setattr("infrastructure.job.scheduler.emit", ignore_events)
    background_work = RecordingBackgroundWork()
    scheduler = Scheduler(
        "ada",
        "project-a",
        FakeRedis(),
        background_work=background_work,
    )

    class ProfileJob:
        name = "profile_refinement"

        async def execute(self, ctx):
            return JobResult(summary="profile complete")

    result = await scheduler._execute_job(
        ProfileJob(),
        JobContext(user_name="ada", project_id="project-a"),
    )

    assert result.success is True
    assert background_work.calls == [
        ("project-a", "profile_refinement", "job:profile_refinement")
    ]


@pytest.mark.no_network
async def test_scheduler_jobs_share_project_fair_background_turns(monkeypatch):
    async def ignore_events(*args, **kwargs):
        pass

    monkeypatch.setattr("infrastructure.job.scheduler.emit", ignore_events)
    coordinator = BackgroundWorkCoordinator(max_concurrency=1)
    order = []
    release_first = asyncio.Event()

    class Job:
        enabled = True

        def __init__(self, name, blocker=None):
            self.name = name
            self.blocker = blocker
            self.started = asyncio.Event()

        async def execute(self, ctx):
            order.append(self.name)
            self.started.set()
            if self.blocker is not None:
                await self.blocker.wait()
            return JobResult(summary=self.name)

    first = Job("a-1", release_first)
    second = Job("a-2")
    other_project = Job("b-1")
    scheduler_a = Scheduler(
        "ada", "project-a", FakeRedis(), background_work=coordinator
    )
    scheduler_b = Scheduler(
        "ada", "project-b", FakeRedis(), background_work=coordinator
    )
    context_a = JobContext(user_name="ada", project_id="project-a")
    context_b = JobContext(user_name="ada", project_id="project-b")

    first_task = asyncio.create_task(scheduler_a._execute_job(first, context_a))
    await first.started.wait()
    second_task = asyncio.create_task(scheduler_a._execute_job(second, context_a))
    other_task = asyncio.create_task(
        scheduler_b._execute_job(other_project, context_b)
    )
    await asyncio.sleep(0)
    release_first.set()

    await asyncio.gather(first_task, second_task, other_task)
    assert order == ["a-1", "b-1", "a-2"]

    await coordinator.shutdown()
