import asyncio

import pytest

from infrastructure.background_work import (
    BackgroundWorkCoordinator,
    BackgroundWorkRejected,
)
from infrastructure.job.base import JobContext, JobResult
from infrastructure.job.scheduler import Scheduler


@pytest.mark.no_network
async def test_background_work_is_global_fifo_without_project_serialization():
    coordinator = BackgroundWorkCoordinator(max_concurrency=2)
    order: list[str] = []
    first_started = asyncio.Event()
    second_started = asyncio.Event()
    release = asyncio.Event()

    async def first_project_a_work():
        order.append("a-1")
        first_started.set()
        await release.wait()
        return "a-1"

    async def second_project_a_work():
        order.append("a-2")
        second_started.set()
        return "a-2"

    first = asyncio.create_task(
        coordinator.submit("project-a", first_project_a_work, name="profile")
    )
    await first_started.wait()
    second = asyncio.create_task(
        coordinator.submit("project-a", second_project_a_work, name="aac")
    )
    await second_started.wait()

    assert order == ["a-1", "a-2"]
    release.set()
    assert await asyncio.gather(first, second) == ["a-1", "a-2"]
    await coordinator.shutdown()


@pytest.mark.no_network
async def test_background_work_preserves_global_submission_order():
    coordinator = BackgroundWorkCoordinator(max_concurrency=1)
    order: list[str] = []
    first_started = asyncio.Event()
    release = asyncio.Event()

    async def operation(label: str, *, block: bool = False):
        order.append(label)
        if block:
            first_started.set()
            await release.wait()
        return label

    first = asyncio.create_task(
        coordinator.submit(
            "project-a", lambda: operation("a-1", block=True), name="profile"
        )
    )
    await first_started.wait()
    second = asyncio.create_task(
        coordinator.submit("project-a", lambda: operation("a-2"), name="profile")
    )
    third = asyncio.create_task(
        coordinator.submit("project-b", lambda: operation("b-1"), name="aac")
    )
    await asyncio.sleep(0)

    release.set()
    assert await asyncio.gather(first, second, third) == ["a-1", "a-2", "b-1"]
    assert order == ["a-1", "a-2", "b-1"]
    await coordinator.shutdown()


@pytest.mark.no_network
async def test_background_work_rejects_when_global_queue_is_full():
    coordinator = BackgroundWorkCoordinator(max_concurrency=1, max_queued_global=1)
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
        coordinator.submit("project-a", active_work, name="aac")
    )
    await asyncio.sleep(0)

    with pytest.raises(BackgroundWorkRejected) as error:
        await coordinator.submit("project-b", active_work, name="document-index")

    assert error.value.details == {
        "project_id": "project-b",
        "name": "document-index",
        "reason": "global_queue_full",
        "limit": 1,
        "queued": 1,
    }
    assert coordinator.snapshot()["rejected_by_reason"] == {"global_queue_full": 1}

    release_active.set()
    await asyncio.gather(active, queued)
    await coordinator.shutdown()


@pytest.mark.no_network
async def test_background_work_coalesces_by_project_and_operation_key():
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
            "project-a", profile_work, name="profile", coalesce_key="profile"
        )
    )
    await started.wait()
    duplicate = asyncio.create_task(
        coordinator.submit(
            "project-a", profile_work, name="profile", coalesce_key="profile"
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
    await coordinator.shutdown()


@pytest.mark.no_network
async def test_cancel_owner_preserves_unrelated_project_work():
    coordinator = BackgroundWorkCoordinator(max_concurrency=1, max_queued_global=3)
    started = asyncio.Event()
    release = asyncio.Event()

    async def owned_work():
        started.set()
        await release.wait()

    async def unrelated_work():
        return "kept"

    active = asyncio.create_task(
        coordinator.submit(
            "project-a",
            owned_work,
            name="document-index",
            owner="project:project-a:document-index",
        )
    )
    await started.wait()
    unrelated = asyncio.create_task(
        coordinator.submit(
            "project-a",
            unrelated_work,
            name="episode",
            owner="project:project-a:episode",
        )
    )

    await coordinator.cancel_owner("project:project-a:document-index")
    with pytest.raises(asyncio.CancelledError):
        await active
    assert await unrelated == "kept"
    await coordinator.shutdown()


@pytest.mark.no_network
async def test_global_shutdown_cancels_queued_and_active_operations():
    coordinator = BackgroundWorkCoordinator(max_concurrency=1)
    active_started = asyncio.Event()
    active_stopped = asyncio.Event()

    async def active_work():
        active_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            active_stopped.set()

    async def queued_work():
        raise AssertionError("queued work must not run after shutdown")

    active = asyncio.create_task(
        coordinator.submit("project-a", active_work, name="profile")
    )
    await active_started.wait()
    queued = asyncio.create_task(
        coordinator.submit("project-b", queued_work, name="aac")
    )
    await asyncio.sleep(0)

    await coordinator.shutdown()
    await active_stopped.wait()
    with pytest.raises(asyncio.CancelledError):
        await active
    with pytest.raises(asyncio.CancelledError):
        await queued
    with pytest.raises(RuntimeError, match="closed"):
        await coordinator.submit("project-a", active_work, name="profile")


@pytest.mark.no_network
async def test_submit_waiting_for_queue_lock_is_rejected_after_shutdown_starts():
    coordinator = BackgroundWorkCoordinator()
    await coordinator.start()

    async def work():
        return "never admitted"

    async with coordinator._condition:
        pending_submit = asyncio.create_task(
            coordinator.submit("project-a", work, name="profile")
        )
        await asyncio.sleep(0)
        shutdown = asyncio.create_task(coordinator.shutdown())
        await asyncio.sleep(0)

    with pytest.raises(RuntimeError, match="closed"):
        await pending_submit
    await shutdown


@pytest.mark.no_network
async def test_scheduler_submits_stable_project_owned_coalesce_key(monkeypatch):
    async def ignore_events(*args, **kwargs):
        pass

    class RecordingBackgroundWork:
        def __init__(self):
            self.calls = []

        async def submit(self, project_id, operation, *, name, coalesce_key=None):
            self.calls.append((project_id, name, coalesce_key))
            return await operation()

    class ProfileJob:
        name = "profile_refinement"

        async def execute(self, _ctx):
            return JobResult(success=True, summary="profile complete")

    monkeypatch.setattr("infrastructure.job.scheduler.emit", ignore_events)
    background_work = RecordingBackgroundWork()
    scheduler = Scheduler("ada", "project-a", background_work=background_work)

    result = await scheduler._execute_job(
        ProfileJob(),
        JobContext(user_name="ada", project_id="project-a"),
    )

    assert result.success is True
    assert background_work.calls == [
        ("project-a", "profile_refinement", "job:profile_refinement")
    ]
