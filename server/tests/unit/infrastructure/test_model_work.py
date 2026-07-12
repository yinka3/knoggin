import asyncio

import pytest

from common.schema.contracts import EngineScope, EngineWorkUnit
from infrastructure.model_work import ModelWorkCoordinator, ModelWorkPriority


class InlineExecutor:
    def submit(self, fn, *args, **kwargs):
        raise AssertionError("run_in_executor should not use InlineExecutor directly")


def parent_work_unit():
    scope = EngineScope(
        user_name="ada",
        session_id="session-1",
        project_id="project-1",
    )
    return EngineWorkUnit.for_message_batch(scope, [1, 2])


@pytest.mark.no_network
async def test_model_work_prioritizes_foreground_items_already_in_queue():
    coordinator = ModelWorkCoordinator(InlineExecutor())
    order = []
    release_first = asyncio.Event()

    async def first_background():
        order.append("first-background")
        await release_first.wait()
        return "first-background"

    async def second_background():
        order.append("second-background")
        return "second-background"

    async def foreground():
        order.append("foreground")
        return "foreground"

    first_task = asyncio.create_task(
        coordinator.submit(
            first_background,
            priority=ModelWorkPriority.BACKGROUND,
            name="first-background",
        )
    )
    await asyncio.sleep(0)
    second_background_task = asyncio.create_task(
        coordinator.submit(
            second_background,
            priority=ModelWorkPriority.BACKGROUND,
            name="second-background",
        )
    )
    foreground_task = asyncio.create_task(
        coordinator.submit(
            foreground,
            priority=ModelWorkPriority.FOREGROUND,
            name="foreground",
        )
    )
    # The background lane is occupied, but the foreground lane is reserved and
    # can complete before the profile-like work is released.
    assert await foreground_task == "foreground"
    assert order == ["first-background", "foreground"]
    release_first.set()

    assert await first_task == "first-background"
    assert await second_background_task == "second-background"
    assert order == ["first-background", "foreground", "second-background"]
    snapshot = coordinator.snapshot()
    assert snapshot["foreground_concurrency"] == 1
    assert snapshot["background_concurrency"] == 1
    assert snapshot["completed"] == 3
    assert snapshot["work_by_name"]["foreground"]["completed"] == 1
    assert snapshot["work_by_name"]["second-background"]["completed"] == 1

    await coordinator.shutdown()


@pytest.mark.no_network
async def test_model_work_shutdown_cancels_queued_items():
    coordinator = ModelWorkCoordinator(InlineExecutor())
    release_first = asyncio.Event()

    async def first_work():
        await release_first.wait()

    async def never_runs():
        raise AssertionError("queued work should be cancelled at shutdown")

    first_task = asyncio.create_task(
        coordinator.submit(
            first_work,
            priority=ModelWorkPriority.BACKGROUND,
            name="first",
        )
    )
    await asyncio.sleep(0)
    task = asyncio.create_task(
        coordinator.submit(
            never_runs,
            priority=ModelWorkPriority.BACKGROUND,
            name="queued",
        )
    )
    await asyncio.sleep(0)
    shutdown_task = asyncio.create_task(coordinator.shutdown())
    release_first.set()
    await shutdown_task

    with pytest.raises(asyncio.CancelledError):
        await task
    await first_task
    assert coordinator.snapshot()["work_by_name"]["queued"]["queued"] == 0


@pytest.mark.no_network
async def test_model_work_records_serializable_child_unit_on_parent():
    coordinator = ModelWorkCoordinator(InlineExecutor())
    parent = parent_work_unit()
    child = EngineWorkUnit.for_model_operation(
        "gliner",
        parent.scope,
        parent_work_unit_id=parent.id,
        priority=parent.priority,
    )

    async def extract_mentions():
        return ["Ada"]

    result = await coordinator.submit(
        extract_mentions,
        priority=ModelWorkPriority.BACKGROUND,
        name="gliner-mentions",
        work_unit=child,
        parent_work_unit=parent,
    )

    assert result == ["Ada"]
    assert child.status == "succeeded"
    assert child.parent_work_unit_id == parent.id
    assert child.trace.queued_at is not None
    assert child.trace.started_at is not None
    assert child.trace.finished_at is not None
    assert parent.metadata["model_work"] == [
        {
            "id": child.id,
            "kind": "gliner",
            "status": "succeeded",
            "priority": parent.priority,
            "stage": "gliner",
            "queue_wait_ms": child.trace.queue_wait_ms,
            "duration_ms": child.trace.duration_ms,
            "summary": None,
        }
    ]
    assert "operation" not in parent.model_dump(mode="json")

    await coordinator.shutdown()


@pytest.mark.no_network
async def test_model_work_marks_queued_child_cancelled_on_shutdown():
    coordinator = ModelWorkCoordinator(InlineExecutor())
    parent = parent_work_unit()
    release_first = asyncio.Event()
    child = EngineWorkUnit.for_model_operation(
        "embedding",
        parent.scope,
        parent_work_unit_id=parent.id,
    )

    async def first_work():
        await release_first.wait()

    async def queued_work():
        raise AssertionError("queued work should not execute")

    first_task = asyncio.create_task(
        coordinator.submit(
            first_work,
            priority=ModelWorkPriority.BACKGROUND,
            name="first",
        )
    )
    await asyncio.sleep(0)
    queued_task = asyncio.create_task(
        coordinator.submit(
            queued_work,
            priority=ModelWorkPriority.BACKGROUND,
            name="embedding-encode",
            work_unit=child,
            parent_work_unit=parent,
        )
    )
    await asyncio.sleep(0)

    shutdown_task = asyncio.create_task(coordinator.shutdown())
    release_first.set()
    await shutdown_task

    with pytest.raises(asyncio.CancelledError):
        await queued_task
    await first_task
    assert child.status == "cancelled"
    assert parent.metadata["model_work"][0]["status"] == "cancelled"


@pytest.mark.no_network
async def test_foreground_deadline_returns_without_interrupting_running_work():
    coordinator = ModelWorkCoordinator(
        InlineExecutor(),
        foreground_timeout_seconds=0.01,
    )
    started = asyncio.Event()
    release = asyncio.Event()

    async def slow_foreground_work():
        started.set()
        await release.wait()
        return "completed after caller timeout"

    task = asyncio.create_task(
        coordinator.submit(
            slow_foreground_work,
            priority=ModelWorkPriority.FOREGROUND,
            name="foreground-test",
        )
    )
    await started.wait()

    with pytest.raises(asyncio.TimeoutError):
        await task
    assert coordinator.snapshot()["timed_out"] == 1

    release.set()
    await asyncio.sleep(0)
    await coordinator.shutdown()
