import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest

from infrastructure.model_work import (
    ModelWorkCoordinator,
    ModelWorkPriority,
    ModelWorkRejected,
)
from infrastructure.work_record import WorkRecord, WorkScope, WorkStatus


@pytest.fixture
async def model_work():
    executor = ThreadPoolExecutor(max_workers=2)
    coordinator = ModelWorkCoordinator(
        executor,
        foreground_concurrency=1,
        background_concurrency=1,
        max_queued_foreground=1,
        max_queued_background=1,
        foreground_timeout_seconds=None,
    )
    try:
        yield coordinator
    finally:
        await coordinator.shutdown()
        executor.shutdown(wait=True)


@pytest.mark.no_network
async def test_model_work_rejects_a_lane_that_reaches_its_queue_limit(model_work):
    started = asyncio.Event()
    release = asyncio.Event()

    async def active_work():
        started.set()
        await release.wait()
        return "active"

    active = asyncio.create_task(
        model_work.submit(
            active_work,
            priority=ModelWorkPriority.FOREGROUND,
            name="embedding",
        )
    )
    await started.wait()
    queued = asyncio.create_task(
        model_work.submit(
            lambda: asyncio.sleep(0, result="queued"),
            priority=ModelWorkPriority.FOREGROUND,
            name="embedding",
        )
    )
    await asyncio.sleep(0)

    with pytest.raises(ModelWorkRejected) as error:
        await model_work.submit(
            lambda: asyncio.sleep(0, result="rejected"),
            priority=ModelWorkPriority.FOREGROUND,
            name="embedding",
        )

    assert error.value.details == {
        "priority": "foreground",
        "name": "embedding",
        "reason": "queue_full",
        "limit": 1,
        "queued": 1,
    }
    assert model_work.snapshot()["rejected"] == 1

    release.set()
    assert await active == "active"
    assert await queued == "queued"


@pytest.mark.no_network
async def test_model_work_cancelling_a_queued_caller_does_not_run_or_leak(model_work):
    started = asyncio.Event()
    release = asyncio.Event()
    queued_calls = 0

    async def active_work():
        started.set()
        await release.wait()

    async def queued_work():
        nonlocal queued_calls
        queued_calls += 1

    active = asyncio.create_task(
        model_work.submit(
            active_work,
            priority=ModelWorkPriority.BACKGROUND,
            name="document-index",
        )
    )
    await started.wait()
    queued = asyncio.create_task(
        model_work.submit(
            queued_work,
            priority=ModelWorkPriority.BACKGROUND,
            name="document-index",
        )
    )
    await asyncio.sleep(0)
    queued.cancel()
    with pytest.raises(asyncio.CancelledError):
        await queued

    release.set()
    await active
    await asyncio.sleep(0)
    snapshot = model_work.snapshot()
    assert queued_calls == 0
    assert snapshot["queued"] == 0
    assert snapshot["in_flight"] == 0
    assert snapshot["cancelled"] == 1


@pytest.mark.no_network
async def test_model_work_records_terminal_state_and_parent_summary(model_work):
    scope = WorkScope(
        user_name="ada", project_id="project-1", session_id="session-1"
    )
    parent = WorkRecord.for_semantic_window(scope, "window-1")
    child = WorkRecord.for_model_operation("embedding", scope, parent_id=parent.id)

    result = await model_work.submit(
        lambda: asyncio.sleep(0, result="embedded"),
        priority=ModelWorkPriority.FOREGROUND,
        name="embedding",
        work_record=child,
        parent_work_record=parent,
    )

    assert result == "embedded"
    assert child.status is WorkStatus.SUCCEEDED
    assert parent.metadata["model_work"][0]["status"] == WorkStatus.SUCCEEDED
    snapshot = model_work.snapshot()
    assert snapshot["queued"] == 0
    assert snapshot["in_flight"] == 0
    assert snapshot["completed"] == 1
    assert snapshot["work_by_name"]["embedding"]["queued"] == 0
