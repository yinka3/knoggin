import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from common.exceptions import WorkspaceConflictError
from core.project.project_manager import ProjectManager

pytestmark = [pytest.mark.runtime, pytest.mark.no_network]


def make_manager():
    metadata = {"id": "p1", "name": "Project", "status": "active", "allowed_projects": []}
    manager = object.__new__(ProjectManager)
    manager.user_name = "ada"
    manager._closed = False
    manager.active_projects = {}
    manager._project_leases = {}
    manager.maintenance_service = SimpleNamespace(lock=asyncio.Lock())

    async def read(project_id):
        return dict(metadata)

    async def readable(project_id, **kwargs):
        return [project_id, *metadata["allowed_projects"]]

    async def validate(project_id, ids):
        return ids

    async def execute(query, params):
        metadata["status"] = params["status"]

    @asynccontextmanager
    async def transaction():
        pending = []

        async def write(query, params):
            if "INSERT" in query:
                pending.append(params["readable"])

        yield SimpleNamespace(execute=write)
        metadata["allowed_projects"] = pending

    manager.get_project = read
    manager.get_readable_project_ids = readable
    manager._validate_allowed_project_ids = validate
    manager.pg = SimpleNamespace(execute=execute, transaction=transaction)
    manager.project_factory = SimpleNamespace(create=AsyncMock(return_value=SimpleNamespace()))
    return manager, metadata


async def start_acquisition(manager):
    started = asyncio.Event()

    async def acquire():
        started.set()
        return await manager.acquire_project_for_session("p1", "s1")

    task = asyncio.create_task(acquire())
    await asyncio.wait_for(started.wait(), 2)
    return task


async def test_archive_keeps_admission_excluded_through_database_write():
    manager, metadata = make_manager()
    entered, finish = asyncio.Event(), asyncio.Event()

    async def execute(query, params):
        entered.set()
        await finish.wait()
        metadata["status"] = params["status"]

    manager.pg.execute = execute
    archive = asyncio.create_task(manager.archive_project("p1"))
    await asyncio.wait_for(entered.wait(), 2)
    acquisition = await start_acquisition(manager)
    try:
        assert not acquisition.done()
        manager.project_factory.create.assert_not_awaited()
    finally:
        finish.set()
    assert (await asyncio.wait_for(archive, 2))["status"] == "archived"
    with pytest.raises(ValueError, match="archived"):
        await asyncio.wait_for(acquisition, 2)
    assert manager.active_projects == manager._project_leases == {}


@pytest.mark.parametrize("phase", ["validation", "transaction"])
async def test_scope_update_blocks_admission_until_new_scope_is_committed(phase):
    manager, metadata = make_manager()
    entered, finish = asyncio.Event(), asyncio.Event()

    async def validate(project_id, ids):
        if phase == "validation":
            entered.set()
            await finish.wait()
        return ids

    @asynccontextmanager
    async def transaction():
        async def write(query, params):
            if phase == "transaction" and "DELETE" in query:
                entered.set()
                await finish.wait()
        yield SimpleNamespace(execute=write)
        metadata["allowed_projects"] = ["p2"]

    manager._validate_allowed_project_ids = validate
    manager.pg.transaction = transaction
    update = asyncio.create_task(manager.update_project("p1", allowed_projects=["p2"]))
    await asyncio.wait_for(entered.wait(), 2)
    acquisition = await start_acquisition(manager)
    try:
        assert not acquisition.done()
        manager.project_factory.create.assert_not_awaited()
    finally:
        finish.set()
    await asyncio.wait_for(update, 2)
    await asyncio.wait_for(acquisition, 2)
    manager.project_factory.create.assert_awaited_once_with(project_id="p1", readable_project_ids=["p1", "p2"])
    assert manager._project_leases == {"p1": {"s1"}}


async def test_acquisition_that_wins_lock_keeps_archive_from_retiring_live_runtime():
    manager, metadata = make_manager()
    entered, finish = asyncio.Event(), asyncio.Event()

    async def create(**kwargs):
        entered.set()
        await finish.wait()
        return SimpleNamespace()

    manager.project_factory.create.side_effect = create
    acquisition = await start_acquisition(manager)
    await asyncio.wait_for(entered.wait(), 2)
    archive = asyncio.create_task(manager.archive_project("p1"))
    finish.set()
    await asyncio.wait_for(acquisition, 2)
    with pytest.raises(WorkspaceConflictError):
        await asyncio.wait_for(archive, 2)
    assert metadata["status"] == "active"
    assert manager._project_leases == {"p1": {"s1"}}


@pytest.mark.parametrize("operation", ["archive", "scope"])
async def test_failed_runtime_shutdown_is_retained_and_mutation_can_retry(operation):
    manager, metadata = make_manager()
    state = SimpleNamespace(shutdown=AsyncMock(side_effect=[RuntimeError("shutdown failed"), None]))
    manager.active_projects["p1"] = state

    async def mutate():
        if operation == "archive":
            return await manager.archive_project("p1")
        return await manager.update_project("p1", allowed_projects=["p2"])

    with pytest.raises(RuntimeError, match="shutdown failed"):
        await mutate()
    assert manager.active_projects["p1"] is state
    assert metadata["status"] == "active" and metadata["allowed_projects"] == []
    assert not manager.maintenance_service.lock.locked()
    await mutate()
    assert manager.active_projects == {}
    assert state.shutdown.await_count == 2


async def test_failed_archive_write_releases_lock_and_can_retry():
    manager, metadata = make_manager()
    original = manager.pg.execute
    manager.pg.execute = AsyncMock(side_effect=RuntimeError("write failed"))
    with pytest.raises(RuntimeError, match="write failed"):
        await manager.archive_project("p1")
    assert metadata["status"] == "active"
    assert not manager.maintenance_service.lock.locked()
    manager.pg.execute = original
    assert (await manager.archive_project("p1"))["status"] == "archived"


@pytest.mark.parametrize("operation", ["archive", "scope"])
async def test_cancellation_settles_owned_mutation_before_releasing_admission_lock(operation):
    manager, metadata = make_manager()
    entered, finish = asyncio.Event(), asyncio.Event()

    async def execute(query, params):
        entered.set()
        await finish.wait()
        metadata["status"] = params["status"]

    manager.pg.execute = execute

    async def validate(project_id, ids):
        entered.set()
        await finish.wait()
        return ids

    manager._validate_allowed_project_ids = validate
    archive = asyncio.create_task(
        manager.archive_project("p1") if operation == "archive"
        else manager.update_project("p1", allowed_projects=["p2"])
    )
    await asyncio.wait_for(entered.wait(), 2)
    archive.cancel()
    acquisition = await start_acquisition(manager)
    try:
        assert manager.maintenance_service.lock.locked()
        assert not acquisition.done()
        archive.cancel()  # repeated caller cancellation must not cancel the owner
    finally:
        finish.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(archive, 2)
    if operation == "archive":
        with pytest.raises(ValueError, match="archived"):
            await asyncio.wait_for(acquisition, 2)
        assert metadata["status"] == "archived"
        assert manager.active_projects == manager._project_leases == {}
    else:
        await asyncio.wait_for(acquisition, 2)
        assert metadata["allowed_projects"] == ["p2"]
        manager.project_factory.create.assert_awaited_once_with(project_id="p1", readable_project_ids=["p1", "p2"])


async def test_scope_write_failure_keeps_old_scope_and_allows_retry():
    manager, metadata = make_manager()
    original = manager.pg.transaction

    @asynccontextmanager
    async def failing_transaction():
        yield SimpleNamespace(execute=AsyncMock(side_effect=RuntimeError("scope write failed")))

    manager.pg.transaction = failing_transaction
    with pytest.raises(RuntimeError, match="scope write failed"):
        await manager.update_project("p1", allowed_projects=["p2"])
    assert metadata["allowed_projects"] == []
    assert not manager.maintenance_service.lock.locked()
    manager.pg.transaction = original
    await manager.update_project("p1", allowed_projects=["p2"])
    assert metadata["allowed_projects"] == ["p2"]


async def test_cancelled_lock_waiter_does_not_start_mutation():
    manager, metadata = make_manager()
    await manager.maintenance_service.lock.acquire()
    archive = asyncio.create_task(manager.archive_project("p1"))
    archive.cancel()
    with pytest.raises(asyncio.CancelledError):
        await archive
    manager.maintenance_service.lock.release()
    assert metadata["status"] == "active"


async def test_reactivation_and_shutdown_gate_share_mutation_ownership():
    manager, metadata = make_manager()
    metadata["status"] = "archived"
    assert (await manager.reactivate_project("p1"))["status"] == "active"
    manager._closed = True
    with pytest.raises(RuntimeError, match="shutting down"):
        await manager.archive_project("p1")
    assert metadata["status"] == "active"
