import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from common.exceptions import StorageWriteError
from core.knowledge.db.writers.artifact_retention_writer import ArtifactRetentionWriter
from core.knowledge.jobs.application_maintenance_scheduler import (
    ApplicationMaintenanceScheduler,
)
from core.project.project_manager import ProjectManager
from infrastructure.background_work import BackgroundWorkCoordinator
from tests.fixtures.fakes import FakeResources


class FakeMaintenanceService:
    def __init__(self):
        self.calls = 0

    async def preflight(self, *, user_name):
        self.calls += 1
        return {"candidate_count": 0, "candidates": [], "llm_required": False}


@pytest.mark.unit
@pytest.mark.no_network
async def test_application_scheduler_runs_global_preflight_without_project_runtime():
    service = FakeMaintenanceService()
    scheduler = ApplicationMaintenanceScheduler(
        maintenance_service=service,
        user_name="ada",
        interval_seconds=3600,
    )

    result = await scheduler.run_once()

    assert result["llm_required"] is False
    assert service.calls == 1
    assert scheduler.snapshot()["running"] is False


@pytest.mark.unit
@pytest.mark.no_network
async def test_application_scheduler_uses_a_distinct_background_owner():
    service = FakeMaintenanceService()
    coordinator = BackgroundWorkCoordinator(max_concurrency=1)
    scheduler = ApplicationMaintenanceScheduler(
        maintenance_service=service,
        user_name="ada",
        background_work=coordinator,
        interval_seconds=3600,
    )

    await scheduler.run_once()
    await scheduler.stop()

    assert service.calls == 1
    assert coordinator.snapshot()["active_by_owner"] == {}
    await coordinator.shutdown()


@pytest.mark.unit
@pytest.mark.no_network
def test_project_manager_composes_user_owned_expiry_without_loaded_projects():
    resources = FakeResources()
    manager = ProjectManager(resources, "ada")
    writer = manager.maintenance_scheduler.artifact_retention_writer
    assert isinstance(writer, ArtifactRetentionWriter)
    assert writer.client is resources.postgres
    assert manager.maintenance_scheduler.user_name == "ada"
    assert manager.active_projects == {}
    assert manager.maintenance_scheduler.interval_seconds == 300


@pytest.mark.unit
@pytest.mark.no_network
async def test_artifact_sweep_runs_directly_and_retries_with_honest_status():
    writer = SimpleNamespace(purge_expired_artifacts=AsyncMock(side_effect=[StorageWriteError("expiry"), 2, 0]))
    scheduler = ApplicationMaintenanceScheduler(
        maintenance_service=FakeMaintenanceService(), user_name="ada",
        artifact_retention_writer=writer,
    )
    with pytest.raises(StorageWriteError):
        await scheduler.purge_expired_artifacts()
    assert scheduler.snapshot()["artifact_retention"] == {"last_deleted_count": None, "last_sweep_succeeded": False}
    assert await scheduler.purge_expired_artifacts() == 2
    assert scheduler.snapshot()["artifact_retention"] == {"last_deleted_count": 2, "last_sweep_succeeded": True}
    assert await scheduler.purge_expired_artifacts() == 0
    assert writer.purge_expired_artifacts.await_args.kwargs == {"user_name": "ada"}


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize("fails", ["artifact", "entity"])
async def test_maintenance_failures_do_not_starve_the_other_job(fails):
    service = FakeMaintenanceService()
    progressed = asyncio.Event()
    sweeps = 0

    async def purge(**_kwargs):
        nonlocal sweeps
        sweeps += 1
        if sweeps >= 2 and service.calls >= 1:
            progressed.set()
        if fails == "artifact":
            raise RuntimeError("failed sweep")
        return 0

    original = service.preflight

    async def preflight(**kwargs):
        result = await original(**kwargs)
        if fails == "entity":
            raise RuntimeError("failed preflight")
        return result

    service.preflight = preflight
    scheduler = ApplicationMaintenanceScheduler(
        maintenance_service=service, user_name="ada",
        artifact_retention_writer=SimpleNamespace(purge_expired_artifacts=purge),
        interval_seconds=0.001,
    )
    try:
        await scheduler.start()
        await asyncio.wait_for(progressed.wait(), timeout=2)
        assert service.calls >= 1 and sweeps >= 2
    finally:
        await scheduler.stop()


@pytest.mark.unit
@pytest.mark.no_network
async def test_stop_cancels_artifact_owner_but_not_unrelated_background_work():
    coordinator = BackgroundWorkCoordinator(max_concurrency=2)
    sweep_started, sweep_closed = asyncio.Event(), asyncio.Event()
    other_started, release_other = asyncio.Event(), asyncio.Event()

    async def purge(**_kwargs):
        sweep_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            sweep_closed.set()

    async def other_work():
        other_started.set()
        await release_other.wait()

    scheduler = ApplicationMaintenanceScheduler(
        maintenance_service=FakeMaintenanceService(), user_name="ada",
        artifact_retention_writer=SimpleNamespace(purge_expired_artifacts=purge),
        background_work=coordinator,
    )
    other = asyncio.create_task(coordinator.submit(
        "other-project", other_work, name="unrelated", owner="project:unrelated",
    ))
    try:
        await scheduler.start()
        await asyncio.wait_for(sweep_started.wait(), timeout=2)
        await asyncio.wait_for(other_started.wait(), timeout=2)
        assert coordinator.snapshot()["active_by_owner"]["application:artifact-retention"] == ["artifact-retention-purge"]
        await scheduler.stop()
        assert sweep_closed.is_set()
        assert not other.done()
        assert coordinator.snapshot()["active_by_owner"] == {"project:unrelated": ["unrelated"]}
    finally:
        release_other.set()
        await asyncio.gather(other, return_exceptions=True)
        await scheduler.stop()
        await coordinator.shutdown()
