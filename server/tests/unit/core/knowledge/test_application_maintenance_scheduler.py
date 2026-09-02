import pytest

from core.knowledge.jobs.application_maintenance_scheduler import (
    ApplicationMaintenanceScheduler,
)
from infrastructure.background_work import BackgroundWorkCoordinator


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
