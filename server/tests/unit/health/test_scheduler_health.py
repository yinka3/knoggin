import asyncio
from datetime import timedelta

import pytest

from common.utils.time_utils import get_now
from infrastructure.job.base import BaseJob, JobContext, JobResult
from infrastructure.job.scheduler import Scheduler


class _Job(BaseJob):
    def __init__(self, name: str = "document_index_recovery"):
        self._name = name

    @property
    def name(self):
        return self._name

    async def should_run(self, _ctx: JobContext) -> bool:
        return False

    async def execute(self, _ctx: JobContext) -> JobResult:
        return JobResult(success=True, summary="completed")


@pytest.mark.unit
@pytest.mark.no_network
async def test_scheduler_health_reports_queued_and_stalled_runs_without_leases():
    scheduler = Scheduler("ada", "project-a")
    scheduler.register(_Job())
    scheduler._is_running = True
    scheduler._started_at = get_now()
    task = asyncio.create_task(asyncio.sleep(30))
    scheduler._running_tasks["document_index_recovery"] = task
    scheduler._job_runs["document_index_recovery"] = {
        "name": "document_index_recovery",
        "state": "running",
        "queued_at": get_now(),
        "started_at": get_now() - timedelta(seconds=2),
        "execution_timeout_seconds": 0.01,
    }

    snapshot = scheduler.snapshot_for_health()

    assert snapshot["state"] == "running"
    assert snapshot["stalled_jobs"] == 1
    assert snapshot["active_jobs"][0]["state"] == "stalled"
    assert snapshot["registered_jobs"] == ["document_index_recovery"]
    assert snapshot["recent_outcomes"] == []
    assert "lease_seconds" not in snapshot["active_jobs"][0]

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


@pytest.mark.unit
@pytest.mark.no_network
def test_scheduler_health_keeps_recent_outcomes_bounded_and_safe():
    scheduler = Scheduler("ada", "project-a")
    scheduler.register(_Job())
    for _ in range(40):
        scheduler._finish_job_run("document_index_recovery", "failed")

    snapshot = scheduler.snapshot_for_health()

    assert len(snapshot["recent_outcomes"]) == 20
    assert all(outcome["state"] == "failed" for outcome in snapshot["recent_outcomes"])
    assert "project-a" not in str(snapshot)
