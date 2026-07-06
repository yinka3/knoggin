from datetime import timedelta

import pytest

from common.schema.settings import MergeRollbackSettings
from common.utils.time_utils import frozen_time, get_now
from infrastructure.job.base import JobContext
from core.knowledge.jobs.merge_rollback_cleanup_job import (
    MergeCleanupJob,
)


class RecordingKnowledgeStore:
    def __init__(self, expire_result=0):
        self.expire_result = expire_result
        self.calls = []

    async def expire_merge_rollback_states(
        self,
        cutoff,
        *,
        user_name,
        project_id,
    ):
        self.calls.append((cutoff, user_name, project_id))
        return self.expire_result


@pytest.mark.no_network
async def test_merge_rollback_cleanup_job_expires_old_available_state():
    store = RecordingKnowledgeStore(expire_result=2)
    job = MergeCleanupJob(
        store,
        retention_hours=5,
        fallback_interval_hours=1,
    )
    ctx = JobContext(user_name="ada", project_id="project-1")

    with frozen_time("2026-01-01T10:00:00+00:00"):
        expected_cutoff = get_now() - timedelta(hours=5)
        result = await job.execute(ctx)

    assert result.success is True
    assert result.summary == "Expired 2 merge rollback states"
    assert store.calls == [
        (
            expected_cutoff,
            "ada",
            "project-1",
        )
    ]


@pytest.mark.no_network
async def test_merge_rollback_cleanup_job_settings_update_cadence_and_window():
    job = MergeCleanupJob(RecordingKnowledgeStore())

    job.update_settings(
        MergeRollbackSettings(
            enabled=False,
            retention_hours=6,
            fallback_interval_hours=2,
        )
    )

    assert job.enabled is False
    assert job.retention_hours == 6
    assert job.cadence_seconds == 7200
