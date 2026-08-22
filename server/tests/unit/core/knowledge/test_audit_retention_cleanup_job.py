from datetime import timedelta

import pytest

from common.schema.settings import AuditRetentionSettings
from common.utils.time_utils import frozen_time, get_now
from core.knowledge.jobs.audit_retention_cleanup_job import (
    AuditRetentionCleanupJob,
)
from infrastructure.job.base import JobContext


class RecordingKnowledgeStore:
    def __init__(self, result=None):
        self.result = result or {
            "tool_audits": 2,
            "merge_audits": 3,
            "merge_proposals": 4,
        }
        self.calls = []

    async def purge_expired_operational_records(self, **kwargs):
        self.calls.append(kwargs)
        return self.result


@pytest.mark.no_network
async def test_audit_retention_cleanup_uses_each_configured_window():
    store = RecordingKnowledgeStore()
    job = AuditRetentionCleanupJob(
        store,
        AuditRetentionSettings(
            interval_hours=2,
            tool_audit_days=180,
            merge_history_days=365,
        ),
    )
    ctx = JobContext(user_name="ada", project_id="project-1")

    with frozen_time("2026-01-01T10:00:00+00:00"):
        now = get_now()
        result = await job.execute(ctx)

    assert result.summary == "Purged 9 expired operational records"
    assert job.cadence_seconds == 7200
    assert store.calls == [
        {
            "user_name": "ada",
            "project_id": "project-1",
            "tool_audit_cutoff": now - timedelta(days=180),
            "merge_history_cutoff": now - timedelta(days=365),
        }
    ]
