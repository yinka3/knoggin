from datetime import timedelta

from loguru import logger

from common.schema.settings import AuditRetentionSettings
from common.utils.time_utils import get_now
from core.knowledge.store import KnowledgeStore
from infrastructure.job.base import BaseJob, JobContext, JobResult


class AuditRetentionCleanupJob(BaseJob):
    """Expires non-canonical operational records after configured windows."""

    def __init__(
        self,
        knowledge_store: KnowledgeStore,
        settings: AuditRetentionSettings,
    ):
        self.knowledge_store = knowledge_store
        self.update_settings(settings)

    @property
    def name(self) -> str:
        return "audit_retention_cleanup"

    @property
    def cadence_seconds(self) -> float:
        return self._interval_seconds

    async def should_run(self, ctx: JobContext) -> bool:
        return False

    async def execute(self, ctx: JobContext) -> JobResult:
        now = get_now()
        counts = await self.knowledge_store.purge_expired_operational_records(
            user_name=ctx.user_name,
            project_id=ctx.project_id,
            tool_audit_cutoff=now - timedelta(days=self.tool_audit_days),
            merge_history_cutoff=now - timedelta(days=self.merge_history_days),
        )
        total = sum(counts.values())
        logger.info(
            "Purged {} expired operational records for project {}",
            total,
            ctx.project_id,
        )
        return JobResult(
            success=True,
            summary=f"Purged {total} expired operational records",
        )

    def update_settings(self, settings: AuditRetentionSettings) -> None:
        self.enabled = settings.enabled
        self._interval_seconds = settings.interval_hours * 3600
        self.tool_audit_days = settings.tool_audit_days
        self.merge_history_days = settings.merge_history_days
