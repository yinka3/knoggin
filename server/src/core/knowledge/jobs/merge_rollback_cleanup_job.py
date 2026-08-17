from datetime import timedelta

from loguru import logger

from common.schema.settings import MergeRollbackSettings
from common.utils.time_utils import get_now
from core.knowledge.store import KnowledgeStore
from infrastructure.job.base import BaseJob, JobContext, JobResult


class MergeCleanupJob(BaseJob):
    """Expires bulky rollback states after the configured undo window."""

    def __init__(
        self,
        knowledge_store: KnowledgeStore,
        settings: MergeRollbackSettings,
    ):
        self.knowledge_store = knowledge_store
        self.update_settings(settings)

    @property
    def name(self) -> str:
        return "merge_rollback_cleanup"

    @property
    def cadence_seconds(self) -> float:
        return self._fallback_interval_seconds

    async def should_run(self, ctx: JobContext) -> bool:
        return False

    async def execute(self, ctx: JobContext) -> JobResult:
        with logger.contextualize(
            user=ctx.user_name, job=self.name, project=ctx.project_id
        ):
            cutoff = get_now() - timedelta(hours=self.retention_hours)
            expired_count = await self.knowledge_store.expire_merge_rollback_states(
                cutoff,
                user_name=ctx.user_name,
                project_id=ctx.project_id,
            )
            expired_count = int(expired_count or 0)
            return JobResult(
                success=True,
                summary=f"Expired {expired_count} merge rollback states",
            )

    def update_settings(self, settings: MergeRollbackSettings) -> None:
        self.enabled = settings.enabled
        self.retention_hours = settings.retention_hours
        self._fallback_interval_seconds = settings.fallback_interval_hours * 3600
        logger.info(
            "MergeCleanupJob settings updated: "
            f"enabled={self.enabled}, retention_hours={self.retention_hours}, "
            f"fallback_hours={settings.fallback_interval_hours}"
        )
