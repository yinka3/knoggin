from datetime import timedelta

import redis.asyncio as aioredis
from loguru import logger

from common.schema.settings import ArchivalSettings
from common.utils.events import emit
from common.utils.time_utils import get_now
from infrastructure.job.base import BaseJob, JobContext, JobResult
from infrastructure.knowledge_store import KnowledgeStore
from infrastructure.redis_client import RedisKeys


class FactArchivalJob(BaseJob):
    """
    Deletes invalidated facts after profile refinement completes.

    The job is triggered by a per-project profile-complete Redis marker. It
    removes invalidated facts older than the configured retention window and
    consumes the marker only after archival succeeds.
    """

    _CONSUME_PROFILE_TRIGGER_SCRIPT = """
    if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('del', KEYS[1])
    end
    return 0
    """

    def __init__(
        self,
        knowledge_store: KnowledgeStore,
        redis_client: aioredis.Redis,
        settings: ArchivalSettings,
    ):
        self.redis = redis_client
        self.knowledge_store = knowledge_store
        self.update_settings(settings)

    @property
    def name(self) -> str:
        return "fact_archival"

    @property
    def cadence_seconds(self) -> float:
        return self._fallback_interval_seconds

    async def should_run(self, ctx: JobContext) -> bool:
        return (
            await self.redis.get(
                RedisKeys.project_profile_complete(ctx.user_name, ctx.project_id)
            )
            is not None
        )

    async def execute(self, ctx: JobContext) -> JobResult:
        with logger.contextualize(
            user=ctx.user_name, job=self.name, project=ctx.project_id
        ):
            project_id = ctx.project_id
            cutoff = get_now() - timedelta(days=self.retention_days)
            profile_complete_key = RedisKeys.project_profile_complete(
                ctx.user_name,
                ctx.project_id,
            )
            profile_trigger = await self.redis.get(profile_complete_key)

            deleted_count = await self.knowledge_store.delete_old_invalidated_facts(
                cutoff, project_id=project_id
            )

            summary = f"Archived {deleted_count} invalidated facts"
            if deleted_count > 0:
                logger.info(summary)
                await emit(
                    ctx.project_id,
                    "job",
                    "facts_archived",
                    {
                        "deleted_count": deleted_count,
                        "retention_days": self.retention_days,
                    },
                )

            if profile_trigger is not None:
                await self.redis.eval(
                    self._CONSUME_PROFILE_TRIGGER_SCRIPT,
                    1,
                    profile_complete_key,
                    profile_trigger,
                )

            return JobResult(success=True, summary=summary)

    def update_settings(self, settings: ArchivalSettings) -> None:
        self.enabled = settings.enabled
        self.retention_days = settings.retention_days
        self._fallback_interval_seconds = settings.fallback_interval_hours * 3600
        logger.info(
            "FactArchivalJob settings updated: "
            f"enabled={self.enabled}, retention_days={self.retention_days}, "
            f"fallback_hours={settings.fallback_interval_hours}"
        )
