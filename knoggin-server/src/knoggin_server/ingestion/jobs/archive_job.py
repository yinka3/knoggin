from datetime import timedelta

import redis.asyncio as aioredis
from loguru import logger

from common.schema.settings import ArchivalSettings
from common.utils.events import emit
from common.utils.time_utils import get_now
from infrastructure.graph_interface import GraphInterface
from infrastructure.job.base import BaseJob, JobContext, JobResult
from infrastructure.redis_client import RedisKeys


class FactArchivalJob(BaseJob):
    """
    Archives old invalidated facts.
    With Fact nodes, we simply delete facts past retention period.
    """

    _CONSUME_PROFILE_TRIGGER_SCRIPT = """
    if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('del', KEYS[1])
    end
    return 0
    """

    def __init__(
        self,
        user_name: str,
        graph_client: GraphInterface,
        redis_client: aioredis.Redis,
        retention_days: int = 14,
        fallback_interval_hours: float = 24,
    ):
        self.user_name = user_name
        self.redis = redis_client
        self.graph_client = graph_client
        self.retention_days = retention_days
        self._fallback_interval_seconds = fallback_interval_hours * 3600

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

            deleted_count = await self.graph_client.delete_old_invalidated_facts(
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
