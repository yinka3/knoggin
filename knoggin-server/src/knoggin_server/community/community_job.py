from loguru import logger

from common.conf.manager import ConfigManager
from common.utils.time_utils import get_now_unix
from infrastructure.job.base import BaseJob, JobContext, JobResult
from infrastructure.redis_client import RedisKeys
from knoggin_server.community.community_manager import CommunityManager
from knoggin_server.project.state import ProjectState


class AACJob(BaseJob):
    """Job that periodically triggers the Autonomous Agent Community discussions."""

    def __init__(self, project_state: ProjectState, resources):
        self.project_state = project_state
        self.resources = resources

    @property
    def name(self) -> str:
        return "aac_discussion"

    async def should_run(self, ctx: JobContext) -> bool:
        config = ConfigManager.get().config
        comm_cfg = config.developer_settings.community
        if not comm_cfg.enabled:
            return False

        interval_min = comm_cfg.interval_minutes
        last_run = await self.resources.redis.get(
            RedisKeys.job_last_run(
                self.name, ctx.user_name, self.project_state.project_id
            )
        )

        if not last_run:
            return True

        try:
            elapsed = get_now_unix() - float(last_run)
        except (ValueError, TypeError):
            return True
        return elapsed >= (interval_min * 60)

    async def execute(self, ctx: JobContext) -> JobResult:
        logger.info(
            f"AAC: Starting scheduled discussion for {ctx.user_name} on project {self.project_state.project_id}"
        )

        manager = CommunityManager(self.project_state, ctx.user_name, self.resources)
        try:
            await manager.trigger_discussion()

            if self.resources.graph_client and self.resources.graph_client.community:
                await self.resources.graph_client.community.delete_old_discussions(30)

            last_run_key = RedisKeys.job_last_run(
                    self.name, ctx.user_name, self.project_state.project_id
                )
            await self.resources.redis.set(last_run_key, get_now_unix())

            return JobResult(success=True, summary="Discussion triggered")
        except Exception as e:
            logger.error(f"AAC: Discussion failed: {e}")
            return JobResult(success=False, summary=str(e))
