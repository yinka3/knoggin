from loguru import logger

from common.conf.manager import ConfigManager
from infrastructure.job.base import BaseJob, JobContext, JobResult
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

    @property
    def cadence_seconds(self) -> float | None:
        comm_cfg = ConfigManager.get().config.developer_settings.community
        if not comm_cfg.enabled:
            return None
        return comm_cfg.interval_minutes * 60

    @property
    def run_immediately_on_first_check(self) -> bool:
        return True

    async def should_run(self, ctx: JobContext) -> bool:
        return False

    async def execute(self, ctx: JobContext) -> JobResult:
        logger.info(
            f"AAC: Starting scheduled discussion for {ctx.user_name} on project {self.project_state.project_id}"
        )

        manager = CommunityManager(self.project_state, ctx.user_name, self.resources)
        try:
            await manager.trigger_discussion()

            if self.resources.graph and self.resources.graph.community:
                await self.resources.graph.community.delete_old_discussions(30)

            return JobResult(success=True, summary="Discussion triggered")
        except Exception as e:
            logger.error(f"AAC: Discussion failed: {e}")
            return JobResult(success=False, summary=str(e))
