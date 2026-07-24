from loguru import logger

from common.conf.manager import ConfigManager
from core.community.community_manager import CommunityManager
from core.project.state import ProjectState
from infrastructure.job.base import BaseJob, JobContext, JobResult


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
            "AAC: Starting scheduled discussion for "
            f"{ctx.user_name} on project {self.project_state.project_id}"
        )

        manager = CommunityManager(
            self.project_state,
            ctx.user_name,
            self.resources,
        )
        try:
            await manager.trigger_discussion()

            if (
                self.resources.knowledge_store
                and self.resources.knowledge_store.community
            ):
                await self.resources.knowledge_store.community.delete_old_discussions(
                    30,
                    user_name=ctx.user_name,
                    project_id=self.project_state.project_id,
                )

            return JobResult(success=True, summary="Discussion triggered")
        except Exception as e:
            logger.error(f"AAC: Discussion failed: {e}")
            return JobResult(success=False, summary=str(e))
