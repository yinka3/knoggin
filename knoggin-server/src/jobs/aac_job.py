import asyncio
from datetime import datetime, timezone
from loguru import logger

from jobs.base import BaseJob, JobContext, JobResult
from common.services.community_manager import CommunityManager
from common.config.base import get_config_value
from common.infra.redis import RedisKeys


class AACJob(BaseJob):
    """Job that periodically triggers the Autonomous Agent Community discussions."""
    
    @property
    def name(self) -> str:
        return "aac_discussion"

    async def should_run(self, ctx: JobContext) -> bool:
        dev_settings = get_config_value("developer_settings") or {}
        config = dev_settings.get("community", {})
        if not config.get("enabled", False):
            return False
            
        interval_min = config.get("interval_minutes", 30)
        last_run = await ctx.resources.redis.get(RedisKeys.job_last_run(self.name, ctx.user_name, "global"))
        
        if not last_run:
            return True
        
        last_dt = datetime.fromisoformat(last_run)
        now = datetime.now(timezone.utc)
        
        return (now - last_dt).total_seconds() >= (interval_min * 60)

    async def execute(self, ctx: JobContext) -> JobResult:
        logger.info(f"AAC: Starting scheduled discussion for {ctx.user_name}")

        manager = CommunityManager(ctx.resources, ctx.user_name)
        try:
            await manager.trigger_discussion()
            
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None, manager.store.delete_old_discussions, 30
            )
            
            await ctx.resources.redis.set(
                RedisKeys.job_last_run(self.name, ctx.user_name, "global"),
                datetime.now(timezone.utc).isoformat()
            )
            
            return JobResult(success=True, summary="Discussion triggered")
        except Exception as e:
            logger.error(f"AAC: Discussion failed: {e}")
            return JobResult(success=False, summary=str(e))
