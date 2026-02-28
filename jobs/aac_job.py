from datetime import datetime, timezone
from loguru import logger

from jobs.base import BaseJob, JobContext, JobResult
from api.community_manager import CommunityManager
from shared.config import get_config_value


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
        last_run = await ctx.resources.redis.get(f"job:last_run:{self.name}:{ctx.user_name}")
        
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
            
            await ctx.resources.redis.set(
                f"job:last_run:{self.name}:{ctx.user_name}",
                datetime.now(timezone.utc).isoformat()
            )
            
            return JobResult(success=True)
        except Exception as e:
            logger.error(f"AAC: Discussion failed: {e}")
            return JobResult(success=False, error=str(e))
