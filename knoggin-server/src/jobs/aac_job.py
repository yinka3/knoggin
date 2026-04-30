import asyncio
from datetime import datetime, timezone
from loguru import logger

from jobs.base import BaseJob, JobContext, JobResult
from services.community_manager import CommunityManager
from common.config.base import get_config_value, get_config
from common.infra.redis import RedisKeys
import redis.asyncio as aioredis
from common.infra.resources import ResourceManager


class AACJob(BaseJob):
    """Job that periodically triggers the Autonomous Agent Community discussions."""
    
    def __init__(self, resources: ResourceManager):
        self.resources = resources

    @property
    def name(self) -> str:
        return "aac_discussion"

    async def should_run(self, ctx: JobContext) -> bool:
        config = get_config()
        comm_cfg = config.developer_settings.community
        if not comm_cfg.enabled:
            return False
            
        interval_min = comm_cfg.interval_minutes
        last_run = await self.resources.redis.get(RedisKeys.job_last_run(self.name, ctx.user_name, "global"))
        
        if not last_run:
            return True
        
        last_dt = datetime.fromisoformat(last_run)
        now = datetime.now(timezone.utc)
        
        return (now - last_dt).total_seconds() >= (interval_min * 60)

    async def execute(self, ctx: JobContext) -> JobResult:
        logger.info(f"AAC: Starting scheduled discussion for {ctx.user_name}")

        manager = CommunityManager(self.resources, ctx.user_name)
        try:
            await manager.trigger_discussion()
            
            await manager.store.delete_old_discussions(30)
            
            await self.resources.redis.set(
                RedisKeys.job_last_run(self.name, ctx.user_name, "global"),
                datetime.now(timezone.utc).isoformat()
            )
            
            return JobResult(success=True, summary="Discussion triggered")
        except Exception as e:
            logger.error(f"AAC: Discussion failed: {e}")
            return JobResult(success=False, summary=str(e))
