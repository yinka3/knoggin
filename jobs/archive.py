import asyncio
from datetime import datetime, timezone, timedelta
from loguru import logger
from jobs.base import BaseJob, JobContext, JobResult
from db.store import MemGraphStore
from shared.events import emit
from shared.redisclient import RedisKeys


class FactArchivalJob(BaseJob):
    """
    Archives old invalidated facts.
    With Fact nodes, we simply delete facts past retention period.
    """
    
    def __init__(self, user_name: str, store: MemGraphStore, retention_days: int = 14):
        self.user_name = user_name
        self.store = store
        self.retention_days = retention_days

    @property
    def name(self) -> str:
        return "fact_archival"

    async def should_run(self, ctx: JobContext) -> bool:
        return await ctx.redis.get(RedisKeys.profile_complete(ctx.user_name, ctx.session_id)) is not None

    async def execute(self, ctx: JobContext) -> JobResult:
        loop = asyncio.get_running_loop()
        
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.retention_days)
        
        deleted_count = await loop.run_in_executor(
            None, 
            self.store.delete_old_invalidated_facts,
            cutoff
        )

        summary = f"Archived {deleted_count} invalidated facts"
        if deleted_count > 0:
            logger.info(summary)
            await emit(ctx.session_id, "job", "facts_archived", {
                "deleted_count": deleted_count,
                "retention_days": self.retention_days
            })
            
        return JobResult(success=True, summary=summary)
    
    def update_settings(self, retention_days: int = None):
        if retention_days is not None:
            self.retention_days = retention_days
            logger.info(f"FactArchivalJob updated: retention_days={retention_days}")

    async def on_shutdown(self, ctx: JobContext) -> None:
        pass