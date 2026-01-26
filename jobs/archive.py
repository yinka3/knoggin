import asyncio
from datetime import datetime, timezone, timedelta
from loguru import logger
from jobs.base import BaseJob, JobContext, JobResult
from db.store import MemGraphStore


class FactArchivalJob(BaseJob):
    """
    Archives old invalidated facts.
    With Fact nodes, we simply delete facts past retention period.
    """
    
    RETENTION_DAYS = 14
    
    def __init__(self, user_name: str, store: MemGraphStore):
        self.user_name = user_name
        self.store = store

    @property
    def name(self) -> str:
        return "fact_archival"

    async def should_run(self, ctx: JobContext) -> bool:
        profile_complete = await ctx.redis.get(f"profile_complete:{ctx.user_name}")
        return profile_complete is not None

    async def execute(self, ctx: JobContext) -> JobResult:
        loop = asyncio.get_running_loop()
        
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.RETENTION_DAYS)
        
        deleted_count = await loop.run_in_executor(
            None, 
            self.store.delete_old_invalidated_facts,
            cutoff
        )

        summary = f"Archived {deleted_count} invalidated facts"
        if deleted_count > 0:
            logger.info(summary)
            
        return JobResult(success=True, summary=summary)

    async def on_shutdown(self, ctx: JobContext) -> None:
        pass