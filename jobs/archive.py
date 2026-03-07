import asyncio
import time
from datetime import datetime, timezone, timedelta
from loguru import logger
from jobs.base import BaseJob, JobContext, JobResult
from db.store import MemGraphStore
from shared.utils.events import emit
from shared.infra.redis import RedisKeys


class FactArchivalJob(BaseJob):
    """
    Archives old invalidated facts.
    With Fact nodes, we simply delete facts past retention period.
    """
    
    def __init__(self, user_name: str, store: MemGraphStore, retention_days: int = 14, fallback_interval_hours: float = 24):
        self.user_name = user_name
        self.store = store
        self.retention_days = retention_days
        self._fallback_interval_seconds = fallback_interval_hours * 3600

    @property
    def name(self) -> str:
        return "fact_archival"

    async def should_run(self, ctx: JobContext) -> bool:
        profile_done = await ctx.redis.get(
            RedisKeys.profile_complete(ctx.user_name, ctx.session_id)
        ) is not None

        if profile_done:
            return True

        last_run_ts = await ctx.redis.get(
            RedisKeys.job_last_run(self.name, ctx.user_name, ctx.session_id)
        )
        if not last_run_ts:
            return False
            
        try:
            elapsed = time.time() - float(last_run_ts)
        except ValueError:
            return False
            
        return elapsed >= self._fallback_interval_seconds

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
    
    def update_settings(self, retention_days: int = None, fallback_interval_hours: float = None):
        if retention_days is not None:
            self.retention_days = retention_days
        if fallback_interval_hours is not None:
            self._fallback_interval_seconds = fallback_interval_hours * 3600
        if retention_days is not None or fallback_interval_hours is not None:
            logger.info(f"FactArchivalJob updated: retention_days={self.retention_days}, fallback_hours={self._fallback_interval_seconds / 3600}")

    async def on_shutdown(self, ctx: JobContext) -> None:
        pass