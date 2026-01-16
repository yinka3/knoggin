import asyncio
import time
from loguru import logger
from jobs.base import BaseJob, JobContext, JobResult
from db.memgraph import MemGraphStore
from main.entity_resolve import EntityResolver


class EntityCleanupJob(BaseJob):
    """
    Removes 'orphan' entities (no relationships) that have been 
    stagnant for >X hours.
    
    Trigger: Time-based (Default Every 24h)
    Safety: Only deletes if last_mentioned < 24h ago.
    """

    RUN_INTERVAL = 600
    ORPHAN_AGE_THRESHOLD_MS = RUN_INTERVAL * 1000

    def __init__(self, user_name: str, store: MemGraphStore, ent_resolver: EntityResolver):
        self.user_name = user_name
        self.store = store
        self.ent_resolver = ent_resolver

        logger.info(f"Start running Cleaner Job every {(self.RUN_INTERVAL // 60) // 60} hours")

    @property
    def name(self) -> str:
        return "entity_cleanup"

    async def should_run(self, ctx: JobContext) -> bool:
        """Run if we haven't run in X hours."""
        last_run_key = f"last_run:{self.name}:{self.user_name}"
        last_run_ts = await ctx.redis.get(last_run_key)
        
        if not last_run_ts:
            await ctx.redis.set(last_run_key, time.time())
            return False
        
        elapsed = time.time() - float(last_run_ts)
        return elapsed >= self.RUN_INTERVAL
    

    async def execute(self, ctx: JobContext) -> JobResult:
        loop = asyncio.get_running_loop()

        await loop.run_in_executor(None, self.store.cleanup_null_entities)

        cutoff_ms = int(time.time() * 1000) - self.ORPHAN_AGE_THRESHOLD_MS
        # Get orphan entity IDs (excludes user entity)
        orphan_ids = await loop.run_in_executor(
            None,
            self.store.get_orphan_entities,
            1,
            cutoff_ms  # protected user ID
        )
        
        
        if not orphan_ids:
            await ctx.redis.set(f"last_run:{self.name}:{self.user_name}", time.time())
            return JobResult(success=True, summary="No old orphans found")
        
        logger.info(f"Found {len(orphan_ids)} stale orphan entities (>{(self.RUN_INTERVAL // 60) // 60 }h old) to clean")
        
        deleted_count = await loop.run_in_executor(
            None,
            self.store.bulk_delete_entities,
            orphan_ids
        )
        
        self.ent_resolver.remove_entities(orphan_ids)
        await ctx.redis.set(f"last_run:{self.name}:{self.user_name}", time.time())
        
        return JobResult(success=True, summary=f"Cleaned {deleted_count} orphan entities")

    async def on_shutdown(self, ctx: JobContext) -> None:
        # No state to flush
        pass