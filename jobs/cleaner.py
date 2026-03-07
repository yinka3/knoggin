import asyncio
import time
from loguru import logger
from jobs.base import BaseJob, JobContext, JobResult
from db.store import MemGraphStore
from main.entity_resolve import EntityResolver
from shared.utils.events import emit
from shared.infra.redis import RedisKeys


class EntityCleanupJob(BaseJob):
    """
    Removes 'orphan' entities (no relationships) that have been 
    stagnant for >X hours.
    
    Trigger: Time-based (Default Every 24h)
    Safety: Only deletes if last_mentioned < 24h ago.
    """

    def __init__(self, user_name: str, store: MemGraphStore, ent_resolver: EntityResolver,
                 interval_hours: int = 24, orphan_age_hours: int = 24, stale_junk_days: int = 30):
        self.user_name = user_name
        self.store = store
        self.ent_resolver = ent_resolver

        self.run_interval_seconds = interval_hours * 3600
        self.orphan_cutoff_ms = orphan_age_hours * 3600 * 1000
        self.stale_cutoff_ms = stale_junk_days * 24 * 3600 * 1000

        logger.info(f"Cleaner Job initialized. Interval: {interval_hours}h")

    @property
    def name(self) -> str:
        return "entity_cleanup"

    async def should_run(self, ctx: JobContext) -> bool:
        """Run if we haven't run in X hours."""
        last_run_key = RedisKeys.job_last_run(self.name, self.user_name, ctx.session_id)
        last_run_ts = await ctx.redis.get(last_run_key)
        
        if not last_run_ts:
            await ctx.redis.set(last_run_key, time.time())
            return False
        
        try:
            elapsed = time.time() - float(last_run_ts)
        except ValueError:
            await ctx.redis.set(last_run_key, time.time())
            return False
            
        return elapsed >= self.run_interval_seconds
    

    async def execute(self, ctx: JobContext) -> JobResult:
        loop = asyncio.get_running_loop()
        
        await loop.run_in_executor(None, self.store.cleanup_null_entities)

        now_ms = int(time.time() * 1000)
        orphan_cutoff = now_ms - self.orphan_cutoff_ms
        junk_cutoff = now_ms - self.stale_cutoff_ms
        
        user_id = self.ent_resolver.get_id(self.user_name)
        if not user_id:
            await ctx.redis.set(RedisKeys.job_last_run(self.name, self.user_name, ctx.session_id), time.time())
            return JobResult(success=True, summary="User entity not initialized")

        orphan_ids = await loop.run_in_executor(
            None,
            self.store.get_orphan_entities,
            user_id,
            orphan_cutoff,
            junk_cutoff
        )
        
        if not orphan_ids:
            await ctx.redis.set(RedisKeys.job_last_run(self.name, self.user_name, ctx.session_id), time.time())
            return JobResult(success=True, summary="No orphans found")
        
        logger.info(f"Found {len(orphan_ids)} entities to clean (Orphans >24h or Junk >30d)")
        
        deleted_count = await loop.run_in_executor(
            None,
            self.store.bulk_delete_entities,
            orphan_ids
        )
        
        self.ent_resolver.remove_entities(orphan_ids)
        await ctx.redis.set(RedisKeys.job_last_run(self.name, self.user_name, ctx.session_id), time.time())

        await emit(ctx.session_id, "job", "entities_cleaned", {
            "orphan_count": len(orphan_ids),
            "deleted_count": deleted_count
        })
        return JobResult(success=True, summary=f"Cleaned {deleted_count} entities")
    
    def update_settings(self, interval_hours: int = None, orphan_age_hours: int = None, stale_junk_days: int = None):
        """
        Override BaseJob to convert hours/days into milliseconds.
        """
        updates = []
        
        if interval_hours is not None:
            self.run_interval_seconds = interval_hours * 3600
            updates.append(f"interval={interval_hours}h")
            
        if orphan_age_hours is not None:
            self.orphan_cutoff_ms = orphan_age_hours * 3600 * 1000
            updates.append(f"orphan_age={orphan_age_hours}h")
            
        if stale_junk_days is not None:
            self.stale_cutoff_ms = stale_junk_days * 24 * 3600 * 1000
            updates.append(f"stale_age={stale_junk_days}d")

        if updates:
            logger.info(f"Cleaner Job reconfigured: {', '.join(updates)}")

    async def on_shutdown(self, ctx: JobContext) -> None:
        # No state to flush
        pass