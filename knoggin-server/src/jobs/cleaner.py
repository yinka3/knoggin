import asyncio
import time
from loguru import logger
from jobs.base import BaseJob, JobContext, JobResult
from db.store import MemGraphStore
from core.entity_resolver import EntityResolver
from common.utils.events import emit
from common.infra.redis import RedisKeys


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
        with logger.contextualize(user=ctx.user_name, job=self.name, session=ctx.session_id):
            await self.store.cleanup_null_entities()

            now_ms = int(time.time() * 1000)
            orphan_cutoff = now_ms - self.orphan_cutoff_ms
            junk_cutoff = now_ms - self.stale_cutoff_ms
            
            user_id = await self.ent_resolver.get_id(self.user_name)
            if not user_id:
                await ctx.redis.set(RedisKeys.job_last_run(self.name, self.user_name, ctx.session_id), time.time())
                return JobResult(success=True, summary="User entity not initialized")

            orphan_ids = await self.store.get_orphan_entities(
                user_id,
                orphan_cutoff,
                junk_cutoff
            )
            
            if not orphan_ids:
                await ctx.redis.set(RedisKeys.job_last_run(self.name, self.user_name, ctx.session_id), time.time())
                return JobResult(success=True, summary="No orphans found")
            
            logger.info(f"Cleanup trigger: Found {len(orphan_ids)} entities (Orphans >24h or Junk >30d)")
            for eid in orphan_ids:
                # We don't fetch names to avoid slow DB calls, but we log the IDs
                logger.debug(f"Cleaning entity ID: {eid}")
                
            batch_size = 100
            deleted_count = 0
            for i in range(0, len(orphan_ids), batch_size):
                batch = orphan_ids[i:i + batch_size]
                deleted_count += await self.store.bulk_delete_entities(batch)
                self.ent_resolver.remove_entities(batch)
                await asyncio.sleep(0.1) # Yield to other tasks

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