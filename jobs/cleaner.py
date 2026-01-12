import asyncio
from loguru import logger
from jobs.base import BaseJob, JobContext, JobResult
from db.memgraph import MemGraphStore
from main.entity_resolve import EntityResolver


class EntityCleanupJob(BaseJob):
    """
    Removes orphan entities (no relationships) from the graph.
    Runs after profile job, before merge detection.
    """

    def __init__(self, user_name: str, store: MemGraphStore, ent_resolver: EntityResolver):
        self.user_name = user_name
        self.store = store
        self.ent_resolver = ent_resolver

    @property
    def name(self) -> str:
        return "entity_cleanup"

    async def should_run(self, ctx: JobContext) -> bool:
        profile_complete = await ctx.redis.get(f"profile_complete:{ctx.user_name}")
        return profile_complete is not None

    async def execute(self, ctx: JobContext) -> JobResult:
        loop = asyncio.get_running_loop()
    
        # Get orphan entity IDs (excludes user entity)
        orphan_ids = await loop.run_in_executor(
            None,
            self.store.get_orphan_entities,
            1  # protected user ID
        )
        
        if not orphan_ids:
            return JobResult(success=True, summary="No orphans found")
        
        logger.info(f"Found {len(orphan_ids)} orphan entities to clean")
        
        deleted_count = await loop.run_in_executor(
            None,
            self.store.bulk_delete_entities,
            orphan_ids
        )
        
        self.ent_resolver.remove_entities(orphan_ids)
        
        return JobResult(success=True, summary=f"Cleaned {deleted_count} orphan entities")

    async def on_shutdown(self, ctx: JobContext) -> None:
        # No state to flush
        pass