# jobs/archival.py

from loguru import logger
from jobs.base import BaseJob, JobContext, JobResult
from db.memgraph import MemGraphStore
from main.entity_resolve import EntityResolver


class FactArchivalJob(BaseJob):
    """
    Archives old invalidated facts from entity ledgers.
    Disabled by default — enable when fact lists get cluttered.
    """
    
    INVALIDATED_THRESHOLD = 10  # Archive when entity has 10+ invalidated facts
    ENABLED = False  # Flip to True when ready
    
    def __init__(self, user_name: str, store: MemGraphStore, ent_resolver: EntityResolver):
        self.user_name = user_name
        self.store = store
        self.ent_resolver = ent_resolver

    @property
    def name(self) -> str:
        return "fact_archival"

    async def should_run(self, ctx: JobContext) -> bool:
        if not self.ENABLED:
            return False
        profile_complete = await ctx.redis.get(f"profile_complete:{ctx.user_name}")
        return profile_complete is not None

    async def execute(self, ctx: JobContext) -> JobResult:
        # TODO: Implement when needed
        # 1. Query entities with many invalidated facts
        # 2. Separate active vs invalidated
        # 3. Archive invalidated to History node or cold storage
        # 4. Update entity with only active facts
        
        archived_count = 0
        
        return JobResult(success=True, summary=f"Archived facts from {archived_count} entities")

    async def on_shutdown(self, ctx: JobContext) -> None:
        pass