# jobs/archival.py

import asyncio
from datetime import datetime, timezone
import re
from loguru import logger
from jobs.base import BaseJob, JobContext, JobResult
from db.memgraph import MemGraphStore
from main.entity_resolve import EntityResolver


class FactArchivalJob(BaseJob):
    """
    Archives old invalidated facts from entity ledgers.
    Disabled by default — enable when fact lists get cluttered.
    """
    
    RETENTION_DAYS = 14
    ENABLED = True
    
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
        loop = asyncio.get_running_loop()

        candidates = await loop.run_in_executor(None, self.store.get_entities_with_invalidated_facts)

        archived_count = 0
        entities_processed = 0
        now = datetime.now(timezone.utc)

        for cand in candidates:
            ent_id = cand["id"]
            raw_facts = cand["facts"]

            active_facts = []
            to_archive = []

            for fact in raw_facts:
                if "[INVALIDATED:" in fact:
                    if self._should_archive(fact, now):
                        to_archive.append(fact)
                    else:
                        active_facts.append(fact)
                else:
                    active_facts.append(fact)
            
            if to_archive:
                await loop.run_in_executor(
                    None, self.store.commit_fact_archival,
                    ent_id,
                    active_facts,
                    to_archive
                )

                if ent_id in self.ent_resolver.entity_profiles:
                    self.ent_resolver.entity_profiles[ent_id]["facts"] = active_facts
                
                archived_count += len(to_archive)
                entities_processed += 1


        summary = f"Archived {archived_count} facts from {entities_processed} entities"
        if archived_count > 0:
            logger.info(summary)
            
        return JobResult(success=True, summary=summary)
    
    def _should_archive(self, fact: str, now: datetime) -> bool:
        """
        Check if the invalidated date is older than RETENTION_DAYS.
        Format expected: ... [INVALIDATED: YYYY-MM-DD HH:MM]
        """
        match = re.search(r"\[INVALIDATED:\s*(\d{4}-\d{2}-\d{2})", fact)
        if not match:
            return False
        try:
            date_str = match.group(1)
            inval_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            
            age = (now - inval_date).days
            return age >= self.RETENTION_DAYS
            
        except ValueError:
            return False

    async def on_shutdown(self, ctx: JobContext) -> None:
        pass