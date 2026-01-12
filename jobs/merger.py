import asyncio
import json
from datetime import datetime, timezone
from typing import List, Optional
import numpy as np
from loguru import logger
from jobs.base import BaseJob, JobContext, JobResult, JobNotifier
from main.prompts import get_merge_judgment_prompt
from main.service import LLMService
from main.entity_resolve import EntityResolver
from db.memgraph import MemGraphStore


class MergeDetectionJob(BaseJob):
    """
    Detects and merges duplicate entities based on embedding similarity.
    
    Trigger: User idle for IDLE_THRESHOLD seconds, hasn't run this session.
    Auto-merges high-confidence pairs (>= 0.93), stores others for HITL review.
    """
    
    AUTO_MERGE_THRESHOLD = 0.93
    HITL_THRESHOLD = 0.65

    def __init__(self, user_name: str, ent_resolver: EntityResolver, store: MemGraphStore, llm_client: LLMService):
        self.user_name = user_name
        self.ent_resolver = ent_resolver
        self.store = store
        self.llm = llm_client
    
    @property
    def name(self) -> str:
        return "merge_detection"
    
    async def should_run(self, ctx: JobContext) -> bool:
        profile_complete = await ctx.redis.get(f"profile_complete:{ctx.user_name}")
        return profile_complete is not None
    
    async def execute(self, ctx: JobContext) -> JobResult:
        await ctx.redis.set(f"merge_ran:{ctx.user_name}", "true")
    

        candidates = self.ent_resolver.detect_merge_candidates()
        if not candidates:
            return JobResult(success=True, summary="No merge candidates found")
        
        auto_merge, hitl = [], []


        for candidate in candidates:
            score = await self._get_merge_judgment(candidate)
            if score is None:
                continue
            
            candidate["llm_score"] = score
            
            if score >= self.AUTO_MERGE_THRESHOLD:
                auto_merge.append(candidate)
            elif score >= self.HITL_THRESHOLD:
                hitl.append(candidate)
            else:
                logger.info(f"Rejected ({candidate['primary_id']}, {candidate['secondary_id']}) | LLM={score:.3f}")

        logger.info(f"Merge split: {len(auto_merge)} auto, {len(hitl)} HITL")
        
        if not auto_merge and not hitl:
            return JobResult(success=True, summary="No merges qualified")
        
        clean_batch = []
        seen_ids = set()

        for c in auto_merge:
            p_id = c["primary_id"]
            s_id = c["secondary_id"]
            
            if p_id in seen_ids or s_id in seen_ids:
                continue # Skip this one for the next run
            
            seen_ids.add(p_id)
            seen_ids.add(s_id)
            clean_batch.append(c)

        logger.info(f"Preparing to merge {len(clean_batch)} pairs in parallel...")
        
        sem = asyncio.Semaphore(2)
        final_merge_list = []

        async def prepare_single_merge(c):
            async with sem:
                p_id = c["primary_id"]
                s_id = c["secondary_id"]
                
                p_profile = self.ent_resolver.entity_profiles.get(p_id, {})
                s_profile = self.ent_resolver.entity_profiles.get(s_id, {})
                
                try:
                    merged_facts = self._merge_facts(
                        p_profile.get("facts", []),
                        s_profile.get("facts", [])
                    )
                    return {
                        "primary_id": p_id,
                        "secondary_id": s_id,
                        "merged_facts": merged_facts,
                        "primary_name": c["primary_name"],
                        "secondary_name": c["secondary_name"]
                    }
                except Exception as e:
                    logger.error(f"Fact merge failed for {p_id}/{s_id}: {e}")
                    return None
        
        tasks = [prepare_single_merge(c) for c in clean_batch]
        results = await asyncio.gather(*tasks)
        final_merge_list = [r for r in results if r is not None]

        warning = "⚠️ **Memory Consolidation in Progress.** Merging duplicate entities."
    
        async with JobNotifier(ctx.redis, warning):
            lock_key = "system:maintenance_lock"
            lock_acquired = await ctx.redis.set(lock_key, "true", nx=True, ex=120)
            if not lock_acquired:
                return JobResult(success=False, summary="Lock held by another job")
            await ctx.redis.set(lock_key, "true", ex=60)
            
            try:
                successful = 0
                failed = 0
                
                for item in final_merge_list:
                    success = await self._execute_merge_db_only(
                        item["primary_id"], 
                        item["secondary_id"], 
                        item["merged_facts"]
                    )
                    
                    if success:
                        successful += 1
                        self._sync_resolver(item["primary_id"], item["secondary_id"])
                        logger.info(f"Merged {item['primary_name']} <- {item['secondary_name']}")
                    else:
                        failed += 1
                
                proposals_stored = await self._store_hitl_proposals(ctx, hitl, seen_ids)
            
            finally:
                await ctx.redis.delete(lock_key)

        return JobResult(
            success=True,
            summary=f"{successful} merged, {failed} failed, {proposals_stored} HITL proposals"
        )
    
    async def _get_merge_judgment(self, candidate: dict) -> Optional[float]:
        system = get_merge_judgment_prompt(self.user_name)
        user_content = json.dumps({
            "entity_a": candidate["profile_a"],
            "entity_b": candidate["profile_b"]
        }, indent=2)
        
        result = await self.llm.call_reasoning(system, user_content)
        
        try:
            return float(result.strip())
        except (ValueError, AttributeError):
            logger.warning(f"Unparseable judgment for ({candidate['primary_id']}, {candidate['secondary_id']}): {result}")
            return None
    

    def _merge_facts(self, facts_a: List[str], facts_b: List[str]) -> List[str]:
        """Combine fact ledgers, deduplicate exact matches."""
        combined = facts_a + facts_b
        seen = set()
        merged = []
        for fact in combined:
            if fact not in seen:
                seen.add(fact)
                merged.append(fact)
        return merged
    
        
    async def _execute_merge_db_only(self, primary_id: int, secondary_id: int, merged_facts: List[str], max_retries: int = 2) -> bool:
            """Execute DB merge with pre-computed facts."""
            loop = asyncio.get_running_loop()
            
            for attempt in range(1, max_retries + 1):
                try:
                    success = await loop.run_in_executor(
                        None,
                        self.store.merge_entities,
                        primary_id,
                        secondary_id,
                        merged_facts
                    )
                    
                    if success:
                        return True
                    else:
                        logger.warning(f"Merge attempt {attempt}/{max_retries} ({primary_id}, {secondary_id}): store returned False")
                        
                except Exception as e:
                    logger.error(f"Merge attempt {attempt}/{max_retries} ({primary_id}, {secondary_id}): {type(e).__name__} - {e}")
                
                if attempt < max_retries:
                    await asyncio.sleep(0.5 * attempt)
            
            return False

    
    def _sync_resolver(self, primary_id: int, secondary_id: int):
        """Update EntityResolver after merge."""
        secondary_aliases = self.ent_resolver.get_mentions_for_id(secondary_id)
        
        with self.ent_resolver._lock:
            for alias in secondary_aliases:
                self.ent_resolver._name_to_id[alias.lower()] = primary_id
            
            if secondary_id in self.ent_resolver.entity_profiles:
                del self.ent_resolver.entity_profiles[secondary_id]
        
        try:
            self.ent_resolver.index_id_map.remove_ids(
                np.array([secondary_id], dtype=np.int64)
            )
        except Exception as e:
            logger.warning(f"FAISS removal failed for {secondary_id}: {e}")
    
    async def _store_hitl_proposals(self, ctx: JobContext, proposals: list, merged_ids: set) -> int:
        stored = 0
        proposal_key = f"merge_proposals:{ctx.user_name}"
        
        for candidate in proposals:
            if candidate["primary_id"] in merged_ids or candidate["secondary_id"] in merged_ids:
                continue
            
            proposal = {
                "primary_id": candidate["primary_id"],
                "secondary_id": candidate["secondary_id"],
                "primary_name": candidate["primary_name"],
                "secondary_name": candidate["secondary_name"],
                "llm_score": candidate["llm_score"],
                "created_at": datetime.now(timezone.utc).isoformat(),
                "status": "pending"
            }
            
            await ctx.redis.rpush(proposal_key, json.dumps(proposal))
            stored += 1
        
        return stored
    
    async def on_shutdown(self, ctx: JobContext) -> None:
        """Set pending flag so next session picks up merge work."""
        await ctx.redis.set(f"pending:{ctx.user_name}:{self.name}", "true")
        logger.debug("Merge detection pending flag set")