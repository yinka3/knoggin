import asyncio
import json
from datetime import datetime, timezone
from typing import Optional
import numpy as np
from loguru import logger
from jobs.base import BaseJob, JobContext, JobResult, JobNotifier
from main.prompts import get_merge_judgment_prompt, get_summary_merge_prompt
from main.service import LLMService
from main.entity_resolve import EntityResolver
from db.memgraph import MemGraphStore


class MergeDetectionJob(BaseJob):
    """
    Detects and merges duplicate entities based on embedding similarity.
    
    Trigger: User idle for IDLE_THRESHOLD seconds, hasn't run this session.
    Auto-merges high-confidence pairs (>= 0.93), stores others for HITL review.
    """
    
    IDLE_THRESHOLD = 15 * 60
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

        warning = "⚠️ **Memory Consolidation in Progress.** Merging duplicate entities."
    
        async with JobNotifier(ctx.redis, warning):
            lock_key = "system:maintenance_lock"
            await ctx.redis.set(lock_key, "true", ex=60)  # Shorter TTL now
            
            try:
                merged_ids = set()
                successful = 0
                failed = 0
                
                for candidate in auto_merge:
                    primary_id = candidate["primary_id"]
                    secondary_id = candidate["secondary_id"]
                    
                    if primary_id in merged_ids or secondary_id in merged_ids:
                        continue
                    
                    success = await self._execute_merge(ctx.user_name, primary_id, secondary_id)
                    
                    if success:
                        merged_ids.add(secondary_id)
                        successful += 1
                        self._sync_resolver(primary_id, secondary_id)
                    else:
                        failed += 1
                
                proposals_stored = await self._store_hitl_proposals(ctx, hitl, merged_ids)
            
            finally:
                await ctx.redis.delete(lock_key)

        return JobResult(
            success=True,
            summary=f"{successful} merged, {failed} failed, {proposals_stored} HITL proposals"
        )
    
    async def on_shutdown(self, ctx: JobContext) -> None:
        """Set pending flag so next session picks up merge work."""
        await ctx.redis.set(f"pending:{ctx.user_name}:{self.name}", "true")
        logger.debug("Merge detection pending flag set")
    
    async def _get_merge_judgment(self, candidate: dict) -> Optional[float]:
        system = get_merge_judgment_prompt(self.user_name)
        user_content = json.dumps({
            "entity_a": candidate["profile_a"],
            "entity_b": candidate["profile_b"]
        })
        
        result = await self.llm.call_reasoning(system, user_content)
        
        try:
            return float(result.strip())
        except (ValueError, AttributeError):
            logger.warning(f"Unparseable judgment for ({candidate['primary_id']}, {candidate['secondary_id']}): {result}")
            return None

    async def _execute_merge(self, user_name: str, primary_id: int, secondary_id: int, max_retries: int = 2) -> bool:
        """Execute merge with retry."""
        loop = asyncio.get_running_loop()
        
        primary_profile = self.ent_resolver.entity_profiles.get(primary_id, {})
        secondary_profile = self.ent_resolver.entity_profiles.get(secondary_id, {})

        if not primary_profile or not secondary_profile:
            logger.error(f"Merge aborted ({primary_id}, {secondary_id}): missing profile(s)")
            return False
        
        primary_name = primary_profile.get("canonical_name", "Unknown")
        secondary_name = secondary_profile.get("canonical_name", "Unknown")

        try:
            merged_summary = await self._merge_summaries_llm(
                user_name,
                primary_name=primary_name,
                entity_type=primary_profile.get("type", "unknown"),
                all_aliases=list(set(
                    self.ent_resolver.get_mentions_for_id(primary_id) +
                    self.ent_resolver.get_mentions_for_id(secondary_id)
                )),
                summary_a=primary_profile.get("summary", ""),
                summary_b=secondary_profile.get("summary", "")
            )
        except Exception as e:
            logger.error(f"Merge ({primary_id}, {secondary_id}) {primary_name} <- {secondary_name}: LLM failed - {e}")
            return False
        
        for attempt in range(1, max_retries + 1):
            try:
                success = await loop.run_in_executor(
                    None,
                    self.store.merge_entities,
                    primary_id,
                    secondary_id,
                    merged_summary
                )
                
                if success:
                    logger.info(f"Merged ({primary_id}, {secondary_id}) {primary_name} <- {secondary_name}")
                    return True
                else:
                    logger.warning(f"Merge attempt {attempt}/{max_retries} ({primary_id}, {secondary_id}): store returned False")
                    
            except Exception as e:
                logger.error(f"Merge attempt {attempt}/{max_retries} ({primary_id}, {secondary_id}): {type(e).__name__} - {e}")
            
            if attempt < max_retries:
                await asyncio.sleep(2.0 * attempt)
        
        logger.error(f"Merge failed permanently ({primary_id}, {secondary_id}) {primary_name} <- {secondary_name}")
        return False
    
    async def _merge_summaries_llm(
        self,
        user_name: str,
        primary_name: str,
        entity_type: str,
        all_aliases: list[str],
        summary_a: str, 
        summary_b: str
    ) -> str:
        """Merge two summaries using VEGAPUNK-07."""
        
        if not summary_a and not summary_b:
            return ""
        if not summary_a:
            return summary_b
        if not summary_b:
            return summary_a
        
        system_prompt = get_summary_merge_prompt(user_name)
        user_content = json.dumps({
            "entity_name": primary_name,
            "entity_type": entity_type,
            "all_aliases": all_aliases,
            "summary_a": summary_a,
            "summary_b": summary_b
        }, indent=2)
        
        result = await self.llm.call_reasoning(system_prompt, user_content)
        
        if result and result.startswith("MERGE_CONFLICT"):
            logger.warning(f"Merge conflict for {primary_name}: {result}")
            return f"{summary_a} {summary_b}"
        
        return result or f"{summary_a} {summary_b}"
    
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