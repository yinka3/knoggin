import asyncio
import json
from datetime import datetime, timezone
import re
from typing import List, Optional, Tuple
import numpy as np
from loguru import logger
from jobs.base import BaseJob, JobContext, JobResult, JobNotifier
from jobs.jobs_utils import cosine_similarity, find_duplicate_facts, has_sufficient_facts, parse_merge_score
from main.prompts import get_merge_judgment_prompt
from main.service import LLMService
from main.entity_resolve import EntityResolver
from db.memgraph import MemGraphStore


class MergeDetectionJob(BaseJob):
    """
    Detects and processes duplicate entities (merge) and parent/child relationships (hierarchy).
    """
    
    AUTO_MERGE_THRESHOLD = 0.93
    HITL_THRESHOLD = 0.65
    HIERARCHY_FUZZ_THRESHOLD = 70

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
            return JobResult(success=True, summary="No candidates found")
        
        merge_candidates = [c for c in candidates if c["relationship"] == "merge"]
        hierarchy_candidates = [c for c in candidates if c["relationship"] == "hierarchy"]
        
        logger.info(f"Processing {len(merge_candidates)} merge, {len(hierarchy_candidates)} hierarchy candidates")
        
        merge_summary = await self._process_merges(ctx, merge_candidates)
        
        hierarchy_summary = await self._process_hierarchy(hierarchy_candidates)
        
        return JobResult(
            success=True,
            summary=f"{merge_summary}; {hierarchy_summary}"
        )
    
    async def _get_merge_judgment(self, candidate: dict) -> Optional[float]:
        system = get_merge_judgment_prompt(self.user_name)
        user_content = json.dumps({
            "entity_a": {
                "canonical_name": candidate["primary_name"],
                "type": candidate.get("primary_type"),
                "facts": [f.content for f in candidate.get("facts_a", [])]
            },
            "entity_b": {
                "canonical_name": candidate["secondary_name"],
                "type": candidate.get("secondary_type"),
                "facts": [f.content for f in candidate.get("facts_b", [])]
            }
        })
        
        result = await self.llm.call_reasoning(system, user_content)
        
        if not result:
            return None
            
        score = parse_merge_score(result)
        
        if score is None:
            logger.warning(f"Unparseable judgment for ({candidate['primary_id']}, {candidate['secondary_id']})")
            
        return score
    
        
    async def _execute_merge_db_only(
        self, 
        primary_id: int, 
        secondary_id: int,
        duplicate_fact_ids: List[str],
        max_retries: int = 2
    ) -> bool:
        """Execute DB merge then invalidate duplicate facts."""
        loop = asyncio.get_running_loop()
        
        for attempt in range(1, max_retries + 1):
            try:
                success = await loop.run_in_executor(
                    None,
                    self.store.merge_entities,
                    primary_id,
                    secondary_id
                )
                
                if success:
                    now = datetime.now(timezone.utc)
                    for fact_id in duplicate_fact_ids:
                        await loop.run_in_executor(
                            None,
                            self.store.invalidate_fact,
                            fact_id,
                            now
                        )
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
        self.ent_resolver.merge_into(primary_id, secondary_id)
    
    async def _judgement(self, candidates: List, auto_merge: List, hitl: List) -> Tuple[List, List]:
        loop = asyncio.get_running_loop()
        collisions = await loop.run_in_executor(None, self.store.find_alias_collisions)
        collision_set = {tuple(sorted([a, b])) for a, b in collisions}

        for candidate in candidates:
            pair_key = tuple(sorted([candidate["primary_id"], candidate["secondary_id"]]))
            
            if pair_key in collision_set:
                logger.info(f"Auto-merge ({candidate['primary_id']}, {candidate['secondary_id']}) | Alias collision")
                auto_merge.append(candidate)
                continue

            if not has_sufficient_facts(candidate):
                logger.info(f"Skipped ({candidate['primary_id']}, {candidate['secondary_id']}) | Insufficient facts")
                continue
            
            emb_a = self.ent_resolver.get_embedding_for_id(candidate["primary_id"])
            emb_b = self.ent_resolver.get_embedding_for_id(candidate["secondary_id"])
            cosine_score = cosine_similarity(emb_a, emb_b)

            if cosine_score >= 0.93:
                logger.info(f"Auto-merge ({candidate['primary_id']}, {candidate['secondary_id']}) | cosine={cosine_score:.3f}")
                auto_merge.append(candidate)
            elif cosine_score < 0.45:
                logger.info(f"Rejected ({candidate['primary_id']}, {candidate['secondary_id']}) | cosine={cosine_score:.3f}")
            else:
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

        return auto_merge, hitl
    
    async def _process_merges(self, ctx: JobContext, candidates: list) -> str:
        if not candidates:
            return "0 merged"
        
        auto_merge, hitl = await self._judgement(candidates, [], [])

        logger.info(f"Merge split: {len(auto_merge)} auto, {len(hitl)} HITL")
        
        if not auto_merge and not hitl:
            return "0 merged"
        
        clean_batch = []
        seen_ids = set()

        for c in auto_merge:
            p_id = c["primary_id"]
            s_id = c["secondary_id"]
            
            if p_id in seen_ids or s_id in seen_ids:
                continue
            
            seen_ids.add(p_id)
            seen_ids.add(s_id)
            clean_batch.append(c)

        sem = asyncio.Semaphore(2)
        final_merge_list = []

        async def prepare_single_merge(c):
            async with sem:
                p_id = c["primary_id"]
                s_id = c["secondary_id"]
                
                loop = asyncio.get_running_loop()
                
                facts_a = await loop.run_in_executor(
                    None, self.store.get_facts_for_entity, p_id, False
                )
                facts_b = await loop.run_in_executor(
                    None, self.store.get_facts_for_entity, s_id, False
                )
                
                duplicate_ids = find_duplicate_facts(facts_a, facts_b)
                
                return {
                    "primary_id": p_id,
                    "secondary_id": s_id,
                    "duplicate_fact_ids": duplicate_ids,
                    "primary_name": c["primary_name"],
                    "secondary_name": c["secondary_name"]
                }
        
        tasks = [prepare_single_merge(c) for c in clean_batch]
        results = await asyncio.gather(*tasks)
        final_merge_list = [r for r in results if r is not None]

        warning = "⚠️ **Memory Consolidation in Progress.** Merging duplicate entities."

        async with JobNotifier(ctx.redis, warning):
            successful = 0
            failed = 0
            
            dirty_ids = []
            for item in final_merge_list:
                success = await self._execute_merge_db_only(
                    item["primary_id"], 
                    item["secondary_id"], 
                    item["duplicate_fact_ids"]
                )
                
                if success:
                    successful += 1
                    self._sync_resolver(item["primary_id"], item["secondary_id"])
                    loop = asyncio.get_running_loop()
                    
                    all_facts = await loop.run_in_executor(
                        None, 
                        self.store.get_facts_for_entity, 
                        item["primary_id"], 
                        True # active_only
                    )
                    
                    resolution_text = f"{item['primary_name']}. " + " ".join([f.content for f in all_facts])
                    
                    new_embedding = self.ent_resolver.update_profile_embedding(
                        item["primary_id"], 
                        resolution_text
                    )
                    
                    await loop.run_in_executor(
                        None,
                        self.store.update_entity_embedding,
                        item["primary_id"],
                        new_embedding
                    )

                    dirty_ids.append(item["primary_id"])

                    logger.info(f"Merged & Re-embedded {item['primary_name']} <- {item['secondary_name']}")
                else:
                    failed += 1
            
            if dirty_ids:
                dirty_key = f"dirty_entities:{ctx.user_name}"
                await ctx.redis.sadd(dirty_key, *[str(eid) for eid in dirty_ids])
                logger.info(f"Queued {len(dirty_ids)} merged entities for immediate profile refinement")
            
            proposals_stored = await self._store_hitl_proposals(ctx, hitl, seen_ids)

        return f"{successful} merged, {failed} failed, {proposals_stored} HITL"
    

    async def _process_hierarchy(self, candidates: list) -> str:
        """Create PART_OF edges for parent/child relationships."""
        if not candidates:
            return "0 hierarchy edges"
        
        # filter to same-session only
        same_session = [
            c for c in candidates 
            if c.get("primary_session") == c.get("secondary_session")
            and c.get("primary_session") is not None
        ]
        
        skipped = len(candidates) - len(same_session)
        if skipped:
            logger.info(f"Skipped {skipped} cross-session hierarchy candidates")
        
        # dedupe
        seen_pairs = set()
        unique_candidates = []
        
        for c in same_session:
            pair_key = (c["parent_id"], c["child_id"])
            if pair_key not in seen_pairs:
                seen_pairs.add(pair_key)
                unique_candidates.append(c)
        
        loop = asyncio.get_running_loop()
        created = 0
        failed = 0
        
        for c in unique_candidates:
            parent_id = c["parent_id"]
            child_id = c["child_id"]
            parent_name = c["primary_name"] if c["primary_id"] == parent_id else c["secondary_name"]
            child_name = c["secondary_name"] if c["secondary_id"] == child_id else c["primary_name"]
            
            try:
                success = await loop.run_in_executor(
                    None,
                    self.store.create_hierarchy_edge,
                    parent_id,
                    child_id
                )
                
                if success:
                    created += 1
                    logger.info(f"Hierarchy: {child_name} -[:PART_OF]-> {parent_name}")
                else:
                    failed += 1
                    
            except Exception as e:
                logger.error(f"Hierarchy edge failed ({parent_id}, {child_id}): {e}")
                failed += 1
        
        return f"{created} hierarchy edges, {failed} failed"
    
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