import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from loguru import logger
from jobs.base import BaseJob, JobContext, JobResult, JobNotifier
from jobs.jobs_utils import cosine_similarity, find_duplicate_facts, format_vp05_input, has_sufficient_facts, parse_merge_score
from main.prompts import get_merge_judgment_prompt
from main.service import LLMService
from main.entity_resolve import EntityResolver
from db.store import MemGraphStore
from main.topics_config import TopicConfig
from schema.dtypes import Fact
from shared.redisclient import RedisKeys


class MergeDetectionJob(BaseJob):
    """
    Detects and processes duplicate entities (merge) and parent/child relationships (hierarchy).
    """
    
    AUTO_MERGE_THRESHOLD = 0.93
    HITL_THRESHOLD = 0.65
    HIERARCHY_FUZZ_THRESHOLD = 70

    def __init__(self, user_name: str, ent_resolver: EntityResolver, store: MemGraphStore, 
                    llm_client: LLMService, topic_config: TopicConfig, executor: ThreadPoolExecutor):
        self.user_name = user_name
        self.ent_resolver = ent_resolver
        self.store = store
        self.llm = llm_client
        self.topic_config = topic_config
        self.executor = executor
    
    @property
    def name(self) -> str:
        return "merge_detection"
    
    async def should_run(self, ctx: JobContext) -> bool:
        return RedisKeys.profile_complete(ctx.user_name, ctx.session_id) is not None
    
    def _same_topic(self, topic_a: str, topic_b: str) -> bool:
        """Check if topics are the same after alias normalization."""
        canonical_a = self.topic_config.normalize_topic(topic_a or "General")
        canonical_b = self.topic_config.normalize_topic(topic_b or "General")
        return canonical_a == canonical_b

    async def execute(self, ctx: JobContext) -> JobResult:
        await ctx.redis.set(RedisKeys.merge_ran(ctx.user_name, ctx.session_id), "true")
    
        candidates = self.ent_resolver.detect_merge_candidates()
        if not candidates:
            return JobResult(success=True, summary="No candidates found")
        
        logger.info(f"Processing {len(candidates)} merge candidates")
    
        merge_summary = await self._process_merges(ctx, candidates)
        hierarchy_summary = await self._detect_hierarchy()
        
        return JobResult(
            success=True,
            summary=f"{merge_summary}; {hierarchy_summary}"
        )
    
    async def _enrich_facts_with_sources(self, facts: List[Fact]) -> List[Dict]:
        """Enrich facts with timestamps and source message content."""
        loop = asyncio.get_running_loop()
        enriched = []
        
        for fact in facts:
            entry = {
                "content": fact.content,
                "recorded_at": fact.valid_at.isoformat() if fact.valid_at else None,
                "source_message": None
            }
            
            if fact.source_msg_id:
                try:
                    msg_id = int(fact.source_msg_id.replace("msg_", ""))
                    text = await loop.run_in_executor(
                        self.executor,
                        self.store.get_message_text,
                        msg_id
                    )
                    if text:
                        entry["source_message"] = text
                except (ValueError, Exception) as e:
                    logger.debug(f"Could not fetch source for {fact.source_msg_id}: {e}")
            
            enriched.append(entry)
        
        return enriched
    
    async def _get_merge_judgment(self, candidate: dict) -> Optional[float]:
        system = get_merge_judgment_prompt(self.user_name)
        
        enriched_facts_a = await self._enrich_facts_with_sources(candidate.get("facts_a", []))
        enriched_facts_b = await self._enrich_facts_with_sources(candidate.get("facts_b", []))
        
        user_content = format_vp05_input(
            {
                "canonical_name": candidate["primary_name"],
                "type": candidate.get("primary_type"),
                "aliases": self.ent_resolver.get_mentions_for_id(candidate["primary_id"]),
                "facts": enriched_facts_a
            },
            {
                "canonical_name": candidate["secondary_name"],
                "type": candidate.get("secondary_type"),
                "aliases": self.ent_resolver.get_mentions_for_id(candidate["secondary_id"]),
                "facts": enriched_facts_b
            }
        )
        
        result = await self.llm.call_llm(system, user_content, reasoning="medium")
        
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
            topic_a = candidate.get("topic_a", "General")
            topic_b = candidate.get("topic_b", "General")
            same_topic = self._same_topic(topic_a, topic_b)
            
            # alias collision path
            if pair_key in collision_set:
                if same_topic:
                    logger.info(f"Auto-merge ({candidate['primary_id']}, {candidate['secondary_id']}) | Alias collision, same topic")
                    auto_merge.append(candidate)
                else:
                    logger.info(f"Cross-topic alias collision ({candidate['primary_id']}, {candidate['secondary_id']}) | Sending to LLM")
                    score = await self._get_merge_judgment(candidate)
                    if score is not None:
                        candidate["llm_score"] = score
                        if score >= self.AUTO_MERGE_THRESHOLD:
                            auto_merge.append(candidate)
                        elif score >= self.HITL_THRESHOLD:
                            hitl.append(candidate)
                continue

            if not has_sufficient_facts(candidate):
                logger.info(f"Skipped ({candidate['primary_id']}, {candidate['secondary_id']}) | Insufficient facts")
                continue
            
            emb_a = self.ent_resolver.get_embedding_for_id(candidate["primary_id"])
            emb_b = self.ent_resolver.get_embedding_for_id(candidate["secondary_id"])
            cosine_score = cosine_similarity(emb_a, emb_b)

            if cosine_score >= 0.93:
                if same_topic:
                    logger.info(f"Auto-merge ({candidate['primary_id']}, {candidate['secondary_id']}) | cosine={cosine_score:.3f}, same topic")
                    auto_merge.append(candidate)
                else:
                    logger.info(f"High cosine but cross-topic ({candidate['primary_id']}, {candidate['secondary_id']}) | cosine={cosine_score:.3f}, sending to LLM")
                    score = await self._get_merge_judgment(candidate)
                    if score is not None:
                        candidate["llm_score"] = score
                        if score >= self.AUTO_MERGE_THRESHOLD:
                            auto_merge.append(candidate)
                        elif score >= self.HITL_THRESHOLD:
                            hitl.append(candidate)
                continue

            if cosine_score < 0.45:
                logger.info(f"Rejected ({candidate['primary_id']}, {candidate['secondary_id']}) | cosine={cosine_score:.3f}")
                continue
            
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

                p_id = item["primary_id"]
                s_id = item["secondary_id"]
                db_success = await self._execute_merge_db_only(
                    p_id, 
                    s_id, 
                    item["duplicate_fact_ids"]
                )
                
                if db_success:
                    try:
                        self._sync_resolver(p_id, s_id)
                        loop = asyncio.get_running_loop()
                        
                        all_facts = await loop.run_in_executor(
                            None, 
                            self.store.get_facts_for_entity, 
                            p_id, 
                            True
                        )
                        
                        resolution_text = f"{item['primary_name']}. " + " ".join([f.content for f in all_facts])
                        
                        new_embedding = self.ent_resolver.compute_embedding(
                            p_id, 
                            resolution_text
                        )
                        
                        await loop.run_in_executor(
                            None,
                            self.store.update_entity_embedding,
                            p_id,
                            new_embedding
                        )

                        dirty_ids.append(p_id)
                        successful += 1
                        logger.info(f"Merged & Re-embedded {item['primary_name']} <- {item['secondary_name']}")
                        
                    except Exception as e:
                        logger.critical(
                            f"Split-brain during merge {p_id}<-{s_id}: {e}. "
                            f"Evicting entities from memory to prevent data corruption."
                        )
                        
                        # HEALING STEP: 
                        # Remove both from RAM. 
                        # Next time they are mentioned, the system will hit the DB (Vector Search)
                        # and find the correct merged state.
                        self.ent_resolver.remove_entities([p_id, s_id])
                        
                        # We don't raise here because we've handled the RAM consistency.
                        # We mark as failed so the job stats are accurate.
                        failed += 1
                else:
                    failed += 1
            
            if dirty_ids:
                dirty_key = RedisKeys.dirty_entities(ctx.user_name, ctx.session_id)
                await ctx.redis.sadd(dirty_key, *[str(eid) for eid in dirty_ids])
                logger.info(f"Queued {len(dirty_ids)} merged entities for immediate profile refinement")
            
            proposals_stored = await self._store_hitl_proposals(ctx, hitl, seen_ids)

        return f"{successful} merged, {failed} failed, {proposals_stored} HITL"
    

    async def _detect_hierarchy(self) -> str:
        """
        Post-merge hierarchy detection.
        Uses RELATED_TO edges + type matching from hierarchy_config.
        """
        if not self.topic_config.hierarchy:
            return "0 hierarchy edges"
        
        loop = asyncio.get_running_loop()
        created = 0
        
        for topic, type_rules in self.topic_config.hierarchy.items():
            if not type_rules:
                continue
                
            for parent_type, child_types in type_rules.items():
                candidates = await loop.run_in_executor(
                    self.executor,
                    self.store.get_hierarchy_candidates,
                    topic,
                    parent_type,
                    child_types,
                    2  # min_weight
                )
                
                for c in candidates:
                    parent_emb = c.get("parent_embedding", [])
                    child_emb = c.get("child_embedding", [])
                    
                    if not parent_emb or not child_emb:
                        continue
                    
                    similarity = cosine_similarity(parent_emb, child_emb)
                    
                    if similarity < 0.65:
                        logger.debug(
                            f"Hierarchy rejected: {c['child_name']} -> {c['parent_name']} "
                            f"(similarity={similarity:.3f})"
                        )
                        continue
                    
                    success = await loop.run_in_executor(
                        self.executor,
                        self.store.create_hierarchy_edge,
                        c["parent_id"],
                        c["child_id"]
                    )
                    
                    if success:
                        created += 1
                        logger.info(
                            f"Hierarchy: {c['child_name']} -[:PART_OF]-> {c['parent_name']} "
                            f"(similarity={similarity:.3f})"
                        )
        
        return f"{created} hierarchy edges"
    
    async def _store_hitl_proposals(self, ctx: JobContext, proposals: list, merged_ids: set) -> int:
        stored = 0
        proposal_key = RedisKeys.merge_proposals(ctx.user_name, ctx.session_id)
        
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
        await ctx.redis.set(RedisKeys.job_pending(ctx.user_name, ctx.session_id, self.name), "true")
        logger.debug("Merge detection pending flag set")