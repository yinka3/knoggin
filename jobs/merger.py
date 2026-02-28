import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import json
from datetime import datetime, timezone
from typing import List, Optional, Tuple
from loguru import logger
from jobs.base import BaseJob, JobContext, JobResult
from jobs.jobs_utils import cosine_similarity, enrich_facts_with_sources, find_duplicate_facts, format_vp05_input, has_sufficient_facts, parse_merge_score
from main.prompts import get_merge_judgment_prompt
from shared.service import MERGE_MODEL, LLMService
from main.entity_resolve import EntityResolver
from db.store import MemGraphStore
from shared.topics_config import TopicConfig
from shared.events import emit
from shared.redisclient import RedisKeys

class MergeDetectionJob(BaseJob):
    """
    Detects and processes duplicate entities (merge) and parent/child relationships (hierarchy).
    """

    def __init__(self, user_name: str, ent_resolver: EntityResolver, store: MemGraphStore, 
                    llm_client: LLMService, topic_config: TopicConfig, executor: ThreadPoolExecutor,
                    auto_threshold: float = 0.93, hitl_threshold: float = 0.65, cosine_threshold: float = 0.65):
        
        self.user_name = user_name
        self.ent_resolver = ent_resolver
        self.store = store
        self.llm = llm_client
        self.topic_config = topic_config
        self.executor = executor

        self.auto_threshold = auto_threshold
        self.hitl_threshold = hitl_threshold
        self.cosine_threshold = cosine_threshold
    
    @property
    def name(self) -> str:
        return "merge_detection"
    
    async def should_run(self, ctx: JobContext) -> bool:
        merge_key = RedisKeys.merge_queue(ctx.user_name, ctx.session_id)
        queue_size = await ctx.redis.scard(merge_key)
        return queue_size > 0
    
    def _same_topic(self, topic_a: str, topic_b: str) -> bool:
        """Check if topics are the same after alias normalization."""
        canonical_a = self.topic_config.normalize_topic(topic_a or "General")
        canonical_b = self.topic_config.normalize_topic(topic_b or "General")
        return canonical_a == canonical_b

    async def execute(self, ctx: JobContext) -> JobResult:

        merge_key = RedisKeys.merge_queue(ctx.user_name, ctx.session_id)
    
        dirty_raw = await ctx.redis.srandmember(merge_key, 50)
        
        if not dirty_raw:
            return JobResult(success=True, summary="No dirty entities to merge")
            
        dirty_ids = {int(eid) for eid in dirty_raw}

        logger.info(f"Merge detection starting: {len(dirty_ids)} dirty entities")
        await emit(ctx.session_id, "job", "merge_job_started", {
            "dirty_count": len(dirty_ids)
        })
        
        candidates = self.ent_resolver.detect_merge_entity_candidates(dirty_ids=dirty_ids)
        
        if not candidates:
            await ctx.redis.srem(merge_key, *[str(eid) for eid in dirty_ids])
            return JobResult(success=True, summary="No candidates found")
        
        logger.info(f"Processing {len(candidates)} merge candidates")
    
        merge_summary = await self._process_merges(ctx, candidates)
        hierarchy_summary = await self._detect_hierarchy(ctx)

        evaluated_ids = [str(eid) for eid in dirty_ids]
        if evaluated_ids:
            await ctx.redis.srem(merge_key, *evaluated_ids)
        
        return JobResult(
            success=True,
            summary=f"{merge_summary}; {hierarchy_summary}"
        )
    
    
    async def _get_merge_judgment(self, candidate: dict, session_id: str = None) -> Optional[float]:
        system = get_merge_judgment_prompt()
        
        enriched_facts_a = await enrich_facts_with_sources(candidate.get("facts_a", []), self.store)
        enriched_facts_b = await enrich_facts_with_sources(candidate.get("facts_b", []), self.store)
        
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
        await emit(session_id, "job", "llm_call", {
            "stage": "merge_judgment",
            "primary": candidate["primary_name"],
            "secondary": candidate["secondary_name"],
            "prompt": user_content
        }, verbose_only=True)
        
        result = await self.llm.call_llm(system, user_content, model=MERGE_MODEL, reasoning="medium")
        
        if not result:
            return None
            
        score = parse_merge_score(result)
        
        if score is None:
            logger.warning(f"Unparseable judgment for ({candidate['primary_id']}, {candidate['secondary_id']}): {result[:200]}")
            
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
                        try:
                            await loop.run_in_executor(
                                None,
                                self.store.invalidate_fact,
                                fact_id,
                                now
                            )
                        except Exception as e:
                            logger.warning(f"Failed to invalidate duplicate fact {fact_id} during merge: {e}")
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
    
    async def _judgement(self, candidates: List, ctx: JobContext) -> Tuple[List, List]:
        auto_merge = []
        hitl = []
        
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
                    score = await self._get_merge_judgment(candidate, ctx.session_id)
                    if score is not None:
                        candidate["llm_score"] = score
                        if score >= self.auto_threshold:
                            auto_merge.append(candidate)
                        elif score >= self.hitl_threshold:
                            hitl.append(candidate)
                continue

            if not has_sufficient_facts(candidate):
                logger.info(f"Skipped ({candidate['primary_id']}, {candidate['secondary_id']}) | Insufficient facts")
                continue
            
            emb_a = self.ent_resolver.get_embedding_for_id(candidate["primary_id"])
            emb_b = self.ent_resolver.get_embedding_for_id(candidate["secondary_id"])
            cosine_score = cosine_similarity(emb_a, emb_b)

            if cosine_score >= self.auto_threshold:
                if same_topic:
                    logger.info(f"Auto-merge ({candidate['primary_id']}, {candidate['secondary_id']}) | cosine={cosine_score:.3f}, same topic")
                    auto_merge.append(candidate)
                else:
                    logger.info(f"High cosine but cross-topic ({candidate['primary_id']}, {candidate['secondary_id']}) | cosine={cosine_score:.3f}, sending to LLM")
                    score = await self._get_merge_judgment(candidate, ctx.session_id)
                    if score is not None:
                        candidate["llm_score"] = score
                        if score >= self.auto_threshold:
                            auto_merge.append(candidate)
                        elif score >= self.hitl_threshold:
                            hitl.append(candidate)
                continue

            if cosine_score < self.cosine_threshold:
                logger.info(f"Rejected ({candidate['primary_id']}, {candidate['secondary_id']}) | cosine={cosine_score:.3f}")
                continue
            
            score = await self._get_merge_judgment(candidate, ctx.session_id)
            if score is None:
                continue
            
            candidate["llm_score"] = score
            
            if score >= self.auto_threshold:
                auto_merge.append(candidate)
            elif score >= self.hitl_threshold:
                hitl.append(candidate)
            else:
                logger.info(f"Rejected ({candidate['primary_id']}, {candidate['secondary_id']}) | LLM={score:.3f}")

        return auto_merge, hitl
    
    async def _process_merges(self, ctx: JobContext, candidates: list) -> str:
        if not candidates:
            return "0 merged"
        
        auto_merge, hitl = await self._judgement(candidates, ctx)

        logger.info(f"Merge split: {len(auto_merge)} auto, {len(hitl)} HITL")
        await emit(ctx.session_id, "job", "merge_judgments_complete", {
            "auto_merge": len(auto_merge),
            "hitl": len(hitl),
            "rejected": len(candidates) - len(auto_merge) - len(hitl)
        })
        
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
                try:
                    p_id = c["primary_id"]
                    s_id = c["secondary_id"]
                    
                    loop = asyncio.get_running_loop()
                    
                    facts_a = await loop.run_in_executor(
                        None, self.store.get_facts_for_entity, p_id, False
                    )
                    facts_b = await loop.run_in_executor(
                        None, self.store.get_facts_for_entity, s_id, False
                    )

                    if facts_a is None or facts_b is None:
                        logger.warning(f"Could not fetch facts for merge ({p_id}, {s_id}), skipping")
                        return None
                    
                    duplicate_ids = await loop.run_in_executor(
                        self.executor, find_duplicate_facts, facts_a, facts_b
                    )
                    
                    return {
                        "primary_id": p_id,
                        "secondary_id": s_id,
                        "duplicate_fact_ids": duplicate_ids,
                        "primary_name": c["primary_name"],
                        "secondary_name": c["secondary_name"]
                    }
                except Exception as e:
                    logger.error(f"Failed to prepare merge ({c['primary_id']}, {c['secondary_id']}): {e}")
                    return None
        
        tasks = [prepare_single_merge(c) for c in clean_batch]
        results = await asyncio.gather(*tasks)
        final_merge_list = [r for r in results if r is not None]

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
                    
                    new_embedding = await loop.run_in_executor(
                        self.executor,
                        partial(self.ent_resolver.compute_embedding, p_id, resolution_text)
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
                    await emit(ctx.session_id, "job", "entities_merged", {
                        "primary": item["primary_name"],
                        "secondary": item["secondary_name"],
                        "duplicate_facts_removed": len(item["duplicate_fact_ids"])
                    }, verbose_only=True)
                    
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
                    await emit(ctx.session_id, "job", "merge_split_brain", {
                        "primary_id": p_id,
                        "secondary_id": s_id,
                        "error": str(e)
                    })
                    
                    # We don't raise here because we've handled the RAM consistency.
                    # We mark as failed so the job stats are accurate.
                    failed += 1
            else:
                failed += 1
            
        if dirty_ids:
            dirty_key = RedisKeys.dirty_entities(ctx.user_name, ctx.session_id)
            await ctx.redis.sadd(dirty_key, *[str(eid) for eid in dirty_ids])
            logger.info(f"Queued {len(dirty_ids)} merged entities for immediate profile refinement")
        
        filtered_hitl = [
            c for c in hitl
            if c["primary_id"] not in seen_ids and c["secondary_id"] not in seen_ids
        ]

        if len(filtered_hitl) < len(hitl):
            logger.info(f"Filtered {len(hitl) - len(filtered_hitl)} HITL proposals that overlap with auto-merges")

        proposals_stored = await self._store_hitl_proposals(ctx, filtered_hitl, seen_ids)

        return f"{successful} merged, {failed} failed, {proposals_stored} HITL"
    

    async def _detect_hierarchy(self, ctx: JobContext) -> str:
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
                    2  # min_weight: ensures they have been mentioned together at least twice
                )
                
                for c in candidates:
                    success = await loop.run_in_executor(
                        self.executor,
                        self.store.create_hierarchy_edge,
                        c["parent_id"],
                        c["child_id"]
                    )
                    
                    if success:
                        created += 1
                        logger.info(
                            f"Hierarchy Established: {c['child_name']} ({c['child_type']}) "
                            f"-[:PART_OF]-> {c['parent_name']} ({c['parent_type']})"
                        )

                        await emit(ctx.session_id, "job", "hierarchy_created", {
                            "parent": c["parent_name"],
                            "child": c["child_name"],
                            "topic": topic
                        }, verbose_only=True)
        
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
    
    def update_settings(self, auto_threshold: float = None, hitl_threshold: float = None, cosine_threshold: float = None):
        updates = []
        
        if auto_threshold is not None:
            self.auto_threshold = auto_threshold
            updates.append(f"auto={auto_threshold}")
        if hitl_threshold is not None:
            self.hitl_threshold = hitl_threshold
            updates.append(f"hitl={hitl_threshold}")
        if cosine_threshold is not None:
            self.cosine_threshold = cosine_threshold
            updates.append(f"cosine={cosine_threshold}")
        
        if updates:
            logger.info(f"MergeDetectionJob updated: {', '.join(updates)}")
    
    async def on_shutdown(self, ctx: JobContext) -> None:
        """Set pending flag so next session picks up merge work."""
        await ctx.redis.set(RedisKeys.job_pending(ctx.user_name, ctx.session_id, self.name), "true")
        logger.debug("Merge detection pending flag set")