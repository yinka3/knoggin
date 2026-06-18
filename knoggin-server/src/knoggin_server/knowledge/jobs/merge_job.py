import asyncio
import json
from typing import Dict, List, Optional, Tuple

import redis.asyncio as aioredis
from loguru import logger

from common.conf.topics_config import TopicConfig
from common.schema.contracts import MergeJudgment
from common.schema.primitives import FactRecord
from common.utils.core_utils import format_vp05_input
from common.utils.data_utils import (
    cosine_similarity,
    find_duplicate_facts,
    has_sufficient_facts,
)
from common.utils.events import emit
from common.utils.json_utils import safe_json_loads
from common.utils.time_utils import get_now, get_now_iso
from infrastructure.graph_client import GraphClient
from infrastructure.job.base import BaseJob, JobContext, JobResult
from infrastructure.llm_client import LLMService
from infrastructure.redis_client import RedisKeys
from knoggin_server.agent.prompts import (
    enrich_facts_with_sources,
    get_merge_judgment_prompt,
)
from knoggin_server.knowledge.services.entity_embedding import (
    build_entity_embedding_text,
)
from knoggin_server.knowledge.services.entity_service import EntityManager


class MergeDetectionJob(BaseJob):
    """
    Detects duplicate entities and parent/child relationships.
    """

    def __init__(
        self,
        user_name: str,
        entities: EntityManager,
        graph_client: GraphClient,
        llm_client: LLMService,
        topic_config: TopicConfig,
        redis_client: aioredis.Redis,
        auto_threshold: float = 0.93,
        hitl_threshold: float = 0.65,
        cosine_threshold: float = 0.65,
        merge_prompt: str = None,
    ):

        self.user_name = user_name
        self.entities = entities
        self.graph_client = graph_client
        self.redis = redis_client
        self.llm = llm_client
        self.topic_config = topic_config

        self.auto_threshold = auto_threshold
        self.hitl_threshold = hitl_threshold
        self.cosine_threshold = cosine_threshold
        self.merge_prompt = merge_prompt

    @property
    def name(self) -> str:
        return "merge_detection"

    async def should_run(self, ctx: JobContext) -> bool:
        merge_key = RedisKeys.merge_queue(ctx.user_name, ctx.project_id)
        queue_size = await self.redis.scard(merge_key)
        return queue_size > 0

    def _same_topic(self, topic_a: str, topic_b: str) -> bool:
        """Check if topics are the same after alias normalization."""
        canonical_a = self.topic_config.normalize_topic(topic_a or "General")
        canonical_b = self.topic_config.normalize_topic(topic_b or "General")
        return canonical_a == canonical_b

    async def execute(self, ctx: JobContext) -> JobResult:
        with logger.contextualize(
            user=ctx.user_name, job=self.name, project=ctx.project_id
        ):
            merge_key = RedisKeys.merge_queue(ctx.user_name, ctx.project_id)

            dirty_raw = await self.redis.srandmember(merge_key, 50)

            if not dirty_raw:
                return JobResult(success=True, summary="No dirty entities to merge")

            dirty_ids = {int(eid) for eid in dirty_raw}

            logger.info(f"Merge detection starting: {len(dirty_ids)} dirty entities")
            await emit(
                ctx.project_id,
                "job",
                "merge_job_started",
                {"dirty_count": len(dirty_ids)},
            )

            candidates = await self.entities.detect_merge_entity_candidates(
                dirty_ids=dirty_ids
            )

            if not candidates:
                await self.redis.srem(merge_key, *[str(eid) for eid in dirty_ids])
                return JobResult(success=True, summary="No candidates found")

            logger.info(f"Processing {len(candidates)} merge candidates")

            # Recovery: check for lingering merge intents from a previous crashed run
            await self.recover_pending_merges(ctx)

            merge_summary = await self._process_merges(ctx, candidates)
            hierarchy_summary = await self._detect_hierarchy(ctx)

            evaluated_ids = [str(eid) for eid in dirty_ids]
            if evaluated_ids:
                await self.redis.srem(merge_key, *evaluated_ids)

            return JobResult(
                success=True, summary=f"{merge_summary}; {hierarchy_summary}"
            )

    async def _get_merge_judgment(
        self, candidate: dict, event_scope_id: str = None
    ) -> Tuple[Optional[float], Optional[str]]:
        system = self.merge_prompt if self.merge_prompt else get_merge_judgment_prompt()

        enriched_facts_a = await enrich_facts_with_sources(
            candidate.get("facts_a", []),
            self.graph_client,
            user_name=self.user_name,
        )
        enriched_facts_b = await enrich_facts_with_sources(
            candidate.get("facts_b", []),
            self.graph_client,
            user_name=self.user_name,
        )

        user_content = format_vp05_input(
            {
                "canonical_name": candidate.get("primary_name", "Unknown"),
                "type": candidate.get("primary_type"),
                "aliases": self.entities.get_mentions_for_id(candidate["primary_id"]),
                "facts": enriched_facts_a,
            },
            {
                "canonical_name": candidate.get("secondary_name", "Unknown"),
                "type": candidate.get("secondary_type"),
                "aliases": self.entities.get_mentions_for_id(candidate["secondary_id"]),
                "facts": enriched_facts_b,
            },
        )
        await emit(
            event_scope_id,
            "job",
            "llm_call",
            {
                "stage": "merge_judgment",
                "primary": candidate.get("primary_name", "Unknown"),
                "secondary": candidate.get("secondary_name", "Unknown"),
                "prompt": user_content,
            },
            verbose_only=True,
        )

        judgment: MergeJudgment = await self.llm.call_llm(
            response_model=MergeJudgment,
            system=system,
            user=user_content,
            model=self.llm.merge_model,
            temperature=0.0,
            reasoning="medium",
        )

        if not judgment:
            return None, None

        if not judgment.should_merge:
            return (
                judgment.confidence * 0.5,
                None,
            )  # Return low score if they shouldn't merge

        return judgment.confidence, judgment.new_canonical_name

    @staticmethod
    def _clean_topic(value) -> Optional[str]:
        if value is None:
            return None
        cleaned = str(value).strip().strip('"')
        return cleaned or None

    @staticmethod
    def _as_int(value) -> int:
        if value is None or value == "":
            return 0
        return int(value)

    @staticmethod
    def _as_float(value) -> float:
        if value is None or value == "":
            return 0.0
        return float(value)

    @classmethod
    def _topic_strength_score(cls, prefix: str, strength: Dict) -> Tuple:
        return (
            cls._as_int(strength.get(f"{prefix}_fact_count")),
            cls._as_int(strength.get(f"{prefix}_relationship_count")),
            cls._as_int(strength.get(f"{prefix}_last")),
            cls._as_float(strength.get(f"{prefix}_conf")),
        )

    @classmethod
    def _select_merge_topic(cls, strength: Dict) -> Optional[str]:
        if not strength:
            return None

        primary_topic = cls._clean_topic(strength.get("p_topic")) or "General"
        secondary_topic = cls._clean_topic(strength.get("s_topic")) or primary_topic
        primary_score = cls._topic_strength_score("p", strength)
        secondary_score = cls._topic_strength_score("s", strength)

        if secondary_score > primary_score:
            return secondary_topic
        return primary_topic

    async def _execute_merge_db_only(
        self,
        primary_id: int,
        secondary_id: int,
        duplicate_fact_ids: List[str],
        project_id: str,
        max_retries: int = 2,
    ) -> bool:
        """Execute DB merge then invalidate duplicate facts."""
        user_id = await self.entities.get_id(self.user_name)
        if user_id is not None and secondary_id == user_id:
            logger.critical(
                "BLOCKED: Attempted to delete user entity "
                f"(id={user_id}) during merge with {primary_id}"
            )
            return False

        for attempt in range(1, max_retries + 1):
            try:
                topic_strength = await self.graph_client.get_merge_topic_strength(
                    primary_id,
                    secondary_id,
                    project_id,
                )
                final_topic = self._select_merge_topic(topic_strength)
                success = await self.graph_client.merge_entities(
                    primary_id,
                    secondary_id,
                    project_id=project_id,
                    final_topic=final_topic,
                )

                if success:
                    now = get_now()
                    for fact_id in duplicate_fact_ids:
                        try:
                            await self.graph_client.invalidate_fact(
                                fact_id, now, project_id=project_id
                            )
                        except Exception as e:
                            logger.warning(
                                "Failed to invalidate duplicate fact "
                                f"{fact_id} during merge: {e}"
                            )
                    return True
                else:
                    logger.warning(
                        f"Merge attempt {attempt}/{max_retries} "
                        f"({primary_id}, {secondary_id}): "
                        "graph_client returned False"
                    )

            except Exception as e:
                logger.error(
                    f"Merge attempt {attempt}/{max_retries} "
                    f"({primary_id}, {secondary_id}): "
                    f"{type(e).__name__} - {e}"
                )

            if attempt < max_retries:
                await asyncio.sleep(0.5 * attempt)

        return False

    async def recover_pending_merges(self, ctx: JobContext):
        """Scan Redis for intents that didn't complete and finish them."""
        index_key = RedisKeys.merge_intents_index(ctx.user_name, ctx.project_id)
        intent_keys = await self.redis.smembers(index_key)

        if not intent_keys:
            return

        logger.info(f"Recovery: Found {len(intent_keys)} pending merge intents")
        for key in intent_keys:
            data_raw = await self.redis.get(key)
            if not data_raw:
                await self.redis.srem(index_key, key)
                continue

            item = safe_json_loads(data_raw)
            if not item or not isinstance(item, dict):
                logger.error(
                    f"Recovery: Corrupt merge intent for key {key}, discarding"
                )
                await self.redis.delete(key)
                await self.redis.srem(index_key, key)
                continue

            p_id = item["primary_id"]
            s_id = item["secondary_id"]

            logger.info(f"Recovery: Finishing merge {p_id} <- {s_id}")
            # We assume the DB merge might have finished (it's idempotent enough),
            # but skipping an aborted transaction can fragment the graph DB.
            # Re-run the DB transaction before finalizing.
            db_success = await self._execute_merge_db_only(
                p_id, s_id, item.get("duplicate_fact_ids", []), ctx.project_id
            )

            if db_success:
                await self._finalize_merge(ctx, item)
            elif item.get("db_merge_completed"):
                logger.info(
                    f"Recovery: DB merge flag set for {p_id} <- {s_id}. "
                    f"Merge succeeded before crash, finalizing."
                )
                await self._finalize_merge(ctx, item)
            else:
                logger.warning(
                    f"Recovery: DB merge failed for {p_id} <- {s_id} "
                    "and no completion flag. Secondary may have been deleted "
                    "by cleanup or absorbed by another merge. "
                    f"Skipping finalization to avoid data corruption."
                )

            # Clean up
            await self.redis.delete(key)
            await self.redis.srem(index_key, key)

    async def _finalize_merge(self, ctx: JobContext, merge_info: dict):
        """
        Finalize by updating metadata and clearing resolver data.

        IMPORTANT: This function is called after a successful database merge.
        Its purpose is strictly to clean up transient state and ensure the cache
        considers the merge complete. It does NOT perform the DB merge itself.
        """

        project_id = ctx.project_id
        async with self.entities.resolution_lock:
            p_id = merge_info["primary_id"]
            s_id = merge_info["secondary_id"]
            p_name = merge_info["primary_name"]
            s_name = merge_info["secondary_name"]
            suggested_name = merge_info.get("suggested_name")

            try:
                refreshed_primary = await self.graph_client.get_entity_by_id(
                    p_id,
                    visible_project_ids=getattr(
                        self.entities,
                        "readable_project_ids",
                        None,
                    ),
                )
                profile_updates = {}
                if refreshed_primary and refreshed_primary.get("topic"):
                    profile_updates["topic"] = refreshed_primary["topic"]

                self._sync_resolver(p_id, s_id, profile_updates)

                if suggested_name and suggested_name != p_name:
                    logger.info(
                        f"Renaming merged entity {p_id}: {p_name} -> {suggested_name}"
                    )
                    await self.graph_client.update_entity_canonical_name(
                        p_id, suggested_name, project_id=project_id
                    )
                    p_name = suggested_name

                all_facts = await self.graph_client.get_facts_for_entity(p_id, True)
                primary_profile = await self.entities.get_profile(p_id)
                primary_type = (
                    primary_profile.get("type", "unknown")
                    if primary_profile
                    else "unknown"
                )
                resolution_text = build_entity_embedding_text(
                    p_name,
                    primary_type,
                    all_facts or [],
                )

                new_embedding = await self.entities.compute_embedding(
                    p_id, resolution_text
                )
                await self.graph_client.update_entity_embedding(
                    p_id, new_embedding, project_id=project_id
                )

            except Exception as e:
                logger.exception(f"Finalize merge failed for {p_id}<-{s_id}: {e}")
            finally:
                # Always mark dirty so profile refinement picks up any incomplete work
                dirty_key = RedisKeys.dirty_entities(ctx.user_name, ctx.project_id)
                await self.redis.sadd(dirty_key, str(p_id))

            await emit(
                ctx.project_id,
                "job",
                "entities_merged",
                {
                    "primary": p_name,
                    "secondary": s_name,
                    "duplicate_facts_removed": len(
                        merge_info.get("duplicate_fact_ids", [])
                    ),
                },
                verbose_only=True,
            )

    def _sync_resolver(
        self,
        primary_id: int,
        secondary_id: int,
        primary_profile_updates: dict = None,
    ):
        """Update EntityManager after merge."""
        self.entities.merge_into(
            primary_id,
            secondary_id,
            primary_profile_updates=primary_profile_updates,
        )

    async def _judgement(self, candidates: List, ctx: JobContext) -> Tuple[List, List]:
        auto_merge = []
        hitl = []

        dirty_ids = {c["primary_id"] for c in candidates} | {
            c["secondary_id"] for c in candidates
        }
        collisions = await self.entities.find_alias_collisions_targeted(dirty_ids)
        collision_set = {tuple(sorted([a, b])) for a, b in collisions}

        llm_tasks = []

        # Step 1: Filter candidates that don't need LLM
        for candidate in candidates:
            pair_key = tuple(
                sorted([candidate["primary_id"], candidate["secondary_id"]])
            )
            topic_a = candidate.get("topic_a", "General")
            topic_b = candidate.get("topic_b", "General")
            same_topic = self._same_topic(topic_a, topic_b)
            type_a = candidate.get("primary_type")
            type_b = candidate.get("secondary_type")

            if type_a and type_b and type_a != type_b:
                logger.debug(
                    "Rejected "
                    f"({candidate['primary_id']}, {candidate['secondary_id']}) "
                    f"| type mismatch {type_a!r} != {type_b!r}"
                )
                continue

            # Alias collision path
            if pair_key in collision_set:
                if same_topic:
                    logger.info(
                        "Auto-merge "
                        f"({candidate['primary_id']}, "
                        f"{candidate['secondary_id']}) "
                        "| Alias collision, same topic"
                    )
                    auto_merge.append(candidate)
                    continue
                else:
                    logger.info(
                        "Cross-topic alias collision "
                        f"({candidate['primary_id']}, "
                        f"{candidate['secondary_id']}) "
                        "| Queuing for LLM"
                    )
                    llm_tasks.append(candidate)
                    continue

            if not has_sufficient_facts(candidate):
                logger.debug(
                    "Skipped "
                    f"({candidate['primary_id']}, "
                    f"{candidate['secondary_id']}) "
                    "| Insufficient facts"
                )
                continue

            cosine_score = 0.0
            profile_a = await self.entities.get_profile(candidate["primary_id"])
            profile_b = await self.entities.get_profile(candidate["secondary_id"])

            if (
                profile_a
                and profile_b
                and profile_a.get("embedding")
                and profile_b.get("embedding")
            ):
                cosine_score = cosine_similarity(
                    profile_a["embedding"], profile_b["embedding"]
                )
            else:
                # Fallback to entities (which handles lazy load) if cache miss
                emb_a = await self.entities.get_embedding_for_id(
                    candidate["primary_id"]
                )
                emb_b = await self.entities.get_embedding_for_id(
                    candidate["secondary_id"]
                )
                cosine_score = cosine_similarity(emb_a, emb_b)

            if cosine_score >= self.auto_threshold:
                if same_topic:
                    logger.info(
                        "Auto-merge "
                        f"({candidate['primary_id']}, "
                        f"{candidate['secondary_id']}) "
                        f"| cosine={cosine_score:.3f}, same topic"
                    )
                    auto_merge.append(candidate)
                    continue
                else:
                    logger.info(
                        "High cosine but cross-topic "
                        f"({candidate['primary_id']}, "
                        f"{candidate['secondary_id']}) "
                        f"| cosine={cosine_score:.3f}, queuing for LLM"
                    )
                    llm_tasks.append(candidate)
                    continue

            if cosine_score < self.cosine_threshold:
                logger.debug(
                    "Rejected "
                    f"({candidate['primary_id']}, "
                    f"{candidate['secondary_id']}) "
                    f"| cosine={cosine_score:.3f}"
                )
                continue

            logger.debug(
                "Undecided "
                f"({candidate['primary_id']}, {candidate['secondary_id']}) "
                f"| cosine={cosine_score:.3f}, queuing for LLM"
            )
            llm_tasks.append(candidate)

        if not llm_tasks:
            return auto_merge, hitl

        # Step 2: Run LLM judgments in parallel with a semaphore
        sem = asyncio.Semaphore(5)
        results = await asyncio.gather(
            *[self._judge_with_sem(c, ctx.project_id, sem) for c in llm_tasks]
        )

        # Step 3: Categorize results
        for candidate, (score, new_name) in results:
            if score is None:
                continue

            candidate["llm_score"] = score
            candidate["suggested_name"] = new_name
            if score >= self.auto_threshold:
                auto_merge.append(candidate)
            elif score >= self.hitl_threshold:
                hitl.append(candidate)
            else:
                logger.info(
                    "Rejected "
                    f"({candidate['primary_id']}, "
                    f"{candidate['secondary_id']}) "
                    f"| LLM={score:.3f}"
                )

        return auto_merge, hitl

    async def _process_merges(self, ctx: JobContext, candidates: list) -> str:
        if not candidates:
            return "0 merged"

        auto_merge, hitl = await self._judgement(candidates, ctx)

        logger.info(f"Merge split: {len(auto_merge)} auto, {len(hitl)} HITL")
        await emit(
            ctx.project_id,
            "job",
            "merge_judgments_complete",
            {
                "auto_merge": len(auto_merge),
                "hitl": len(hitl),
                "rejected": len(candidates) - len(auto_merge) - len(hitl),
            },
        )

        if not auto_merge and not hitl:
            return "0 merged"

        clean_batch = []
        seen_ids = set()

        for c in auto_merge:
            p_id = c["primary_id"]
            s_id = c["secondary_id"]

            if p_id in seen_ids or s_id in seen_ids:
                continue

            user_id = await self.entities.get_id(self.user_name)
            if user_id is not None and s_id == user_id:
                c["primary_id"], c["secondary_id"] = s_id, p_id
                c["primary_name"], c["secondary_name"] = (
                    c["secondary_name"],
                    c["primary_name"],
                )
                c["primary_type"], c["secondary_type"] = (
                    c["secondary_type"],
                    c["primary_type"],
                )
                p_id, s_id = c["primary_id"], c["secondary_id"]

            seen_ids.add(p_id)
            seen_ids.add(s_id)
            clean_batch.append(c)

        all_merge_ids = []
        for c in clean_batch:
            all_merge_ids.extend([c["primary_id"], c["secondary_id"]])
        all_merge_facts = await self.graph_client.get_facts_for_entities(
            list(set(all_merge_ids)), active_only=False
        )

        sem = asyncio.Semaphore(2)
        tasks = [
            self._prepare_single_merge(c, sem, all_merge_facts) for c in clean_batch
        ]
        results = await asyncio.gather(*tasks)
        final_merge_list = [r for r in results if r is not None]

        successful = 0
        failed = 0

        index_key = RedisKeys.merge_intents_index(ctx.user_name, ctx.project_id)

        for item in final_merge_list:
            item_dict: dict = item
            p_id: int = item_dict["primary_id"]
            s_id: int = item_dict["secondary_id"]

            intent_key = RedisKeys.merge_intent(
                ctx.user_name, ctx.project_id, p_id, s_id
            )
            await self.redis.set(intent_key, json.dumps(item_dict))
            await self.redis.sadd(index_key, intent_key)

            db_success = await self._execute_merge_db_only(
                p_id, s_id, item_dict["duplicate_fact_ids"], ctx.project_id
            )

            if db_success:
                item_dict["db_merge_completed"] = True
                await self.redis.set(intent_key, json.dumps(item_dict))

                await self._finalize_merge(ctx, item_dict)
                successful += 1

                await self.redis.delete(intent_key)
                await self.redis.srem(index_key, intent_key)
            else:
                failed += 1
                await self.redis.delete(intent_key)
                await self.redis.srem(index_key, intent_key)

        filtered_hitl = [
            c
            for c in hitl
            if c["primary_id"] not in seen_ids and c["secondary_id"] not in seen_ids
        ]

        if len(filtered_hitl) < len(hitl):
            logger.info(
                f"Filtered {len(hitl) - len(filtered_hitl)} HITL proposals "
                "that overlap with auto-merges"
            )

        proposals_stored = await self._store_hitl_proposals(
            ctx, filtered_hitl, seen_ids
        )

        return f"{successful} merged, {failed} failed, {proposals_stored} HITL"

    async def _detect_hierarchy(self, ctx: JobContext) -> str:
        """
        Post-merge hierarchy detection.
        Uses RELATED_TO edges + type matching from hierarchy_config.
        """
        if not self.topic_config.hierarchy:
            return "0 hierarchy edges"

        created = 0
        project_id = ctx.project_id

        for topic, type_rules in self.topic_config.hierarchy.items():
            if not type_rules:
                continue

            for parent_type, child_types in type_rules.items():
                candidates = await self.graph_client.get_hierarchy_candidates(
                    project_id,
                    topic,
                    parent_type,
                    child_types,
                    2,
                )

                for c in candidates:
                    success = await self.graph_client.create_hierarchy_edge(
                        c["parent_id"], c["child_id"], project_id=project_id
                    )

                    if success:
                        created += 1
                        logger.info(
                            "Hierarchy Established: "
                            f"{c['child_name']} ({c['child_type']}) "
                            f"-[:PART_OF]-> {c['parent_name']} ({c['parent_type']})"
                        )

                        await emit(
                            ctx.project_id,
                            "job",
                            "hierarchy_created",
                            {
                                "parent": c["parent_name"],
                                "child": c["child_name"],
                                "topic": topic,
                            },
                            verbose_only=True,
                        )

        return f"{created} hierarchy edges"

    async def _store_hitl_proposals(
        self, ctx: JobContext, proposals: list, merged_ids: set
    ) -> int:
        stored = 0
        proposal_key = RedisKeys.merge_proposals(ctx.user_name, ctx.project_id)

        for candidate in proposals:
            if (
                candidate["primary_id"] in merged_ids
                or candidate["secondary_id"] in merged_ids
            ):
                continue

            proposal = {
                "primary_id": candidate["primary_id"],
                "secondary_id": candidate["secondary_id"],
                "primary_name": candidate.get("primary_name", "Unknown"),
                "secondary_name": candidate.get("secondary_name", "Unknown"),
                "llm_score": candidate["llm_score"],
                "created_at": get_now_iso(),
                "status": "pending",
            }

            await self.redis.rpush(proposal_key, json.dumps(proposal))
            stored += 1

        if stored > 0:
            await self.redis.expire(proposal_key, 7 * 24 * 3600)

        return stored

    def update_settings(
        self,
        auto_threshold: float = None,
        hitl_threshold: float = None,
        cosine_threshold: float = None,
    ):
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
        await self.redis.set(
            RedisKeys.job_pending(ctx.user_name, ctx.project_id, self.name), "true"
        )
        logger.debug("Merge detection pending flag set")

    async def _judge_with_sem(
        self, candidate: dict, event_scope_id: str, sem: asyncio.Semaphore
    ) -> Tuple[dict, Tuple[Optional[float], Optional[str]]]:
        async with sem:
            try:
                score, new_name = await self._get_merge_judgment(
                    candidate, event_scope_id
                )
                return candidate, (score, new_name)
            except Exception as e:
                logger.error(
                    "LLM judgment failed for "
                    f"({candidate['primary_id']}, "
                    f"{candidate['secondary_id']}): {e}"
                )
                return candidate, (None, None)

    async def _prepare_single_merge(
        self,
        candidate: dict,
        sem: asyncio.Semaphore,
        facts_cache: Dict[int, List[FactRecord]],
    ):
        async with sem:
            try:
                p_id = candidate["primary_id"]
                s_id = candidate["secondary_id"]

                facts_a = facts_cache.get(p_id, [])
                facts_b = facts_cache.get(s_id, [])

                if facts_a is None or facts_b is None:
                    logger.warning(
                        f"Could not fetch facts for merge ({p_id}, {s_id}), skipping"
                    )
                    return None

                duplicate_ids = await asyncio.to_thread(
                    find_duplicate_facts, facts_a, facts_b
                )

                return {
                    "primary_id": p_id,
                    "secondary_id": s_id,
                    "duplicate_fact_ids": duplicate_ids,
                    "primary_name": candidate.get("primary_name", "Unknown"),
                    "secondary_name": candidate.get("secondary_name", "Unknown"),
                    "suggested_name": candidate.get("suggested_name"),
                }
            except Exception as e:
                logger.error(
                    "Failed to prepare merge "
                    f"({candidate['primary_id']}, "
                    f"{candidate['secondary_id']}): {e}"
                )
                return None
