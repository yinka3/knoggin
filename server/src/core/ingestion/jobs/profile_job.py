import asyncio
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple

import redis.asyncio as aioredis
from loguru import logger

from common.schema.contracts import EntityProfilesResult
from common.schema.primitives import FactRecord
from common.schema.settings import ProfileSettings
from common.scoping import IDENTITY_SCOPE
from common.utils.core_utils import format_vp04_input
from common.utils.data_utils import process_extracted_facts
from common.utils.events import emit
from common.utils.time_utils import get_now_unix, parse_iso_time_or_now
from core.ingestion.prompts import (
    enrich_facts_with_sources,
    get_profile_extraction_prompt,
)
from core.knowledge.entity.embedding import (
    build_entity_embedding_text,
)
from core.knowledge.entity.profile import EntityProfile
from core.knowledge.entity.resolver import EntityResolver
from core.knowledge.services.embedding_service import EmbeddingService
from core.knowledge.services.fact_resolution import FactResolver
from infrastructure.job.base import BaseJob, JobContext, JobResult
from infrastructure.knowledge_store import KnowledgeStore
from infrastructure.llm_client import LLMService
from infrastructure.redis_client import RedisKeys

ProfileTargetSelection = Tuple[List[int], bool, Optional[int]]
EntityRefinementResult = Tuple[List[Dict[str, Any]], List[int], bool]


class ProfileRefinementJob(BaseJob):
    """
    Refines profiles for entities marked dirty by upstream graph changes.

    The job runs when the dirty-entity queue reaches the configured volume
    threshold. It uses recent project conversation plus existing facts to ask the
    LLM for profile facts, resolves those facts through FactResolver, updates
    entity embeddings/profile checkpoints, and marks updated entities for merge
    detection. When a volume-triggered run includes a dirty user marker, it also
    refines the identity profile without treating the user as a normal project
    entity.
    """

    def __init__(
        self,
        llm: LLMService,
        entities: EntityResolver,
        knowledge_store: KnowledgeStore,
        executor: ThreadPoolExecutor,
        embedding_service: EmbeddingService,
        redis_client: aioredis.Redis,
        settings: ProfileSettings,
    ):

        self.llm = llm
        self.entities = entities
        self.knowledge_store = knowledge_store
        self.redis = redis_client
        self.executor = executor
        self.embedding_service = embedding_service
        self.batch_semaphore = asyncio.Semaphore(2)

        self.update_settings(settings)

    @property
    def name(self) -> str:
        return "profile_refinement"

    def update_settings(self, settings: ProfileSettings) -> None:
        self.msg_window = settings.msg_window
        self.volume_threshold = settings.volume_threshold
        self.profile_batch_size = settings.profile_batch_size
        self.max_facts_context = settings.max_facts_context
        self.contradiction_sim_low = settings.contradiction_sim_low
        self.contradiction_batch_size = settings.contradiction_batch_size
        logger.info("ProfileRefinementJob settings updated")

    async def _emit_fact_merge_diagnostics(
        self, ctx: JobContext, entity_id: int, merge_result
    ) -> None:
        skipped_count = len(merge_result.skipped)
        missing_targets_count = len(merge_result.missing_targets)
        if not skipped_count and not missing_targets_count:
            return

        reasons = {}
        for change in [*merge_result.skipped, *merge_result.missing_targets]:
            reasons[change.reason] = reasons.get(change.reason, 0) + 1

        await emit(
            ctx.project_id,
            "job",
            "facts_skipped",
            {
                "entity_id": entity_id,
                "skipped_count": skipped_count,
                "missing_targets_count": missing_targets_count,
                "reasons": reasons,
            },
            verbose_only=True,
        )

    async def should_run(self, ctx: JobContext) -> bool:
        dirty_key = RedisKeys.dirty_entities(ctx.user_name, ctx.project_id)
        count = await self.redis.scard(dirty_key)

        if count == 0:
            return False

        if count >= self.volume_threshold:
            logger.info(
                "Profile trigger: Volume threshold met "
                f"({count} >= {self.volume_threshold})"
            )
            await emit(
                ctx.project_id,
                "job",
                "profile_trigger_volume",
                {
                    "trigger": "volume",
                    "dirty_count": count,
                    "threshold": self.volume_threshold,
                },
            )
            return True

        await emit(
            ctx.project_id,
            "job",
            "profile_skipped",
            {"dirty_count": count, "threshold": self.volume_threshold},
        )
        return False

    async def _maybe_refine_user(self, ctx: JobContext, curr_msg_id: int) -> bool:
        """
        Check conditions and trigger user profile refinement if needed.
        Returns True if refinement ran.
        """
        ran_key = RedisKeys.project_user_profile_ran(ctx.user_name, ctx.project_id)
        if await self.redis.get(ran_key):
            return False

        user_id = await self.entities.get_id(ctx.user_name)
        if not user_id:
            logger.warning(f"User entity {ctx.user_name} not found in entities")
            return False

        profile = self.entities.get_cached_profile(user_id)
        if not profile:
            logger.warning(f"User profile {user_id} not found")
            return False

        success = await self._refine_user_profile(ctx, user_id, profile, curr_msg_id)

        if success:
            await self.redis.setex(ran_key, 300, "true")

        return success

    async def _get_conversation_context(
        self,
        ctx: JobContext,
        num_turns: int,
        user_ratio: float = 0.75,
        up_to_msg_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch recent conversation with user/assistant ratio splitting."""
        fetch_count = int(num_turns * 2)
        turns = await self.knowledge_store.get_recent_project_messages(
            ctx.user_name,
            ctx.project_id,
            fetch_count,
            before_message_id=up_to_msg_id,
        )

        if not turns:
            return []

        turns_by_role = defaultdict(list)
        for turn in turns:
            role_label = "USER" if turn["role"] == "user" else "AGENT"
            ts = parse_iso_time_or_now(turn["timestamp"])
            date_str = ts.strftime("%Y-%m-%d %H:%M")
            user_msg_id = int(turn["id"]) if turn["role"] == "user" else None
            formatted = (
                f"[MSG_{user_msg_id}] [{date_str}] [{role_label}]: {turn['content']}"
                if user_msg_id is not None
                else f"[{date_str}] [{role_label}]: {turn['content']}"
            )
            turns_by_role[turn["role"]].append({
                **turn,
                "role_label": role_label,
                "formatted": formatted,
                "raw": turn["content"],
                "user_msg_id": user_msg_id,
            })

        user_turns = turns_by_role["user"]
        assistant_turns = turns_by_role["assistant"]

        user_count = min(len(user_turns), int(num_turns * user_ratio))
        assistant_count = min(len(assistant_turns), num_turns - user_count)

        selected_user = user_turns[-user_count:] if user_count else []
        selected_assistant = (
            assistant_turns[-assistant_count:] if assistant_count else []
        )

        combined = selected_user + selected_assistant
        combined.sort(key=lambda x: str(x["timestamp"]))

        return combined

    @staticmethod
    def _source_session_by_msg_id(conversation: List[Dict[str, Any]]) -> Dict[int, str]:
        return {
            int(turn["user_msg_id"]): turn["session_id"]
            for turn in conversation
            if turn.get("user_msg_id") is not None and turn.get("session_id")
        }

    async def _select_profile_targets(
        self,
        ctx: JobContext,
        target_ids: Optional[List[int]],
    ) -> ProfileTargetSelection:
        if target_ids:
            raw_ids = [str(eid) for eid in target_ids]
            logger.info(f"Targeted refinement for {len(raw_ids)} entities")
        else:
            dirty_key = RedisKeys.dirty_entities(ctx.user_name, ctx.project_id)
            raw_ids = await self.redis.srandmember(
                dirty_key, self.volume_threshold
            )

        user_id = await self.entities.get_id(ctx.user_name)
        candidate_ids = []
        user_requested = False
        if raw_ids:
            for id_str in raw_ids:
                try:
                    eid = int(id_str)
                except (ValueError, TypeError):
                    logger.warning(f"Non-numeric entity ID in dirty set: {id_str}")
                    continue
                if eid == user_id:
                    user_requested = True
                    continue
                candidate_ids.append(eid)

        # Targeted refinement is usually agent-initiated; avoid repeating recent work.
        if not target_ids or not candidate_ids:
            return candidate_ids, user_requested, user_id

        keys = [
            RedisKeys.last_profile_update(ctx.user_name, ctx.project_id, eid)
            for eid in candidate_ids
        ]
        last_updates = await self.redis.mget(*keys)
        now = get_now_unix()
        entity_ids = []
        for eid, last_update in zip(candidate_ids, last_updates):
            if last_update:
                age = now - float(last_update)
                if age < 60:
                    logger.info(
                        "Skipping targeted refinement for entity "
                        f"{eid} (refined {age:.1f}s ago)"
                    )
                    continue
            entity_ids.append(eid)

        return entity_ids, user_requested, user_id

    async def _run_entity_refinement(
        self,
        ctx: JobContext,
        entity_ids: List[int],
        current_msg_id: int,
    ) -> EntityRefinementResult:
        if not entity_ids:
            return [], [], False

        conversation = await self._get_conversation_context(
            ctx, self.msg_window, up_to_msg_id=current_msg_id
        )

        if not conversation:
            logger.warning("Profile refinement: no conversation context")
            await emit(
                ctx.project_id,
                "job",
                "profile_refinement_failed",
                {
                    "entity_count": len(entity_ids),
                    "error": "No context found",
                },
            )
            return [], [], True

        try:
            updates, clear_ids = await self._run_updates(
                ctx, entity_ids, conversation
            )

            if updates:
                await self._write_updates(updates, ctx.project_id)
                updated_ids = [str(update["id"]) for update in updates]
                await self.redis.sadd(
                    RedisKeys.merge_queue(ctx.user_name, ctx.project_id),
                    *updated_ids,
                )
                await emit(
                    ctx.project_id,
                    "job",
                    "profiles_refined",
                    {
                        "count": len(updates),
                        "entities": [u["canonical_name"] for u in updates],
                    },
                )

            # Update recency timestamps for all successfully processed entities.
            for eid in clear_ids:
                await self.redis.setex(
                    RedisKeys.last_profile_update(ctx.user_name, ctx.project_id, eid),
                    3600,  # Keep for 1 hour
                    str(get_now_unix()),
                )

            return updates, clear_ids, False

        except Exception as e:
            logger.exception(f"Profile refinement batch process failed: {e}")
            await emit(
                ctx.project_id,
                "job",
                "profile_refinement_failed",
                {"entity_count": len(entity_ids), "error": str(e)},
            )
            return [], [], False

    async def _mark_profile_complete(self, ctx: JobContext) -> None:
        await self.redis.setex(
            RedisKeys.project_profile_complete(ctx.user_name, ctx.project_id),
            300,
            str(get_now_unix()),
        )

    async def execute(
        self,
        ctx: JobContext,
        target_ids: Optional[List[int]] = None,
    ) -> JobResult:
        """
        Refines entity embeddings and profiles based on new facts.
        :param target_ids: If provided, process these specific entities directly.
        """
        # Establish structured logging context for the job
        with logger.contextualize(
            user=ctx.user_name, job=self.name, project=ctx.project_id
        ):
            current_msg_id = await self.redis.get(
                RedisKeys.project_last_processed(ctx.user_name, ctx.project_id)
            )
            current_msg_id = int(current_msg_id) if current_msg_id else 0

            dirty_key = RedisKeys.dirty_entities(ctx.user_name, ctx.project_id)
            entity_ids, user_requested, user_id = await self._select_profile_targets(
                ctx, target_ids
            )

            target_tag = f" (target_ids={len(target_ids)})" if target_ids else ""
            logger.info(
                "Profile refinement starting: "
                f"{len(entity_ids)} entities to process{target_tag}"
            )

            updates, clear_ids, entity_context_missing = (
                await self._run_entity_refinement(
                    ctx,
                    entity_ids,
                    current_msg_id,
                )
            )

            user_refined = False
            if user_requested or entity_ids:
                user_refined = await self._maybe_refine_user(ctx, current_msg_id)

                # Clear IDs from dirty queue to prevent infinite loop
                # Clear updated entities and entities with no new context/facts.
                processed_ids = []
                if clear_ids:
                    processed_ids.extend([str(eid) for eid in clear_ids])
                if user_requested and user_id:
                    processed_ids.append(str(user_id))
                processed_ids = list(dict.fromkeys(processed_ids))

                if processed_ids:
                    await self.redis.srem(dirty_key, *processed_ids)
                    await emit(
                        ctx.project_id,
                        "job",
                        "dirty_entities_cleared",
                        {
                            "user_name": ctx.user_name,
                            "project_id": ctx.project_id,
                            "dirty_key": dirty_key,
                            "entity_ids": processed_ids,
                            "cleared_count": len(processed_ids),
                            "reason": "profile_processed",
                        },
                    )
                    logger.debug(
                        f"Cleared {len(processed_ids)} entities from dirty queue"
                    )

                parts = []
                if updates:
                    parts.append(f"Refined {len(updates)} profiles")
                if user_refined:
                    parts.append(f"refined {ctx.user_name}")

                summary = ", ".join(parts) if parts else "No profiles to update"

                if entity_context_missing and not user_refined:
                    return JobResult(success=False, summary="No context found")

                await self._mark_profile_complete(ctx)

                return JobResult(success=True, summary=summary)
            return JobResult(success=True, summary="No profiles to update")

    async def _refine_user_profile(
        self, ctx: JobContext, user_id: int, profile: EntityProfile, curr_msg_id: int
    ) -> bool:
        """Execute user profile refinement."""
        conversation = await self._get_conversation_context(
            ctx, int(self.msg_window * 1.5), up_to_msg_id=curr_msg_id
        )

        if not conversation:
            logger.warning("User profile refinement: no conversation context")
            return False

        conversation_text = "\n".join([turn["formatted"] for turn in conversation])

        if not conversation_text:
            logger.warning("User profile refinement: empty conversation text")
            return False

        # Fetch existing facts from DB
        existing_facts = await self.knowledge_store.get_facts_for_entity(
            user_id,
            visible_project_ids=self.entities.readable_project_ids,
            active_only=True,
        )

        if existing_facts is None:
            logger.warning("Could not fetch user facts, skipping refinement")
            return False

        system_reasoning = get_profile_extraction_prompt(ctx.user_name)

        enriched_facts = await enrich_facts_with_sources(
            existing_facts,
            self.knowledge_store,
            self.entities.readable_project_ids,
            user_name=ctx.user_name,
        )
        if len(enriched_facts) > self.max_facts_context:
            enriched_facts = enriched_facts[-self.max_facts_context :]

        llm_input = [
            {
                "entity_name": ctx.user_name,
                "entity_type": "person",
                "existing_facts": enriched_facts,
                "known_aliases": self.entities.get_mentions_for_id(user_id)
                or [ctx.user_name],
            }
        ]
        user_content = format_vp04_input(llm_input, conversation_text)

        await emit(
            ctx.project_id,
            "job",
            "llm_call",
            {"stage": "user_profile_extraction", "prompt": user_content},
            verbose_only=True,
        )

        profiles_result: EntityProfilesResult = await self.llm.generate_structured(
            response_model=EntityProfilesResult,
            system=system_reasoning,
            user=user_content,
            temperature=0.0,
        )

        if not profiles_result or not profiles_result.profiles:
            logger.warning("No profiles extracted for user")
            return False

        profile_map = {p.canonical_name.lower(): p for p in profiles_result.profiles}
        profile_out = profile_map.get(ctx.user_name.lower())

        if not profile_out:
            logger.warning(f"User {ctx.user_name} not found in parsed response")
            return False

        new_facts = profile_out.facts

        if not new_facts:
            logger.debug("No new facts extracted for user profile")
            return False

        merge_result = process_extracted_facts(existing_facts, new_facts)
        await self._emit_fact_merge_diagnostics(ctx, user_id, merge_result)

        valid_msg_ids = {
            int(turn["user_msg_id"])
            for turn in conversation
            if turn.get("user_msg_id") is not None
        }
        source_session_by_msg_id = self._source_session_by_msg_id(conversation)

        fact_summary = await FactResolver.apply_fact_changes(
            user_id,
            merge_result,
            existing_facts,
            valid_msg_ids,
            ctx.project_id,
            self.knowledge_store,
            self.embedding_service,
            self.llm,
            user_name=ctx.user_name,
            project_id=IDENTITY_SCOPE,
            contradiction_sim_low=self.contradiction_sim_low,
            contradiction_batch_size=self.contradiction_batch_size,
            source_session_by_msg_id=source_session_by_msg_id,
            audit_change_type="profile_extraction",
            actor="profile_refinement",
            reason="user_profile_extraction",
        )

        resolution_text = self._build_resolution_text(
            ctx.user_name, "person", fact_summary.active_facts
        )
        embedding = await self.entities.embedding_service.encode_single(resolution_text)
        await self.entities.compute_embedding(user_id, resolution_text, embedding)

        await self.knowledge_store.update_entity_profile(
            entity_id=user_id,
            canonical_name=ctx.user_name,
            embedding=embedding,
            last_msg_id=curr_msg_id,
            project_id=IDENTITY_SCOPE,
        )

        logger.info(f"Refined user profile for {ctx.user_name}")
        await emit(
            ctx.project_id,
            "job",
            "user_profile_refined",
            {
                "user_name": ctx.user_name,
                "facts_invalidated": len(fact_summary.invalidated_fact_ids),
                "facts_created": len(fact_summary.created_facts),
            },
        )

        return True

    async def _process_single_batch(
        self,
        ctx: JobContext,
        batch: List[Dict],
        ents_to_facts: Dict[int, List[FactRecord]],
        current_msg_id: int,
        valid_msg_ids: set,
        source_session_by_msg_id: Dict[int, str],
    ) -> List[Dict]:
        """Process one batch of entities. Returns list of updates."""
        async with self.batch_semaphore:
            llm_input, combined_conversation = await self._build_llm_input(
                batch, ctx.user_name
            )

            system_reasoning = get_profile_extraction_prompt(ctx.user_name)

            user_content = format_vp04_input(llm_input, combined_conversation)

            await emit(
                ctx.project_id,
                "job",
                "llm_call",
                {
                    "stage": "profile_extraction",
                    "entities": [e["entity_name"] for e in batch],
                    "prompt": user_content,
                },
                verbose_only=True,
            )

            profiles_result: EntityProfilesResult = await self.llm.generate_structured(
                response_model=EntityProfilesResult,
                system=system_reasoning,
                user=user_content,
                temperature=0.0,
            )

            if not profiles_result or not profiles_result.profiles:
                logger.warning(
                    f"No profiles extracted for: {[e['entity_name'] for e in batch]}"
                )
                return []

            profile_map = {
                p.canonical_name.lower(): p for p in profiles_result.profiles
            }

            # Phase 1: resolve facts for each entity
            resolved = []
            for orig in batch:
                profile_out = profile_map.get(orig["entity_name"].lower())
                if not profile_out:
                    continue

                new_facts = profile_out.facts
                if not new_facts:
                    logger.debug(f"No new facts extracted for {orig['entity_name']}")
                    continue

                existing_facts = ents_to_facts[orig["ent_id"]]
                merge_result = process_extracted_facts(existing_facts, new_facts)
                await self._emit_fact_merge_diagnostics(
                    ctx, orig["ent_id"], merge_result
                )

                fact_summary = await FactResolver.apply_fact_changes(
                    orig["ent_id"],
                    merge_result,
                    existing_facts,
                    valid_msg_ids,
                    ctx.project_id,
                    self.knowledge_store,
                    self.embedding_service,
                    self.llm,
                    user_name=ctx.user_name,
                    project_id=ctx.project_id,
                    contradiction_sim_low=self.contradiction_sim_low,
                    contradiction_batch_size=self.contradiction_batch_size,
                    source_session_by_msg_id=source_session_by_msg_id,
                    audit_change_type="profile_extraction",
                    actor="profile_refinement",
                    reason="profile_extraction",
                )

                resolved.append((orig, fact_summary))

            # Phase 2: batch encode all resolution texts, then write updates
            resolution_texts = [
                self._build_resolution_text(
                    orig["entity_name"],
                    orig["entity_type"],
                    fact_summary.active_facts,
                )
                for orig, fact_summary in resolved
            ]
            embeddings = await self.embedding_service.encode(resolution_texts)

            updates = []
            for (orig, _), resolution_text, embedding in zip(
                resolved, resolution_texts, embeddings
            ):
                await self.entities.compute_embedding(
                    orig["ent_id"], resolution_text, embedding
                )
                updates.append(
                    {
                        "id": orig["ent_id"],
                        "canonical_name": orig["entity_name"],
                        "embedding": embedding,
                        "last_msg_id": current_msg_id,
                        "project_id": ctx.project_id,
                    }
                )

            updated_ids = {u["id"] for u in updates}
            no_update_ents = [
                orig for orig in batch if orig["ent_id"] not in updated_ids
            ]

            await asyncio.gather(*[
                self.knowledge_store.update_entity_checkpoint(
                    orig["ent_id"], current_msg_id, project_id=ctx.project_id
                )
                for orig in no_update_ents
            ])

            return updates

    async def _run_updates(
        self, ctx: JobContext, entity_ids: List[int], conversation: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[int]]:
        current_msg_id = await self.redis.get(
            RedisKeys.project_last_processed(ctx.user_name, ctx.project_id)
        )
        current_msg_id = int(current_msg_id) if current_msg_id else 0

        valid_entities = []
        missing_profile_ids = []
        for ent_id in entity_ids:
            profile = self.entities.get_cached_profile(ent_id)
            if profile:
                valid_entities.append((ent_id, profile))
            else:
                missing_profile_ids.append(ent_id)

        if missing_profile_ids:
            logger.warning(
                "Profile refinement skipping dirty entities without cached profiles: "
                f"{missing_profile_ids}"
            )

        if not valid_entities:
            return (
                [],
                entity_ids,
            )  # if invalid, we should clear them from dirty queue too

        valid_entity_ids = [ent_id for ent_id, _ in valid_entities]

        ents_to_facts = await self.knowledge_store.get_facts_for_entities(
            valid_entity_ids,
            visible_project_ids=self.entities.readable_project_ids,
            active_only=True,
        )

        if ents_to_facts is None:
            logger.error(
                "Failed to fetch facts for entities, skipping profile refinement"
            )
            return [], []

        # Batch fetch last_profiled_msg_id for all entities to avoid N+1
        entities_data = await self.knowledge_store.get_entities_by_ids(
            valid_entity_ids,
            visible_project_ids=self.entities.readable_project_ids,
        )
        profiled_checkpoints = {
            e["id"]: e.get("last_profiled_msg_id", 0) for e in entities_data
        }

        entity_inputs = []
        for ent_id, profile in valid_entities:
            existing_facts = ents_to_facts.get(ent_id, [])

            # Filter conversation to only new turns since last profiling
            checkpoint = profiled_checkpoints.get(ent_id, 0)
            entity_conversation = [
                turn
                for turn in conversation
                if (turn.get("user_msg_id") or 0) > checkpoint
            ]

            if not entity_conversation:
                logger.debug(
                    f"No new conversation for entity {ent_id} since msg_{checkpoint}"
                )
                continue

            entity_inputs.append(
                {
                    "ent_id": ent_id,
                    "entity_name": profile.canonical_name or "Unknown",
                    "entity_type": profile.entity_type or "unknown",
                    "existing_facts": existing_facts,
                    "known_aliases": self.entities.get_mentions_for_id(ent_id),
                    "conversation_text": "\n".join(
                        [t["formatted"] for t in entity_conversation]
                    ),
                }
            )

        if not entity_inputs:
            return [], entity_ids  # all evaluated and had no new context, clear them

        batches = [
            entity_inputs[i : i + self.profile_batch_size]
            for i in range(0, len(entity_inputs), self.profile_batch_size)
        ]

        valid_msg_ids = {
            int(turn["user_msg_id"])
            for turn in conversation
            if turn.get("user_msg_id") is not None
        }
        source_session_by_msg_id = self._source_session_by_msg_id(conversation)

        tasks = [
            self._process_single_batch(
                ctx,
                batch,
                ents_to_facts,
                current_msg_id,
                valid_msg_ids,
                source_session_by_msg_id,
            )
            for batch in batches
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_updates = []
        successful_entity_ids = []

        # Track which entities weren't even in entity_inputs (no new conversation)
        input_ids = {inp["ent_id"] for inp in entity_inputs}
        skipped_ids = [eid for eid in entity_ids if eid not in input_ids]
        successful_entity_ids.extend(skipped_ids)

        for i, res in enumerate(results):
            batch_ents = [inp["ent_id"] for inp in batches[i]]
            if isinstance(res, Exception):
                logger.error(f"Batch {i} failed with {type(res).__name__}: {res}")
            else:
                all_updates.extend(res)
                # A successful batch is done even if no facts were found.
                successful_entity_ids.extend(batch_ents)

        return all_updates, successful_entity_ids

    def _build_resolution_text(
        self,
        canonical_name: str,
        entity_type: str,
        active_facts: List[FactRecord],
    ) -> str:
        """Build the text used for embedding from entity metadata and facts."""
        return build_entity_embedding_text(canonical_name, entity_type, active_facts)

    async def _build_llm_input(
        self, batch: List[Dict], user_name: str
    ) -> Tuple[List[Dict], str]:
        """Enrich entity facts and build LLM input for a batch."""
        llm_input = []
        for e in batch:
            enriched_facts = await enrich_facts_with_sources(
                e["existing_facts"],
                self.knowledge_store,
                self.entities.readable_project_ids,
                user_name=user_name,
            )
            if len(enriched_facts) > self.max_facts_context:
                enriched_facts = enriched_facts[-self.max_facts_context :]
            llm_input.append(
                {
                    "entity_name": e["entity_name"],
                    "entity_type": e["entity_type"],
                    "existing_facts": enriched_facts,
                    "known_aliases": e["known_aliases"],
                }
            )
        combined_conversation = "\n---\n".join(
            e["conversation_text"] for e in batch
        )
        return llm_input, combined_conversation

    async def _write_updates(self, updates: List[Dict], project_id: str):
        """Write profile updates to KnowledgeStore sequentially."""

        for update in updates:
            await self.knowledge_store.update_entity_profile(
                entity_id=update["id"],
                canonical_name=update["canonical_name"],
                embedding=update["embedding"],
                last_msg_id=update["last_msg_id"],
                project_id=update.get("project_id") or project_id,
            )

        logger.info(f"Wrote {len(updates)} profile updates to graph")
