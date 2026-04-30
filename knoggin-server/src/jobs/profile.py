import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import uuid
from core.utils import fetch_conversation_turns
from loguru import logger
import numpy as np
from db.store import MemGraphStore
from jobs.base import BaseJob, JobContext, JobResult
from common.rag.embedding import EmbeddingService
from services.llm_service import LLMService
from core.pipeline.entity_resolver import EntityResolver
from core.prompts import get_contradiction_judgment_prompt, get_profile_extraction_prompt
from jobs.utils import enrich_facts_with_sources, extract_fact_with_source, format_vp04_input, process_extracted_facts
from common.schema.dtypes import Fact, FactRecord, EntityProfilesResult, BulkContradictionResult
from common.utils.events import emit
from common.infra.redis import RedisKeys
import redis.asyncio as aioredis
from common.config.base import get_config

class ProfileRefinementJob(BaseJob):
    """
    Scans entities that have been 'touched' recently and updates their profiles
    using the sliding window of recent messages.
    
    Triggers:
    1. VOLUME: If >=20 entities are dirty (ensures we catch them in the 75-msg window).
    2. TIME: If user is idle for >5 minutes and we have ANY dirty entities.
    """

    def __init__(self, llm: LLMService, resolver: EntityResolver, store: MemGraphStore, 
                executor: ThreadPoolExecutor, embedding_service: EmbeddingService, redis_client: aioredis.Redis,
                msg_window: int = 30, volume_threshold: int = 15, idle_threshold: int = 90,
                contradiction_sim_low: float = 0.70, contradiction_sim_high: float = 0.95,
                contradiction_batch_size: int = 4, profile_batch_size: int = 8,
                max_facts_context: int = 50,
                profile_prompt: str = None, contradiction_prompt: str = None):
        
        self.llm = llm
        self.resolver = resolver
        self.store = store
        self.redis = redis_client
        self.executor = executor
        self.embedding_service = embedding_service
        self.batch_semaphore = asyncio.Semaphore(2)

        self.profile_batch_size = profile_batch_size
        self.msg_window = msg_window
        self.volume_threshold = volume_threshold
        self.idle_threshold = idle_threshold

        self.contradiction_sim_low = contradiction_sim_low
        self.contradiction_sim_high = contradiction_sim_high
        self.contradiction_batch_size = contradiction_batch_size
        self.max_facts_context = max_facts_context
        self.profile_prompt = profile_prompt
        self.contradiction_prompt = contradiction_prompt
        

    @property
    def name(self) -> str:
        return "profile_refinement"

    async def should_run(self, ctx: JobContext) -> bool:
        dirty_key = RedisKeys.dirty_entities(ctx.user_name, ctx.session_id)
        count = await self.redis.scard(dirty_key)
        
        if count == 0:
            return False
            
        if count >= self.volume_threshold:
            logger.info(f"Profile trigger: Volume threshold met ({count} >= {self.volume_threshold})")
            await emit(ctx.session_id, "job", "profile_trigger_volume", {
                "trigger": "volume",
                "dirty_count": count,
                "threshold": self.volume_threshold
            })
            return True
            
        if ctx.idle_seconds >= self.idle_threshold:
            logger.info(f"Profile trigger: Idle threshold met ({ctx.idle_seconds:.1f}s >= {self.idle_threshold}s)")
            await emit(ctx.session_id, "job", "profile_trigger_idle", {
                "trigger": "idle",
                "idle_seconds": ctx.idle_seconds,
                "threshold": self.idle_threshold,
                "dirty_count": count
            })
            return True
        
        await emit(ctx.session_id, "job", "profile_skipped", {
            "dirty_count": count,
            "idle_seconds": ctx.idle_seconds
        })
        return False
    
    
    async def _maybe_refine_user(self, ctx: JobContext, curr_msg_id: int) -> bool:
        """
        Check conditions and trigger user profile refinement if needed.
        Returns True if refinement ran.
        """
        ran_key = RedisKeys.user_profile_ran(ctx.user_name, ctx.session_id)
        if await self.redis.get(ran_key):
            return False
        
        user_id = await self.resolver.get_id(ctx.user_name)
        if not user_id:
            logger.warning(f"User entity {ctx.user_name} not found in resolver")
            return False
        
        profile = self.resolver.entity_profiles.get(user_id)
        if not profile:
            logger.warning(f"User profile {user_id} not found")
            return False
        
        success = await self._refine_user_profile(ctx, user_id, profile, curr_msg_id)

        await self.redis.setex(ran_key, 300, "true")
        
        return success
    
    async def _get_conversation_context(self, ctx: JobContext, num_turns: int, user_ratio: float = 0.75, up_to_msg_id: int = None) -> List[Dict[str, Any]]:
        """Fetch recent conversation with user/assistant ratio splitting."""
        fetch_count = int(num_turns * 2)
        turns = await fetch_conversation_turns(
            self.redis, ctx.user_name, ctx.session_id,
            fetch_count, up_to_msg_id
        )

        if not turns:
            return []

        user_turns = []
        assistant_turns = []

        for turn in turns:
            role_label = "USER" if turn["role"] == "user" else "AGENT"
            ts = datetime.fromisoformat(turn["timestamp"])
            date_str = ts.strftime("%Y-%m-%d %H:%M")

            if turn["role"] == "user" and turn.get("user_msg_id") is not None:
                formatted = f"[MSG_{turn['user_msg_id']}] [{date_str}] [{role_label}]: {turn['content']}"
            else:
                formatted = f"[{date_str}] [{role_label}]: {turn['content']}"

            enriched = {
                **turn,
                "role_label": role_label,
                "formatted": formatted,
                "raw": turn["content"],
            }

            if turn["role"] == "user":
                user_turns.append(enriched)
            else:
                assistant_turns.append(enriched)

        user_count = min(len(user_turns), int(num_turns * user_ratio))
        assistant_count = min(len(assistant_turns), num_turns - user_count)

        selected_user = user_turns[-user_count:] if user_count else []
        selected_assistant = assistant_turns[-assistant_count:] if assistant_count else []

        combined = selected_user + selected_assistant
        combined.sort(key=lambda x: str(x["timestamp"]))

        return combined

    async def execute(self, ctx: JobContext, force: bool = False, target_ids: Optional[List[int]] = None) -> JobResult:
        """
        Refines entity embeddings and profiles based on new facts.
        :param force: If True, ignore volume thresholds and process as many as possible.
        :param target_ids: If provided, only process these specific entities.
        """
        # Establish structured logging context for the job
        with logger.contextualize(user=ctx.user_name, job=self.name, session=ctx.session_id):
            current_msg_id = await self.redis.get(RedisKeys.last_processed(ctx.user_name, ctx.session_id))
            current_msg_id = int(current_msg_id) if current_msg_id else 0

            dirty_key = RedisKeys.dirty_entities(ctx.user_name, ctx.session_id)
            
            if target_ids:
                # Targeted mode: use provided IDs, but verify they are still in the dirty set
                raw_ids = [str(eid) for eid in target_ids]
                logger.info(f"Targeted refinement for {len(raw_ids)} entities")
            else:
                # If forced, take up to 3x the normal batch size to clear the queue
                limit = self.volume_threshold * 3 if force else self.volume_threshold
                raw_ids = await self.redis.srandmember(dirty_key, limit)
        
            user_id = await self.resolver.get_id(ctx.user_name)
            candidate_ids = [int(id_str) for id_str in raw_ids if int(id_str) != user_id] if raw_ids else []

            # Recency Guard (Targeted only)
            # Avoid refining if it was updated in the last 60 seconds
            entity_ids = []
            if target_ids and candidate_ids:
                for eid in candidate_ids:
                    last_update = await self.redis.get(RedisKeys.last_profile_update(ctx.user_name, ctx.session_id, eid))
                    if last_update:
                        age = datetime.now(timezone.utc).timestamp() - float(last_update)
                        if age < 60:
                            logger.info(f"Skipping targeted refinement for entity {eid} (refined {age:.1f}s ago)")
                            continue
                    entity_ids.append(eid)
            else:
                entity_ids = candidate_ids

            force_tag = " (force=True)" if force else ""
            target_tag = f" (target_ids={len(target_ids)})" if target_ids else ""
            logger.info(f"Profile refinement starting: {len(entity_ids)} entities to process{force_tag}{target_tag}")
            
            updates = []
            
            if entity_ids:
                conversation = await self._get_conversation_context(ctx, self.msg_window, up_to_msg_id=current_msg_id)

                if not conversation:
                    return JobResult(success=False, summary="No context found")
                
                updates = []
                clear_ids = []
                try:
                    updates, clear_ids = await self._run_updates(ctx, entity_ids, conversation)
                    
                    if updates:
                        await self._write_updates(updates)
                        await emit(ctx.session_id, "job", "profiles_refined", {
                            "count": len(updates),
                            "entities": [u["canonical_name"] for u in updates]
                        })
                        
                        # Update recency timestamps for all processed entities
                        for eid in clear_ids:
                            await self.redis.setex(
                                RedisKeys.last_profile_update(ctx.user_name, ctx.session_id, eid),
                                3600, # Keep for 1 hour
                                str(datetime.now(timezone.utc).timestamp())
                            )

                        merge_queue = RedisKeys.merge_queue(ctx.user_name, ctx.session_id)
                        updated_ids = [str(u["id"]) for u in updates]
                        
                        if updated_ids:
                            config = get_config()
                            merger_enabled = config.developer_settings.jobs.merger.enabled

                            if updated_ids and merger_enabled:
                                await self.redis.sadd(merge_queue, *updated_ids)
                                logger.info(f"Passed {len(updated_ids)} updated entities to Merge Queue")

                except Exception as e:
                    logger.exception(f"Profile refinement batch process failed: {e}")
                    await emit(ctx.session_id, "job", "profile_refinement_failed", {
                        "entity_count": len(entity_ids),
                        "error": str(e)
                    })
                        # We don't return here so that user refinement still has a chance
                
                user_refined = await self._maybe_refine_user(ctx, current_msg_id)
                
                # Clear IDs from dirty queue to prevent infinite loop
                # We clear both successfully updated entities AND entities that had no new context/facts
                processed_ids = []
                if clear_ids:
                    processed_ids.extend([str(eid) for eid in clear_ids])
                    
                if processed_ids:
                    await self.redis.srem(dirty_key, *processed_ids)
                    logger.debug(f"Cleared {len(processed_ids)} entities from dirty queue")
                
                parts = []
                if updates:
                    parts.append(f"Refined {len(updates)} profiles")
                if user_refined:
                    parts.append(f"refined {ctx.user_name}")
                
                summary = ", ".join(parts) if parts else "No profiles to update"

                await self.redis.setex(
                    RedisKeys.profile_complete(ctx.user_name, ctx.session_id),
                    300,
                    str(datetime.now(timezone.utc).timestamp())
                )
                
                return JobResult(success=True, summary=summary)
            else:
                return JobResult(success=True, summary="No profiles to update")
    
    async def _refine_user_profile(self, ctx: JobContext, user_id: int, profile: dict, curr_msg_id: int) -> bool:
        """Execute user profile refinement."""
        conversation = await self._get_conversation_context(ctx, int(self.msg_window * 1.5), up_to_msg_id=curr_msg_id)

        if not conversation:
            logger.warning("User profile refinement: no conversation context")
            return False
        
        conversation_text = "\n".join([turn["formatted"] for turn in conversation])

        if not conversation_text:
            logger.warning("User profile refinement: empty conversation text")
            return False
        
        # Get current message ID for checkpoint
        current_msg_id = await self.redis.get(RedisKeys.last_processed(ctx.user_name, ctx.session_id))
        current_msg_id = int(current_msg_id) if current_msg_id else 0
        
        # Fetch existing facts from DB
        existing_facts = await self.store.get_facts_for_entity(
            user_id,
            True  # active_only
        )

        if existing_facts is None:
            logger.warning("Could not fetch user facts, skipping refinement")
            return False
        
        if self.profile_prompt:
            system_reasoning = self.profile_prompt.replace("{user_name}", ctx.user_name)
        else:
            system_reasoning = get_profile_extraction_prompt(ctx.user_name)
            
        enriched_facts = await enrich_facts_with_sources(existing_facts, self.store)
        if len(enriched_facts) > self.max_facts_context:
            enriched_facts = enriched_facts[-self.max_facts_context:]
            
        llm_input = [{
            "entity_name": ctx.user_name,
            "entity_type": "person",
            "existing_facts": enriched_facts,
            "known_aliases": [alias for alias in profile.get("aliases", [ctx.user_name])]
        }]
        user_content = format_vp04_input(llm_input, conversation_text)

        await emit(ctx.session_id, "job", "llm_call", {
            "stage": "user_profile_extraction",
            "prompt": user_content
        }, verbose_only=True)

        profiles_result: EntityProfilesResult = await self.llm.call_llm(
            response_model=EntityProfilesResult,
            system=system_reasoning,
            user=user_content,
            temperature=0.0
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
        
        valid_msg_ids = {int(turn['user_msg_id']) for turn in conversation if turn.get('user_msg_id') is not None}

        final_active_facts, failed_invalidations = await self._apply_fact_changes(user_id, merge_result, existing_facts, valid_msg_ids, ctx.session_id)
        if failed_invalidations:
            dirty_key = RedisKeys.dirty_entities(ctx.user_name, ctx.session_id)
            await self.redis.sadd(dirty_key, str(user_id))
            logger.warning(f"Re-dirtied user entity {user_id}: {len(failed_invalidations)} invalidations failed")
        
        embedding = await self._update_entity_embedding(user_id, ctx.user_name, final_active_facts)
        
        
        await self.store.update_entity_profile(
            entity_id=user_id,
            canonical_name=ctx.user_name,
            embedding=embedding,
            last_msg_id=current_msg_id
        )
        
        logger.info(f"Refined user profile for {ctx.user_name}")
        await emit(ctx.session_id, "job", "user_profile_refined", {
            "user_name": ctx.user_name,
            "facts_invalidated": len(merge_result.to_invalidate),
            "facts_created": len(merge_result.new_contents)
        })
        
        return True

    async def _process_single_batch(
        self, 
        ctx: JobContext,
        batch: List[Dict],
        ents_to_facts: Dict[int, List[FactRecord]],
        current_msg_id: int,
        valid_msg_ids: set
    ) -> List[Dict]:
        """Process one batch of entities. Returns list of updates."""
        async with self.batch_semaphore:
            llm_input = []
            for e in batch:
                enriched_facts = await enrich_facts_with_sources(e["existing_facts"], self.store)
                if len(enriched_facts) > self.max_facts_context:
                    enriched_facts = enriched_facts[-self.max_facts_context:]
                llm_input.append({
                    "entity_name": e["entity_name"],
                    "entity_type": e["entity_type"],
                    "existing_facts": enriched_facts,
                    "known_aliases": e["known_aliases"]
                })

            combined_conversation = "\n---\n".join([e["conversation_text"] for e in batch])

            if self.profile_prompt:
                system_reasoning = self.profile_prompt.replace("{user_name}", ctx.user_name)
            else:
                system_reasoning = get_profile_extraction_prompt(ctx.user_name)
                
            user_content = format_vp04_input(llm_input, combined_conversation)

            await emit(ctx.session_id, "job", "llm_call", {
                "stage": "profile_extraction",
                "entities": [e["entity_name"] for e in batch],
                "prompt": user_content
            }, verbose_only=True)

            profiles_result: EntityProfilesResult = await self.llm.call_llm(
                response_model=EntityProfilesResult,
                system=system_reasoning,
                user=user_content,
                temperature=0.0
            )
            
            if not profiles_result or not profiles_result.profiles:
                logger.warning(f"No profiles extracted for: {[e['entity_name'] for e in batch]}")
                return []
            
            updates = []
            profile_map = {p.canonical_name.lower(): p for p in profiles_result.profiles}
            
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
                
                # Pass local facts to avoid N+1 DB fetch
                final_active_facts, failed_invalidations = await self._apply_fact_changes(orig["ent_id"], merge_result, existing_facts, valid_msg_ids, ctx.session_id)
                if failed_invalidations:
                    dirty_key = RedisKeys.dirty_entities(ctx.user_name, ctx.session_id)
                    await self.redis.sadd(dirty_key, str(orig["ent_id"]))
                    logger.warning(f"Re-dirtied entity {orig['ent_id']}: {len(failed_invalidations)} invalidations failed")
                
                embedding = await self._update_entity_embedding(orig["ent_id"], orig["entity_name"], final_active_facts)
                
                updates.append({
                    "id": orig["ent_id"],
                    "canonical_name": orig["entity_name"],
                    "embedding": embedding,
                    "last_msg_id": current_msg_id
                })

            updated_ids = {u["id"] for u in updates}
            no_update_ents = [orig for orig in batch if orig["ent_id"] not in updated_ids]
            
            for orig in no_update_ents:
                await self.store.update_entity_checkpoint(
                    orig["ent_id"],
                    current_msg_id
                )

            return updates
    

    async def _run_updates(self, ctx: JobContext, entity_ids: List[int], conversation: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[int]]:
        current_msg_id = await self.redis.get(RedisKeys.last_processed(ctx.user_name, ctx.session_id))
        current_msg_id = int(current_msg_id) if current_msg_id else 0

        loop = asyncio.get_running_loop()

        valid_entities = []
        for ent_id in entity_ids:
            profile = self.resolver.entity_profiles.get(ent_id)
            if profile:
                valid_entities.append((ent_id, profile))

        if not valid_entities:
            return [], entity_ids # if invalid, we should clear them from dirty queue too


        ents_to_facts = await self.store.get_facts_for_entities(
            [ent_id for ent_id, _ in valid_entities],
            True
        )

        if ents_to_facts is None:
            logger.error("Failed to fetch facts for entities, skipping profile refinement")
            return [], []


        # Batch fetch last_profiled_msg_id for all entities to avoid N+1
        entities_data = await self.store.get_entities_by_ids([ent_id for ent_id, _ in valid_entities])
        profiled_checkpoints = {e["id"]: e.get("last_profiled_msg_id", 0) for e in entities_data}

        entity_inputs = []
        for ent_id, profile in valid_entities:
            existing_facts = ents_to_facts.get(ent_id, [])
            
            # Filter conversation to only new turns since last profiling
            checkpoint = profiled_checkpoints.get(ent_id, 0)
            entity_conversation = [
                turn for turn in conversation
                if (turn.get("user_msg_id") or 0) > checkpoint
            ]
            
            if not entity_conversation:
                logger.debug(f"No new conversation for entity {ent_id} since msg_{checkpoint}")
                continue


            entity_inputs.append({
                "ent_id": ent_id,
                "entity_name": profile.get("canonical_name", "Unknown"),
                "entity_type": profile.get("type", "unknown"),
                "existing_facts": existing_facts,
                "known_aliases": self.resolver.get_mentions_for_id(ent_id),
                "conversation_text": "\n".join([t["formatted"] for t in entity_conversation])
            })

        if not entity_inputs:
            return [], entity_ids # all evaluated and had no new context, clear them


        batches = [
            entity_inputs[i:i + self.profile_batch_size]
            for i in range(0, len(entity_inputs), self.profile_batch_size)
        ]

        valid_msg_ids = {int(turn['user_msg_id']) for turn in conversation if turn.get('user_msg_id') is not None}

        tasks = [
            self._process_single_batch(ctx, batch, ents_to_facts, current_msg_id, valid_msg_ids)
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
                # If the batch succeeded, these entities are considered "done" even if no facts found
                successful_entity_ids.extend(batch_ents)

        return all_updates, successful_entity_ids
    

    async def _apply_fact_changes(
        self, entity_id, merge_result, existing_facts, valid_msg_ids, session_id
    ) -> Tuple[List[FactRecord], List[str]]:
        """
        Invalidate old facts and create new ones. Creates first, invalidates after.
        Returns the final set of active facts.
        """
        now = datetime.now(timezone.utc)
        
        to_invalidate = set(merge_result.to_invalidate)
        active_existing = [f for f in existing_facts if f.invalid_at is None and f.id not in to_invalidate]
        
        facts_to_create = []

        for fact_update in merge_result.new_contents:
            content, msg_id = extract_fact_with_source(fact_update)
            
            if msg_id is not None and valid_msg_ids is not None and msg_id not in valid_msg_ids:
                msg_type = type(msg_id).__name__
                valid_type = type(list(valid_msg_ids)[0]).__name__ if valid_msg_ids else "empty"
                logger.warning(
                    f"[{session_id}] ProfileRefinementJob: "
                    f"Invalid msg_id {msg_id} (type {msg_type}) not in conversation window "
                    f"{valid_msg_ids} (type {valid_type})"
                )
                msg_id = None
            
            embedding = await self.embedding_service.encode_single(content)
            
            contradicted_ids = await self._detect_contradictions(content, embedding, active_existing, msg_id, session_id)
            
            to_invalidate.update(contradicted_ids)
            
            if contradicted_ids:
                contradicted_set = set(contradicted_ids)
                active_existing = [f for f in active_existing if f.id not in contradicted_set]
            
            fact = FactRecord(
                id=str(uuid.uuid4()),
                content=content,
                valid_at=now,
                source_msg_id=msg_id,
                embedding=embedding,
                source_entity_id=entity_id
            )
            facts_to_create.append(fact)
            active_existing.append(fact)

        if facts_to_create:
            try:
                count = await self.store.create_facts_batch(
                    entity_id,
                    facts_to_create
                )
                logger.debug(f"Created {count} facts for entity {entity_id}")
                
                failed_invalidations = []
                for fact_id in to_invalidate:
                    try:
                        await self.store.invalidate_fact(
                            fact_id,
                            now
                        )
                    except Exception as e:
                        logger.warning(f"Failed to invalidate fact {fact_id}: {e}")
                        failed_invalidations.append(fact_id)

                if failed_invalidations:
                    await emit(session_id, "job", "invalidation_failures", {
                        "entity_id": entity_id,
                        "failed_fact_ids": failed_invalidations
                    })
                
                await emit(session_id, "job", "facts_changed", {
                    "entity_id": entity_id,
                    "invalidated": len(to_invalidate),
                    "created": len(facts_to_create)
                }, verbose_only=True)

                return active_existing, failed_invalidations
                
            except Exception as e:
                logger.error(f"Failed to write facts for {entity_id}, skipping invalidations. Error: {e}")
                await emit(session_id, "job", "facts_write_failed", {
                    "entity_id": entity_id,
                    "fact_count": len(facts_to_create),
                    "error": str(e)
                })
                return [f for f in active_existing if f not in facts_to_create], list(to_invalidate)
        elif to_invalidate:
            failed_invalidations = []
            for fact_id in to_invalidate:
                try:
                    await self.store.invalidate_fact(
                        fact_id,
                        now
                    )
                except Exception as e:
                    logger.warning(f"Failed to invalidate fact {fact_id}: {e}")
                    failed_invalidations.append(fact_id)

            if failed_invalidations:
                await emit(session_id, "job", "invalidation_failures", {
                    "entity_id": entity_id,
                    "failed_fact_ids": failed_invalidations
                })
            
            await emit(session_id, "job", "facts_changed", {
                "entity_id": entity_id,
                "invalidated": len(to_invalidate),
                "created": 0
            }, verbose_only=True)

            return active_existing, failed_invalidations
        
        return active_existing, []
                
    async def _detect_contradictions(
        self,
        new_content: str,
        new_embedding: List[float],
        existing_facts: List[FactRecord],
        new_msg_id: Optional[int] = None,
        session_id: str = None
    ) -> List[str]:
        """
        Find existing fact that new fact contradicts.
        Uses embedding filter + LLM judgment.
        Returns fact ID to invalidate, or None.
        """
        if not existing_facts:
            return []
        
        new_emb = np.array(new_embedding)
        new_emb = new_emb / np.linalg.norm(new_emb)
        
        candidates = []
        
        for fact in existing_facts:
            if not fact.embedding:
                continue
            
            existing_emb = np.array(fact.embedding)
            existing_emb = existing_emb / np.linalg.norm(existing_emb)
            
            similarity = float(np.dot(new_emb, existing_emb))
            
            if self.contradiction_sim_low <= similarity < self.contradiction_sim_high:
                if new_content.lower().strip() != fact.content.lower().strip():
                    if new_msg_id and fact.source_msg_id:
                        if new_msg_id < fact.source_msg_id:
                            logger.debug(f"Skipping contradiction check: msg_{new_msg_id} older than msg_{fact.source_msg_id} for '{new_content[:40]}...'")
                            continue
                        if new_msg_id == fact.source_msg_id:
                            logger.debug(f"Skipping contradiction check: msg_{new_msg_id} same as source msg for '{new_content[:40]}...'")
                            continue
                    candidates.append((fact, similarity))
        
        if not candidates:
            return []

        candidates: List[Tuple[FactRecord, float]] = sorted(candidates, key=lambda x: x[1], reverse=True)
        
        to_invalidate = []
    
        for i in range(0, len(candidates), self.contradiction_batch_size):
            batch = candidates[i:i + self.contradiction_batch_size]
            
            pairs = [(fact.content, new_content) for fact, _ in batch]
            
            judgments = await self._llm_judge_contradiction(pairs, session_id)
            
            for idx, is_contradiction in judgments.items():
                if is_contradiction:
                    if 0 <= idx < len(batch):
                        fact, sim = batch[idx]
                        logger.info(f"LLM confirmed contradiction: '{new_content[:50]}' supersedes '{fact.content[:50]}' (sim={sim:.3f})")
                        to_invalidate.append(fact.id)
                    else:
                        logger.warning(f"LLM returned out-of-range contradiction index {idx} (batch size={len(batch)})")
        
        await emit(session_id, "job", "contradictions_detected", {
            "new_fact": new_content,
            "invalidated_count": len(to_invalidate)
        }, verbose_only=True)
        
        return to_invalidate


    async def _llm_judge_contradiction(self, pairs: List[Tuple[str, str]], session_id: str) -> Dict[int, bool]:
        """
        Ask LLM if new facts contradict existing facts.
        """
        if not pairs:
            return {}
        
        system = self.contradiction_prompt if self.contradiction_prompt else get_contradiction_judgment_prompt()
        
        lines = []
        lines.append("## Facts to evaluate for contradictions:")
        for i, (existing, new) in enumerate(pairs, start=1):
            lines.append(f'{i}. FACT_A: "{existing}" | FACT_B: "{new}"')
        user = "\n".join(lines)
        
        try:
            await emit(session_id, "job", "llm_call", {
                "stage": "contradiction_judgment",
                "pair_count": len(pairs),
                "prompt": user
            }, verbose_only=True)

            bulk_contradiction: BulkContradictionResult = await self.llm.call_llm(
                response_model=BulkContradictionResult,
                system=system,
                user=user,
                temperature=0.0
            )

            if not bulk_contradiction or not bulk_contradiction.judgments:
                logger.warning("LLM returned no contradiction judgments")
                return {}
            
            # Map index (1-based) to is_contradiction
            return {j.index - 1: j.is_contradiction for j in bulk_contradiction.judgments}

        except Exception as e:
            logger.error(f"Structured contradiction detection failed: {e}")
            return {}
    
    async def _update_entity_embedding(
        self,
        entity_id: int, 
        canonical_name: str,
        active_facts: Optional[List[FactRecord]] = None
    ) -> List[float]:
        """Recompute entity embedding from current active facts."""
        if active_facts is None:
            active_facts = await self.store.get_facts_for_entity(
                entity_id,
                True
            )
            if active_facts is None:
                logger.warning(f"Could not fetch facts for embedding update, using name only")
                active_facts = []
        
        resolution_text = f"{canonical_name}. " + " ".join([f.content for f in active_facts])
        
        new_emb = await self.resolver.compute_embedding(entity_id, resolution_text)

        return new_emb

    async def _write_updates(self, updates: List[Dict]):
        """Write profile updates to Memgraph sequentially."""
        
        for update in updates:
            await self.store.update_entity_profile(
                entity_id=update["id"],
                canonical_name=update["canonical_name"],
                embedding=update["embedding"],
                last_msg_id=update["last_msg_id"]
            )

        logger.info(f"Wrote {len(updates)} profile updates to graph")