import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from functools import partial
import json
import re
from typing import Dict, List, Optional, Tuple
import uuid
from loguru import logger
import numpy as np
from db.store import MemGraphStore
from jobs.base import BaseJob, JobContext, JobResult
from shared.embedding import EmbeddingService
from shared.service import LLMService
from main.entity_resolve import EntityResolver
from main.prompts import get_contradiction_judgment_prompt, get_profile_extraction_prompt
from jobs.jobs_utils import enrich_facts_with_sources, extract_fact_with_source, format_vp04_input, process_extracted_facts, parse_new_facts
from shared.schema.dtypes import Fact, FactMergeResult
from shared.events import emit
from shared.redisclient import RedisKeys
from shared.config import get_config_value

class ProfileRefinementJob(BaseJob):
    """
    Scans entities that have been 'touched' recently and updates their profiles
    using the sliding window of recent messages.
    
    Triggers:
    1. VOLUME: If >=20 entities are dirty (ensures we catch them in the 75-msg window).
    2. TIME: If user is idle for >5 minutes and we have ANY dirty entities.
    """

    def __init__(self, llm: LLMService, resolver: EntityResolver, store: MemGraphStore, 
                executor: ThreadPoolExecutor, embedding_service: EmbeddingService,
                msg_window: int = 30, volume_threshold: int = 15, idle_threshold: int = 90,
                contradiction_sim_low: float = 0.70, contradiction_sim_high: float = 0.95,
                contradiction_batch_size: int = 4, profile_batch_size: int = 8):
        
        self.llm = llm
        self.resolver = resolver
        self.store = store
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
        

    @property
    def name(self) -> str:
        return "profile_refinement"

    async def should_run(self, ctx: JobContext) -> bool:
        dirty_key = RedisKeys.dirty_entities(ctx.user_name, ctx.session_id)
        count = await ctx.redis.scard(dirty_key)
        
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
        if await ctx.redis.get(ran_key):
            return False
        
        user_id = self.resolver.get_id(ctx.user_name)
        if not user_id:
            logger.warning(f"User entity {ctx.user_name} not found in resolver")
            return False
        
        profile = self.resolver.entity_profiles.get(user_id)
        if not profile:
            logger.warning(f"User profile {user_id} not found")
            return False
        
        success = await self._refine_user_profile(ctx, user_id, profile, curr_msg_id)

        await ctx.redis.setex(ran_key, 300, "true")
        
        return success
    
    async def _get_conversation_context(self, ctx: JobContext, num_turns: int, user_ratio: float = 0.75, up_to_msg_id: int = None) -> List[Dict]:
        """Fetch recent conversation with both user and agent turns."""
        sorted_key = RedisKeys.recent_conversation(ctx.user_name, ctx.session_id)
        conv_key = RedisKeys.conversation(ctx.user_name, ctx.session_id)
        
        fetch_count = int(num_turns * 2)
        if up_to_msg_id:
            turn_key = await ctx.redis.hget(
                RedisKeys.msg_to_turn_lookup(ctx.user_name, ctx.session_id),
                f"msg_{up_to_msg_id}"
            )
            if turn_key:
                turn_score = await ctx.redis.zscore(sorted_key, turn_key)
                turn_ids = await ctx.redis.zrevrangebyscore(
                    sorted_key,
                    f"({turn_score}",
                    "-inf",
                    start=0,
                    num=fetch_count
                )
                turn_ids = list(reversed(turn_ids))
            else:
                turn_ids = await ctx.redis.zrevrange(sorted_key, 0, fetch_count - 1)
                turn_ids = list(turn_ids)
                turn_ids.reverse()
        else:
            turn_ids = await ctx.redis.zrevrange(sorted_key, 0, fetch_count - 1)
            if not turn_ids:
                return []
            turn_ids = list(turn_ids)
            turn_ids.reverse()
        
        if not turn_ids:
            return []
        
        turn_data = await ctx.redis.hmget(conv_key, *turn_ids)
        
        user_turns = []
        assistant_turns = []
        
        for turn_id, data in zip(turn_ids, turn_data):
            if not data:
                continue
            
            parsed = json.loads(data)
            ts = datetime.fromisoformat(parsed['timestamp'])
            date_str = ts.strftime("%Y-%m-%d %H:%M")
            role_label = "USER" if parsed["role"] == "user" else "AGENT"
            
            if parsed["role"] == "user" and parsed.get("user_msg_id") is not None:
                formatted = f"[MSG_{parsed['user_msg_id']}] [{date_str}] [{role_label}]: {parsed['content']}"
            else:
                formatted = f"[{date_str}] [{role_label}]: {parsed['content']}"
            
            turn = {"turn_id": turn_id, "role": parsed["role"], "role_label": role_label, "content": parsed["content"],
                "formatted": formatted, "raw": parsed["content"], "timestamp": parsed["timestamp"], "user_msg_id": parsed.get("user_msg_id")
            }
            
            if parsed["role"] == "user":
                user_turns.append(turn)
            else:
                assistant_turns.append(turn)
        
        user_count = min(len(user_turns), int(num_turns * user_ratio))
        assistant_count = min(len(assistant_turns), num_turns - user_count)
        
        if user_count < int(num_turns * user_ratio):
            assistant_count = min(len(assistant_turns), num_turns - user_count)
        
        selected_user = user_turns[-user_count:] if user_count else []
        selected_assistant = assistant_turns[-assistant_count:] if assistant_count else []
        
        combined = selected_user + selected_assistant
        combined.sort(key=lambda x: x["timestamp"])
        
        return combined

    async def execute(self, ctx: JobContext) -> JobResult:

        current_msg_id = await ctx.redis.get(RedisKeys.last_processed(ctx.user_name, ctx.session_id))
        current_msg_id = int(current_msg_id) if current_msg_id else 0

        dirty_key = RedisKeys.dirty_entities(ctx.user_name, ctx.session_id)
        
        raw_ids = await ctx.redis.srandmember(dirty_key, self.volume_threshold)
        
        user_id = self.resolver.get_id(ctx.user_name)
        entity_ids = [int(id_str) for id_str in raw_ids if int(id_str) != user_id] if raw_ids else []

        logger.info(f"Profile refinement starting: {len(entity_ids)} entities to process")
        
        updates = []
        
        if entity_ids:
            conversation = await self._get_conversation_context(ctx, self.msg_window, up_to_msg_id=current_msg_id)

            if not conversation:
                return JobResult(success=False, summary="No context found")
            
            try:

                updates, clear_ids = await self._run_updates(ctx, entity_ids, conversation)
                
                if updates:
                    await self._write_updates(updates)
                    await emit(ctx.session_id, "job", "profiles_refined", {
                        "count": len(updates),
                        "entities": [u["canonical_name"] for u in updates]
                    })
                    merge_queue = RedisKeys.merge_queue(ctx.user_name, ctx.session_id)
                    updated_ids = [str(u["id"]) for u in updates]
                    
                    dev_settings = get_config_value("developer_settings", {})
                    merger_enabled = dev_settings.get("jobs", {}).get("merger", {}).get("enabled", True)

                    if updated_ids and merger_enabled:
                        await ctx.redis.sadd(merge_queue, *updated_ids)
                        logger.info(f"Passed {len(updated_ids)} updated entities to Merge Queue")

            except Exception as e:
                logger.error(f"Profile refinement failed: {e}")
                await emit(ctx.session_id, "job", "profile_refinement_failed", {
                    "entity_count": len(entity_ids),
                    "error": str(e)
                })
                return JobResult(success=False, summary=f"Failed: {e}")
        
        user_refined = await self._maybe_refine_user(ctx, current_msg_id)
        
        # Clear IDs from dirty queue to prevent infinite loop
        # We clear both successfully updated entities AND entities that had no new context/facts
        processed_ids = []
        if 'clear_ids' in locals() and clear_ids:
            processed_ids.extend([str(eid) for eid in clear_ids])
            
        if processed_ids:
            await ctx.redis.srem(dirty_key, *processed_ids)
            logger.debug(f"Cleared {len(processed_ids)} entities from dirty queue")
        
        parts = []
        if updates:
            parts.append(f"Refined {len(updates)} profiles")
        if user_refined:
            parts.append(f"refined {ctx.user_name}")
        
        summary = ", ".join(parts) if parts else "No profiles to update"

        await ctx.redis.setex(
            RedisKeys.profile_complete(ctx.user_name, ctx.session_id),
            300,
            str(datetime.now(timezone.utc).timestamp())
        )
        
        return JobResult(success=True, summary=summary)
    
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
        current_msg_id = await ctx.redis.get(RedisKeys.last_processed(ctx.user_name, ctx.session_id))
        current_msg_id = int(current_msg_id) if current_msg_id else 0
        
        # Fetch existing facts from DB
        loop = asyncio.get_running_loop()
        existing_facts = await loop.run_in_executor(
            self.executor,
            self.store.get_facts_for_entity,
            user_id,
            True  # active_only
        )

        if existing_facts is None:
            logger.warning("Could not fetch user facts, skipping refinement")
            return False
        
        system_reasoning = get_profile_extraction_prompt(ctx.user_name)
        enriched_facts = await enrich_facts_with_sources(existing_facts, self.store)
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
        
        reasoning = await self.llm.call_llm(system_reasoning, user_content)

        if not reasoning:
            logger.warning("VEGAPUNK-06 returned None for user profile")
            return False
        
        response = parse_new_facts(reasoning)
        
        if not response or not response:
            logger.warning("No facts parsed for user profile")
            return False

        profile_map = {p.canonical_name.lower(): p for p in response}
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

        await self._apply_fact_changes(user_id, merge_result, existing_facts, valid_msg_ids, ctx.session_id)
        
        embedding = await self._update_entity_embedding(user_id, ctx.user_name)
        
        await loop.run_in_executor(
            self.executor,
            partial(
                self.store.update_entity_profile,
                entity_id=user_id,
                canonical_name=ctx.user_name,
                embedding=embedding,
                last_msg_id=current_msg_id
            )
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
        ents_to_facts: Dict[int, List[Fact]],
        current_msg_id: int,
        valid_msg_ids: set
    ) -> List[Dict]:
        """Process one batch of entities. Returns list of updates."""
        async with self.batch_semaphore:
            llm_input = []
            for e in batch:
                enriched_facts = await enrich_facts_with_sources(e["existing_facts"], self.store)
                llm_input.append({
                    "entity_name": e["entity_name"],
                    "entity_type": e["entity_type"],
                    "existing_facts": enriched_facts,
                    "known_aliases": e["known_aliases"]
                })

            combined_conversation = "\n---\n".join([e["conversation_text"] for e in batch])

            system_reasoning = get_profile_extraction_prompt(ctx.user_name)
            user_content = format_vp04_input(llm_input, combined_conversation)

            await emit(ctx.session_id, "job", "llm_call", {
                "stage": "profile_extraction",
                "entities": [e["entity_name"] for e in batch],
                "prompt": user_content
            }, verbose_only=True)
            
            reasoning = await self.llm.call_llm(system_reasoning, user_content)
            
            if not reasoning:
                logger.warning(f"VEGAPUNK-06 returned None for: {[e['entity_name'] for e in batch]}")
                return []
            
            response = parse_new_facts(reasoning)
            
            if not response:
                logger.warning(f"No facts parsed for: {[e['entity_name'] for e in batch]}")
                return []
            
            updates = []
            profile_map = {p.canonical_name.lower(): p for p in response}
            
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
                
                await self._apply_fact_changes(orig["ent_id"], merge_result, existing_facts, valid_msg_ids, ctx.session_id)
                
                # Update entity embedding
                embedding = await self._update_entity_embedding(orig["ent_id"], orig["entity_name"])
                
                updates.append({
                    "id": orig["ent_id"],
                    "canonical_name": orig["entity_name"],
                    "embedding": embedding,
                    "last_msg_id": current_msg_id
                })

            loop = asyncio.get_running_loop()
            updated_ids = {u["id"] for u in updates}
            no_update_ents = [orig for orig in batch if orig["ent_id"] not in updated_ids]
            
            for orig in no_update_ents:
                await loop.run_in_executor(
                    None,
                    self.store.update_entity_checkpoint,
                    orig["ent_id"],
                    current_msg_id
                )

            return updates
    

    async def _run_updates(self, ctx: JobContext, entity_ids: List[int], conversation: List[Dict]) -> Tuple[List[Dict], List[int]]:
        current_msg_id = await ctx.redis.get(RedisKeys.last_processed(ctx.user_name, ctx.session_id))
        current_msg_id = int(current_msg_id) if current_msg_id else 0

        loop = asyncio.get_running_loop()

        valid_entities = []
        for ent_id in entity_ids:
            profile = self.resolver.entity_profiles.get(ent_id)
            if profile:
                valid_entities.append((ent_id, profile))

        if not valid_entities:
            return [], entity_ids # if invalid, we should clear them from dirty queue too


        ents_to_facts = await loop.run_in_executor(
            None,
            self.store.get_facts_for_entities,
            [ent_id for ent_id, _ in valid_entities],
            True
        )

        if ents_to_facts is None:
            logger.error("Failed to fetch facts for entities, skipping profile refinement")
            return [], []


        # Fetch last_profiled_msg_id for each entity
        profiled_checkpoints = {}
        for ent_id, _ in valid_entities:
            entity_data = await loop.run_in_executor(
                None, self.store.get_entity_by_id, ent_id
            )
            if entity_data and entity_data.get("last_profiled_msg_id"):
                profiled_checkpoints[ent_id] = entity_data["last_profiled_msg_id"]

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

        results = await asyncio.gather(*tasks)
        
        # Return the generated updates, along with the ENTIRE list of entity_ids we evaluated,
        # so they can all be removed from the dirty queue and stop the infinite loop.
        # - If an entity had no new conversation turns, it was skipped, so it should be removed.
        # - If an entity was evaluated but got no new facts, it should be removed.
        # - If an entity was updated, it should also be removed.
        return [update for batch_updates in results for update in batch_updates], entity_ids
    

    async def _apply_fact_changes(
        self,
        entity_id: int,
        merge_result: FactMergeResult,
        existing_facts: List[Fact],
        valid_msg_ids: Optional[set] = None,
        session_id: str = None
    ):
        """Invalidate old facts and create new ones. Creates first, invalidates after."""
        loop = asyncio.get_running_loop()
        now = datetime.now(timezone.utc)
        
        to_invalidate = set(merge_result.to_invalidate)
        active_existing = [f for f in existing_facts if f.invalid_at is None and f.id not in to_invalidate]
        
        facts_to_create = []

        for raw_content in merge_result.new_contents:
            content, msg_id = extract_fact_with_source(raw_content)
            
            if msg_id and valid_msg_ids and msg_id not in valid_msg_ids:
                logger.warning(f"Invalid msg_id {msg_id} (type {type(msg_id)}) not in conversation window {valid_msg_ids} (type {type(list(valid_msg_ids)[0]) if valid_msg_ids else 'empty'}), setting to None")
                msg_id = None
            
            embedding = await loop.run_in_executor(
                self.executor,
                self.embedding_service.encode_single,
                content
            )
            
            contradicted_ids = await self._detect_contradictions(content, embedding, active_existing, msg_id, session_id)
            
            to_invalidate.update(contradicted_ids)
            
            if contradicted_ids:
                contradicted_set = set(contradicted_ids)
                active_existing = [f for f in active_existing if f.id not in contradicted_set]
            
            fact = Fact(
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
                count = await loop.run_in_executor(
                    self.executor,
                    self.store.create_facts_batch,
                    entity_id,
                    facts_to_create
                )
                logger.debug(f"Created {count} facts for entity {entity_id}")
                
                failed_invalidations = []
                for fact_id in to_invalidate:
                    try:
                        await loop.run_in_executor(
                            self.executor,
                            self.store.invalidate_fact,
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
                
            except Exception as e:
                logger.error(f"Failed to write facts for {entity_id}, skipping invalidations. Error: {e}")
                await emit(session_id, "job", "facts_write_failed", {
                    "entity_id": entity_id,
                    "fact_count": len(facts_to_create),
                    "error": str(e)
                })
        elif to_invalidate:
            failed_invalidations = []
            for fact_id in to_invalidate:
                try:
                    await loop.run_in_executor(
                        self.executor,
                        self.store.invalidate_fact,
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
                
    async def _detect_contradictions(
        self,
        new_content: str,
        new_embedding: List[float],
        existing_facts: List[Fact],
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
                    candidates.append((fact, similarity))
        
        if not candidates:
            return []

        candidates.sort(key=lambda x: x[1], reverse=True)
        
        to_invalidate = []
    
        for i in range(0, len(candidates), self.contradiction_batch_size):
            batch = candidates[i:i + self.contradiction_batch_size]
            
            pairs = [(fact.content, new_content) for fact, _ in batch]
            
            judgments = await self._llm_judge_contradiction(pairs, session_id)
            
            for idx, is_contradiction in judgments.items():
                if is_contradiction:
                    fact, sim = batch[idx]
                    logger.info(f"LLM confirmed contradiction: '{new_content[:50]}' supersedes '{fact.content[:50]}' (sim={sim:.3f})")
                    to_invalidate.append(fact.id)
        
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
        
        system = get_contradiction_judgment_prompt()
        
        lines = []
        lines.append("## Facts to evaluate for contradictions:")
        for i, (existing, new) in enumerate(pairs, start=1):
            lines.append(f'{i}. FACT_A: "{existing}" | FACT_B: "{new}"')
        user = "\n".join(lines)
        
        result = await self.llm.call_llm(system, user)

        await emit(session_id, "job", "llm_call", {
            "stage": "contradiction_judgment",
            "pair_count": len(pairs),
            "prompt": user
        }, verbose_only=True)
        
        if not result:
            logger.warning("LLM returned empty for contradiction check, skipping")
            return {}
        
        match = re.search(r"<results>\s*(.*?)\s*</results>", result, re.DOTALL | re.IGNORECASE)
    
        if match:
            results_block = match.group(1).strip()
        else:
            # Fallback: try parsing raw output
            logger.warning("Missing <results> tags, attempting raw parse")
            results_block = result.strip()
        
        judgments = {}
        
        for line in results_block.split("\n"):
            line = line.strip()
            if not line:
                continue
            
            line_match = re.match(r"(\d+)\s*:\s*(true|false)", line, re.IGNORECASE)
            if not line_match:
                continue  # Skip malformed lines instead of raising
            
            idx = int(line_match.group(1)) - 1
            value = line_match.group(2).lower() == "true"
            judgments[idx] = value
        
        if len(judgments) != len(pairs):
            missing = len(pairs) - len(judgments)
            logger.warning(f"Contradiction check: {missing}/{len(pairs)} indices missing")
            await emit(session_id, "job", "contradiction_partial_result", {
                "expected": len(pairs),
                "received": len(judgments),
                "missing": missing
            }, verbose_only=True)
        
        return judgments
    
    async def _update_entity_embedding(
        self,
        entity_id: int, 
        canonical_name: str
    ) -> List[float]:
        """Recompute entity embedding from current active facts."""
        loop = asyncio.get_running_loop()
        
        active_facts = await loop.run_in_executor(
            self.executor,
            self.store.get_facts_for_entity,
            entity_id,
            True
        )

        if active_facts is None:
            logger.warning(f"Could not fetch facts for embedding update, using name only")
            active_facts = []
        
        resolution_text = f"{canonical_name}. " + " ".join([f.content for f in active_facts])
        
        embedding = await loop.run_in_executor(
            self.executor,
            partial(self.resolver.compute_embedding, entity_id, resolution_text)
        )

        return embedding

    async def _write_updates(self, updates: List[Dict]):
        """Write profile updates to Memgraph sequentially."""
        loop = asyncio.get_running_loop()
        
        for update in updates:
            await loop.run_in_executor(
                self.executor,
                partial(
                    self.store.update_entity_profile,
                    entity_id=update["id"],
                    canonical_name=update["canonical_name"],
                    embedding=update["embedding"],
                    last_msg_id=update["last_msg_id"]
                )
            )

        logger.info(f"Wrote {len(updates)} profile updates to graph")