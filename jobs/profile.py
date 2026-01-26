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
from jobs.base import BaseJob, JobContext, JobNotifier, JobResult
from main.service import LLMService
from main.entity_resolve import EntityResolver
from main.prompts import get_contradiction_judgment_prompt, get_profile_extraction_prompt
from jobs.jobs_utils import extract_fact_with_source, process_extracted_facts, parse_new_facts
from schema.dtypes import Fact, FactMergeResult

class ProfileRefinementJob(BaseJob):
    """
    Scans entities that have been 'touched' recently and updates their profiles
    using the sliding window of recent messages.
    
    Triggers:
    1. VOLUME: If >=20 entities are dirty (ensures we catch them in the 75-msg window).
    2. TIME: If user is idle for >5 minutes and we have ANY dirty entities.
    """
    
    MSG_WINDOW = 30
    VOLUME_THRESHOLD = 30
    IDLE_THRESHOLD = 60
    PROFILE_BATCH_SIZE = 8

    def __init__(self, llm: LLMService, resolver: EntityResolver, store: MemGraphStore, executor: ThreadPoolExecutor):
        self.llm = llm
        self.resolver = resolver
        self.store = store
        self.executor = executor
        self.batch_semaphore = asyncio.Semaphore(2)

    @property
    def name(self) -> str:
        return "profile_refinement"

    async def should_run(self, ctx: JobContext) -> bool:
        dirty_key = f"dirty_entities:{ctx.user_name}"
        return await ctx.redis.scard(dirty_key) > 0
    
    async def _maybe_refine_user(self, ctx: JobContext, curr_msg_id: int) -> bool:
        """
        Check conditions and trigger user profile refinement if needed.
        Returns True if refinement ran.
        """
        ran_key = f"user_profile_ran:{ctx.user_name}"
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
        """Fetch recent conversation with both user and STELLA turns."""
        sorted_key = f"recent_conversation:{ctx.user_name}"
        conv_key = f"conversation:{ctx.user_name}"
        
        fetch_count = int(num_turns * 2)
        if up_to_msg_id:
            turn_key = await ctx.redis.hget(
                f"lookup:msg_to_turn:{ctx.user_name}",
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
            role_label = "User" if parsed["role"] == "user" else "STELLA"
            
            if parsed["role"] == "user" and parsed.get("user_msg_id") is not None:
                formatted = f"[MSG_{parsed['user_msg_id']}] [{date_str}] [{role_label}]: {parsed['content']}"
            else:
                formatted = f"[{date_str}] [{role_label}]: {parsed['content']}"
            
            turn = {
                "turn_id": turn_id,
                "role": parsed["role"],
                "role_label": role_label,
                "content": parsed["content"],
                "formatted": formatted,
                "raw": parsed["content"],
                "timestamp": parsed["timestamp"],
                "user_msg_id": parsed.get("user_msg_id")
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
        warning = "⚠️ **Deepening Profiles.** I am reading through recent conversations to update entity details. Please wait a moment for the best results."

        async with JobNotifier(ctx.redis, warning):
            current_msg_id = await ctx.redis.get(f"last_processed_msg:{ctx.user_name}")
            current_msg_id = int(current_msg_id) if current_msg_id else 0

            dirty_key = f"dirty_entities:{ctx.user_name}"
            raw_ids = await ctx.redis.spop(dirty_key, self.VOLUME_THRESHOLD)
            
            user_id = self.resolver.get_id(ctx.user_name)
            entity_ids = [int(id_str) for id_str in raw_ids if int(id_str) != user_id] if raw_ids else []
            
            updates = []
            
            if entity_ids:
                conversation = await self._get_conversation_context(ctx, self.MSG_WINDOW, up_to_msg_id=current_msg_id)
        
                if not conversation:
                    await ctx.redis.sadd(dirty_key, *[str(eid) for eid in entity_ids])
                    return JobResult(success=False, summary="No context found")
                
                try:
                    updates = await self._run_updates(ctx, entity_ids, conversation)
                    
                    if updates:
                        await self._write_updates(updates)
                except Exception as e:
                    logger.error(f"Profile refinement failed, re-queuing {len(entity_ids)} entities: {e}")
                    await ctx.redis.sadd(dirty_key, *[str(eid) for eid in entity_ids])
                    return JobResult(success=False, summary=f"Failed: {e}")
            
            user_refined = await self._maybe_refine_user(ctx, current_msg_id)
            
            parts = []
            if updates:
                parts.append(f"Refined {len(updates)} profiles")
            if user_refined:
                parts.append(f"refined {ctx.user_name}")
            
            summary = ", ".join(parts) if parts else "No profiles to update"

            await ctx.redis.setex(
                f"profile_complete:{ctx.user_name}",
                300,
                str(datetime.now(timezone.utc).timestamp())
            )
            
            return JobResult(success=True, summary=summary)
    
    async def _refine_user_profile(self, ctx: JobContext, user_id: int, profile: dict, curr_msg_id: int) -> bool:
        """Execute user profile refinement."""
        conversation = await self._get_conversation_context(ctx, int(self.MSG_WINDOW * 1.5), up_to_msg_id=curr_msg_id)

        if not conversation:
            logger.warning("User profile refinement: no conversation context")
            return False
        
        conversation_text = "\n".join([turn["formatted"] for turn in conversation])

        if not conversation_text:
            logger.warning("User profile refinement: empty conversation text")
            return False
        
        # Get current message ID for checkpoint
        current_msg_id = await ctx.redis.get(f"last_processed_msg:{ctx.user_name}")
        current_msg_id = int(current_msg_id) if current_msg_id else 0
        
        # Fetch existing facts from DB
        loop = asyncio.get_running_loop()
        existing_facts = await loop.run_in_executor(
            self.executor,
            self.store.get_facts_for_entity,
            user_id,
            True  # active_only
        )
        
        system_reasoning = get_profile_extraction_prompt(ctx.user_name)
        enriched_facts = await self._enrich_facts_with_sources(existing_facts)
        user_content = json.dumps({
            "entities": [{
                "entity_name": ctx.user_name,
                "entity_type": "person",
                "existing_facts": enriched_facts,
                "known_aliases": [alias for alias in profile.get("aliases", [ctx.user_name])]
            }],
            "conversation": conversation_text
        })
        
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
        
        valid_msg_ids = {turn['user_msg_id'] for turn in conversation if turn.get('user_msg_id') is not None}

        await self._apply_fact_changes(user_id, merge_result, existing_facts, valid_msg_ids)
        
        embedding = await self._update_entity_embedding(user_id, ctx.user_name)
        
        # Update entity profile in graph
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
        return True

    async def _process_single_batch(
        self, 
        ctx: JobContext,
        batch: List[Dict],
        conversation_text: str,
        ents_to_facts: Dict[int, List[Fact]],
        current_msg_id: int,
        valid_msg_ids: set
    ) -> List[Dict]:
        """Process one batch of entities. Returns list of updates."""
        async with self.batch_semaphore:
            llm_input = []
            for e in batch:
                enriched_facts = await self._enrich_facts_with_sources(e["existing_facts"])
                llm_input.append({
                    "entity_name": e["entity_name"],
                    "entity_type": e["entity_type"],
                    "existing_facts": enriched_facts,
                    "known_aliases": e["known_aliases"]
                })
            
            system_reasoning = get_profile_extraction_prompt(ctx.user_name)
            user_content = json.dumps({
                "entities": llm_input,
                "conversation": conversation_text
            })
            
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
                
                await self._apply_fact_changes(orig["ent_id"], merge_result, existing_facts, valid_msg_ids)
                
                # Update entity embedding
                embedding = await self._update_entity_embedding(orig["ent_id"], orig["entity_name"])
                
                updates.append({
                    "id": orig["ent_id"],
                    "canonical_name": orig["entity_name"],
                    "embedding": embedding,
                    "last_msg_id": current_msg_id
                })

            return updates

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
                    text = await loop.run_in_executor(
                        self.executor,
                        self.store.get_message_text,
                        fact.source_msg_id
                    )
                    if text:
                        entry["source_message"] = text
                except Exception as e:
                    logger.debug(f"Could not fetch source for msg {fact.source_msg_id}: {e}")
            
            enriched.append(entry)
        
        return enriched
    

    async def _run_updates(self, ctx: JobContext, entity_ids: List[int], conversation: List[Dict]):
        """Process entities in batches instead of individually."""
        
        current_msg_id = await ctx.redis.get(f"last_processed_msg:{ctx.user_name}")
        current_msg_id = int(current_msg_id) if current_msg_id else 0
        conversation_text = "\n".join([turn["formatted"] for turn in conversation])

        loop = asyncio.get_running_loop()

        entity_inputs = []
        ents_to_facts = {}
        for ent_id in entity_ids:
            profile = self.resolver.entity_profiles.get(ent_id)
            if not profile:
                continue
            
            existing_facts = await loop.run_in_executor(None,
                                        self.store.get_facts_for_entity, ent_id, True)
            
            ents_to_facts[ent_id] = existing_facts

            entity_inputs.append({
                "ent_id": ent_id,
                "entity_name": profile.get("canonical_name", "Unknown"),
                "entity_type": profile.get("type", "unknown"),
                "existing_facts": existing_facts,
                "known_aliases": self.resolver.get_mentions_for_id(ent_id)
            })
        
        if not entity_inputs:
            return []
        
        conversation_text = "\n".join([turn["formatted"] for turn in conversation])
        
        batches = [
            entity_inputs[i:i + self.PROFILE_BATCH_SIZE]
            for i in range(0, len(entity_inputs), self.PROFILE_BATCH_SIZE)
        ]
        
        valid_msg_ids = {turn['user_msg_id'] for turn in conversation if turn.get('user_msg_id') is not None}

        tasks = [
            self._process_single_batch(ctx, batch, conversation_text, ents_to_facts, current_msg_id, valid_msg_ids)
            for batch in batches
        ]
        
        results = await asyncio.gather(*tasks)
        return [update for batch_updates in results for update in batch_updates]
    

    async def _apply_fact_changes(
        self,
        entity_id: int,
        merge_result: FactMergeResult,
        existing_facts: List[Fact],
        valid_msg_ids: Optional[set] = None
    ):
        """Invalidate old facts and create new ones."""
        loop = asyncio.get_running_loop()
        now = datetime.now(timezone.utc)
        
        for fact_id in merge_result.to_invalidate:
            await loop.run_in_executor(
                self.executor,
                self.store.invalidate_fact,
                fact_id,
                now
            )
        
        invalidated_ids = set(merge_result.to_invalidate)
        active_existing = [f for f in existing_facts if f.invalid_at is None and f.id not in invalidated_ids]
        
        facts_to_create = []

        for raw_content in merge_result.new_contents:
            content, msg_id = extract_fact_with_source(raw_content)
            
            if msg_id and valid_msg_ids and msg_id not in valid_msg_ids:
                logger.warning(f"Invalid msg_id {msg_id} not in conversation window, setting to None")
                msg_id = None
            
            embedding = await loop.run_in_executor(
                self.executor,
                self.resolver.embedding_model.encode,
                [content]
            )
            embedding = embedding[0].tolist()
            
            contradicted_ids = await self._detect_contradictions(content, embedding, active_existing, new_msg_id=msg_id)
            for contradicted_id in contradicted_ids:
                await loop.run_in_executor(
                    self.executor,
                    self.store.invalidate_fact,
                    contradicted_id,
                    now
                )
            
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
                logger.debug(f"Batched created {count} facts for entity {entity_id}")
            except Exception as e:
                logger.error(f"Failed to write facts for {entity_id}, dropping {len(facts_to_create)} facts. Error: {e}")
    
    async def _detect_contradictions(
        self,
        new_content: str,
        new_embedding: List[float],
        existing_facts: List[Fact],
        new_msg_id: Optional[int] = None,
        similarity_low: float = 0.70,
        similarity_high: float = 0.95,
        batch_size: int = 4
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
            
            if similarity_low <= similarity < similarity_high:
                if new_content.lower().strip() != fact.content.lower().strip():
                    if new_msg_id and fact.source_msg_id:
                        try:
                            if new_msg_id < fact.source_msg_id:
                                logger.debug(f"Skipping contradiction check: msg_{new_msg_id} older than msg_{fact.source_msg_id} for '{new_content[:40]}...'")
                                continue
                        except ValueError:
                            pass
                    candidates.append((fact, similarity))
        
        if not candidates:
            return []

        candidates.sort(key=lambda x: x[1], reverse=True)
        
        to_invalidate = []
    
        for i in range(0, len(candidates), batch_size):
            batch = candidates[i:i + batch_size]
            
            pairs = [(fact.content, new_content) for fact, _ in batch]
            
            judgments = await self._llm_judge_contradiction(pairs)
            
            for idx, is_contradiction in judgments.items():
                if is_contradiction:
                    fact, sim = batch[idx]
                    logger.info(f"LLM confirmed contradiction: '{new_content[:50]}' supersedes '{fact.content[:50]}' (sim={sim:.3f})")
                    to_invalidate.append(fact.id)
        
        return to_invalidate


    async def _llm_judge_contradiction(self, pairs: List[Tuple[str, str]]) -> Dict[int, bool]:
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
        
        if not result:
            raise ValueError("LLM returned empty response for contradiction judgment")
        
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
            logger.warning(f"Expected {len(pairs)} results, got {len(judgments)}. Missing indices treated as false.")
        
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