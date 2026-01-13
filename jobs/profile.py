import asyncio
from datetime import datetime, timezone
from functools import partial
import json
from typing import Dict, List
from loguru import logger
from db.memgraph import MemGraphStore
from jobs.base import BaseJob, JobContext, JobNotifier, JobResult
from main.service import LLMService
from main.entity_resolve import EntityResolver
from main.prompts import get_profile_extraction_prompt
from jobs.utils import process_extracted_facts, parse_new_facts

class ProfileRefinementJob(BaseJob):
    """
    Scans entities that have been 'touched' recently and updates their profiles
    using the sliding window of recent messages.
    
    Triggers:
    1. VOLUME: If >=20 entities are dirty (ensures we catch them in the 75-msg window).
    2. TIME: If user is idle for >5 minutes and we have ANY dirty entities.
    """
    
    MSG_WINDOW = 20
    VOLUME_THRESHOLD = 30
    IDLE_THRESHOLD = 60
    PROFILE_BATCH_SIZE = 5

    def __init__(self, llm: LLMService, resolver: EntityResolver, store: MemGraphStore, executor):
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
    
    async def _maybe_refine_user(self, ctx: JobContext) -> bool:
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
        
        success = await self._refine_user_profile(ctx, user_id, profile)

        await ctx.redis.setex(ran_key, 300, "true")
        
        return success
    
    async def _get_conversation_context(self, ctx: JobContext, num_turns: int, user_ratio: float = 0.75) -> List[Dict]:
        """Fetch recent conversation with both user and STELLA turns."""
        sorted_key = f"recent_conversation:{ctx.user_name}"
        conv_key = f"conversation:{ctx.user_name}"
        
        fetch_count = int(num_turns * 2)
        turn_ids = await ctx.redis.zrevrange(sorted_key, 0, fetch_count - 1)
        if not turn_ids:
            return []
        
        turn_ids.reverse()
        turn_data = await ctx.redis.hmget(conv_key, *turn_ids)
        
        user_turns = []
        assistant_turns = []
        
        for turn_id, data in zip(turn_ids, turn_data):
            if not data:
                continue
            
            parsed = json.loads(data)
            ts = datetime.fromisoformat(parsed['timestamp'])
            date_str = ts.strftime("%Y-%m-%d")
            
            role_label = "User" if parsed["role"] == "user" else "STELLA"
            turn = {
                "turn_id": turn_id,
                "role": parsed["role"],
                "role_label": role_label,
                "content": parsed["content"],
                "formatted": f"[{date_str}] [{role_label}]: {parsed['content']}",
                "raw": parsed["content"],
                "timestamp": parsed["timestamp"]
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
            dirty_key = f"dirty_entities:{ctx.user_name}"
            raw_ids = await ctx.redis.spop(dirty_key, self.VOLUME_THRESHOLD) # 
            
            user_id = self.resolver.get_id(ctx.user_name)
            entity_ids = [int(id_str) for id_str in raw_ids if int(id_str) != user_id] if raw_ids else []
            
            updates = []
            
            if entity_ids:
                conversation = await self._get_conversation_context(ctx, self.MSG_WINDOW)
        
                if not conversation:
                    await ctx.redis.sadd(dirty_key, *[str(eid) for eid in entity_ids])
                    return JobResult(success=False, summary="No context found")
                
                updates = await self._run_updates(ctx, entity_ids, conversation)
                
                if updates:
                    await self._write_updates(updates)
            
            user_refined = await self._maybe_refine_user(ctx)
            
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
    
    async def _refine_user_profile(self, ctx: JobContext, user_id: int, profile: dict) -> bool:
        """Execute user profile refinement."""
        conversation = await self._get_conversation_context(ctx, int(self.MSG_WINDOW * 1.5))

        if not conversation:
            logger.warning("User profile refinement: no conversation context")
            return False
        
        conversation_text = "\n".join([turn["formatted"] for turn in conversation])

        if not conversation_text:
            logger.warning("User profile refinement: empty conversation text")
            return False
        
        existing_facts = profile.get("facts", [])
        
        system_reasoning = get_profile_extraction_prompt(ctx.user_name)
        user_content = json.dumps({
            "entities": [{
                "entity_name": ctx.user_name,
                "entity_type": "person",
                "existing_facts": existing_facts,
                "known_aliases": [ctx.user_name]
            }],
            "conversation": conversation_text
        }, indent=2)
        
        reasoning = await self.llm.call_reasoning(system_reasoning, user_content)

        if not reasoning:
            logger.warning("VEGAPUNK-06 returned None for user profile")
            return False
        
        response = parse_new_facts(reasoning)
        
        if not response or not response.profiles:
            logger.warning("No facts parsed for user profile")
            return False

        profile_map = {p.canonical_name.lower(): p for p in response.profiles}
        profile_out = profile_map.get(ctx.user_name.lower())
        
        if not profile_out:
            logger.warning(f"User {ctx.user_name} not found in parsed response")
            return False
        
        new_facts = profile_out.facts
    
        if not new_facts:
            logger.debug("No new facts extracted for user profile")
            return False
        
        merged_facts = process_extracted_facts(
            existing_facts=existing_facts,
            new_facts=new_facts,
            timestamp=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
        )
        
        if merged_facts == existing_facts:
            logger.debug("No fact changes after merge for user profile")
            return False
        
        logger.info(f"User profile: {len(existing_facts)} existing -> {len(merged_facts)} merged facts")
        
        resolution_text = f"{ctx.user_name}. " + " ".join(merged_facts)
        
        loop = asyncio.get_running_loop()
        embedding = await loop.run_in_executor(
            self.executor,
            partial(self.resolver.update_profile_embedding, user_id, resolution_text)
        )
        
        current_msg_id = await ctx.redis.get("global:next_msg_id")
        current_msg_id = int(current_msg_id) if current_msg_id else 0
        
        await loop.run_in_executor(
            self.executor,
            partial(
                self.store.update_entity_profile,
                entity_id=user_id,
                canonical_name=ctx.user_name,
                facts=merged_facts,
                embedding=embedding,
                last_msg_id=current_msg_id
            )
        )
        
        self.resolver.entity_profiles[user_id]["facts"] = merged_facts
        
        logger.info(f"Refined user profile for {ctx.user_name}")
        return True

    async def _process_single_batch(
        self, 
        ctx: JobContext,
        batch: List[Dict], 
        conversation_text: str, 
        current_msg_id: int
    ) -> List[Dict]:
        """Process one batch of entities. Returns list of updates."""
        async with self.batch_semaphore:
            llm_input = [{
                "entity_name": e["entity_name"],
                "entity_type": e["entity_type"],
                "existing_facts": e["existing_facts"],
                "known_aliases": e["known_aliases"]
            } for e in batch]
            
            system_reasoning = get_profile_extraction_prompt(ctx.user_name)
            user_content = json.dumps({
                "entities": llm_input,
                "conversation": conversation_text
            }, indent=2)
            
            reasoning = await self.llm.call_reasoning(system_reasoning, user_content)
            
            if not reasoning:
                logger.warning(f"VEGAPUNK-06 returned None for: {[e['entity_name'] for e in batch]}")
                return []
            
            response = parse_new_facts(reasoning)
            
            if not response or not response.profiles:
                logger.warning(f"No facts parsed for: {[e['entity_name'] for e in batch]}")
                return []
            
            updates = []
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
            profile_map = {p.canonical_name.lower(): p for p in response.profiles}
            for orig in batch:

                profile_out = profile_map.get(orig["entity_name"].lower())
                if not profile_out:
                    continue

                new_facts = profile_out.facts
                
                if not new_facts:
                    continue
            
                merged_facts = process_extracted_facts(
                    existing_facts=orig["existing_facts"],
                    new_facts=new_facts,
                    timestamp=timestamp
                )
                
                if merged_facts == orig["existing_facts"]:
                    continue
                
                resolution_text = f"{orig['entity_name']}. " + " ".join(merged_facts)
                
                loop = asyncio.get_running_loop()
                embedding = await loop.run_in_executor(
                    self.executor,
                    partial(self.resolver.update_profile_embedding, orig["ent_id"], resolution_text)
                )
                
                self.resolver.entity_profiles[orig["ent_id"]]["facts"] = merged_facts
                
                logger.info(f"Refined facts for {orig['entity_name']}: {len(orig['existing_facts'])} -> {len(merged_facts)}")
                
                updates.append({
                    "id": orig["ent_id"],
                    "canonical_name": orig["entity_name"],
                    "facts": merged_facts,
                    "embedding": embedding,
                    "last_msg_id": current_msg_id
                })
    
            return updates
    

    async def _run_updates(self, ctx: JobContext, entity_ids: List[int], conversation: List[Dict]):
        """Process entities in batches instead of individually."""
        
        current_msg_id = await ctx.redis.get("global:next_msg_id")
        current_msg_id = int(current_msg_id) if current_msg_id else 0
        
        entity_inputs = []
        for ent_id in entity_ids:
            profile = self.resolver.entity_profiles.get(ent_id)
            if not profile:
                continue
            
            existing_facts = profile.get("facts", [])
            
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
        
        tasks = [
            self._process_single_batch(ctx, batch, conversation_text, current_msg_id)
            for batch in batches
        ]
        
        results = await asyncio.gather(*tasks)
        return [update for batch_updates in results for update in batch_updates]

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
                    facts=update["facts"],
                    embedding=update["embedding"],
                    last_msg_id=update["last_msg_id"]
                )
            )
        
        logger.info(f"Wrote {len(updates)} profile updates to graph")