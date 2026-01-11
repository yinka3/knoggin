import asyncio
from datetime import datetime, timezone
from functools import partial
import json
import re
from typing import Dict, List, Optional
from loguru import logger
from db.memgraph import MemGraphStore
from jobs.base import BaseJob, JobContext, JobNotifier, JobResult
from main.service import LLMService
from main.entity_resolve import EntityResolver
from main.prompts import get_profile_update_prompt
from schema.dtypes import BatchProfileResponse, ProfileUpdate


class ProfileRefinementJob(BaseJob):
    """
    Scans entities that have been 'touched' recently and updates their profiles
    using the sliding window of recent messages.
    
    Triggers:
    1. VOLUME: If >=20 entities are dirty (ensures we catch them in the 75-msg window).
    2. TIME: If user is idle for >5 minutes and we have ANY dirty entities.
    """
    
    MSG_WINDOW = 40
    VOLUME_THRESHOLD = 20
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
    
    async def _maybe_refine_user(self, ctx: JobContext, has_updates: bool = False) -> bool:
        """
        Check conditions and trigger user profile refinement if needed.
        Returns True if refinement ran.
        """
        ran_key = f"user_profile_ran:{ctx.user_name}"
        if await ctx.redis.get(ran_key):
            return False
        
        if not has_updates:
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
    
    async def _get_conversation_context(self, ctx: JobContext, num_turns: int) -> List[Dict]:
        """Fetch recent conversation with both user and STELLA turns."""
        sorted_key = f"recent_conversation:{ctx.user_name}"
        conv_key = f"conversation:{ctx.user_name}"
        
        turn_ids = await ctx.redis.zrevrange(sorted_key, 0, num_turns - 1)
        if not turn_ids:
            return []
        
        turn_ids.reverse()
        turn_data = await ctx.redis.hmget(conv_key, *turn_ids)
        
        results = []
        
        for data in turn_data:
            if data:
                parsed = json.loads(data)
                ts = datetime.fromisoformat(parsed['timestamp'])
                date_str = ts.strftime("%Y-%m-%d")
                
                
                role_label = "User" if parsed["role"] == "user" else "STELLA"
                results.append({
                    "role": parsed["role"],
                    "role_label": role_label,
                    "content": parsed["content"],
                    "formatted": f"[{date_str}] [{role_label}]: {parsed['content']}",
                    "raw": parsed["content"]
                })
        
        return results

    async def execute(self, ctx: JobContext) -> JobResult:
        warning = "⚠️ **Deepening Profiles.** I am reading through recent conversations to update entity details. Please wait a moment for the best results."

        async with JobNotifier(ctx.redis, warning):
            dirty_key = f"dirty_entities:{ctx.user_name}"
            dirty_count = await ctx.redis.scard(dirty_key)
            raw_ids = await ctx.redis.spop(dirty_key, 30) # 
            
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
            
            user_refined = await self._maybe_refine_user(ctx, has_updates=bool(updates))
            
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
            return False
        
        observations = [turn["formatted"] for turn in conversation]
    
        if not observations:
            return False
        
        context_text = "\n".join(observations)
        system_prompt = get_profile_update_prompt(ctx.user_name)
        user_content = json.dumps({
            "entity_name": ctx.user_name,
            "entity_type": "person",
            "existing_summary": profile.get("summary", ""),
            "new_observations": context_text,
            "known_aliases": [ctx.user_name]
        }, indent=2)
        
        raw_response = await self.llm.call_structured(system_prompt, user_content, ProfileUpdate)

        if not raw_response:
            return None

        new_summary = raw_response.summary
        
        if not new_summary or new_summary == profile.get("summary", ""):
            return None
        
        loop = asyncio.get_running_loop()
        embedding = await loop.run_in_executor(
            self.executor,
            partial(self.resolver.update_profile_summary, user_id, new_summary)
        )
        
        current_msg_id = await ctx.redis.get("global:next_msg_id")
        current_msg_id = int(current_msg_id) if current_msg_id else 0
        
        await loop.run_in_executor(
            self.executor,
            partial(
                self.store.update_entity_profile,
                entity_id=user_id,
                canonical_name=raw_response.canonical_name,
                summary=new_summary,
                embedding=embedding,
                last_msg_id=current_msg_id,
                topic=raw_response.topic
            )
        )
        
        logger.info(f"Refined user profile for {ctx.user_name}")
        return True


    async def _process_single_batch(
        self, 
        ctx: JobContext,
        batch: List[dict], 
        conversation_text: str, 
        current_msg_id: int
    ) -> List[dict]:
        """Process one batch of entities. Returns list of updates."""
        async with self.batch_semaphore:
            llm_input = [{
                "entity_name": e["entity_name"],
                "entity_type": e["entity_type"],
                "existing_summary": e["existing_summary"],
                "known_aliases": e["known_aliases"]
            } for e in batch]
            
            system_prompt = get_profile_update_prompt(ctx.user_name)
            user_content = json.dumps({
                "entities": llm_input,
                "conversation": conversation_text
            }, indent=2)
            
            response = await self.llm.call_structured(
                system_prompt, 
                user_content, 
                BatchProfileResponse
            )
            
            if not response or not response.profiles:
                logger.warning(f"Batch profile failed for entities: {[e['entity_name'] for e in batch]}")
                return []
            
            updates = []
            for j, profile_out in enumerate(response.profiles):
                if j >= len(batch):
                    break
                
                orig = batch[j]
                new_summary = profile_out.summary
                
                if not new_summary or new_summary == orig["existing_summary"]:
                    continue
                
                loop = asyncio.get_running_loop()
                embedding = await loop.run_in_executor(
                    self.executor,
                    partial(self.resolver.update_profile_summary, orig["ent_id"], new_summary)
                )
                
                logger.info(f"Refined profile for {orig['entity_name']} (ID: {orig['ent_id']})")
                
                updates.append({
                    "id": orig["ent_id"],
                    "canonical_name": orig["entity_name"],
                    "summary": new_summary,
                    "topic": profile_out.topic or orig["topic"],
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
            
            entity_inputs.append({
                "ent_id": ent_id,
                "entity_name": profile.get("canonical_name", "Unknown"),
                "entity_type": profile.get("type", "unknown"),
                "existing_summary": profile.get("summary", ""),
                "known_aliases": self.resolver.get_mentions_for_id(ent_id),
                "topic": profile.get("topic", "General")
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

    async def _write_updates(self, updates: List[dict]):
        """Write profile updates to Memgraph sequentially."""
        loop = asyncio.get_running_loop()
        
        for update in updates:
            await loop.run_in_executor(
                self.executor,
                partial(
                    self.store.update_entity_profile,
                    entity_id=update["id"],
                    canonical_name=update["canonical_name"],
                    summary=update["summary"],
                    embedding=update["embedding"],
                    last_msg_id=update["last_msg_id"],
                    topic=update["topic"]
                )
            )
        
        logger.info(f"Wrote {len(updates)} profile updates to graph")