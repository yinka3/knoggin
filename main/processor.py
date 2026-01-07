
import json
import time
import asyncio
from functools import partial
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
import redis.asyncio as redis
from loguru import logger
from typing import Dict, List, Set, Tuple, Optional
from db.memgraph import MemGraphStore
from main.service import LLMService
from main.nlp_pipe import NLPPipeline
from main.entity_resolve import EntityResolver
from main.prompts import (
    get_disambiguation_reasoning_prompt,
    get_disambiguation_formatter_prompt,
    get_connection_reasoning_prompt,
    get_connection_formatter_prompt,
)
from schema.dtypes import (
    DisambiguationResult,
    ConnectionExtractionResponse,
)


@dataclass
class BatchResult:
    """Result of processing a batch of messages."""
    entity_ids: List[int] = field(default_factory=list)
    new_entity_ids: Set[int] = field(default_factory=set)
    alias_updated_ids: Set[int] = field(default_factory=set)
    extraction_result: Optional[ConnectionExtractionResponse] = None
    emotions: List[str] = field(default_factory=list)
    success: bool = True
    error: Optional[str] = None

class BatchProcessor:

    def __init__(
            self,
            redis_client: redis.Redis,
            llm: LLMService,
            ent_resolver: EntityResolver,
            nlp_pipe: NLPPipeline,
            store: MemGraphStore,
            cpu_executor: ThreadPoolExecutor,
            user_name: str,
            active_topics: List[str],
            get_next_ent_id):
        
            self.redis = redis_client
            self.llm = llm
            self.ent_resolver = ent_resolver
            self.nlp = nlp_pipe
            self.store = store
            self.executor = cpu_executor
            self.user_name = user_name
            self.topics = active_topics
            self._get_next_ent_id = get_next_ent_id
        
    async def run(self, messages: List[Dict], session_text: str) -> BatchResult:
        """
        Process a batch of messages. Returns BatchResult with entity IDs and connections.
        Caller responsible for lock acquisition and publishing results.
        """
        result = BatchResult()
        
        if not messages:
            return result
        
        logger.debug(f"Processing batch of {len(messages)} messages: {[m['id'] for m in messages]}")
        
        try:
            mentions_dict, emotions = await self._extract_mentions(messages)
            result.emotions = emotions
            
            if not mentions_dict:
                logger.info("No mentions found in batch, skipping LLM calls")
                return result
            
            mentions = [(name, data["type"], data["topic"]) for name, data in mentions_dict.items()]
            
            known_entities = await self._build_known_entities(mentions)
            
            disambiguation = await self._disambiguate(mentions, messages, known_entities, session_text)
            if not disambiguation.entries:
                logger.error("Disambiguation failed - no results returned")
                result.success = False
                result.error = "VEGAPUNK-02 returned empty disambiguation"
                return result
            
            entity_ids, new_ids, alias_ids = await self._resolve(disambiguation)
            result.entity_ids = entity_ids
            result.new_entity_ids = new_ids
            result.alias_updated_ids = alias_ids
            
            user_id = self.ent_resolver.get_id(self.user_name)
            if user_id and user_id not in entity_ids:
                entity_ids.append(user_id)
            
            connections = await self._extract_connections(entity_ids, messages, session_text)
            if not connections:
                logger.error("Connection extraction failed")
            result.extraction_result = connections
            
            return result
            
        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            result.success = False
            result.error = str(e)
            return result
    
    async def _extract_mentions(self, messages: List[Dict]) -> Tuple[Dict[str, Dict], List[str]]:
        """Run NER and emotion detection across all messages."""
        # loop = asyncio.get_running_loop()
        
        combined_text = "\n".join([f"[MSG {m['id']}]: {m['message']}" for m in messages])
        mentions = await self.nlp.extract_mentions(self.user_name, self.topics, combined_text)
        
        unique_mentions: Dict[str, Dict] = {}
        for text, typ, topic in mentions:
            if text not in unique_mentions:
                unique_mentions[text] = {"type": typ, "topic": topic}
        
        # emotion_tasks = [
        #     loop.run_in_executor(self.executor, self.nlp.analyze_emotion, m["message"])
        #     for m in messages
        # ]
        # all_emotions = await asyncio.gather(*emotion_tasks)
        
        emotions = []
        # for emotion_list in all_emotions:
        #     if emotion_list:
        #         dominant = max(emotion_list, key=lambda x: x["score"])
        #         emotions.append(dominant["label"])
        
        return unique_mentions, emotions

    async def _build_known_entities(self, mentions: List[Tuple[str, str, str]]) -> List[Dict]:
        mention_names = [name for name, _, _ in mentions]
        entities = self.store.get_entities_by_names(mention_names)
        
        seen_ids = set()
        known = []
        for ent in entities:
            if ent["id"] not in seen_ids:
                seen_ids.add(ent["id"])
                known.append({
                    "canonical_name": ent["canonical_name"],
                    "type": ent["type"],
                    "aliases": ent["aliases"] or [],
                    "summary": ent.get("summary") or ""
                })
        return known

    async def _disambiguate(
        self,
        mentions: List[Tuple[str, str, str]],
        messages: List[Dict],
        known_entities: List[Dict],
        session_text: str
    ) -> DisambiguationResult:
        """Two-phase disambiguation: reasoning → structuring."""

        messages_text = "\n".join([f"{m['id']}: \"{m['message']}\"" for m in messages])
        mentions_fmt = [{"name": m[0], "type": m[1], "topic": m[2]} for m in mentions]
        
        system_02 = get_disambiguation_reasoning_prompt(self.user_name, messages_text)
        user_02 = json.dumps({
            "mentions": mentions_fmt,
            "known_entities": known_entities,
            "session_context": session_text
        }, indent=2)
        
        reasoning = await self.llm.call_reasoning(system_02, user_02)
        if not reasoning:
            logger.error("VEGAPUNK-02 failed")
            return DisambiguationResult(entries=[])
        
        if "<resolution>" not in reasoning:
            logger.warning("No <resolution> block in VEGAPUNK-02 output")
        
        system_03 = get_disambiguation_formatter_prompt()
        user_03 = json.dumps({
            "mentions": mentions_fmt,
            "reasoning_output": reasoning
        }, indent=2)
        
        result = await self.llm.call_structured(system_03, user_03, DisambiguationResult)
        return result or DisambiguationResult(entries=[])

    async def _resolve(self, disambiguation: DisambiguationResult) -> Tuple[List[int], Set[int], Set[int]]:
        """Validate disambiguation, update resolver. Returns (all_ids, new_ids, alias_updated_ids)."""
        
        loop = asyncio.get_running_loop()
        
        entity_ids = []
        new_ids = set()
        alias_ids = set()
        
        for entry in disambiguation.entries:
            if entry.verdict == "EXISTING":
                ent_id, aliases_added = self.ent_resolver.validate_existing(
                    entry.canonical_name, entry.mentions
                )
                if ent_id is None:
                    logger.warning(f"EXISTING '{entry.canonical_name}' not found, demoting to NEW")
                    canonical = entry.mentions[0]
                    ent_id = await self._get_next_ent_id()
                    await loop.run_in_executor(
                        self.executor,
                        partial(
                            self.ent_resolver.register_entity,
                            ent_id, canonical, entry.mentions, entry.entity_type, entry.topic
                        )
                    )
                    new_ids.add(ent_id)
                elif aliases_added:
                    alias_ids.add(ent_id)
            else:
                canonical = (
                    max(entry.mentions, key=lambda m: (len(m), m))
                    if entry.verdict == "NEW_GROUP"
                    else entry.mentions[0]
                )
                ent_id = await self._get_next_ent_id()
                await loop.run_in_executor(
                    self.executor,
                    partial(
                        self.ent_resolver.register_entity,
                        ent_id, canonical, entry.mentions, entry.entity_type, entry.topic
                    )
                )
                new_ids.add(ent_id)
            
            entity_ids.append(ent_id)
        
        return entity_ids, new_ids, alias_ids

    async def _extract_connections(
        self,
        entity_ids: List[int],
        messages: List[Dict],
        session_text: str
    ) -> Optional[ConnectionExtractionResponse]:
        """Extract connections between entities."""
        candidates = []
        for ent_id in entity_ids:
            profile = self.ent_resolver.entity_profiles.get(ent_id)
            if profile:
                candidates.append({
                    "name": profile["canonical_name"],
                    "type": profile["type"],
                    "mentions": self.ent_resolver.get_mentions_for_id(ent_id)
                })
        
        messages_text = "\n".join([f"{m['id']}: \"{m['message']}\"" for m in messages])
        
        system_04 = get_connection_reasoning_prompt(self.user_name, messages_text, session_text)
        user_04 = json.dumps({"candidate_entities": candidates, "messages": messages})
        
        reasoning = await self.llm.call_reasoning(system_04, user_04)
        if not reasoning:
            return None
        
        system_05 = get_connection_formatter_prompt()
        return await self.llm.call_structured(system_05, reasoning, ConnectionExtractionResponse)


    async def get_buffered_messages(self, buffer_key: str, batch_size: int) -> List[Dict]:
        """Read messages from Redis buffer."""
        raw = await self.redis.lrange(buffer_key, 0, batch_size - 1)
        return [json.loads(m) for m in raw] if raw else []

    async def move_to_dead_letter(self, messages: List[Dict], error: str):
        """Store failed batch in DLQ."""
        
        dlq_key = f"dlq:{self.user_name}"
        entry = {
            "timestamp": time.time(),
            "error": error,
            "batch_size": len(messages),
            "messages": messages
        }
        try:
            await self.redis.rpush(dlq_key, json.dumps(entry))
            logger.warning(f"Failed batch stored in DLQ: {dlq_key}")
        except Exception as e:
            logger.critical(f"DLQ storage failed: {e}. Data: {messages}")