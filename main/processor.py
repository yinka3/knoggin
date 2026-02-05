
import json
import time
import asyncio
from functools import partial
from concurrent.futures import ThreadPoolExecutor
import redis.asyncio as aioredis
from loguru import logger
from typing import Dict, List, Set, Tuple, Optional
from db.store import MemGraphStore
from shared.service import LLMService
from main.nlp_pipe import NLPPipeline
from main.entity_resolve import EntityResolver
from main.prompts import (
    get_disambiguation_reasoning_prompt,
    get_connection_reasoning_prompt
)
from shared.topics_config import TopicConfig
from main.utils import (
    dedupe_entries, 
    format_vp02_input, 
    format_vp03_input, 
    parse_connection_response, 
    parse_disambiguation
)
from schema.dtypes import (
    BatchResult,
    MessageConnections,
    ResolutionEntry,
)
from shared.events import emit
from shared.redisclient import RedisKeys


class BatchProcessor:

    def __init__(
            self,
            session_id: str,
            redis_client: aioredis.Redis,
            llm: LLMService,
            ent_resolver: EntityResolver,
            nlp_pipe: NLPPipeline,
            store: MemGraphStore,
            cpu_executor: ThreadPoolExecutor,
            user_name: str,
            topic_config: TopicConfig,
            get_next_ent_id):

        self.session_id = session_id
        self.redis = redis_client
        self.llm = llm
        self.ent_resolver = ent_resolver
        self.nlp = nlp_pipe
        self.store = store
        self.executor = cpu_executor
        self.user_name = user_name
        self.topic_config = topic_config
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

        await emit(self.session_id, "pipeline", "batch_start", {
            "size": len(messages),
            "msg_ids": [m["id"] for m in messages]
        })
        
        try:
            mentions = await self._extract_mentions(messages, self.session_id)
            await emit(self.session_id, "pipeline", "mentions_extracted", {
                "count": len(mentions),
                "mentions": [(msg_id, text, typ) for msg_id, text, typ, _ in mentions]
            }, verbose_only=True)

            valid_mentions = []
            for msg_id, text, typ, topic in mentions:
                if not text: 
                    continue # Skip empty names
                
                safe_topic = topic if topic else "General"
                norm_topic = self.topic_config.normalize_topic(safe_topic)
                valid_mentions.append((msg_id, text, typ, norm_topic))
            
            mentions = valid_mentions
            
            msg_texts = [m['message'] for m in messages]
    
            loop = asyncio.get_running_loop()
            embeddings = await loop.run_in_executor(
                self.executor, 
                self.ent_resolver.compute_batch_embeddings, 
                msg_texts
            )

            for i, msg in enumerate(messages):
                msg['embedding'] = embeddings[i]
                result.message_embeddings[msg['id']] = embeddings[i]

            if not mentions:
                logger.info("No mentions found in batch, skipping LLM calls")
                return result
            
            known_entities = await self._build_known_entities(mentions)
            
            disambiguation = await self._disambiguate(mentions, messages, known_entities, session_text)
            if disambiguation is None:
                logger.error("Disambiguation failed - no results returned")
                result.success = False
                result.error = "VEGAPUNK-02 returned empty disambiguation"
                await emit(self.session_id, "pipeline", "disambiguation_failed", {
                    "mention_count": len(mentions),
                    "known_entity_count": len(known_entities)
                })
                return result
            
            await emit(self.session_id, "pipeline", "disambiguation_complete", {
                "new": len([e for e in disambiguation if e.verdict != "EXISTING"]),
                "existing": len([e for e in disambiguation if e.verdict == "EXISTING"])
            })
            
            entity_ids, new_ids, alias_ids, entity_msg_map = await self._resolve(disambiguation, messages)
            result.entity_ids = entity_ids
            result.new_entity_ids = new_ids
            result.alias_updated_ids = alias_ids
            
            user_id = self.ent_resolver.get_id(self.user_name)
            if user_id and user_id not in entity_ids:
                entity_ids.append(user_id)
            
            connections = await self._extract_connections(entity_ids, entity_msg_map, messages, session_text)
            if connections is None:
                logger.error("Connection extraction failed")
                result.success = False
                result.error = "Connection extraction failed (VP-04)"
                await emit(self.session_id, "pipeline", "connections_failed", {
                    "entity_count": len(entity_ids)
                })
                return result
            
            total_pairs = sum(len(mc.entity_pairs) for mc in connections)
            await emit(self.session_id, "pipeline", "connections_extracted", {
                "messages_with_connections": len(connections),
                "total_pairs": total_pairs,
                "pairs": [
                    {"a": pair.entity_a, "b": pair.entity_b, "confidence": pair.confidence}
                    for mc in connections
                    for pair in mc.entity_pairs
                ]
            }, verbose_only=True)
            
            result.extraction_result = connections

            await emit(self.session_id, "pipeline", "batch_complete", {
                "entities": len(result.entity_ids),
                "new_entities": len(result.new_entity_ids),
                "success": result.success
            })
            
            return result
            
        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            result.success = False
            result.error = str(e)
            return result

    
    async def _extract_mentions(self, messages: List[Dict], session_id: str) -> List[Tuple[int, str, str, str]]:
        """Run NER across all messages."""
        
        mentions = await self.nlp.extract_mentions(self.user_name, messages, session_id)

        normalized_mentions = []
        for msg_id, text, typ, topic in mentions:
            norm_topic = self.topic_config.normalize_topic(topic)
            normalized_mentions.append((msg_id, text, typ, norm_topic))
        
        logger.debug(f"Extracted {normalized_mentions} mentions")
        return normalized_mentions

    async def _build_known_entities(
        self, 
        mentions: List[Tuple[int, str, str, str]],
        max_amount: int = 50
    ) -> List[Dict]:
        
        unique_names = list({name for _, name, _, _ in mentions if name})
        loop = asyncio.get_running_loop()
        
        if unique_names:
            embeddings_array = await loop.run_in_executor(
                self.executor,
                self.ent_resolver.embedding_service.encode,
                unique_names
            )
            embedding_map = {
                name: emb.tolist() 
                for name, emb in zip(unique_names, embeddings_array)
            }
        else:
            embedding_map = {}
                
        candidate_scores: Dict[int, float] = {}

        for msg_id, name, _, _ in mentions:
            if not name:
                continue
            candidates = self.ent_resolver.get_candidate_ids(
                name, 
                precomputed_embedding=embedding_map.get(name)
            )
            
            for rank, (eid, _) in enumerate(candidates):
                score = 1.0 / (rank + 1) # higher ranks gets boosted score
                if eid not in candidate_scores or score > candidate_scores[eid]:
                    candidate_scores[eid] = score
                logger.debug(f"Mention '{name}' (MSG {msg_id}) candidate entity ID {eid} = score {score:.3f}")
        

        sorted_ids = sorted(candidate_scores.keys(), key=lambda x: candidate_scores[x], reverse=True)

        if not sorted_ids:
            await emit(self.session_id, "pipeline", "no_known_candidates", {
                "mention_count": len(mentions)
            }, verbose_only=True)
            return []
        
        loop = asyncio.get_running_loop()
        facts_map = await loop.run_in_executor(
            self.executor,
            partial(self.store.get_facts_for_entities, sorted_ids, active_only=True)
        )
        
        known = []
        for eid in sorted_ids[:max_amount]:
            profile = self.ent_resolver.entity_profiles.get(eid)
            if profile:
                connections = self.store.get_neighbor_entities(eid, limit=5)
                entity_facts = [f.content for f in facts_map.get(eid, [])]

                known.append({
                    "canonical_name": profile["canonical_name"],
                    "facts": entity_facts,
                    "connected_to": connections
                })
                logger.debug(f"Known entity added: {known[-1]["canonical_name"]} with {len(connections)} connections.")
        
        return known
    
    async def _try_fast_path(
        self, 
        mentions: List[Tuple[int, str, str, str]], 
        known_entities: List[Dict]   
    ) -> Optional[List[ResolutionEntry]]:
        
        if not known_entities:
            return [
                ResolutionEntry(
                    verdict="NEW_SINGLE",
                    canonical_name=name,
                    mentions=[name],
                    entity_type=typ,
                    topic=topic,
                    msg_ids=[msg_id]
                )
                for msg_id, name, typ, topic in mentions
            ]
        
        entries: List[ResolutionEntry] = []
        existing_ids: List[int] = []

        for msg_id, name, typ, topic in mentions:
            exact_id = self.ent_resolver.get_id(name)
            
            if exact_id is not None:
                profile = self.ent_resolver.entity_profiles.get(exact_id)
                entries.append(ResolutionEntry(
                    verdict="EXISTING",
                    canonical_name=profile["canonical_name"],
                    mentions=[name],
                    entity_type=typ,
                    topic=topic,
                    msg_ids=[msg_id]
                ))
                existing_ids.append(exact_id)
                continue
            
            fuzzy_candidates = self.ent_resolver.get_candidate_ids(name)
            
            if len(fuzzy_candidates) == 0:
                entries.append(ResolutionEntry(
                    verdict="NEW_SINGLE",
                    canonical_name=name,
                    mentions=[name],
                    entity_type=typ,
                    topic=topic,
                    msg_ids=[msg_id]
                ))
            else:
                return None
        
        if existing_ids:
            loop = asyncio.get_running_loop()
            valid_ids = await loop.run_in_executor(
                self.executor,
                self.store.validate_existing_ids,
                existing_ids
            )
            
            zombies = set(existing_ids) - valid_ids
            if zombies:
                logger.warning(f"Fast path found {len(zombies)} zombie entities, falling back to LLM")
                self.ent_resolver.remove_entities(list(zombies))
                return None
        
        return entries

    async def _disambiguate(
        self,
        mentions: List[Tuple[int, str, str, str]],
        messages: List[Dict],
        known_entities: List[Dict],
        session_text: str
    ) -> Optional[List[ResolutionEntry]]:

        fast_result = await self._try_fast_path(mentions, known_entities)
        if fast_result is not None:
            await emit(self.session_id, "pipeline", "fast_path_used", {
                "mention_count": len(mentions),
                "all_new": len(known_entities) == 0,
                "all_existing": all(e.verdict == "EXISTING" for e in fast_result)
            })
            return fast_result

        system_02 = get_disambiguation_reasoning_prompt(self.user_name)
        user_02 = format_vp02_input(
            known_entities,
            mentions,
            [{"id": m["id"], "text": m["message"]} for m in messages],
            session_text
        )

        await emit(self.session_id, "pipeline", "llm_call", {
            "stage": "disambiguation",
            "prompt": user_02
        }, verbose_only=True)
        
        reasoning = await self.llm.call_llm(system_02, user_02)
        if not reasoning:
            logger.error("VEGAPUNK-02 failed")
            return None
        
        if "<resolution>" not in reasoning:
            logger.warning("No <resolution> block in VEGAPUNK-02 output")
        
        result = parse_disambiguation(reasoning, mentions)
        return result if result else None


    async def _resolve(
        self,
        disambiguation: List[ResolutionEntry],
        messages: List[Dict]
    ) -> Tuple[List[int], Set[int], Set[int], Dict[int, List[int]], Dict[int, List[str]]]:
        
        loop = asyncio.get_running_loop()
        
        msg_text_map = {m["id"]: m["message"] for m in messages}
        entity_ids = []
        new_ids = set()
        alias_ids = set()
        entity_msg_map: Dict[int, List[int]] = {}
        created_in_batch: Dict[str, int] = {}
        alias_updates: Dict[int, List[str]] = {}

        entries = dedupe_entries(disambiguation)
        for entry in entries:
            if not entry.canonical_name:
                continue
                
            canonical_clean = entry.canonical_name.strip()
            canonical_lower = canonical_clean.lower()

            if canonical_lower in created_in_batch:
                ent_id = created_in_batch[canonical_lower]
                entity_ids.append(ent_id)
                if entry.msg_ids:
                    if ent_id not in entity_msg_map:
                        entity_msg_map[ent_id] = []
                    entity_msg_map[ent_id].extend(entry.msg_ids)
                logger.debug(f"Reusing entity ID {ent_id} for '{canonical_clean}' created earlier in batch")
                continue

            if entry.verdict == "EXISTING":
                try:
                    ent_id, aliases_added, new_aliases = self.ent_resolver.validate_existing(
                        entry.canonical_name, entry.mentions
                    )
                    
                    if ent_id is not None:
                        valid_ids = await loop.run_in_executor(
                            self.executor,
                            self.store.validate_existing_ids, 
                            [ent_id]
                        )
                        
                        if valid_ids is None:
                            logger.warning(f"Could not validate entity {ent_id}, assuming valid")
                        elif not valid_ids:
                            logger.warning(f"Zombie Entity Detected: Resolver has ID {ent_id} for '{entry.canonical_name}', but DB does not. Treating as NEW.")
                            self.ent_resolver.remove_entities([ent_id])
                            ent_id = None

                    if ent_id is None:
                        logger.info(f"Creating replacement entity for '{entry.canonical_name}'")
                        canonical = entry.canonical_name if entry.canonical_name else entry.mentions[0]
                        
                        ent_id = await self._get_next_ent_id()
                        await loop.run_in_executor(
                            self.executor,
                            partial(
                                self.ent_resolver.register_entity,
                                ent_id, canonical, entry.mentions, entry.entity_type, entry.topic
                            )
                        )
                        new_ids.add(ent_id)
                        created_in_batch[canonical_lower] = ent_id
                    elif aliases_added:
                        self.ent_resolver.commit_new_aliases(ent_id, new_aliases)
                        alias_ids.add(ent_id)
                        alias_updates[ent_id] = new_aliases
                except Exception as e:
                    logger.error(f"Failed to register entity '{canonical_clean}': {e}")
                    ent_id = None
            
            else:
                try:
                    ent_id = await self._get_next_ent_id()
                    source_context = None
                    if entry.msg_ids:
                        first_msg_id = entry.msg_ids[0]
                        source_context = msg_text_map.get(first_msg_id)

                    await loop.run_in_executor(
                        self.executor,
                        partial(
                            self.ent_resolver.register_entity,
                            ent_id, 
                            canonical_clean, 
                            entry.mentions, 
                            entry.entity_type, 
                            entry.topic, 
                            self.session_id,
                            source_context
                        )
                    )
                    new_ids.add(ent_id)
                    created_in_batch[canonical_lower] = ent_id
                except Exception as e:
                    logger.error(f"Failed to register entity '{canonical_clean}': {e}")
                    ent_id = None
            
            if ent_id is not None:
                entity_ids.append(ent_id)
                if entry.msg_ids:
                    entity_msg_map[ent_id] = entry.msg_ids
        
        return entity_ids, new_ids, alias_ids, entity_msg_map, alias_updates

    async def _extract_connections(
        self,
        entity_ids: List[int],
        entity_msg_map: Dict[int, List[int]],
        messages: List[Dict],
        session_text: str
    ) -> Optional[List[MessageConnections]]:
        """Extract connections between entities."""

        if not entity_ids:
            return None
        
        candidates = []
        for ent_id in entity_ids:
            profile = self.ent_resolver.entity_profiles.get(ent_id)
            if profile:
                candidates.append({
                    "canonical_name": profile["canonical_name"],
                    "type": profile["type"],
                    "mentions": self.ent_resolver.get_mentions_for_id(ent_id),
                    "source_msgs": entity_msg_map.get(ent_id, [])
                })
                
        system_03 = get_connection_reasoning_prompt(self.user_name)
        user_03 = format_vp03_input(
            candidates, 
            [{"id": m["id"], "text": m["message"]} for m in messages],
            session_text
        )
        
        await emit(self.session_id, "pipeline", "llm_call", {
            "stage": "connections",
            "prompt": user_03
        }, verbose_only=True)
        reasoning = await self.llm.call_llm(system_03, user_03)
        if not reasoning:
            return None
        
        return parse_connection_response(reasoning)


    async def move_to_dead_letter(
        self, 
        messages: List[Dict], 
        error: str, 
        stage: str = "processing",
        session_text: str = None,
        batch_result: BatchResult = None,
        attempt: int = 1
    ) -> bool:
        """Store failed batch in DLQ with stage info for smart retry."""
        
        dlq_key = RedisKeys.dlq(self.user_name, self.session_id)
        entry = {
            "timestamp": time.time(),
            "error": error,
            "attempt": attempt,
            "stage": stage,
            "batch_size": len(messages),
            "messages": messages
        }
        
        if stage == "processing" and session_text is not None:
            entry["session_text"] = session_text
        elif stage == "graph_write" and batch_result is not None:
            entry["batch_result"] = batch_result.to_dict()
        
        try:
            await self.redis.rpush(dlq_key, json.dumps(entry))
            logger.warning(f"DLQ [{stage}]: {len(messages)} messages stored")

            await emit(self.session_id, "pipeline", "dlq_enqueued", {
                "msg_ids": [m["id"] for m in messages],
                "error": error,
                "stage": stage,
                "attempt": attempt
            })
            return True
        except Exception as e:
            logger.critical(f"DLQ storage failed: {e}")
            return False