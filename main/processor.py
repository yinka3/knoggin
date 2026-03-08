
import json
import time
import asyncio
from functools import partial
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import redis.asyncio as aioredis
from loguru import logger
from typing import Dict, List, Set, Tuple, Optional
from db.store import MemGraphStore
from shared.services.llm import LLMService
from main.nlp_pipe import NLPPipeline
from main.entity_resolve import EntityResolver
from main.prompts import (
    get_connection_reasoning_prompt
)
from shared.config.topics_config import TopicConfig
from main.utils import ( 
    format_vp03_input, 
    parse_connection_response
)
from shared.models.schema.dtypes import (
    BatchResult,
    MessageConnections,
    ResolutionResult
)
from shared.utils.events import emit
from shared.infra.redis import RedisKeys


def _safe_json(obj):
    """Fallback serializer for numpy types in DLQ payloads."""
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


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
            get_next_ent_id,
            resolution_threshold: float = 0.85,
            connection_prompt: str = None):

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
        self.resolution_threshold = resolution_threshold
        self.connection_prompt = connection_prompt
    
        
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

            mentions = [(msg_id, text, typ, topic) for msg_id, text, typ, topic in mentions if text]
            
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
            
            res = await self._resolve_mentions(mentions, messages)

            await emit(self.session_id, "pipeline", "resolution_complete", {
                "new": len(res.new_ids),
                "existing": len(res.entity_ids) - len(res.new_ids),
                "aliases_added": len(res.alias_ids)
            })

            result.entity_ids = res.entity_ids
            result.new_entity_ids = res.new_ids
            result.alias_updated_ids = res.alias_ids
            result.alias_updates = res.alias_updates
            user_id = self.ent_resolver.get_id(self.user_name)
            if user_id and user_id not in res.entity_ids:
                res.entity_ids.append(user_id)
            
            connections = await self._extract_connections(res.entity_ids, res.entity_msg_map, messages, session_text)
            if connections is None:
                logger.error("Connection extraction failed")
                result.success = False
                result.error = "Connection extraction failed (VP-03)"
                await emit(self.session_id, "pipeline", "connections_failed", {
                    "entity_count": len(res.entity_ids)
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
            norm_topic = self.topic_config.normalize_topic(topic or "General")
            if norm_topic is None:
                logger.debug(f"Skipping mention '{text}' — topic '{topic}' could not be resolved")
                continue
            normalized_mentions.append((msg_id, text, typ, norm_topic))
        
        logger.debug(f"Extracted {len(normalized_mentions)} mentions from {len(mentions)} raw")
        return normalized_mentions

    async def _resolve_mentions(
        self,
        mentions: List[Tuple[int, str, str, str]],
        messages: List[Dict]
    ) -> ResolutionResult:
        """
        Deterministic entity resolution using 4 scoring signals.
        Replaces VP-02 LLM disambiguation.
        """
        loop = asyncio.get_running_loop()
        msg_text_map = {m["id"]: m["message"] for m in messages}

        entity_ids = []
        new_ids = set()
        alias_ids = set()
        entity_msg_map: Dict[int, List[int]] = {}
        created_in_batch: Dict[str, int] = {}
        alias_updates: Dict[int, List[str]] = {}
        batch_matched_ids: Set[int] = set()

        # Precompute embeddings for unique mention names
        unique_names = list({name for _, name, _, _ in mentions if name})
        embedding_map = {}
        if unique_names:
            embeddings_array = await loop.run_in_executor(
                self.executor,
                self.ent_resolver.embedding_service.encode,
                unique_names
            )
            embedding_map = {
                name: emb
                for name, emb in zip(unique_names, embeddings_array)
            }

        # First pass: collect base candidates for all mentions
        mention_candidates = []
        first_pass_results = {}
        for msg_id, name, typ, topic in mentions:
            if not name:
                mention_candidates.append(None)
                continue

            canonical_lower = name.strip().lower()

            if canonical_lower in first_pass_results:
                mention_candidates.append(first_pass_results[canonical_lower])
                continue

            precomputed = embedding_map.get(name)
            candidates = self.ent_resolver.get_candidate_ids(
                name, precomputed_embedding=precomputed
            )

            if candidates:
                top_id, top_score = candidates[0]
                if top_score >= self.resolution_threshold:
                    entry = ("candidate", top_id, top_score)
                else:
                    entry = ("new", None)
            else:
                entry = ("new", None)
                
            first_pass_results[canonical_lower] = entry
            mention_candidates.append(entry)

        # Second pass: batch-boost all candidates with graph signals
        pairs_to_boost = []
        boost_indices = []

        for i, entry in enumerate(mention_candidates):
            if entry and entry[0] == "candidate":
                _, top_id, top_score = entry
                msg_id = mentions[i][0]
                pairs_to_boost.append((top_id, top_score, msg_id))
                boost_indices.append(i)

        boosted_scores = {}
        if pairs_to_boost:
            boosted_scores = await self._boost_candidates(
                pairs_to_boost, msg_text_map, batch_matched_ids
            )

        for i, (msg_id, name, typ, topic) in enumerate(mentions):
            if not name:
                continue

            entry = mention_candidates[i]
            if entry is None:
                continue

            canonical_lower = name.strip().lower()
            ent_id = None

            # Batch dedup
            if entry[0] == "batch_dedup":
                ent_id = entry[1]
                entity_ids.append(ent_id)
                if ent_id not in entity_msg_map:
                    entity_msg_map[ent_id] = []
                entity_msg_map[ent_id].append(msg_id)
                continue

            # Candidate match
            if entry[0] == "candidate":
                top_id = entry[1]
                boosted = boosted_scores.get(top_id, entry[2])

                if boosted >= self.resolution_threshold:
                    ent_id = top_id
                    batch_matched_ids.add(ent_id)

                    existing_id, aliases_added, new_aliases = self.ent_resolver.validate_existing(
                        name.strip(), [name.strip()]
                    )
                    if existing_id and aliases_added:
                        self.ent_resolver.commit_new_aliases(existing_id, new_aliases)
                        alias_ids.add(existing_id)
                        alias_updates[existing_id] = new_aliases
                else:
                    verified = await loop.run_in_executor(
                        self.executor,
                        self.store.validate_existing_ids, [top_id]
                    )
                    if not verified:
                        logger.warning(f"Zombie candidate {top_id} for '{name}', evicting")
                        self.ent_resolver.remove_entities([top_id])

            # New entity — re-check batch dedup before creating
            if ent_id is None:
                if canonical_lower in created_in_batch:
                    ent_id = created_in_batch[canonical_lower]
                else:
                    try:
                        ent_id = await self._get_next_ent_id()
                        source_context = msg_text_map.get(msg_id)

                        await loop.run_in_executor(
                            self.executor,
                            self.ent_resolver.register_entity,
                            ent_id, name.strip(), [name.strip()],
                            typ, topic, self.session_id,
                            source_context
                        )
                        new_ids.add(ent_id)
                        created_in_batch[canonical_lower] = ent_id
                        batch_matched_ids.add(ent_id)
                    except Exception as e:
                        logger.error(f"Failed to register entity '{name}': {e}")
                        ent_id = None

            if ent_id is not None:
                entity_ids.append(ent_id)
                if ent_id not in entity_msg_map:
                    entity_msg_map[ent_id] = []
                entity_msg_map[ent_id].append(msg_id)

        return ResolutionResult(
            entity_ids=entity_ids,
            new_ids=new_ids,
            alias_ids=alias_ids,
            entity_msg_map=entity_msg_map,
            alias_updates=alias_updates
        )
    
    async def _boost_candidates(
        self,
        candidate_pairs: List[Tuple[int, float, int]],
        msg_text_map: Dict[int, str],
        batch_matched_ids: Set[int]
    ) -> Dict[int, float]:
        """
        Enhance base scores with graph signals.
        Signal 3: LLM fact relevance (batched, single call)
        Signal 4: Connection co-occurrence
        """
        results = {}
        loop = asyncio.get_running_loop()

        # --- Signal 3: Fact relevance via LLM ---
        llm_pairs = []
        pair_keys = []

        for candidate_id, base_score, msg_id in candidate_pairs:
            msg_text = msg_text_map.get(msg_id, "")
            if not msg_text:
                results[candidate_id] = base_score
                continue

            facts = await loop.run_in_executor(
                self.executor,
                self.store.get_facts_for_entity, candidate_id, True
            )

            if not facts:
                results[candidate_id] = base_score
                continue

            fact_strs = [f.content for f in facts[:5]]
            llm_pairs.append((msg_text, fact_strs))
            pair_keys.append((candidate_id, base_score))

        if llm_pairs:
            lines = []
            for i, (msg, facts) in enumerate(llm_pairs, 1):
                lines.append(f"{i}. Message: \"{msg}\" | Facts: {', '.join(facts)}")

            prompt = (
                "For each pair, does the message relate to the entity's facts? "
                "Answer YES or NO per line, nothing else.\n\n"
                + "\n".join(lines)
            )

            try:
                response = await self.llm.call_llm(
                    system="You are a relevance judge. Answer only YES or NO per line.",
                    user=prompt
                )

                if response:
                    import re
                    # Match patterns like "1. YES", "2: NO", "3 YES"
                    matches = re.findall(r"^\s*\d+[\.\:\-]?\s*(YES|NO)", response, re.IGNORECASE | re.MULTILINE)
                    
                    if matches:
                        for i, (candidate_id, base_score) in enumerate(pair_keys):
                            current = results.get(candidate_id, base_score)
                            if i < len(matches) and "YES" in matches[i].upper():
                                results[candidate_id] = max(current, base_score + 0.05)
                            else:
                                results[candidate_id] = max(current, base_score)
                    else:
                        # Fallback if the LLM didn't number them but just output YES/NO lines
                        lines = [line.strip().upper() for line in response.strip().split("\n") if line.strip()]
                        valid_lines = [line for line in lines if "YES" in line or "NO" in line]
                        
                        for i, (candidate_id, base_score) in enumerate(pair_keys):
                            current = results.get(candidate_id, base_score)
                            if i < len(valid_lines) and "YES" in valid_lines[i]:
                                results[candidate_id] = max(current, base_score + 0.05)
                            else:
                                results[candidate_id] = max(current, base_score)
                else:
                    for candidate_id, base_score in pair_keys:
                        results[candidate_id] = max(results.get(candidate_id, base_score), base_score)

            except Exception as e:
                logger.warning(f"Fact relevance LLM failed, using base scores: {e}")
                for candidate_id, base_score in pair_keys:
                    results[candidate_id] = max(results.get(candidate_id, base_score), base_score)

        # --- Signal 4: Connection co-occurrence ---
        processed_candidates = set()
        for candidate_id, base_score, msg_id in candidate_pairs:
            if candidate_id in processed_candidates:
                continue
            processed_candidates.add(candidate_id)
            
            score = results.get(candidate_id, base_score)

            if batch_matched_ids:
                neighbors = await loop.run_in_executor(
                    self.executor,
                    self.store.get_neighbor_ids, candidate_id
                )
                overlap = batch_matched_ids & neighbors
                if overlap:
                    score += min(len(overlap) * 0.03, 0.05)

            results[candidate_id] = score

        return results
    

    async def _extract_connections(
        self,
        entity_ids: List[int],
        entity_msg_map: Dict[int, List[int]],
        messages: List[Dict],
        session_text: str
    ) -> Optional[List[MessageConnections]]:
        """Extract connections between entities."""

        if not entity_ids:
            return []
        
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
                
        if self.connection_prompt:
            system_03 = self.connection_prompt.replace("{user_name}", self.user_name)
        else:
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
            await self.redis.rpush(dlq_key, json.dumps(entry, default=_safe_json))
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