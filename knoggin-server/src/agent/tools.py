import asyncio
from datetime import datetime, timezone
import json
import re
import httpx
from functools import partial
from typing import List, Dict, Optional

from loguru import logger
import redis.asyncio as aioredis
from jobs.utils import cosine_similarity
from core.entity_resolver import EntityResolver
from db.store import MemGraphStore
from common.rag.file_rag import FileRAGService
from common.config.topics_config import TopicConfig
from common.infra.redis import RedisKeys

TOOL_DISPATCH = {
    "search_messages": ("search_messages", ["query", "limit"]),
    "search_entity": ("search_entity", ["query", "limit"]),
    "get_connections": ("get_connections", ["entity_name"]),
    "get_recent_activity": ("get_recent_activity", ["entity_name", "hours"]),
    "fact_check": ("fact_check", ["entity_name", "query"]),
    "find_path": ("find_path", ["entity_a", "entity_b"]),
    "get_hierarchy": ("get_hierarchy", ["entity_name", "direction"]),
    "save_memory": ("save_memory", ["content", "topic"]),
    "forget_memory": ("forget_memory", ["memory_id"]),
    "search_files": ("search_files", ["query", "file_name", "limit"]),
    "web_search": ("web_search", ["query", "limit", "freshness"]),
    "news_search": ("news_search", ["query", "limit", "freshness"]),
    "request_clarification": None,  # handled specially
    "save_insight": ("save_insight", ["content"]),
    "spawn_specialist": ("spawn_specialist", ["name", "persona", "initial_rules", "initial_preferences", "initial_icks"]),
}

class Tools:
    
    def __init__(self, user_name: str, store: MemGraphStore, ent_resolver: EntityResolver, 
                redis_client: aioredis.Redis, session_id: str, topic_config: TopicConfig = None, 
                search_config: dict = None, file_rag: FileRAGService =None, mcp_manager=None, memory=None):
        self.session_id = session_id
        self.store = store
        self.resolver = ent_resolver
        self.user_name = user_name
        self.redis = redis_client
        self.embedding_service = ent_resolver.embedding_service
        self.topic_config = topic_config
        self.file_rag = file_rag
        self.active_topics = topic_config.active_topics if topic_config else None
        self.search_cfg = search_config or {}
        self.mcp_manager = mcp_manager
        self.memory = memory

        self._http_client = httpx.AsyncClient(timeout=10.0)
    
    async def _resolve_entity_name(self, entity: str) -> Optional[str]:
        """Resolve user input to canonical entity name via exact or fuzzy match."""
        return await self.resolver.resolve_entity_name(entity)
    
    @staticmethod
    def _is_message_id(msg_id) -> bool:
        """Check if numeric ID belongs to message collection or turn collection."""
        if isinstance(msg_id, str):
            return msg_id.startswith("msg_")
        return msg_id < 1_000_000_000

    @staticmethod
    def _format_message_id(msg_id) -> str:
        """Format an ID as a string for message/turn reference."""
        if isinstance(msg_id, str):
            return msg_id
        return f"msg_{msg_id}" if msg_id < 1_000_000_000 else f"turn_{msg_id - 1_000_000_000}"

    async def _hydrate_evidence(self, evidence_ids: list[str], timeout: float = 5.0) -> list[dict]:
        """
        Fetch full message payloads from Redis for a list of string evidence IDs.
        Falls back to PostgreSQL lookup if Redis cache misses.
        """
        if not evidence_ids:
            return []
        
        content_key = RedisKeys.message_content(self.user_name, self.session_id)
        conv_key = RedisKeys.conversation(self.user_name, self.session_id)
        
        pipe = self.redis.pipeline()
        for msg_id in evidence_ids:
            if msg_id.startswith("msg_"):
                pipe.hget(content_key, msg_id)
            else:
                pipe.hget(conv_key, msg_id)
                
        try:
            raw_results = await asyncio.wait_for(
                pipe.execute(),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            logger.warning(f"Redis hydrate timed out for {len(evidence_ids)} evidence IDs")
            return []
        
        results = []
        missing_ids_numerical = []
        
        for msg_id, raw in zip(evidence_ids, raw_results):
            if raw:
                try:
                    data = json.loads(raw)
                    results.append({
                        "id": msg_id,
                        "message": data.get("message", data.get("content", "")),
                        "timestamp": data.get("timestamp", "")
                    })
                except json.JSONDecodeError:
                    logger.warning(f"Malformed evidence data for {msg_id}")
            else:
                if msg_id.startswith("msg_"):
                    try:
                        missing_ids_numerical.append(int(msg_id.split("_")[1]))
                    except (ValueError, IndexError):
                        pass
                elif msg_id.startswith("turn_"):
                    try:
                        missing_ids_numerical.append(int(msg_id.split("_")[1]) + 1_000_000_000)
                    except (ValueError, IndexError):
                        pass

        if missing_ids_numerical:
            fallback_msgs = await self.store.get_messages_by_ids(missing_ids_numerical)
            for m in fallback_msgs:
                ts_iso = ""
                if "timestamp" in m and isinstance(m["timestamp"], (int, float)):
                    ts_iso = datetime.fromtimestamp(m["timestamp"]/1000.0, timezone.utc).isoformat()
                
                if m['id'] >= 1_000_000_000:
                    str_id = f"turn_{m['id'] - 1_000_000_000}"
                else:
                    str_id = f"msg_{m['id']}"

                results.append({
                    "id": str_id,
                    "message": m["content"],
                    "timestamp": ts_iso
                })
                
        return results
        

    async def _get_surrounding_context(self, msg_id: str, forward: int = 3, target_total: int = 10) -> List[Dict]:
        """
        Given a specific message or turn ID, retrieve the surrounding conversational 
        context (previous and succeeding turns) to provide continuity in search results.
        """
        sorted_key = RedisKeys.recent_conversation(self.user_name, self.session_id)
        conv_key = RedisKeys.conversation(self.user_name, self.session_id)
        lookup_key = RedisKeys.msg_to_turn_lookup(self.user_name, self.session_id)
        
        target_turn_id = msg_id
        is_msg_id = msg_id.startswith("msg_")
        if is_msg_id:
            target_turn_id = await self.redis.hget(lookup_key, msg_id)
            
        rank = None
        if target_turn_id:
            rank = await self.redis.zrank(sorted_key, target_turn_id)
            
        if rank is None:
            if is_msg_id:
                try:
                    numerical_msg_id = int(msg_id.split("_")[1])
                    fallback_msgs = await self.store.get_surrounding_messages(numerical_msg_id, forward, target_total)
                    
                    formatted_fallback = []
                    for m in fallback_msgs:
                        ts_iso = ""
                        if "timestamp" in m and isinstance(m["timestamp"], (int, float)):
                            ts_iso = datetime.fromtimestamp(m["timestamp"]/1000.0, timezone.utc).isoformat()
                            
                        formatted_fallback.append({
                            "role": m["role"],
                            "timestamp": ts_iso,
                            "content": m["content"],
                            "id": f"msg_{m['id']}",
                            "is_hit": m["id"] == numerical_msg_id
                        })
                    return formatted_fallback
                except (ValueError, IndexError):
                    pass
            return []

            
        back_fetch = target_total * 2 
        start = max(0, rank - back_fetch)
        end = rank + forward + 1
            
        turn_ids = await self.redis.zrange(sorted_key, start, end)
        if not turn_ids:
            return []  
            
        pipe = self.redis.pipeline()
        for _id in turn_ids:
            pipe.hget(conv_key, _id)
        results = await pipe.execute()

        raw_map = {tid: res for tid, res in zip(turn_ids, results) if res}
        
        if target_turn_id not in turn_ids: return []
        target_index = turn_ids.index(target_turn_id)

        pre_context = []
        post_context = []

        current_back_count = 0
        max_back = target_total - forward

        for i in range(target_index - 1, -1, -1):
            tid = turn_ids[i]
            if tid not in raw_map: continue
            
            try:
                data = json.loads(raw_map[tid])
            except json.JSONDecodeError:
                continue
                
            role = data.get("role", "unknown")
            content = data.get("content", "") or ""
            
            pre_context.append({
                "role": role,
                "timestamp": data.get("timestamp", ""),
                "content": content,
                "id": tid
            })
            
            current_back_count += 1
            if current_back_count >= max_back:
                break

        pre_context.reverse()

        try:
            tgt_data = json.loads(raw_map[target_turn_id])
            target_msg = {
                "role": tgt_data.get("role", "unknown"),
                "timestamp": tgt_data.get("timestamp", ""),
                "content": tgt_data.get("content", ""),
                "id": target_turn_id,
                "is_hit": True
            }
        except json.JSONDecodeError:
            target_msg = {
                "role": "unknown",
                "timestamp": "",
                "content": "",
                "id": target_turn_id,
                "is_hit": True
            }

        for i in range(target_index + 1, min(len(turn_ids), target_index + forward + 1)):
            tid = turn_ids[i]
            if tid not in raw_map: continue
            
            try:
                data = json.loads(raw_map[tid])
            except json.JSONDecodeError:
                continue
            post_context.append({
                "role": data.get("role", "unknown"),
                "timestamp": data.get("timestamp", ""),
                "content": data.get("content", ""),
                "id": tid
            })

        return pre_context + [target_msg] + post_context


    async def _search_messages(self, query: str, k: int) -> list[tuple[str, float]]:
        """
        Asynchronous internal method executing hybrid vector + FTS search over messages, 
        followed by an optional cross-encoder reranking step if candidates exceed 1.
        """
        vector_limit = self.search_cfg.get("vector_limit", 50)
        fts_limit = self.search_cfg.get("fts_limit", 50)
        rerank_candidates = self.search_cfg.get("rerank_candidates", 45)

        results = {}
        query_embedding = await self.embedding_service.encode_single(query)
        
        sem_results = await self.store.search_messages_vector(query_embedding, vector_limit)

        for msg_id, score in sem_results:
            msg_key = self._format_message_id(msg_id)
            results[msg_key] = ("semantic", float(score))
        
        fts_results = await self.store.search_messages_fts(query, fts_limit)
        
        max_fts = max([s for _, s in fts_results], default=1.0) or 1.0

        for msg_id, raw_score in fts_results:
            msg_key = self._format_message_id(msg_id)
            
            norm_score = raw_score / max_fts if max_fts > 0 else 0
            
            logger.debug(f"FTS result: {msg_key} score={norm_score:.3f}")

            if msg_key in results:
                _, sem_score = results[msg_key]
                results[msg_key] = ("both", sem_score + norm_score)
            else:
                results[msg_key] = ("keyword", norm_score)
        
        if not results:
            return []
        
        try:
            if len(results) > 1:
                candidate_keys = list(results.keys())[:rerank_candidates]

                hydrated = await self._hydrate_evidence(candidate_keys)
                text_map = {h["id"]: h.get("message", "") for h in hydrated}
                texts = [text_map.get(k, "") for k in candidate_keys]

                scores = await self.embedding_service.rerank(query, texts)
                reranked = sorted(zip(candidate_keys, scores), key=lambda x: x[1], reverse=True)
                return [(msg_key, float(score)) for msg_key, score in reranked[:k]]
        except Exception as e:
            logger.warning(f"Rerank failed, falling back to raw scores: {e}")
            
        # Fallback: single result
        sorted_results = sorted(results.items(), key=lambda x: x[1][1], reverse=True)[:k]
        return [(key, score) for key, (_, score) in sorted_results]
    
    async def search_messages(self, query: str, limit: int = None) -> List[Dict]:
        """
        Search the user's actual messages by keyword or phrase. 
        Use when you need their exact words, a direct quote, or when entity-based tools found nothing relevant. 
        This is raw recall, not summarized knowledge.

        Args:
            query: Keywords or phrase to search for
            limit: Max results (default 10)

        Returns: List of turns with id, role, message, timestamp, score, 
                and surrounding context (adjacent turns for continuity).
        """
        limit = limit or self.search_cfg.get("default_message_limit", 8)
        results = await self._search_messages(query, limit)
        
        if not results:
            return []
        
        msg_keys = [msg_key for msg_key, _ in results]
        scores = {msg_key: score for msg_key, score in results}
        
        lookup_key = RedisKeys.msg_to_turn_lookup(self.user_name, self.session_id)
        user_msg_keys = [k for k in msg_keys if k.startswith("msg_")]
        
        if user_msg_keys:
            turn_mappings = await self.redis.hmget(lookup_key, *user_msg_keys)
            msg_to_turn = dict(zip(user_msg_keys, turn_mappings))
        else:
            msg_to_turn = {}
        
        turn_keys = []
        for msg_key in msg_keys:
            if msg_key.startswith("msg_"):
                turn_keys.append(msg_to_turn.get(msg_key))
            else:
                turn_keys.append(msg_key)
        
        contexts = await asyncio.gather(*[
            self._get_surrounding_context(msg_key) for msg_key in msg_keys
        ])
        
        content_key = RedisKeys.message_content(self.user_name, self.session_id)
        conv_key = RedisKeys.conversation(self.user_name, self.session_id)
        
        assistant_msg_keys = [k for k in msg_keys if not k.startswith("msg_")]
        
        user_contents = {}
        if user_msg_keys:
            raw_contents = await self.redis.hmget(content_key, *user_msg_keys)
            user_contents = dict(zip(user_msg_keys, raw_contents))
        
        assistant_contents = {}
        if assistant_msg_keys:
            raw_contents = await self.redis.hmget(conv_key, *assistant_msg_keys)
            assistant_contents = dict(zip(assistant_msg_keys, raw_contents))
        
        seen_turns = set()
        output = []
        
        for msg_key, turn_key, context in zip(msg_keys, turn_keys, contexts):
            if not turn_key or turn_key in seen_turns:
                continue
            
            for msg in context:
                seen_turns.add(msg['id'])
            
            if msg_key.startswith("msg_"):
                raw = user_contents.get(msg_key)
                if raw:
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    output.append({
                        "id": msg_key,
                        "role": "user",
                        "message": data.get("message", ""),
                        "timestamp": data.get("timestamp", ""),
                        "score": scores[msg_key],
                        "context": context
                    })
            else:
                raw = assistant_contents.get(msg_key)
                if raw:
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    output.append({
                        "id": msg_key,
                        "role": data.get("role", "assistant"),
                        "message": data.get("content", ""),
                        "timestamp": data.get("timestamp", ""),
                        "score": scores[msg_key],
                        "context": context
                    })
        
        return output
        

    async def search_entity(self, query: str, limit: int = None) -> List[Dict]:
        """
        Find a person, place, or thing by name. 
        Returns their full profile (type, summary, aliases, topic) and their 5 strongest connections.
        Connections only include canonical name and aliases — use this tool again on a connection's name if you need their full profile.
        
        Args:
            query: Name or partial name to search
            limit: Max results to return (default 5)
        
        Returns: 
            List of matching entities with id, name, summary snippet, type, and top connections.
        """
        limit = limit or self.search_cfg.get("default_entity_limit", 5)
        results = await self.store.search_entity(query, self.active_topics, limit)
    
        if not results:
            return []
        
        for entity in results:
            for conn in entity.get("top_connections", []):
                evidence_ids = conn.pop("evidence_ids", [])
                string_ids = [self._format_message_id(x) for x in evidence_ids]
                conn["evidence"] = await self._hydrate_evidence(string_ids)
        
        return results

    async def get_connections(self, entity_name: str) -> List[Dict]:
        """
        Get the full relationship network for an entity.
        Returns all connections (up to 50) with evidence — the actual messages that established each connection. 
        Use when you need comprehensive relationship details beyond the top 5 from search_entity.
        
        Args:
            entity_name: The entity to find connections for.
        
        Returns: 
            List of connections with target entity, connection strength, and hydrated evidence messages.
        """
        canonical = await self._resolve_entity_name(entity_name)
        if not canonical:
            return [{"error": f"Entity not found: '{entity_name}'"}]
        
        results = await self.store.get_related_entities([canonical], active_topics=self.active_topics)

        if results:
            for r in results:
                evidence_ids = r.pop("evidence_ids", [])
                string_ids = [self._format_message_id(x) for x in evidence_ids]
                r["evidence"] = await self._hydrate_evidence(string_ids)
            return results
        
        # Try looking without topic filtering to see if it's "hidden"
        hidden_results = await self.store.get_related_entities([canonical], active_topics=None)
        
        if hidden_results:
            return [{
                "hidden": True,
                "count": len(hidden_results),
                "message": f"{len(hidden_results)} connection(s) exist through inactive topics"
            }]
        
        return []

    async def get_recent_activity(self, entity_name: str, hours: int = 24) -> List[Dict]:
        """
        Get recent interactions involving an entity within a time window. 
        Use for 'what happened with X lately' or 'any updates on X this week'. 
        Default is 24 hours; use 168 for a week.
        
        Args:
            entity_name: Entity to check activity for
            hours: How far back to look (default 24, use 168 for "this week")
        
        Returns: Recent interactions with timestamps and evidence message IDs.
        """
        canonical = await self._resolve_entity_name(entity_name)
        if not canonical:
            return [{"error": f"Entity not found: '{entity_name}'"}]
        
        hours = hours or self.search_cfg.get("default_activity_hours", 24)
        results = await self.store.get_recent_activity(canonical, active_topics=self.active_topics, hours=hours)
        
        for r in results:
            evidence_ids = r.pop("evidence_ids", [])
            string_ids = [self._format_message_id(x) for x in evidence_ids]
            r["evidence"] = await self._hydrate_evidence(string_ids)
        
        return results

    async def find_path(self, entity_a: str, entity_b: str) -> List[Dict]:
        """
        Trace the connection chain between two specific entities. 
        Use for 'how is X connected to Y' or 'what links X to Y'. Returns the shortest path showing each hop. 
        Requires both entities to exist in memory.

        Args:
            entity_a: First entity name
            entity_b: Second entity name

        Returns: 
            Step-by-step path showing each entity in the chain with evidence.
            If path exists only through inactive topics: [{"hidden": True, "message": "..."}]
            Empty list if no connection found.
        """
        canonical_a = await self._resolve_entity_name(entity_a)
        canonical_b = await self._resolve_entity_name(entity_b)
        if not canonical_a and not canonical_b:
            return [{"error": f"Neither entity found: '{entity_a}' and '{entity_b}'"}]
        if not canonical_a:
            return [{"error": f"Entity not found: '{entity_a}'"}]
        if not canonical_b:
            return [{"error": f"Entity not found: '{entity_b}'"}]
            

        # Trace path
        path, has_inactive_shortcut = await self.store.find_path_filtered(canonical_a, canonical_b, active_topics=self.active_topics, max_depth=4)
        
        if path:
            for step in path:
                evidence_refs = step.pop("evidence_refs", [])
                string_ids = [self._format_message_id(x) for x in evidence_refs]
                step["evidence"] = await self._hydrate_evidence(string_ids)
            if has_inactive_shortcut:
                path.append({"note": "A shorter connection exists through inactive topics"})
            return path
        
        if has_inactive_shortcut:
            full_path, _ = await self.store.find_path_filtered(canonical_a, canonical_b, active_topics=None, max_depth=4)

            safe_path = []
            for step in full_path:
                topic_a = step.get('topic_a', 'General')
                topic_b = step.get('topic_b', 'General')
                
                both_active = self.active_topics is not None and topic_a in self.active_topics and topic_b in self.active_topics
                
                if both_active:
                    safe_path.append(step)
                else:
                    inactive_topics = []
                    if self.active_topics is not None:
                        if topic_a not in self.active_topics:
                            inactive_topics.append(topic_a)
                        if topic_b not in self.active_topics:
                            inactive_topics.append(topic_b)
                    else:
                        inactive_topics.extend([topic_a, topic_b])

                    safe_path.append({
                        "step": step.get('step'),
                        "entity_a": step.get('entity_a'),
                        "entity_b": step.get('entity_b'),
                        "topic_a": topic_a,
                        "topic_b": topic_b,
                        "status": "LOCKED",
                        "locked_reason": f"Inactive topic(s): {', '.join(inactive_topics)}",
                        "evidence": []
                    })
                    
            return safe_path
        
        return []

    async def get_hot_topic_context(self, hot_topics: List[str], slim: bool = False) -> Dict[str, Dict]:
        """
        Retrieve pre-cached context for frequently accessed topics.
        Called automatically at start — you already have this data in hot_topic_context.
        Only call manually if hot topics changed mid-conversation.
        
        Args:
            hot_topics: List of topic names marked as "hot"
            slim: Returns if you want more information or not

        Returns: Dict mapping topic name to list of top entities with summaries.
        """
        if not hot_topics:
            return {}
        
        # Fetch context
        raw = await self.store.get_hot_topic_context_with_messages(hot_topics, msg_limit=10, slim=slim)
        content_key = RedisKeys.message_content(self.user_name, self.session_id)
        
        for _, data in raw.items():
            msg_ids = data.get("message_ids", [])
            
            if msg_ids:
                raw_msgs = await self.redis.hmget(content_key, *msg_ids)
                messages = []
                for msg_id, raw_msg in zip(msg_ids, raw_msgs):
                    if raw_msg:
                        try:
                            parsed = json.loads(raw_msg)
                            messages.append({
                                "id": msg_id,
                                "message": parsed.get("message", "")
                            })
                        except json.JSONDecodeError:
                            continue
                data["messages"] = messages
            else:
                data["messages"] = []
            
            data.pop("message_ids", None)
        
        return raw
    
    async def get_hierarchy(self, entity_name: str, direction: str = "both") -> List[Dict]:
        """
        Get hierarchy relationships for an entity.
        
        Args:
            entity_name: Entity to check hierarchy for
            direction: "up" (parents), "down" (children), or "both"
        
        Returns:
            Dict with parent chain and/or children list
        """
        canonical = await self._resolve_entity_name(entity_name)
        if not canonical:
            return []
        
        entity_id = await self.resolver.get_id(canonical)
        if not entity_id:
            return []
        
        result = {
            "entity": canonical,
            "entity_id": entity_id
        }
        
        if direction in ("up", "both"):
            parents = await self.store.get_parent_entities(entity_id)
            result["parents"] = parents
            
            if parents:
                ancestry = []
                current_id = entity_id
                visited = {current_id}
                
                while True:
                    parent_list = await self.store.get_parent_entities(current_id)
                    if not parent_list:
                        break
                    parent = parent_list[0]  # assume single parent for now
                    if parent["id"] in visited:
                        break  # cycle protection
                    visited.add(parent["id"])
                    ancestry.append(parent["canonical_name"])
                    current_id = parent["id"]
                
                result["ancestry"] = ancestry
        
        if direction in ("down", "both"):
            children = await self.store.get_child_entities(entity_id)
            result["children"] = children
        
        return [result]
    
    async def fact_check(self, entity_name: str, query: str) -> Dict:
        """
        Retrieve and verify stored facts about a specific entity from the knowledge graph.
        Uses a resolution cascade: exact lookup → vector search → message search fallback.

        Args:
            entity_name: The entity to look up facts for.
            query: A natural language hint describing what you're looking for.

        Returns:
            Dict with resolution method and matching facts or search results.
        """
        entity_id = await self.resolver.get_id(entity_name)

        if entity_id is not None:
            facts = await self.store.get_facts_for_entity(entity_id, active_only=False)

            profile = await self.resolver.get_profile(entity_id)
            canonical = profile["canonical_name"] if profile else entity_name

            return {
                "resolution": "exact",
                "results": [
                    {
                        "entity_name": canonical,
                        "similarity": 1.0,
                        "facts": [f.to_dict() for f in (facts or [])]
                    }
                ]
            }

        embedding = await self.embedding_service.encode_single(entity_name)

        candidates = await self.store.search_entities_by_embedding(embedding, limit=5, score_threshold=0.69)

        if candidates:
            candidate_ids = [eid for eid, _ in candidates]
            similarity_map = {eid: sim for eid, sim in candidates}
            
            facts_by_entity = await self.store.get_facts_for_entities(candidate_ids, active_only=False)

            total_facts = sum(len(facts) for facts in facts_by_entity.values())

            if total_facts > 1000:
                query_embedding = await self.embedding_service.encode_single(query)

                for eid, facts in facts_by_entity.items():
                    scored = []
                    for fact in facts:
                        if fact.embedding:
                            sim = cosine_similarity(query_embedding, fact.embedding)
                            scored.append((fact, sim))
                        else:
                            scored.append((fact, 0.0))

                    scored.sort(key=lambda x: x[1], reverse=True)
                    facts_by_entity[eid] = [f for f, _ in scored[:500]]

            results = []
            for eid in candidate_ids:
                profile = await self.resolver.get_profile(eid)
                canonical = profile["canonical_name"] if profile else str(eid)

                results.append({
                    "entity_name": canonical,
                    "similarity": similarity_map[eid],
                    "facts": [f.to_dict() for f in facts_by_entity.get(eid, [])]
                })

            return {
                "resolution": "vector",
                "results": results
            }

        fallback = await self.search_messages(query)
        return {
            "resolution": "fallback",
            "results": fallback
        }

    async def save_memory(self, content: str, topic: str = "General") -> Dict:
        """Save a note to persistent session memory."""
        if self.memory:
            return await self.memory.save_memory_dict(content, topic)

        return {"error": "No memory manager configured"}
    
    async def forget_memory(self, memory_id: str) -> Dict:
        """Remove a memory by ID."""
        if self.memory:
            return await self.memory.forget_memory_dict(memory_id)
        return {"error": "No memory manager configured"}
    
    
    async def get_memory_blocks(self, hot_topics: List[str] = None) -> Dict[str, List[Dict]]:
        """Fetch memory blocks for prompt injection."""
        if self.memory:
            return await self.memory.get_memory_blocks_dict(hot_topics)
        return {}
    
    
    async def search_files(self, query: str, file_name: str = None, limit: int = 5) -> List[Dict]:
        """
        Search uploaded session files for relevant content.
        
        Args:
            query: What to search for
            file_name: Optional filename to restrict search to
            limit: Max chunks to return
        
        Returns:
            List of matching chunks with file name, content, and relevance score.
        """
        if not self.file_rag:
            return [{"error": "No file service available for this session"}]
        
        files = []
        if self.file_rag:
            files = self.file_rag.list_files()
    
        if not files:
            return [{"error": "No files uploaded to this session"}]
        
        file_filter = None
        if file_name:
            for f in files:
                if f["original_name"].lower() == file_name.lower():
                    file_filter = f["file_id"]
                    break
            if not file_filter:
                available = [f["original_name"] for f in files]
                return [{"error": f"File '{file_name}' not found. Available: {', '.join(available)}"}]
        
        results = await self.file_rag.search(query, n_results=limit, file_filter=file_filter)
        
        if not results:
            return [{"info": "No relevant content found in uploaded files"}]
        
        return results
    
    
    
    async def web_search(self, query: str, limit: int = 5, freshness: str = None) -> List[Dict]:
        """
        Search the web using the best available provider.
        Tier: configured provider > Brave > Tavily > DuckDuckGo (free default).
        """
        provider = self.search_cfg.get("provider", "auto")
        brave_key = self.search_cfg.get("brave_api_key", "")
        tavily_key = self.search_cfg.get("tavily_api_key", "")

        if provider == "brave" and brave_key:
            return await self._search_brave(query, limit, brave_key, freshness)
        elif provider == "tavily" and tavily_key:
            return await self._search_tavily(query, limit, tavily_key)
        elif provider == "duckduckgo":
            return await self._search_duckduckgo(query, limit, freshness)

        if brave_key:
            return await self._search_brave(query, limit, brave_key, freshness)
        if tavily_key:
            return await self._search_tavily(query, limit, tavily_key)
        return await self._search_duckduckgo(query, limit, freshness)

    async def news_search(self, query: str, limit: int = 5, freshness: str = None) -> List[Dict]:
        """
        Search for news articles. Requires Brave Search API key.
        """
        brave_key = self.search_cfg.get("brave_api_key", "")
        if not brave_key:
            return [{"title": "Not Available", "url": "", "snippet": "News search requires a Brave Search API key. Configure one in Settings → Web Search."}]
        return await self._news_brave(query, limit, brave_key, freshness or "pw")

    async def _search_duckduckgo(self, query: str, limit: int, freshness: str = None) -> List[Dict]:
        """Free web search via DuckDuckGo — no API key required."""
        loop = asyncio.get_running_loop()
        try:
            from duckduckgo_search import DDGS
            ddgs = DDGS()
            timelimit = {"pd": "d", "pw": "w", "pm": "m", "py": "y"}.get(freshness)
            
            raw = await loop.run_in_executor(
                None,
                partial(ddgs.text, query, max_results=min(limit, 10), timelimit=timelimit)
            )

            if not raw:
                return [{"title": "No Results", "url": "", "snippet": f"No web results found for: {query}"}]

            results = []
            for r in raw:
                results.append({
                    "title": r.get("title", "Untitled"),
                    "url": r.get("href", r.get("link", "")),
                    "snippet": r.get("body", r.get("snippet", ""))
                })
            return results
        except Exception as e:
            logger.error(f"DuckDuckGo search failed: {e}")
            return [{"title": "Search Error", "url": "", "snippet": f"DuckDuckGo search failed: {e}"}]


    async def _search_tavily(self, query: str, limit: int, api_key: str) -> List[Dict]:
        """Web search via Tavily API"""
        url = "https://api.tavily.com/search"
        payload = {
            "api_key": api_key,
            "query": query,
            "max_results": min(limit, 10),
            "search_depth": "basic",
            "include_answer": False,
        }

        try:
            response = await self._http_client.post(url, json=payload, timeout=10.0)

            if response.status_code == 401:
                logger.warning("Tavily API key invalid, falling back to DuckDuckGo")
                return await self._search_duckduckgo(query, limit)
            if response.status_code == 429:
                logger.warning("Tavily rate limit hit, falling back to DuckDuckGo")
                return await self._search_duckduckgo(query, limit)

            response.raise_for_status()
            data = response.json()

            results = []
            for r in data.get("results", []):
                results.append({
                    "title": r.get("title", "Untitled"),
                    "url": r.get("url", ""),
                    "snippet": r.get("content", "")
                })

            if not results:
                return [{"title": "No Results", "url": "", "snippet": f"No web results found for: {query}"}]
            return results
        except httpx.TimeoutException:
            logger.warning("Tavily timed out, falling back to DuckDuckGo")
            return await self._search_duckduckgo(query, limit)
        except Exception as e:
            logger.error(f"Tavily search failed: {e}")
            return await self._search_duckduckgo(query, limit)

    async def _search_brave(self, query: str, limit: int, api_key: str, freshness: str = None) -> List[Dict]:
        """Premium web search via Brave Search API."""
        url = "https://api.search.brave.com/res/v1/web/search"
        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "X-Subscription-Token": api_key
        }
        params = {
            "q": query,
            "count": min(limit, 10),
            "extra_snippets": True,
            "spellcheck": 1,
        }
        if freshness and freshness in ("pd", "pw", "pm", "py"):
            params["freshness"] = freshness

        try:

            response = await self._http_client.get(url, headers=headers, params=params)

            if response.status_code == 401:
                logger.warning("Brave API key invalid, falling back")
                return await self._search_tavily(query, limit, self.search_cfg.get("tavily_api_key", "")) \
                    if self.search_cfg.get("tavily_api_key") else await self._search_duckduckgo(query, limit)
            if response.status_code == 429:
                logger.warning("Brave rate limit hit, falling back")
                return await self._search_tavily(query, limit, self.search_cfg.get("tavily_api_key", "")) \
                    if self.search_cfg.get("tavily_api_key") else await self._search_duckduckgo(query, limit)

            response.raise_for_status()
            data = response.json()

            results = []
            for result in data.get("web", {}).get("results", []):
                snippet = result.get("description", result.get("snippet", ""))
                snippet = re.sub(r"<[^>]+>", "", snippet)
                # Append extra snippets for richer context
                extra = result.get("extra_snippets", [])
                if extra:
                    snippet += " ... " + " ... ".join(re.sub(r"<[^>]+>", "", s) for s in extra[:2])
                results.append({
                    "title": result.get("title", "Untitled"),
                    "url": result.get("url", ""),
                    "snippet": snippet
                })

            if not results:
                return [{"title": "No Results", "url": "", "snippet": f"No web results found for: {query}"}]
            return results
        except httpx.TimeoutException:
            logger.warning("Brave timed out, falling back to DuckDuckGo")
            return await self._search_duckduckgo(query, limit)
        except Exception as e:
            logger.error(f"Brave search failed: {e}")
            return await self._search_duckduckgo(query, limit)

    async def _news_brave(self, query: str, limit: int, api_key: str, freshness: str = "pw") -> List[Dict]:
        """News search via Brave News API."""
        url = "https://api.search.brave.com/res/v1/news/search"
        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "X-Subscription-Token": api_key
        }
        params = {
            "q": query,
            "count": min(limit, 20),
            "spellcheck": 1,
            "freshness": freshness,
        }

        try:
            response = await self._http_client.get(url, headers=headers, params=params)

            if response.status_code in (401, 429):
                logger.warning(f"Brave news API returned {response.status_code}")
                return [{"title": "Error", "url": "", "snippet": f"Brave News API error ({response.status_code}). Check your API key in Settings."}]

            response.raise_for_status()
            data = response.json()

            results = []
            for article in data.get("results", []):
                snippet = article.get("description", "")
                snippet = re.sub(r"<[^>]+>", "", snippet)
                results.append({
                    "title": article.get("title", "Untitled"),
                    "url": article.get("url", ""),
                    "snippet": snippet,
                    "source": article.get("meta_url", {}).get("hostname", ""),
                    "date": article.get("age", ""),
                })

            if not results:
                return [{"title": "No Results", "url": "", "snippet": f"No news found for: {query}"}]
            return results
        except httpx.TimeoutException:
            logger.warning("Brave news timed out")
            return [{"title": "Timeout", "url": "", "snippet": "News search timed out. Try a simpler query."}]
        except Exception as e:
            logger.error(f"Brave news search failed: {e}")
            return [{"title": "Search Error", "url": "", "snippet": f"News search failed: {e}"}]
    
    async def save_insight(self, content: str) -> Dict:
        return {"error": "save_insight is only available in community discussions."}

    async def spawn_specialist(self, name: str, persona: str,
                            initial_rules: List[str] = None,
                            initial_preferences: List[str] = None,
                            initial_icks: List[str] = None) -> Dict:
        return {"error": "spawn_specialist is only available in community discussions."}


    def get_file_manifest(self) -> List[Dict]:
        """Get list of uploaded files for prompt context."""
        if not self.file_rag:
            return []
        return self.file_rag.list_files()
    
    async def close(self):
        await self._http_client.aclose()