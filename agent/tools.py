import asyncio
import json
import os
import re
import httpx
from typing import List, Dict, Optional

from loguru import logger
import redis.asyncio as aioredis
from main.entity_resolve import EntityResolver
from db.store import MemGraphStore
from shared.file_rag import FileRAGService
from shared.topics_config import TopicConfig
from shared.redisclient import RedisKeys 
from shared.events import emit



class Tools:
    
    def __init__(self, user_name: str, store: MemGraphStore, ent_resolver: EntityResolver, 
                redis_client: aioredis.Redis, session_id: str, topic_config: TopicConfig = None, 
                search_config: dict = None, file_rag: FileRAGService =None, mcp_manager=None):
        self.session_id = session_id
        self.store = store
        self.resolver = ent_resolver
        self.user_name = user_name
        self.redis = redis_client
        self.embedding_service = ent_resolver.embedding_service
        self.topic_config = topic_config
        self.file_rag = file_rag
        self.active_topics = topic_config.active_topics if topic_config else []
        self.search_cfg = search_config or {}
        self.mcp_manager = mcp_manager
    
    def _resolve_entity_name(self, entity: str) -> Optional[str]:
        """Resolve user input to canonical entity name via exact or fuzzy match."""
        return self.resolver.resolve_entity_name(entity)
    

    async def _hydrate_evidence(self, evidence_ids: list[str], timeout: float = 5.0) -> list[dict]:
        if not evidence_ids:
            return []
        
        content_key = RedisKeys.message_content(self.user_name, self.session_id)
        raw_results = []
        try:
            raw_results = await asyncio.wait_for(
                self.redis.hmget(content_key, *evidence_ids),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            logger.warning(f"Redis hydrate timed out for {len(evidence_ids)} evidence IDs")
            return []
        
        results = []
        for msg_id, raw in zip(evidence_ids, raw_results):
            if raw:
                data = json.loads(raw)
                results.append({
                    "id": msg_id,
                    "message": data["message"],
                    "timestamp": data["timestamp"]
                })
        return results
    
    def _normalize_output(self, entity_list: List[Dict]) -> List[Dict]:
        if not entity_list:
            return entity_list
        
        for entity in entity_list:
            if "top_connections" in entity:
                self._normalize_output(entity["top_connections"])
        
        return entity_list
        

    async def _get_surrounding_context(self, msg_id: str, forward: int = 3, target_total: int = 10) -> List[Dict]:
        """Get surrounding turns for context."""
        sorted_key = RedisKeys.recent_conversation(self.user_name, self.session_id)
        conv_key = RedisKeys.conversation(self.user_name, self.session_id)
        lookup_key = RedisKeys.msg_to_turn_lookup(self.user_name, self.session_id)
        
        target_turn_id = msg_id
        if msg_id.startswith("msg_"):
            target_turn_id = await self.redis.hget(lookup_key, msg_id)
            if not target_turn_id:
                return []
        
        rank = await self.redis.zrank(sorted_key, target_turn_id)
        if rank is None:
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
            
            data = json.loads(raw_map[tid])
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

        tgt_data = json.loads(raw_map[target_turn_id])
        target_msg = {
            "role": tgt_data.get("role", "unknown"),
            "timestamp": tgt_data.get("timestamp", ""),
            "content": tgt_data.get("content", ""),
            "id": target_turn_id,
            "is_hit": True
        }

        for i in range(target_index + 1, min(len(turn_ids), target_index + forward + 1)):
            tid = turn_ids[i]
            if tid not in raw_map: continue
            
            data = json.loads(raw_map[tid])
            post_context.append({
                "role": data.get("role", "unknown"),
                "timestamp": data.get("timestamp", ""),
                "content": data.get("content", ""),
                "id": tid
            })

        return pre_context + [target_msg] + post_context


    def _search_messages(self, query: str, k: int) -> list[tuple[str, float]]:

        vector_limit = self.search_cfg.get("vector_limit", 50)
        fts_limit = self.search_cfg.get("fts_limit", 50)
        rerank_candidates = self.search_cfg.get("rerank_candidates", 45)

        results = {}
        query_embedding = self.embedding_service.encode_single(query)
        sem_results = self.store.search_messages_vector(query_embedding, limit=vector_limit)

        for msg_id, score in sem_results:
            msg_key = f"msg_{msg_id}" if msg_id < 1_000_000_000 else f"turn_{msg_id - 1_000_000_000}"
            results[msg_key] = ("semantic", float(score))
        
        fts_results = self.store.search_messages_fts(query, limit=fts_limit)
        max_fts = max([s for _, s in fts_results]) if fts_results else 1.0

        for msg_id, raw_score in fts_results:
            msg_key = f"msg_{msg_id}"
            
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
                texts = []
                for msg_key in candidate_keys:
                    msg_id = int(msg_key.split("_")[1])
                    texts.append(self.store.get_message_text(msg_id))
                
                scores = self.embedding_service.rerank(query, texts)
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
        loop = asyncio.get_running_loop()
        limit = limit or self.search_cfg.get("default_message_limit", 8)
        results = await loop.run_in_executor(None, self._search_messages, query, limit)
        
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
                    data = json.loads(raw)
                    output.append({
                        "id": msg_key,
                        "role": "user",
                        "message": data["message"],
                        "timestamp": data["timestamp"],
                        "score": scores[msg_key],
                        "context": context
                    })
            else:
                raw = assistant_contents.get(msg_key)
                if raw:
                    data = json.loads(raw)
                    output.append({
                        "id": msg_key,
                        "role": data["role"],
                        "message": data["content"],
                        "timestamp": data["timestamp"],
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
        
        Returns: List of matching entities with id, name, summary snippet, type.
        """
        limit = limit or self.search_cfg.get("default_entity_limit", 5)
        results = self.store.search_entity(query, self.active_topics, limit)
    
        if not results:
            return []
        
        results = self._normalize_output(results)
        for entity in results:
            for conn in entity.get("top_connections", []):
                evidence_ids = conn.pop("evidence_ids", [])
                conn["evidence"] = await self._hydrate_evidence(evidence_ids)
        
        return results

    async def get_connections(self, entity_name: str) -> List[Dict]:
        """
        Get the full relationship network for an entity.
        Returns all connections (up to 50) with evidence — the actual messages that established each connection. 
        Use when you need comprehensive relationship details beyond the top 5 from search_entity..
        
        Args:
            entity_name: The entity to find connections for
            active_only: If True, exclude entities from inactive topics (default True)
        
        Returns: List of connections with target entity, connection strength, evidence message IDs.
        """
        canonical = self._resolve_entity_name(entity_name)
        if not canonical:
            return [{"error": f"Entity not found: '{entity_name}'"}]
        
        results = self.store.get_related_entities([canonical], active_topics=self.active_topics)
        results = self._normalize_output(results)
        if results:
            for r in results:
                r["evidence"] = await self._hydrate_evidence(r.pop("evidence_ids", []))
            return results
        
        hidden_results = self.store.get_related_entities([canonical], active_topics=None)
        
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
        Default is 24 hours; use 168 for a week..
        
        Args:
            entity_name: Entity to check activity for
            hours: How far back to look (default 24, use 168 for "this week")
        
        Returns: Recent interactions with timestamps and evidence message IDs.
        """
        canonical = self._resolve_entity_name(entity_name)
        if not canonical:
            return [{"error": f"Entity not found: '{entity_name}'"}]
        
        hours = hours or self.search_cfg.get("default_activity_hours", 24)
        results = self.store.get_recent_activity(canonical, active_topics=self.active_topics, hours=hours)
        
        for r in results:
            r["evidence"] = await self._hydrate_evidence(r.pop("evidence_ids", []))
        
        return results

    async def find_path(self, entity_a: str, entity_b: str) -> List[Dict]:
        """
        "Trace the connection chain between two specific entities. 
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
        canonical_a = self._resolve_entity_name(entity_a)
        canonical_b = self._resolve_entity_name(entity_b)
        if not canonical_a and not canonical_b:
            return [{"error": f"Neither entity found: '{entity_a}' and '{entity_b}'"}]
        if not canonical_a:
            return [{"error": f"Entity not found: '{entity_a}'"}]
        if not canonical_b:
            return [{"error": f"Entity not found: '{entity_b}'"}]
            

        path, has_inactive_shortcut = self.store.find_path_filtered(canonical_a, canonical_b, active_topics=self.active_topics)
        
        if path:
            for step in path:
                step["evidence"] = await self._hydrate_evidence(step.pop("evidence_refs", []))
            if has_inactive_shortcut:
                path.append({"note": "A shorter connection exists through inactive topics"})
            return path
        
        if has_inactive_shortcut:
            full_path, _ = self.store.find_path_filtered(canonical_a, canonical_b, active_topics=None)

            safe_path = []
            for step in full_path:
                topic_a = step.get('topic_a', 'General')
                topic_b = step.get('topic_b', 'General')
                
                both_active = topic_a in self.active_topics and topic_b in self.active_topics
                
                if both_active:
                    safe_path.append(step)
                else:
                    inactive_topics = []
                    if topic_a not in self.active_topics:
                        inactive_topics.append(topic_a)
                    if topic_b not in self.active_topics:
                        inactive_topics.append(topic_b)

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
        
        raw = self.store.get_hot_topic_context_with_messages(hot_topics, msg_limit=10, slim=slim)
        content_key = RedisKeys.message_content(self.user_name, self.session_id)
        
        for _, data in raw.items():
            msg_ids = data.get("message_ids", [])
            
            if msg_ids:
                raw_msgs = await self.redis.hmget(content_key, *msg_ids)
                messages = []
                for msg_id, raw_msg in zip(msg_ids, raw_msgs):
                    if raw_msg:
                        parsed = json.loads(raw_msg)
                        messages.append({
                            "id": msg_id,
                            "message": parsed["message"]
                        })
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
        canonical = self._resolve_entity_name(entity_name)
        if not canonical:
            return []
        
        entity_id = self.resolver.get_id(canonical)
        if not entity_id:
            return []
        
        result = {
            "entity": canonical,
            "entity_id": entity_id
        }
        
        if direction in ("up", "both"):
            parents = self.store.get_parent_entities(entity_id)
            result["parents"] = parents
            
            if parents:
                ancestry = []
                current_id = entity_id
                visited = {current_id}
                
                while True:
                    parent_list = self.store.get_parent_entities(current_id)
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
            children = self.store.get_child_entities(entity_id)
            result["children"] = children
        
        return [result]

    async def save_memory(self, content: str, topic: str = "General") -> Dict:
        """
        Save a note to persistent session memory.
        
        Args:
            content: The fact or note to remember
            topic: Topic this memory belongs to (default: General)
        
        Returns:
            Confirmation with memory ID, or error if limit reached.
        """
        if not content or not content.strip():
            return {"error": "Empty memory content"}
        
        content = content.strip()
        if len(content) > 200:
            return {
                "error": f"Memory too long ({len(content)} chars). Max 200. Condense and retry."
            }
        
        normalized_topic = self.topic_config.normalize_topic(topic) if topic else None
        if not normalized_topic:
            # If no topic resolved (e.g. General disabled), try to use first active topic
            normalized_topic = self.active_topics[0] if self.active_topics else None
            if not normalized_topic:
                return {"error": "No active topics available to save memory to."}
        if normalized_topic not in self.active_topics:
            return {"error": f"Topic '{topic}' is not active. Use an active topic."}
        
        memory_key = RedisKeys.agent_memory(self.user_name, self.session_id, normalized_topic)
        
        existing = await self.redis.hgetall(memory_key)
        if len(existing) >= 10:
            return {
                "error": f"Memory block '{normalized_topic}' is full (10/10). Use forget_memory to remove outdated entries first.",
                "current_entries": len(existing)
            }
        
        import uuid
        from datetime import datetime, timezone
        
        memory_id = f"mem_{uuid.uuid4().hex[:8]}"
        payload = json.dumps({
            "content": content,
            "topic": normalized_topic,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "source_session": self.session_id
        })
        
        await self.redis.hset(memory_key, memory_id, payload)
        
        await emit(self.session_id, "agent", "memory_saved", {
            "topic": normalized_topic,
            "memory_id": memory_id
        })
        
        return {
            "saved": True,
            "memory_id": memory_id,
            "topic": normalized_topic,
            "content": content
        }
    
    async def forget_memory(self, memory_id: str) -> Dict:
        """
        Remove a memory by ID.
        
        Args:
            memory_id: The ID of the memory to remove
        
        Returns:
            Confirmation or error if not found.
        """
        if not memory_id:
            return {"error": "No memory_id provided"}
        
        all_topics = list(set(self.active_topics + list(self.topic_config.raw.keys())))
        for topic in all_topics:
            memory_key = RedisKeys.agent_memory(self.user_name, self.session_id, topic)
            removed = await self.redis.hdel(memory_key, memory_id)
            if removed:
                await emit(self.session_id, "agent", "memory_forgotten", {
                    "topic": topic,
                    "memory_id": memory_id
                })
                return {"removed": True, "memory_id": memory_id, "topic": topic}
        
        return {"error": f"Memory '{memory_id}' not found in any block"}
    
    
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
        
        loop = asyncio.get_running_loop()
        results = await loop.run_in_executor(
            None,
            lambda: self.file_rag.search(query, n_results=limit, file_filter=file_filter)
        )
        
        if not results:
            return [{"info": "No relevant content found in uploaded files"}]
        
        return results
    
    async def get_memory_blocks(self, hot_topics: List[str]) -> Dict[str, List[Dict]]:
        """
        Fetch memory blocks for prompt injection.
        Always includes General. Adds hot topic blocks.
        """
        topics_to_fetch = []
        if self.topic_config.is_active("General"):
            topics_to_fetch.append("General")
        for t in hot_topics:
            if t not in topics_to_fetch:
                topics_to_fetch.append(t)
        
        blocks = {}
        
        for topic in topics_to_fetch:
            memory_key = RedisKeys.agent_memory(self.user_name, self.session_id, topic)
            raw = await self.redis.hgetall(memory_key)
            
            if not raw:
                continue
            
            entries = []
            for mem_id, payload in raw.items():
                data = json.loads(payload)
                entries.append({
                    "id": mem_id,
                    "content": data["content"],
                    "created_at": data.get("created_at", "")
                })
            
            entries.sort(key=lambda x: x["created_at"])
            blocks[topic] = entries
        
        return blocks
    
    async def web_search(self, query: str, limit: int = 5, freshness: str = None) -> List[Dict]:
        """
        Search the web using the best available provider.
        Tier: configured provider > Brave > Tavily > DuckDuckGo (free default).
        """
        provider = self.search_cfg.get("provider", "auto")
        brave_key = self.search_cfg.get("brave_api_key", "")
        tavily_key = self.search_cfg.get("tavily_api_key", "")

        # Explicit provider selection
        if provider == "brave" and brave_key:
            return await self._search_brave(query, limit, brave_key, freshness)
        elif provider == "tavily" and tavily_key:
            return await self._search_tavily(query, limit, tavily_key)
        elif provider == "duckduckgo":
            return await self._search_duckduckgo(query, limit, freshness)

        # Auto mode: use best available
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
        try:
            from duckduckgo_search import DDGS
            ddgs = DDGS()
            timelimit = {"pd": "d", "pw": "w", "pm": "m", "py": "y"}.get(freshness)
            raw = ddgs.text(query, max_results=min(limit, 10), timelimit=timelimit)

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
        """Web search via Tavily API — optimized for AI agents."""
        url = "https://api.tavily.com/search"
        payload = {
            "api_key": api_key,
            "query": query,
            "max_results": min(limit, 10),
            "search_depth": "basic",
            "include_answer": False,
        }

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(url, json=payload, timeout=10.0)

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
            async with httpx.AsyncClient() as client:
                response = await client.get(url, headers=headers, params=params, timeout=10.0)

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
            async with httpx.AsyncClient() as client:
                response = await client.get(url, headers=headers, params=params, timeout=10.0)

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




    def get_file_manifest(self) -> List[Dict]:
        """Get list of uploaded files for prompt context."""
        if not self.file_rag:
            return []
        return self.file_rag.list_files()