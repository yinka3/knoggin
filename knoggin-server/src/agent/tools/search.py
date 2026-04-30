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

class SearchToolsMixin:
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
        Connections only include canonical name and aliases ΓÇö use this tool again on a connection's name if you need their full profile.
        
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
            return [{"title": "Not Available", "url": "", "snippet": "News search requires a Brave Search API key. Configure one in Settings ΓåÆ Web Search."}]
        return await self._news_brave(query, limit, brave_key, freshness or "pw")

    async def _resolve_entity_name(self, entity: str) -> Optional[str]:
        """Resolve user input to canonical entity name via exact or fuzzy match."""
        return await self.resolver.resolve_entity_name(entity)

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

