import json
from typing import List, Dict, Optional

import redis
from main.entity_resolve import EntityResolver
from db.memgraph import MemGraphStore
from main.topics_config import TopicConfig



class Tools:
    
    def __init__(self, user_name: str, store: MemGraphStore, ent_resolver: EntityResolver, 
                 redis_client: redis.Redis, topic_config: TopicConfig = None):
        self.store = store
        self.resolver = ent_resolver
        self.user_name = user_name
        self.redis = redis_client
        self.topic_config = topic_config
        self.active_topics = topic_config.active_topics if topic_config else []
    
    def _resolve_entity_name(self, entity: str) -> Optional[str]:
        """Resolve user input to canonical entity name via exact or fuzzy match."""
        return self.resolver.resolve_entity_name(entity)
    
    async def _hydrate_evidence(self, evidence_ids: list[str]) -> list[dict]:
        if not evidence_ids:
            return []
        
        content_key = f"message_content:{self.user_name}"
        raw_results = await self.redis.hmget(content_key, *evidence_ids)
        
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
        """
        Renames legacy types to the modern schema before showing the Agent.
        Example: type="Library" -> type="Dependency"
        """
        if not entity_list or not self.topic_config:
            return entity_list
        
        # build label alias map from config
        alias_map = {}
        for _, config in self.topic_config.raw.items():
            label_aliases = config.get("label_aliases", {})
            for legacy, canonical in label_aliases.items():
                alias_map[legacy] = canonical
        
        for entity in entity_list:
            raw_type = entity.get("type")
            if raw_type in alias_map:
                entity["type"] = alias_map[raw_type]
            
            if "top_connections" in entity:
                self._normalize_output(entity["top_connections"])
        
        return entity_list
    

    async def _get_surrounding_context(self, msg_id: str, forward: int = 3, target_total: int = 10) -> List[Dict]:
        """Get surrounding turns for context."""
        sorted_key = f"recent_conversation:{self.user_name}"
        conv_key = f"conversation:{self.user_name}"
        lookup_key = f"lookup:msg_to_turn:{self.user_name}"
        
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
            
            # if role != "user":
            #     if len(content) > 200:
            #         content = content[:200] + "...(truncated)"
            
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

    
    async def search_messages(self, query: str, limit: int = 8) -> List[Dict]:
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
        results = self.resolver._search_messages(query, limit)
        seen_ids = set()
        output = []
        for msg_id, score in results:
            if msg_id in seen_ids:
                continue

            context = await self._get_surrounding_context(msg_id)
            for msg in context:
                seen_ids.add(msg['id'])

            if msg_id.startswith("msg_"):
                content_key = f"message_content:{self.user_name}"
                raw = await self.redis.hget(content_key, msg_id)
                if raw:
                    data = json.loads(raw)
                    output.append({
                        "id": msg_id,
                        "role": "user",
                        "message": data["message"],
                        "timestamp": data["timestamp"],
                        "score": score,
                        "context": context
                    })
                    
            elif msg_id.startswith("turn_"):
                conv_key = f"conversation:{self.user_name}"
                raw = await self.redis.hget(conv_key, msg_id)
                if raw:
                    data = json.loads(raw)
                    output.append({
                        "id": msg_id,
                        "role": data["role"],
                        "message": data["content"],
                        "timestamp": data["timestamp"],
                        "score": score,
                        "context": context
                    })
        
        return output

    async def search_entity(self, query: str, limit: int = 5) -> List[Dict]:
        """
        Find a person, place, or thing by name. 
        Returns their full profile (type, summary, aliases, topic) and their 5 strongest connections.
        Connections only include canonical name and aliases — use this tool again on a connection's name if you need their full profile.
        
        Args:
            query: Name or partial name to search
            limit: Max results to return (default 5)
        
        Returns: List of matching entities with id, name, summary snippet, type.
        """
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
            return []
        
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
            return []
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
        if not canonical_a or not canonical_b:
            return []

        path, has_inactive_shortcut = self.store._find_path_filtered(canonical_a, canonical_b, active_topics=self.active_topics)
        
        if path:
            for step in path:
                step["evidence"] = await self._hydrate_evidence(step.pop("evidence_refs", []))
            if has_inactive_shortcut:
                path.append({"note": "A shorter connection exists through inactive topics"})
            return path
        
        if has_inactive_shortcut:
            full_path, _ = self.store._find_path_filtered(canonical_a, canonical_b, active_topics=None)
    
            safe_path = []
            for step in full_path:
                if step['topic'] in self.active_topics:
                    safe_path.append(step)
                else:
                    safe_path.append({
                        "canonical_name": step['canonical_name'],
                        "topic": step['topic'],
                        "status": "LOCKED (Inactive Topic)",
                        "summary": "[REDACTED]" 
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
        content_key = f"message_content:{self.user_name}"
        
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
    
    async def get_hierarchy(self, entity_name: str, direction: str = "both") -> Dict:
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
            return {"error": f"Entity '{entity_name}' not found"}
        
        entity_id = self.resolver.get_id(canonical)
        if not entity_id:
            return {"error": f"Could not resolve '{canonical}' to ID"}
        
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
        
        return result




    # def web_search(self, query: str) -> List[Dict]:
    #     """
    #     Search the web for external information.
    #     Use ONLY for current events, external facts, or information not in the user's graph.
    #     This is a separate path — once you go web, you cannot use internal tools.
        
    #     Args:
    #         query: Search query
        
    #     Returns: List of web results with title, snippet, url.
    #     """
    #     # TODO: Implement web search
    #     return []

        # async def get_profile(self, entity_name: str) -> Optional[Dict]:
        # """
        # Get full profile for a specific entity.
        # Use when you know the exact entity name and need complete information.
        
        # Args:
        #     entity_name: Exact canonical name of the entity
        
        # Returns: Full profile with summary, type, aliases, topic, last_mentioned.
        # Returns None if entity not found.
        # """
        # canonical = self._resolve_entity_name(entity_name)
        # if not canonical:
        #     return None
        
        # entity_id = self.resolver.get_id(canonical)
        # if entity_id:
        #     profile = self.resolver.entity_profiles.get(entity_id)
        #     if profile:
        #         return profile
            
        # return self.store.get_entity_profile(canonical)