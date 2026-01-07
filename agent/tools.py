import json
from typing import List, Dict, Optional

from rapidfuzz import process as fuzzy_process, fuzz
import redis
from main.entity_resolve import EntityResolver
from db.memgraph import MemGraphStore



class Tools:
    
    def __init__(self, user_name: str, store: MemGraphStore, ent_resolver: EntityResolver, redis_client: redis.Redis, active_topics: List[str] = None):
        self.store = store
        self.resolver = ent_resolver
        self.user_name = user_name
        self.redis = redis_client
        self.active_topics = active_topics or []
    
    def _resolve_entity_name(self, entity: str) -> Optional[str]:
        """Resolve user input to canonical entity name via exact or fuzzy match."""
        
        
        entity_id = self.resolver.get_id(entity)
        if entity_id:
            profile = self.resolver.entity_profiles.get(entity_id)
            return profile["canonical_name"] if profile else entity
        
        if not self.resolver._name_to_id:
            return None
        
        result = fuzzy_process.extractOne(
            query=entity,
            choices=self.resolver._name_to_id.keys(),
            scorer=fuzz.WRatio,
            score_cutoff=85
        )
        
        if result:
            matched_name, _, _ = result
            entity_id = self.resolver._name_to_id[matched_name]
            profile = self.resolver.entity_profiles.get(entity_id)
            return profile["canonical_name"] if profile else matched_name
        
        return None
    
    async def _hydrate_evidence(self, evidence_ids: list[str]) -> list[dict]:
        if not evidence_ids:
            return []
        content_key = f"message_content:{self.user_name}"
        results = []
        for msg_id in evidence_ids:
            raw = await self.redis.hget(content_key, msg_id)
            if raw:
                data = json.loads(raw)
                results.append({
                    "id": msg_id,
                    "message": data["message"],
                    "timestamp": data["timestamp"]
                })
        return results
    

    async def _get_surrounding_context(self, msg_id: str, window: int = 5) -> List[Dict]:
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

            
        start = max(0, rank - (window * 5))
        end = rank + window
            
        turn_ids = await self.redis.zrange(sorted_key, start, end)
        if not turn_ids:
            return []  
            
        pipe = self.redis.pipeline()
        for _id in turn_ids:
            pipe.hget(conv_key, _id)
        results = await pipe.execute()

        user_turns = []
        assistant_turns = []
        
        for t_id, raw in zip(turn_ids, results):
            if raw and t_id != target_turn_id:
                data = json.loads(raw)
                turn = {
                    "role": data["role"],
                    "timestamp": data["timestamp"]
                }

                if data["role"] == "user":
                    turn["content"] = data["content"]
                else:
                    turn["content"] = data["content"][:250]

                if data["role"] == "user":
                    user_turns.append(turn)
                else:
                    assistant_turns.append(turn)
        
        user_turns = user_turns[:(window * 5)]
        assistant_turns = assistant_turns[:window]
        
        context = user_turns + assistant_turns
        context.sort(key=lambda x: x["timestamp"])
        
        return context

    
    async def search_messages(self, query: str, limit: int = 10) -> List[Dict]:
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
        
        output = []
        for msg_id, score in results:
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
                        "context": await self._get_surrounding_context(msg_id)
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
                        "context": await self._get_surrounding_context(msg_id)
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
        results = self.store.search_entity(query, limit)
    
        if not results:
            return []
        
        for entity in results:
            for conn in entity.get("top_connections", []):
                evidence_ids = conn.pop("evidence_ids", [])
                conn["evidence"] = await self._hydrate_evidence(evidence_ids)
        
        return results

    async def get_connections(self, entity_name: str, active_only: bool = True) -> List[Dict]:
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
        results = self.store.get_related_entities([canonical], active_only) or []
        
        for r in results:
            r["evidence"] = await self._hydrate_evidence(r.pop("evidence_ids", []))
        
        return results

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
        results = self.store.get_recent_activity(canonical, hours) or []
        
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

        path = self.store._find_path_filtered(canonical_a, canonical_b, active_only=True)
        if path:
            for step in path:
                step["evidence"] = await self._hydrate_evidence(step.pop("evidence_refs", []))
            return path

        full_path = self.store._find_path_filtered(canonical_a, canonical_b, active_only=False)
        if full_path:
            return [{"hidden": True, "message": "Connection exists through inactive topics"}]
        
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
    
        # Hydrate message IDs from Redis
        content_key = f"message_content:{self.user_name}"
        
        for _, data in raw.items():
                messages = []
                for msg_id in data.pop("message_ids", []):
                    raw_msg = await self.redis.hget(content_key, msg_id)
                    if raw_msg:
                        parsed = json.loads(raw_msg)
                        messages.append({
                            "id": msg_id,
                            "message": parsed["message"]
                        })
                data["messages"] = messages
        
        return raw




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