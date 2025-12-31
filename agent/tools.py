import json
from typing import List, Dict, Optional, TYPE_CHECKING

import faiss
import numpy as np
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
    

    async def _get_surrounding_context(self, msg_id: str, window: int = 2) -> List[Dict]:
        """Get surrounding turns for context using pipeline."""
        sorted_key = f"recent_conversation:{self.user_name}"
        conv_key = f"conversation:{self.user_name}"
        
        # Find target turn
        if msg_id.startswith("msg_"):
            user_msg_id = int(msg_id.replace("msg_", ""))
            # Need to scan for matching user_msg_id — unavoidable without reverse index
            all_turns = await self.redis.zrange(sorted_key, 0, -1)
            all_data = await self.redis.hmget(conv_key, *all_turns)
            
            target_idx = None
            parsed_turns = []
            for i, (turn_id, raw) in enumerate(zip(all_turns, all_data)):
                if raw:
                    data = json.loads(raw)
                    parsed_turns.append({"turn_id": turn_id, **data})
                    if data.get("user_msg_id") == user_msg_id:
                        target_idx = i
            
            if target_idx is None:
                return []
            
            # Slice window
            start = max(0, target_idx - window)
            end = min(len(parsed_turns), target_idx + window + 1)
            
            return [
                {"role": t["role"], "content": t["content"], "timestamp": t["timestamp"]}
                for t in parsed_turns[start:end]
                if t["turn_id"] != f"turn_{target_idx}"  # exclude the match itself
            ]
        
        else:

            all_turns = await self.redis.zrange(sorted_key, 0, -1)
            
            if msg_id not in all_turns:
                return []
            
            target_idx = all_turns.index(msg_id)
            start = max(0, target_idx - window)
            end = min(len(all_turns), target_idx + window + 1)
            
            context_turns = [t for t in all_turns[start:end] if t != msg_id]
            
            if not context_turns:
                return []
            
            pipe = self.redis.pipeline()
            for turn_id in context_turns:
                pipe.hget(conv_key, turn_id)
            results = await pipe.execute()
            
            context = []
            for raw in results:
                if raw:
                    data = json.loads(raw)
                    context.append({
                        "role": data["role"],
                        "content": data["content"],
                        "timestamp": data["timestamp"]
                    })
            
            return context

    
    async def search_messages(self, query: str, limit: int = 5) -> List[Dict]:
        """
        Search past conversation turns by semantic similarity.
        Searches both user messages and STELLA responses.

        Args:
            query: Keywords or phrase to search for
            limit: Max results (default 5)

        Returns: List of turns with id, role, message, timestamp, score, 
                and surrounding context (adjacent turns for continuity).
        """
        results = self.resolver.search_messages(query, limit)
        
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

    async def search_entities(self, query: str, limit: int = 5) -> List[Dict]:
        """
        Search for entities by name or alias.
        Use when you need to find a person, place, or thing but aren't sure of exact name.
        Returns partial matches — use get_profile for full details after identifying.
        
        Args:
            query: Name or partial name to search
            limit: Max results to return (default 5)
        
        Returns: List of matching entities with id, name, summary snippet, type.
        """
        return self.store.search_entity(query, limit) or []

    async def get_profile(self, entity_name: str) -> Optional[Dict]:
        """
        Get full profile for a specific entity.
        Use when you know the exact entity name and need complete information.
        
        Args:
            entity_name: Exact canonical name of the entity
        
        Returns: Full profile with summary, type, aliases, topic, last_mentioned.
        Returns None if entity not found.
        """
        canonical = self._resolve_entity_name(entity_name)
        if not canonical:
            return None
        
        entity_id = self.resolver.get_id(canonical)
        if entity_id:
            profile = self.resolver.entity_profiles.get(entity_id)
            if profile:
                return profile
            
        return self.store.get_entity_profile(canonical)

    async def get_connections(self, entity_name: str, active_only: bool = True) -> List[Dict]:
        """
        Find all entities connected to a given entity.
        Use when asked about someone's relationships, network, or "who knows who".
        
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
        Use when asked "what happened with X recently" or "any updates on X".
        
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
        Find the shortest connection path between two entities.
        Use when asked "how is X connected to Y" or "what's the relationship between X and Y".
        Requires both entities to be known — use get_profile first if unsure.

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

    async def get_hot_topic_context(self, hot_topics: List[str]) -> Dict[str, List[Dict]]:
        """
        Retrieve pre-cached context for frequently accessed topics.
        Called automatically at start — you already have this data in hot_topic_context.
        Only call manually if hot topics changed mid-conversation.
        
        Args:
            hot_topics: List of topic names marked as "hot"
        
        Returns: Dict mapping topic name to list of top entities with summaries.
        """
        if not hot_topics:
            return {}
        return self.store.get_hot_topic_context(hot_topics)

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