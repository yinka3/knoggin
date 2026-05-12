from __future__ import annotations

import json
from typing import TYPE_CHECKING, Dict, List, Optional

if TYPE_CHECKING:
    from infrastructure.database.memgraph_client import MemgraphClient
    from knoggin.knowledge.services.embedding_service import EmbeddingService
    from knoggin.knowledge.services.entity_service import EntityManager

from common.utils.data_utils import cosine_similarity
from infrastructure.redis.redis_client import RedisKeys


class GraphTools:
    # Attributes provided by the composed Tools class
    memgraph: MemgraphClient
    entities: EntityManager
    embedding_service: EmbeddingService
    active_topics: Optional[List[str]]
    search_cfg: Dict
    user_name: str
    session_id: str

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

        results = await self.memgraph.get_related_entities(
            [canonical], active_topics=self.active_topics, limit=50
        )

        if results:
            for r in results:
                evidence_ids = r.pop("evidence_ids", [])
                string_ids = [self._format_message_id(x) for x in evidence_ids]
                r["evidence"] = await self._hydrate_evidence(string_ids)
            return results

        # Try looking without topic filtering to see if it's "hidden"
        hidden_results = await self.memgraph.get_related_entities(
            [canonical], active_topics=None
        )

        if hidden_results:
            return [
                {
                    "hidden": True,
                    "count": len(hidden_results),
                    "message": f"{len(hidden_results)} connection(s) exist through inactive topics",
                }
            ]

        return []

    async def get_recent_activity(
        self, entity_name: str, hours: int = 24
    ) -> List[Dict]:
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
        results = await self.memgraph.get_recent_activity(
            canonical, active_topics=self.active_topics, hours=hours
        )

        for r in results:
            evidence_ids = r.pop("evidence_ids", [])
            string_ids = [self._format_message_id(x) for x in evidence_ids]
            r["evidence"] = await self._hydrate_evidence(string_ids)

        return results

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
        entity_id = await self.entities.get_id(entity_name)

        if entity_id is not None:
            facts = await self.memgraph.get_facts_for_entity(
                entity_id, active_only=False
            )

            profile = await self.entities.get_profile(entity_id)
            canonical = profile["canonical_name"] if profile else entity_name

            return {
                "resolution": "exact",
                "results": [
                    {
                        "entity_name": canonical,
                        "similarity": 1.0,
                        "facts": [f.to_dict() for f in (facts or [])],
                    }
                ],
            }

        embedding = await self.embedding_service.encode_single(entity_name)

        candidates = await self.memgraph.search_entities_by_embedding(
            embedding, limit=5, score_threshold=0.69
        )

        if candidates:
            candidate_ids = [eid for eid, _ in candidates]
            similarity_map = {eid: sim for eid, sim in candidates}

            facts_by_entity = await self.memgraph.get_facts_for_entities(
                candidate_ids, active_only=False
            )

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
                profile = await self.entities.get_profile(eid)
                canonical = profile["canonical_name"] if profile else str(eid)

                results.append(
                    {
                        "entity_name": canonical,
                        "similarity": similarity_map[eid],
                        "facts": [f.to_dict() for f in facts_by_entity.get(eid, [])],
                    }
                )

            return {"resolution": "vector", "results": results}

        fallback = await self.search_messages(query)
        return {"resolution": "fallback", "results": fallback}

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
        path, has_inactive_shortcut = await self.memgraph.find_path_filtered(
            canonical_a, canonical_b, active_topics=self.active_topics, max_depth=4
        )

        if path:
            for step in path:
                evidence_refs = step.pop("evidence_refs", [])
                string_ids = [self._format_message_id(x) for x in evidence_refs]
                step["evidence"] = await self._hydrate_evidence(string_ids)
            if has_inactive_shortcut:
                path.append(
                    {"note": "A shorter connection exists through inactive topics"}
                )
            return path

        if has_inactive_shortcut:
            full_path, _ = await self.memgraph.find_path_filtered(
                canonical_a, canonical_b, active_topics=None, max_depth=4
            )

            safe_path = []
            for step in full_path:
                topic_a = step.get("topic_a", "General")
                topic_b = step.get("topic_b", "General")

                both_active = (
                    self.active_topics is not None
                    and topic_a in self.active_topics
                    and topic_b in self.active_topics
                )

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

                    safe_path.append(
                        {
                            "step": step.get("step"),
                            "entity_a": step.get("entity_a"),
                            "entity_b": step.get("entity_b"),
                            "topic_a": topic_a,
                            "topic_b": topic_b,
                            "status": "LOCKED",
                            "locked_reason": f"Inactive topic(s): {', '.join(inactive_topics)}",
                            "evidence": [],
                        }
                    )

            return safe_path

        return []

    async def get_hierarchy(
        self, entity_name: str, direction: str = "both"
    ) -> List[Dict]:
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

        entity_id = await self.entities.get_id(canonical)
        if not entity_id:
            return []

        result = {"entity": canonical, "entity_id": entity_id}

        if direction in ("up", "both"):
            parents = await self.memgraph.get_parent_entities(entity_id)
            result["parents"] = parents

            if parents:
                ancestry = []
                current_id = entity_id
                visited = {current_id}

                while True:
                    parent_list = await self.memgraph.get_parent_entities(current_id)
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
            children = await self.memgraph.get_child_entities(entity_id)
            result["children"] = children

        return [result]

    async def get_hot_topic_context(
        self, hot_topics: List[str], slim: bool = False
    ) -> Dict[str, Dict]:
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
        raw = await self.memgraph.get_hot_topic_context_with_messages(
            hot_topics, msg_limit=10, slim=slim
        )
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
                            messages.append(
                                {"id": msg_id, "message": parsed.get("message", "")}
                            )
                        except json.JSONDecodeError:
                            continue
                data["messages"] = messages
            else:
                data["messages"] = []

            data.pop("message_ids", None)

        return raw
