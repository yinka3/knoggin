from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Optional

if TYPE_CHECKING:
    from infrastructure.knowledge_store import KnowledgeStore
    from core.knowledge.entity.resolver import EntityResolver
    from core.knowledge.services.embedding_service import EmbeddingService

class GraphTools:
    # Attributes provided by the composed Tools class
    knowledge_store: KnowledgeStore
    entities: EntityResolver
    embedding_service: EmbeddingService
    active_topics: Optional[List[str]]
    search_cfg: Dict
    user_name: str
    session_id: str
    readable_project_ids: Optional[List[str]]

    async def get_connections(self, entity_name: str) -> List[Dict]:
        """
        Get the full relationship network for an entity.
        Returns all connections (up to 50) with evidence.
        Use for comprehensive relationship details beyond search_entity.

        Args:
            entity_name: The entity to find connections for.

        Returns:
            Connections with target entity, strength, and hydrated evidence.
        """
        canonical = await self._resolve_entity_name(entity_name)
        if not canonical:
            return [{"error": f"Entity not found: '{entity_name}'"}]

        results = await self.knowledge_store.get_related_entities(
            [canonical],
            active_topics=self.active_topics,
            limit=50,
            visible_project_ids=self.readable_project_ids,
        )

        if results:
            for r in results:
                evidence_refs = r.pop("evidence_refs", r.pop("evidence_ids", []))
                r["evidence"] = await self._hydrate_evidence(evidence_refs)
            return results

        # Try looking without topic filtering to see if it's "hidden"
        hidden_results = await self.knowledge_store.get_related_entities(
            [canonical],
            active_topics=None,
            visible_project_ids=self.readable_project_ids,
        )

        if hidden_results:
            return [
                {
                    "hidden": True,
                    "count": len(hidden_results),
                    "message": (
                        f"{len(hidden_results)} connection(s) exist through "
                        "inactive topics"
                    ),
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
        results = await self.knowledge_store.get_recent_activity(
            canonical,
            active_topics=self.active_topics,
            hours=hours,
            visible_project_ids=self.readable_project_ids,
        )

        for r in results:
            evidence_refs = r.pop("evidence_refs", r.pop("evidence_ids", []))
            r["evidence"] = await self._hydrate_evidence(evidence_refs)

        return results

    async def episode_check(
        self, query: str, entity_name: Optional[str] = None
    ) -> Dict:
        """
        Retrieve contextual episodic memory for an entity or a natural-language
        question. Uses entity-first lookup when an entity is supplied, then
        searches episode summaries before falling back to raw messages.

        Args:
            query: The question or natural-language retrieval hint.
            entity_name: Optional entity whose episodic memory to inspect.

        Returns:
            Dict with resolution method and matching episodes or search results.
        """
        entity_name = (entity_name or "").strip()
        query = query.strip()
        entity_id = await self.entities.get_id(entity_name) if entity_name else None

        if entity_id is not None:
            episodes = await self.knowledge_store.get_episodes_for_entity(
                entity_id,
                user_name=self.user_name,
                project_id=self.project_id,
                session_id=self.session_id,
            )
            profile = await self.entities.get_profile(entity_id)
            canonical = profile.canonical_name if profile else entity_name
            return {
                "resolution": "exact",
                "results": [
                    {
                        "entity_name": canonical,
                        "similarity": 1.0,
                        "episodes": await self._serialize_episodes(episodes),
                    }
                ],
            }

        if entity_name:
            embedding = await self.embedding_service.encode_single(entity_name)

            candidates = await self.knowledge_store.search_entities_by_embedding(
                embedding,
                limit=5,
                score_threshold=0.69,
                visible_project_ids=self.readable_project_ids,
            )

            if candidates:
                candidate_ids = [eid for eid, _ in candidates]
                similarity_map = {eid: sim for eid, sim in candidates}

                results = []
                for eid in candidate_ids:
                    profile = await self.entities.get_profile(eid)
                    canonical = profile.canonical_name if profile else str(eid)
                    episodes = await self.knowledge_store.get_episodes_for_entity(
                        eid,
                        user_name=self.user_name,
                        project_id=self.project_id,
                        session_id=self.session_id,
                    )

                    results.append(
                        {
                            "entity_name": canonical,
                            "similarity": similarity_map[eid],
                            "episodes": await self._serialize_episodes(episodes),
                        }
                    )

                return {"resolution": "vector", "results": results}

        episodes = await self.knowledge_store.search_episodes(
            query,
            user_name=self.user_name,
            project_id=self.project_id,
            session_id=self.session_id,
        )
        if episodes:
            return {
                "resolution": "question",
                "results": [
                    {
                        "query": query,
                        "episodes": await self._serialize_episodes(episodes),
                    }
                ],
            }

        fallback = await self.search_messages(query)
        return {"resolution": "fallback", "results": fallback}

    async def read_episode(self, episode_id: str) -> List[Dict]:
        """Expand one retrieved episode into all of its source messages."""

        episode = await self.knowledge_store.get_episode(
            episode_id,
            user_name=self.user_name,
            project_id=self.project_id,
            session_id=self.session_id,
        )
        if episode is None:
            return []
        sources = await self.knowledge_store.get_episode_source_messages(
            episode.episode_id,
            user_name=self.user_name,
            project_id=self.project_id,
            session_id=self.session_id,
        )
        return [self._as_message_evidence(source) for source in sources]

    async def _serialize_episodes(self, episodes) -> List[Dict]:
        """Expose bounded summaries with influence-ranked source evidence."""

        serialized = []
        for episode in episodes or []:
            sources = await self.knowledge_store.get_episode_source_messages(
                episode.episode_id,
                user_name=self.user_name,
                project_id=self.project_id,
                session_id=self.session_id,
            )
            evidence = sorted(
                (self._as_message_evidence(source) for source in sources),
                key=lambda source: float(source.get("influence_weight", 0.0)),
                reverse=True,
            )[:5]
            serialized.append(
                {
                    "episode_id": episode.episode_id,
                    "summary": episode.summary,
                    "new_developments": episode.new_developments,
                    "updates": episode.updates,
                    "unresolved": episode.unresolved,
                    "importance": episode.importance,
                    "entities": [
                        {
                            "entity_id": entity.entity_id,
                            "prominence_weight": entity.prominence_weight,
                            "role": entity.role,
                            "is_focus_entity": entity.is_focus_entity,
                            "source_message_count": entity.source_message_count,
                        }
                        for entity in episode.entities
                    ],
                    "relationships": [
                        {
                            "relationship_id": relationship.relationship_id,
                            "prominence_weight": relationship.prominence_weight,
                            "is_central_relationship": relationship.is_central_relationship,
                            "source_message_count": relationship.source_message_count,
                        }
                        for relationship in episode.relationships
                    ],
                    "evidence": evidence,
                }
            )
        return serialized

    @staticmethod
    def _as_message_evidence(source: Dict) -> Dict:
        """Normalize episode provenance for the agent's message formatter."""

        return {
            "id": source.get("message_id"),
            "message_id": source.get("message_id"),
            "message": source.get("content", ""),
            "content": source.get("content", ""),
            "role": source.get("role", "assistant"),
            "timestamp_ms": source.get("timestamp_ms"),
            "influence_weight": source.get("influence_weight", 0.0),
            "influence_reason": source.get("influence_reason"),
            "score": source.get("influence_weight", 0.0),
            "context": [
                {
                    "role": source.get("role", "assistant"),
                    "timestamp": source.get("timestamp_ms", ""),
                    "content": source.get("content", ""),
                    "is_hit": True,
                }
            ],
        }

    async def find_path(self, entity_a: str, entity_b: str) -> List[Dict]:
        """
        Trace the connection chain between two specific entities.
        Use for 'how is X connected to Y' or 'what links X to Y'.
        Returns the shortest path showing each hop.
        Requires both entities to exist in memory.

        Args:
            entity_a: First entity name
            entity_b: Second entity name

        Returns:
            Step-by-step path showing each entity in the chain with evidence.
            Hidden-path marker if only inactive topics connect the entities.
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
        path, has_inactive_shortcut = await self.knowledge_store.find_path_filtered(
            canonical_a,
            canonical_b,
            active_topics=self.active_topics,
            max_depth=4,
            visible_project_ids=self.readable_project_ids,
        )

        if path:
            for step in path:
                evidence_refs = step.pop("evidence_refs", [])
                step["evidence"] = await self._hydrate_evidence(evidence_refs)
            if has_inactive_shortcut:
                path.append(
                    {"note": "A shorter connection exists through inactive topics"}
                )
            return path

        if has_inactive_shortcut:
            full_path, _ = await self.knowledge_store.find_path_filtered(
                canonical_a,
                canonical_b,
                active_topics=None,
                max_depth=4,
                visible_project_ids=self.readable_project_ids,
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
                    evidence_refs = step.pop("evidence_refs", [])
                    step["evidence"] = await self._hydrate_evidence(evidence_refs)
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
                            "locked_reason": (
                                f"Inactive topic(s): {', '.join(inactive_topics)}"
                            ),
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
            parents = await self.knowledge_store.get_parent_entities(
                entity_id,
                visible_project_ids=self.readable_project_ids,
            )
            result["parents"] = parents

            if parents:
                ancestry = []
                current_id = entity_id
                visited = {current_id}

                while True:
                    parent_list = await self.knowledge_store.get_parent_entities(
                        current_id,
                        visible_project_ids=self.readable_project_ids,
                    )
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
            children = await self.knowledge_store.get_child_entities(
                entity_id,
                visible_project_ids=self.readable_project_ids,
            )
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
        raw = await self.knowledge_store.get_hot_topic_context_with_messages(
            hot_topics,
            msg_limit=10,
            slim=slim,
            visible_project_ids=self.readable_project_ids,
        )
        for _, data in raw.items():
            message_refs = data.get("message_refs", data.get("message_ids", []))
            data["messages"] = await self._hydrate_evidence(message_refs)
            data.pop("message_refs", None)
            data.pop("message_ids", None)

        return raw
