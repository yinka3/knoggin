"""Project-scoped internal knowledge retrieval for agent-facing reads.

This service owns retrieval strategy over durable messages, graph observations,
and episodes.  It deliberately does not own document or external-web retrieval:
those remain separate product surfaces.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from time import perf_counter
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger

from common.scoping import require_scope_value, require_visible_project_ids
from common.utils.events import emit

DEFAULT_EPISODE_RETRIEVAL_LIMIT = 5


class KnowledgeRetrieval:
    """One project-scoped policy boundary for internal knowledge reads.

    ``project_id`` remains the active project for episode ownership.  The
    directional ``readable_project_ids`` set governs all cross-project reads.
    Methods accept a session ID only where message context or telemetry needs
    it; the service itself is intentionally not session-owned.
    """

    def __init__(
        self,
        *,
        project_id: str,
        readable_project_ids: List[str],
        user_name: str,
        entities,
        embedding_service,
        knowledge_store,
        postgres,
        search_config: Optional[Dict] = None,
        active_topics: Optional[List[str]] = None,
    ) -> None:
        self.project_id = require_scope_value(
            project_id, "project_id", "KnowledgeRetrieval"
        )
        self.readable_project_ids = require_visible_project_ids(
            readable_project_ids, "KnowledgeRetrieval"
        )
        self.user_name = require_scope_value(
            user_name, "user_name", "KnowledgeRetrieval"
        )
        self.entities = entities
        self.embedding_service = embedding_service
        self.knowledge_store = knowledge_store
        self.postgres = postgres
        self.search_cfg = search_config or {}
        self.active_topics = list(active_topics) if active_topics else None

    def set_active_topics(self, active_topics: List[str]) -> None:
        """Install the current project's immutable domain topic snapshot."""
        self.active_topics = list(active_topics)

    async def search_messages(
        self,
        query: str,
        *,
        session_id: str,
        limit: Optional[int] = None,
    ) -> List[Dict]:
        """Search durable message memory and expand bounded context."""
        session_id = require_scope_value(
            session_id, "session_id", "KnowledgeRetrieval.search_messages"
        )
        limit = limit or self.search_cfg.get("default_message_limit", 8)
        results = await self._search_messages(query, session_id=session_id, k=limit)
        if not results:
            return []

        hits = [
            {
                "id": msg_key,
                "score": score,
                "user_name": self.user_name,
                "session_id": result_session_id or session_id,
            }
            for msg_key, score, result_session_id in results
        ]
        contexts = await asyncio.gather(
            *[
                self._get_surrounding_context(
                    hit["id"], session_id=hit["session_id"]
                )
                for hit in hits
            ]
        )

        seen_turns = set()
        output = []
        for hit_result, context in zip(hits, contexts):
            msg_key = hit_result["id"]
            result_session_id = hit_result["session_id"]
            hit = next((message for message in context if message.get("is_hit")), None)
            if not hit:
                continue

            turn_marker = f"{result_session_id}:{hit['id']}"
            if turn_marker in seen_turns:
                continue
            seen_turns.add(turn_marker)
            output.append(
                {
                    "id": msg_key,
                    "user_name": self.user_name,
                    "session_id": result_session_id,
                    "role": hit.get("role", "user"),
                    "message": hit.get("content", ""),
                    "timestamp": hit.get("timestamp", ""),
                    "score": hit_result["score"],
                    "context": context,
                }
            )
        return output

    async def search_entities(
        self,
        query: str,
        *,
        session_id: str,
        limit: Optional[int] = None,
    ) -> List[Dict]:
        """Search visible entities and hydrate their relationship evidence."""
        limit = limit or self.search_cfg.get("default_entity_limit", 5)
        results = await self.knowledge_store.search_entity(
            query,
            visible_project_ids=self.readable_project_ids,
            active_topics=self.active_topics,
            limit=limit,
        )
        for entity in results or []:
            for connection in entity.get("top_connections", []):
                refs = connection.pop(
                    "evidence_refs", connection.pop("evidence_ids", [])
                )
                connection["evidence"] = await self._hydrate_evidence(
                    refs, session_id=session_id
                )
        return results or []

    async def get_connections(
        self, entity_name: str, *, session_id: str
    ) -> List[Dict]:
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
            for result in results:
                refs = result.pop(
                    "evidence_refs", result.pop("evidence_ids", [])
                )
                result["evidence"] = await self._hydrate_evidence(
                    refs, session_id=session_id
                )
            return results

        hidden_results = await self.knowledge_store.get_related_entities(
            [canonical], active_topics=None, visible_project_ids=self.readable_project_ids
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
        self, entity_name: str, *, session_id: str, hours: int = 24
    ) -> List[Dict]:
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
        for result in results:
            refs = result.pop("evidence_refs", result.pop("evidence_ids", []))
            result["evidence"] = await self._hydrate_evidence(
                refs, session_id=session_id
            )
        return results

    async def episode_check(
        self,
        query: str,
        *,
        session_id: str,
        entity_name: Optional[str] = None,
    ) -> Dict:
        """Retrieve episodes, then fall back to raw durable messages."""
        entity_name = (entity_name or "").strip()
        query = query.strip()
        started_at = perf_counter()
        entity_id = await self.entities.get_id(entity_name) if entity_name else None

        if entity_id is not None:
            episodes = await self.knowledge_store.get_project_episodes_for_entities(
                [entity_id],
                user_name=self.user_name,
                project_id=self.project_id,
                session_id=session_id,
                limit=self._episode_retrieval_limit(),
                visible_project_ids=self.readable_project_ids,
            )
            profile = await self.entities.get_profile(entity_id)
            metrics: Dict[str, int | float] = {}
            serialized = await self._serialize_episodes(
                episodes, session_id=session_id, metrics=metrics
            )
            await self._emit_episode_retrieval(
                session_id=session_id,
                strategy="exact_entity",
                started_at=started_at,
                episode_count=len(episodes),
                focus_episode_count=self._focus_episode_count(episodes, entity_id),
                metrics=metrics,
            )
            return {
                "resolution": "exact",
                "results": [
                    {
                        "entity_name": (
                            profile.canonical_name if profile else entity_name
                        ),
                        "similarity": 1.0,
                        "episodes": serialized,
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
                metrics: Dict[str, int | float] = {}
                results = []
                episode_count = 0
                focus_episode_count = 0
                for entity_id, similarity in candidates:
                    profile = await self.entities.get_profile(entity_id)
                    episodes = await self.knowledge_store.get_project_episodes_for_entities(
                        [entity_id],
                        user_name=self.user_name,
                        project_id=self.project_id,
                        limit=self._episode_retrieval_limit(),
                        visible_project_ids=self.readable_project_ids,
                    )
                    episode_count += len(episodes)
                    focus_episode_count += self._focus_episode_count(
                        episodes, entity_id
                    )
                    results.append(
                        {
                            "entity_name": (
                                profile.canonical_name if profile else str(entity_id)
                            ),
                            "similarity": similarity,
                            "episodes": await self._serialize_episodes(
                                episodes, session_id=session_id, metrics=metrics
                            ),
                        }
                    )
                await self._emit_episode_retrieval(
                    session_id=session_id,
                    strategy="vector_entity",
                    started_at=started_at,
                    episode_count=episode_count,
                    focus_episode_count=focus_episode_count,
                    metrics=metrics,
                )
                return {"resolution": "vector", "results": results}

        if self.embedding_service is not None:
            embedding = await self.embedding_service.encode_single(query)
            semantic_matches = await self.knowledge_store.search_project_episodes_by_embedding(
                embedding,
                user_name=self.user_name,
                project_id=self.project_id,
                limit=self._episode_retrieval_limit(),
                visible_project_ids=self.readable_project_ids,
            )
            if semantic_matches:
                episodes, similarities = zip(*semantic_matches)
                metrics: Dict[str, int | float] = {}
                serialized = await self._serialize_episodes(
                    episodes,
                    session_id=session_id,
                    similarity_by_episode={
                        episode.episode_id: similarity
                        for episode, similarity in zip(episodes, similarities)
                    },
                    metrics=metrics,
                )
                await self._emit_episode_retrieval(
                    session_id=session_id,
                    strategy="semantic",
                    started_at=started_at,
                    episode_count=len(episodes),
                    focus_episode_count=0,
                    metrics=metrics,
                )
                return {
                    "resolution": "semantic",
                    "results": [{"query": query, "episodes": serialized}],
                }

        episodes = await self.knowledge_store.search_project_episodes(
            query,
            user_name=self.user_name,
            project_id=self.project_id,
            limit=self._episode_retrieval_limit(),
            visible_project_ids=self.readable_project_ids,
        )
        if episodes:
            metrics: Dict[str, int | float] = {}
            serialized = await self._serialize_episodes(
                episodes, session_id=session_id, metrics=metrics
            )
            await self._emit_episode_retrieval(
                session_id=session_id,
                strategy="lexical",
                started_at=started_at,
                episode_count=len(episodes),
                focus_episode_count=0,
                metrics=metrics,
            )
            return {
                "resolution": "question",
                "results": [{"query": query, "episodes": serialized}],
            }

        fallback = await self.search_messages(query, session_id=session_id)
        await self._emit_episode_retrieval(
            session_id=session_id,
            strategy="raw_message_fallback",
            started_at=started_at,
            episode_count=0,
            focus_episode_count=0,
            metrics={"used_raw_message_fallback": 1},
        )
        return {"resolution": "fallback", "results": fallback}

    async def read_episode(self, episode_id: str, *, session_id: str) -> List[Dict]:
        episode = await self.knowledge_store.get_project_episode(
            episode_id,
            user_name=self.user_name,
            project_id=self.project_id,
            visible_project_ids=self.readable_project_ids,
        )
        if episode is None:
            return []
        started_at = perf_counter()
        sources = await self.knowledge_store.get_project_episode_source_messages(
            episode.episode_id,
            user_name=self.user_name,
            project_id=self.project_id,
            visible_project_ids=self.readable_project_ids,
        )
        await emit(
            session_id,
            "agent",
            "episode_source_messages_expanded",
            {
                "project_id": self.project_id,
                "session_id": session_id,
                "episode_id": episode.episode_id,
                "source_message_count": len(sources),
                "source_message_expansion_latency_ms": round(
                    (perf_counter() - started_at) * 1000, 3
                ),
            },
        )
        return [self._as_message_evidence(source) for source in sources]

    async def read_recent_episodes(self, *, session_id: str, limit: int = 2) -> Dict:
        if limit <= 0:
            raise ValueError("read_recent_episodes limit must be positive")
        effective_limit = min(limit, self._episode_retrieval_limit())
        started_at = perf_counter()
        episodes = await self.knowledge_store.get_recent_project_episodes(
            user_name=self.user_name,
            project_id=self.project_id,
            limit=effective_limit,
            visible_project_ids=self.readable_project_ids,
        )
        metrics: Dict[str, int | float] = {}
        serialized = await self._serialize_episodes(
            episodes, session_id=session_id, metrics=metrics
        )
        await self._emit_episode_retrieval(
            session_id=session_id,
            strategy="recent",
            started_at=started_at,
            episode_count=len(episodes),
            focus_episode_count=0,
            metrics=metrics,
        )
        return {
            "resolution": "recent",
            "results": [
                {
                    "query": f"{effective_limit} most recently updated episodes",
                    "episodes": serialized,
                }
            ],
        }

    async def find_path(
        self, entity_a: str, entity_b: str, *, session_id: str
    ) -> List[Dict]:
        canonical_a = await self._resolve_entity_name(entity_a)
        canonical_b = await self._resolve_entity_name(entity_b)
        if not canonical_a and not canonical_b:
            return [{"error": f"Neither entity found: '{entity_a}' and '{entity_b}'"}]
        if not canonical_a:
            return [{"error": f"Entity not found: '{entity_a}'"}]
        if not canonical_b:
            return [{"error": f"Entity not found: '{entity_b}'"}]

        path, has_inactive_shortcut = await self.knowledge_store.find_path_filtered(
            canonical_a,
            canonical_b,
            active_topics=self.active_topics,
            max_depth=4,
            visible_project_ids=self.readable_project_ids,
        )
        if path:
            for step in path:
                step["evidence"] = await self._hydrate_evidence(
                    step.pop("evidence_refs", []), session_id=session_id
                )
            if has_inactive_shortcut:
                path.append({"note": "A shorter connection exists through inactive topics"})
            return path

        if not has_inactive_shortcut:
            return []
        full_path, _ = await self.knowledge_store.find_path_filtered(
            canonical_a,
            canonical_b,
            active_topics=None,
            max_depth=4,
            visible_project_ids=self.readable_project_ids,
        )
        safe_path = []
        for step in full_path:
            topic_a, topic_b = step.get("topic_a", "General"), step.get("topic_b", "General")
            both_active = (
                self.active_topics is not None
                and topic_a in self.active_topics
                and topic_b in self.active_topics
            )
            if both_active:
                step["evidence"] = await self._hydrate_evidence(
                    step.pop("evidence_refs", []), session_id=session_id
                )
                safe_path.append(step)
                continue
            inactive = (
                [topic for topic in (topic_a, topic_b) if topic not in self.active_topics]
                if self.active_topics is not None
                else [topic_a, topic_b]
            )
            safe_path.append(
                {
                    "step": step.get("step"),
                    "entity_a": step.get("entity_a"),
                    "entity_b": step.get("entity_b"),
                    "topic_a": topic_a,
                    "topic_b": topic_b,
                    "status": "LOCKED",
                    "locked_reason": f"Inactive topic(s): {', '.join(inactive)}",
                    "evidence": [],
                }
            )
        return safe_path

    async def get_hot_topic_context(
        self, hot_topics: List[str], *, session_id: str, slim: bool = False
    ) -> Dict[str, Dict]:
        if not hot_topics:
            return {}
        raw = await self.knowledge_store.get_hot_topic_context_with_messages(
            hot_topics,
            msg_limit=5,
            visible_project_ids=self.readable_project_ids,
        )
        for data in raw.values():
            refs = data.get("message_refs", data.get("message_ids", []))
            data["messages"] = (
                []
                if slim
                else await self._hydrate_evidence(refs, session_id=session_id)
            )
            data.pop("message_refs", None)
            data.pop("message_ids", None)
        return raw

    async def _resolve_entity_name(self, entity: str) -> Optional[str]:
        return await self.entities.resolve_entity_name(entity)

    async def _search_messages(
        self, query: str, *, session_id: str, k: int
    ) -> List[Tuple[str, float, Optional[str]]]:
        fts_limit = self.search_cfg.get("fts_limit", 50)
        rerank_candidates = self.search_cfg.get("rerank_candidates", 25)
        visible_sessions = await self._get_visible_session_ids(session_id)
        fts_results = await self.knowledge_store.search_messages_fts(
            query,
            user_name=self.user_name,
            session_ids=visible_sessions,
            visible_project_ids=self.readable_project_ids,
            limit=fts_limit,
        )
        max_fts = max([score for _, score, _ in fts_results], default=1.0) or 1.0
        results = {
            (result_session_id, self._format_message_id(message_id)): (
                raw_score / max_fts if max_fts > 0 else 0.0,
                result_session_id,
            )
            for message_id, raw_score, result_session_id in fts_results
        }
        if not results:
            return []

        try:
            if len(results) > 1:
                candidates = sorted(
                    results.items(), key=lambda item: item[1][0], reverse=True
                )[:rerank_candidates]
                candidate_keys = [key for key, _ in candidates]
                hydrated = await self._hydrate_evidence(
                    [
                        {
                            "user_name": self.user_name,
                            "session_id": result_session_id or session_id,
                            "message_id": self._parse_message_ref_id(message_key),
                        }
                        for result_session_id, message_key in candidate_keys
                    ],
                    session_id=session_id,
                )
                text_by_key = {
                    (item.get("session_id"), item["id"]): item.get("message", "")
                    for item in hydrated
                }
                scores = await self.embedding_service.rerank(
                    query,
                    [
                        text_by_key.get((result_session_id, message_key), "")
                        for result_session_id, message_key in candidate_keys
                    ],
                )
                return [
                    (message_key, float(score), result_session_id)
                    for (result_session_id, message_key), score in sorted(
                        zip(candidate_keys, scores), key=lambda item: item[1], reverse=True
                    )[:k]
                ]
        except Exception as exc:
            logger.warning("Message rerank failed; using lexical scores: {}", exc)

        return [
            (message_key, score, stored_session_id or result_session_id)
            for (result_session_id, message_key), (score, stored_session_id) in sorted(
                results.items(), key=lambda item: item[1][0], reverse=True
            )[:k]
        ]

    async def _hydrate_evidence(
        self, evidence_refs: List, *, session_id: str
    ) -> List[Dict]:
        if not evidence_refs:
            return []
        normalized = []
        for index, ref in enumerate(evidence_refs):
            item = self._normalize_evidence_ref(ref, session_id=session_id)
            if item:
                item["idx"] = index
                normalized.append(item)
        if not normalized:
            return []

        grouped: Dict[tuple[str, str], List[Dict]] = {}
        for item in normalized:
            grouped.setdefault((item["user_name"], item["session_id"]), []).append(item)

        results_by_idx: Dict[int, Dict] = {}
        for (user_name, reference_session_id), items in grouped.items():
            durable = await self.knowledge_store.get_messages_by_ids(
                [item["message_id"] for item in items],
                user_name=user_name,
                session_ids=[reference_session_id],
                visible_project_ids=self.readable_project_ids,
            )
            for message in durable:
                timestamp = message.get("timestamp")
                rendered_timestamp = (
                    datetime.fromtimestamp(timestamp / 1000.0, timezone.utc).isoformat()
                    if isinstance(timestamp, (int, float))
                    else ""
                )
                for item in items:
                    if (
                        item["idx"] not in results_by_idx
                        and item["user_name"] == user_name
                        and item["session_id"] == reference_session_id
                        and item["message_id"] == message["id"]
                    ):
                        hydrated = {
                            "id": f"msg_{message['id']}",
                            "user_name": message.get("user_name"),
                            "session_id": message.get("session_id"),
                            "message": message["content"],
                            "timestamp": rendered_timestamp,
                        }
                        if message.get("role") is not None:
                            hydrated["role"] = message["role"]
                        results_by_idx[item["idx"]] = hydrated
        return [results_by_idx[index] for index in sorted(results_by_idx)]

    async def _get_visible_session_ids(self, session_id: str) -> List[str]:
        visible = {session_id}
        rows = await self.postgres.fetch_all(
            """
            SELECT session_id
            FROM public.sessions
            WHERE user_name = %s
              AND project_id = ANY(%s)
            """,
            (self.user_name, self.readable_project_ids),
        )
        visible.update(str(row["session_id"]) for row in rows)
        return sorted(visible)

    async def _get_surrounding_context(
        self,
        message_key: str,
        *,
        session_id: str,
        forward: int = 3,
        target_total: int = 10,
    ) -> List[Dict]:
        if not message_key.startswith("msg_"):
            return []
        try:
            message_id = self._parse_message_ref_id(message_key)
        except (TypeError, ValueError, IndexError):
            return []
        messages = await self.knowledge_store.get_surrounding_messages(
            message_id,
            user_name=self.user_name,
            session_id=session_id,
            visible_project_ids=self.readable_project_ids,
            forward=forward,
            target_total=target_total,
        )
        return [
            {
                "role": message["role"],
                "timestamp": (
                    datetime.fromtimestamp(message["timestamp"] / 1000.0, timezone.utc).isoformat()
                    if isinstance(message.get("timestamp"), (int, float))
                    else ""
                ),
                "content": message["content"],
                "id": f"msg_{message['id']}",
                "is_hit": message["id"] == message_id,
            }
            for message in messages
        ]

    def _normalize_evidence_ref(self, ref: Any, *, session_id: str) -> Optional[Dict]:
        if isinstance(ref, dict):
            raw_id = ref.get("message_id", ref.get("id"))
            user_name = ref.get("user_name") or self.user_name
            reference_session_id = ref.get("session_id") or session_id
        else:
            raw_id = ref
            user_name = self.user_name
            reference_session_id = session_id
        if raw_id is None or not user_name or not reference_session_id:
            return None
        try:
            message_id = self._parse_message_ref_id(raw_id)
        except (TypeError, ValueError, IndexError):
            return None
        return {
            "user_name": user_name,
            "session_id": reference_session_id,
            "message_id": message_id,
            "key": self._format_message_id(message_id),
        }

    async def _serialize_episodes(
        self,
        episodes,
        *,
        session_id: str,
        similarity_by_episode: Optional[Dict[str, float]] = None,
        metrics: Optional[Dict[str, int | float]] = None,
    ) -> List[Dict]:
        serialized = []
        expansion_latency_ms = 0.0
        expanded_count = 0
        returned_count = 0
        for episode in episodes or []:
            started_at = perf_counter()
            sources = await self.knowledge_store.get_project_episode_source_messages(
                episode.episode_id,
                user_name=self.user_name,
                project_id=self.project_id,
                visible_project_ids=self.readable_project_ids,
            )
            expansion_latency_ms += (perf_counter() - started_at) * 1000
            expanded_count += len(sources)
            evidence = sorted(
                (self._as_message_evidence(source) for source in sources),
                key=lambda source: float(source.get("influence_weight", 0.0)),
                reverse=True,
            )
            returned_count += len(evidence)
            source_reader = getattr(
                self.knowledge_store, "get_project_episode_source_refs", None
            )
            sources_consulted = (
                await source_reader(
                    episode.episode_id,
                    user_name=self.user_name,
                    project_id=episode.project_id,
                )
                if callable(source_reader)
                else []
            )
            item = {
                "episode_id": episode.episode_id,
                "summary": episode.summary,
                "new_developments": episode.new_developments,
                "updates": episode.updates,
                "unresolved": episode.unresolved,
                "importance": episode.importance,
                "source_message_count": episode.source_message_count,
                "first_message_at": (
                    episode.first_message_at.isoformat() if episode.first_message_at else None
                ),
                "last_message_at": (
                    episode.last_message_at.isoformat() if episode.last_message_at else None
                ),
                "entities": [
                    {
                        "entity_id": entity.entity_id,
                        "prominence_weight": entity.prominence_weight,
                        "role": entity.role,
                        "is_focus_entity": entity.is_focus_entity,
                        "source_message_count": entity.source_message_count,
                        "first_seen_at": entity.first_seen_at.isoformat() if entity.first_seen_at else None,
                        "last_seen_at": entity.last_seen_at.isoformat() if entity.last_seen_at else None,
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
                "version_history": [
                    version.model_dump(mode="json") for version in episode.version_history
                ],
                "evidence": evidence,
                "sources_consulted": [
                    source.model_dump(mode="json") if hasattr(source, "model_dump") else source
                    for source in sources_consulted
                ],
            }
            if similarity_by_episode and episode.episode_id in similarity_by_episode:
                item["similarity"] = similarity_by_episode[episode.episode_id]
            serialized.append(item)
        if metrics is not None:
            metrics["source_message_expansion_latency_ms"] = round(
                float(metrics.get("source_message_expansion_latency_ms", 0))
                + expansion_latency_ms,
                3,
            )
            metrics["expanded_source_message_count"] = int(
                metrics.get("expanded_source_message_count", 0)
            ) + expanded_count
            metrics["returned_evidence_count"] = int(
                metrics.get("returned_evidence_count", 0)
            ) + returned_count
        return serialized

    async def _emit_episode_retrieval(
        self,
        *,
        session_id: str,
        strategy: str,
        started_at: float,
        episode_count: int,
        focus_episode_count: int,
        metrics: Dict[str, int | float],
    ) -> None:
        await emit(
            session_id,
            "agent",
            "episode_retrieval_completed",
            {
                "project_id": self.project_id,
                "session_id": session_id,
                "strategy": strategy,
                "episode_count": episode_count,
                "focus_episode_count": focus_episode_count,
                "focus_entity_retrieval": strategy in {"exact_entity", "vector_entity"},
                "retrieval_latency_ms": round((perf_counter() - started_at) * 1000, 3),
                **metrics,
            },
        )

    @staticmethod
    def _format_message_id(message_id: Any) -> str:
        return message_id if isinstance(message_id, str) else f"msg_{message_id}"

    @staticmethod
    def _parse_message_ref_id(raw_id: Any) -> int:
        if isinstance(raw_id, str):
            if raw_id.startswith("msg_"):
                return int(raw_id.split("_", 1)[1])
            if raw_id.startswith("turn_"):
                raise ValueError("Conversation turn IDs are not canonical message IDs")
        return int(raw_id)

    @staticmethod
    def _focus_episode_count(episodes, entity_id: int) -> int:
        return sum(
            any(
                entity.entity_id == entity_id and entity.is_focus_entity
                for entity in episode.entities
            )
            for episode in episodes
        )

    @staticmethod
    def _as_message_evidence(source: Dict) -> Dict:
        return {
            "id": source.get("message_id"),
            "message_id": source.get("message_id"),
            "message": source.get("content", ""),
            "content": source.get("content", ""),
            "role": source.get("role", "assistant"),
            "timestamp_ms": source.get("timestamp_ms"),
            "influence_weight": source.get("influence_weight", 0.0),
            "influence_reason": source.get("influence_reason"),
            "attached_at": (
                source["attached_at"].isoformat()
                if source.get("attached_at") and hasattr(source["attached_at"], "isoformat")
                else source.get("attached_at")
            ),
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

    @staticmethod
    def _episode_retrieval_limit() -> int:
        return DEFAULT_EPISODE_RETRIEVAL_LIMIT
