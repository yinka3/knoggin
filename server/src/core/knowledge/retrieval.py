"""Project-scoped internal knowledge retrieval for agent-facing reads.

This service owns retrieval strategy over durable messages, graph observations,
and episodes.  It deliberately does not own document or external-web retrieval:
those remain separate product surfaces.
"""

from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import datetime, timezone
from time import perf_counter
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger

from common.schema.evidence import EvidenceTraversalLimits
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
        search_config: Optional[Dict] = None,
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
        self.search_cfg = search_config or {}

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
        query = self._require_query(query, "search_messages")
        limit = self._positive_int(
            limit,
            "search_messages limit",
            default=self.search_cfg.get("default_message_limit", 8),
        )
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
                self._get_surrounding_context(hit["id"], session_id=hit["session_id"])
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
        limit: Optional[int] = None,
    ) -> List[Dict]:
        """Discover visible entities before requesting a stable-ID follow-up."""
        query = self._require_query(query, "search_entities")
        limit = self._positive_int(
            limit,
            "search_entities limit",
            default=self.search_cfg.get("default_entity_limit", 5),
        )
        results = await self.knowledge_store.search_entity(
            query,
            visible_project_ids=self.readable_project_ids,
            limit=limit,
        )
        return results or []

    async def get_connections(
        self,
        entity_id: int,
        *,
        session_id: str,
        limit: int = 40,
    ) -> List[Dict]:
        session_id = require_scope_value(
            session_id, "session_id", "KnowledgeRetrieval.get_connections"
        )
        limit = self._positive_int(limit, "get_connections limit")
        if await self.entities.get_profile(entity_id) is None:
            return [{"error": f"Entity not found: '{entity_id}'"}]

        results = await self.knowledge_store.get_related_entities(
            [entity_id],
            limit=limit,
            visible_project_ids=self.readable_project_ids,
        )
        return await self._hydrate_result_evidence(results, session_id=session_id)

    async def get_recent_activity(
        self, entity_id: int, *, session_id: str, hours: Optional[int] = None
    ) -> List[Dict]:
        session_id = require_scope_value(
            session_id, "session_id", "KnowledgeRetrieval.get_recent_activity"
        )
        hours = self._positive_int(
            hours,
            "get_recent_activity hours",
            default=self.search_cfg.get("default_activity_hours", 24),
        )
        if await self.entities.get_profile(entity_id) is None:
            return [{"error": f"Entity not found: '{entity_id}'"}]
        results = await self.knowledge_store.get_recent_activity(
            entity_id,
            hours=hours,
            visible_project_ids=self.readable_project_ids,
        )
        return await self._hydrate_result_evidence(results, session_id=session_id)

    async def episode_check(
        self,
        query: str,
        *,
        session_id: str,
        entity_id: Optional[int] = None,
    ) -> Dict:
        """Retrieve episodes, then fall back to raw durable messages."""
        session_id = require_scope_value(
            session_id, "session_id", "KnowledgeRetrieval.episode_check"
        )
        query = self._require_query(query, "episode_check")
        started_at = perf_counter()

        if entity_id is not None:
            profile = await self.entities.get_profile(entity_id)
            if profile is None:
                return {"resolution": "exact", "results": []}
            episodes = await self.knowledge_store.get_project_episodes_for_entities(
                [entity_id],
                user_name=self.user_name,
                project_id=self.project_id,
                limit=DEFAULT_EPISODE_RETRIEVAL_LIMIT,
                visible_project_ids=self.readable_project_ids,
            )
            metrics: Dict[str, int | float] = {}
            serialized = await self._serialize_episodes(episodes, metrics=metrics)
            await self._emit_episode_retrieval(
                session_id=session_id,
                strategy="exact_entity",
                started_at=started_at,
                episode_count=len(episodes),
                matched_entity_episode_count=self._matched_entity_episode_count(
                    episodes, entity_id
                ),
                metrics=metrics,
            )
            return {
                "resolution": "exact",
                "results": [
                    {
                        "entity_name": (
                            profile.canonical_name if profile else str(entity_id)
                        ),
                        "entity_id": entity_id,
                        "similarity": 1.0,
                        "episodes": serialized,
                    }
                ],
            }

        # Both channels contribute: a weak semantic hit must not suppress an
        # exact keyword match. Retrieve a bounded pool before rank fusion.
        candidate_limit = DEFAULT_EPISODE_RETRIEVAL_LIMIT * 3
        lexical = await self.knowledge_store.search_project_episodes(
            query,
            user_name=self.user_name,
            project_id=self.project_id,
            limit=candidate_limit,
            visible_project_ids=self.readable_project_ids,
        )
        semantic_matches = []
        if self.embedding_service is not None:
            try:
                embedding = await self.embedding_service.encode_query(query)
                semantic_matches = (
                    await self.knowledge_store.search_project_episodes_by_embedding(
                        embedding,
                        user_name=self.user_name,
                        project_id=self.project_id,
                        limit=candidate_limit,
                        visible_project_ids=self.readable_project_ids,
                    )
                )
            except Exception as exc:
                logger.warning(
                    "Episode semantic search unavailable; using lexical results: {}",
                    exc,
                )

        scores = {}
        by_id = {}
        for channel in (lexical, [episode for episode, _ in semantic_matches]):
            seen = set()
            for rank, episode in enumerate(channel, start=1):
                key = episode.episode_id
                if key in seen:
                    continue
                seen.add(key)
                by_id[key] = episode
                scores[key] = scores.get(key, 0.0) + 1.0 / (60 + rank)
        # Stable ties preserve lexical candidates first, without imposing a
        # blanket recency penalty on legitimate historical questions.
        episodes = [by_id[key] for key in sorted(scores, key=lambda key: -scores[key])][
            :DEFAULT_EPISODE_RETRIEVAL_LIMIT
        ]
        if episodes:
            metrics: Dict[str, int | float] = {}
            serialized = await self._serialize_episodes(
                episodes,
                similarity_by_episode={
                    episode.episode_id: score for episode, score in semantic_matches
                },
                metrics=metrics,
            )
            strategy = (
                "hybrid"
                if lexical and semantic_matches
                else "lexical"
                if lexical
                else "semantic"
            )
            await self._emit_episode_retrieval(
                session_id=session_id,
                strategy=strategy,
                started_at=started_at,
                episode_count=len(episodes),
                matched_entity_episode_count=0,
                metrics=metrics,
            )
            return {
                "resolution": strategy,
                "results": [{"query": query, "episodes": serialized}],
            }

        fallback = await self.search_messages(query, session_id=session_id)
        await self._emit_episode_retrieval(
            session_id=session_id,
            strategy="raw_message_fallback",
            started_at=started_at,
            episode_count=0,
            matched_entity_episode_count=0,
            metrics={"used_raw_message_fallback": 1},
        )
        return {"resolution": "fallback", "results": fallback}

    async def read_episode(self, episode_id: str, *, session_id: str) -> List[Dict]:
        session_id = require_scope_value(
            session_id, "session_id", "KnowledgeRetrieval.read_episode"
        )
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
        session_id = require_scope_value(
            session_id, "session_id", "KnowledgeRetrieval.read_recent_episodes"
        )
        limit = self._positive_int(limit, "read_recent_episodes limit")
        effective_limit = min(limit, DEFAULT_EPISODE_RETRIEVAL_LIMIT)
        started_at = perf_counter()
        episodes = await self.knowledge_store.get_recent_project_episodes(
            user_name=self.user_name,
            project_id=self.project_id,
            limit=effective_limit,
            visible_project_ids=self.readable_project_ids,
        )
        metrics: Dict[str, int | float] = {}
        serialized = await self._serialize_episodes(episodes, metrics=metrics)
        await self._emit_episode_retrieval(
            session_id=session_id,
            strategy="recent",
            started_at=started_at,
            episode_count=len(episodes),
            matched_entity_episode_count=0,
            metrics=metrics,
        )
        return {
            "resolution": "recent",
            "results": [
                {
                    "query": f"{effective_limit} most recent episodes by source chronology",
                    "episodes": serialized,
                }
            ],
        }

    async def find_path(
        self, entity_a_id: int, entity_b_id: int, *, session_id: str
    ) -> List[Dict]:
        session_id = require_scope_value(
            session_id, "session_id", "KnowledgeRetrieval.find_path"
        )
        entity_a = await self.entities.get_profile(entity_a_id)
        entity_b = await self.entities.get_profile(entity_b_id)
        if entity_a is None and entity_b is None:
            return [
                {"error": f"Neither entity found: '{entity_a_id}' and '{entity_b_id}'"}
            ]
        if entity_a is None:
            return [{"error": f"Entity not found: '{entity_a_id}'"}]
        if entity_b is None:
            return [{"error": f"Entity not found: '{entity_b_id}'"}]

        path = await self.knowledge_store.find_path(
            entity_a_id,
            entity_b_id,
            max_depth=4,
            visible_project_ids=self.readable_project_ids,
        )
        return await self._hydrate_result_evidence(path, session_id=session_id)

    async def read_observation_evidence(self, observation_id: int) -> Dict:
        """Expand one path observation through the scoped evidence traversal."""

        if (
            not isinstance(observation_id, int)
            or isinstance(observation_id, bool)
            or observation_id <= 0
        ):
            raise ValueError("observation_id must be a positive integer")
        bundle = await self.knowledge_store.get_visible_relationship_observation_evidence(
            observation_id,
            user_name=self.user_name,
            visible_project_ids=self.readable_project_ids,
            limits=EvidenceTraversalLimits(
                max_observations=1,
                max_context_blocks=4,
                max_leaf_evidence=8,
                max_edges=16,
            ),
        )
        return (
            bundle.model_dump(mode="json")
            if hasattr(bundle, "model_dump")
            else dict(bundle)
        )

    async def get_hot_topic_context(
        self, hot_topics: List[str], *, session_id: str
    ) -> Dict[str, Dict]:
        session_id = require_scope_value(
            session_id, "session_id", "KnowledgeRetrieval.get_hot_topic_context"
        )
        if not hot_topics:
            return {}
        raw = await self.knowledge_store.get_hot_topic_context_with_messages(
            hot_topics,
            msg_limit=5,
            project_id=self.project_id,
        )
        for data in raw.values():
            refs = data.get("message_refs", data.get("message_ids", []))
            data["messages"] = await self._hydrate_evidence(refs, session_id=session_id)
            data.pop("message_refs", None)
            data.pop("message_ids", None)
        return raw

    async def _search_messages(
        self, query: str, *, session_id: str, k: int
    ) -> List[Tuple[str, float, Optional[str]]]:
        fts_limit = self.search_cfg.get("fts_limit", 50)
        semantic_limit = self.search_cfg.get("semantic_message_limit", fts_limit)
        semantic_threshold = self.search_cfg.get("semantic_message_threshold", 0.25)
        rerank_candidates = self.search_cfg.get("rerank_candidates", 25)
        visible_sessions = await self.knowledge_store.get_visible_session_ids(
            user_name=self.user_name,
            visible_project_ids=self.readable_project_ids,
        )
        async def semantic_search():
            query_embedding = await self.embedding_service.encode_query(query)
            return await self.knowledge_store.search_messages_semantic(
                query_embedding,
                user_name=self.user_name,
                session_ids=visible_sessions,
                visible_project_ids=self.readable_project_ids,
                limit=semantic_limit,
                threshold=semantic_threshold,
            )

        lexical_result, semantic_result = await asyncio.gather(
            self.knowledge_store.search_messages_fts(
                query,
                user_name=self.user_name,
                session_ids=visible_sessions,
                visible_project_ids=self.readable_project_ids,
                limit=fts_limit,
            ),
            semantic_search(),
            return_exceptions=True,
        )
        if isinstance(lexical_result, Exception):
            raise lexical_result
        fts_results = lexical_result
        if isinstance(semantic_result, Exception):
            logger.warning(
                "Semantic message candidate search failed; using lexical candidates: {}",
                semantic_result,
            )
            semantic_results = []
        else:
            semantic_results = semantic_result

        channel_scores: dict[tuple[str, str], list[float]] = {}
        for channel in (fts_results, semantic_results):
            maximum = max((score for _, score, _ in channel), default=0.0)
            if maximum <= 0:
                continue
            for message_id, raw_score, result_session_id in channel:
                key = (result_session_id, self._format_message_id(message_id))
                channel_scores.setdefault(key, []).append(float(raw_score) / maximum)

        results = {
            key: (min(1.0, max(scores) + 0.15 * min(scores)), key[0])
            for key, scores in channel_scores.items()
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
                if len(scores) != len(candidate_keys):
                    raise ValueError(
                        "Message reranker returned an unexpected score count"
                    )
                return [
                    (message_key, float(score), result_session_id)
                    for (result_session_id, message_key), score in sorted(
                        zip(candidate_keys, scores),
                        key=lambda item: item[1],
                        reverse=True,
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
        requested_indexes: Dict[tuple[str, str, int], List[int]] = {}
        for item in normalized:
            grouped.setdefault((item["user_name"], item["session_id"]), []).append(item)
            requested_indexes.setdefault(
                (item["user_name"], item["session_id"], item["message_id"]),
                [],
            ).append(item["idx"])

        results_by_idx: Dict[int, Dict] = {}
        for (user_name, reference_session_id), items in grouped.items():
            durable = await self.knowledge_store.get_messages_by_ids(
                list(dict.fromkeys(item["message_id"] for item in items)),
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
                key = (
                    str(message.get("user_name") or user_name),
                    str(message.get("session_id") or reference_session_id),
                    int(message["id"]),
                )
                hydrated = {
                    "id": f"msg_{message['id']}",
                    "user_name": message.get("user_name") or user_name,
                    "session_id": message.get("session_id") or reference_session_id,
                    "message": message["content"],
                    "timestamp": rendered_timestamp,
                }
                if message.get("role") is not None:
                    hydrated["role"] = message["role"]
                for index in requested_indexes.get(key, ()):
                    results_by_idx[index] = dict(hydrated)
        return [results_by_idx[index] for index in sorted(results_by_idx)]

    async def _hydrate_result_evidence(
        self,
        results: List[Dict],
        *,
        session_id: str,
    ) -> List[Dict]:
        """Hydrate message and observation support without changing its meaning."""

        detached_results = [deepcopy(result) for result in results]
        message_refs_by_result: list[list] = []
        observation_refs_by_result: list[list[dict]] = []
        all_message_refs: list = []
        for result in detached_results:
            refs = result.pop("evidence_refs", None)
            if refs is None:
                refs = result.pop("evidence_ids", [])
            else:
                result.pop("evidence_ids", None)
            message_refs, observation_refs = self._split_evidence_refs(refs)
            message_refs_by_result.append(message_refs)
            all_message_refs.extend(message_refs)
            observation_refs_by_result.append(observation_refs)

        hydrated_messages, observation_bundles = await asyncio.gather(
            self._hydrate_evidence(all_message_refs, session_id=session_id),
            self._hydrate_observation_evidence(observation_refs_by_result),
        )
        messages_by_key = {
            self._message_evidence_key(message): message
            for message in hydrated_messages
        }
        for result, message_refs, bundles in zip(
            detached_results,
            message_refs_by_result,
            observation_bundles,
        ):
            messages = []
            for ref in message_refs:
                normalized = self._normalize_evidence_ref(ref, session_id=session_id)
                if normalized is None:
                    continue
                message = messages_by_key.get(
                    (
                        normalized["user_name"],
                        normalized["session_id"],
                        normalized["message_id"],
                    )
                )
                if message is not None:
                    messages.append(deepcopy(message))
            result["evidence"] = [*messages, *bundles]
        return detached_results

    def _split_evidence_refs(self, refs: Any) -> tuple[list, list[dict]]:
        if not isinstance(refs, list):
            return [], []
        message_refs: list = []
        observation_refs: list[dict] = []
        for ref in refs:
            if isinstance(ref, dict) and ref.get("kind") == "relationship_observation":
                observation_refs.append(self._normalize_observation_ref(ref))
            else:
                message_refs.append(ref)
        return message_refs, observation_refs

    def _normalize_observation_ref(self, ref: Dict) -> dict:
        observation_id = ref.get("observation_id")
        project_id = ref.get("project_id")
        user_name = ref.get("user_name")
        if (
            not isinstance(observation_id, int)
            or isinstance(observation_id, bool)
            or observation_id <= 0
            or not isinstance(project_id, str)
            or project_id not in self.readable_project_ids
            or user_name != self.user_name
        ):
            raise ValueError("relationship observation evidence is outside read scope")
        return {"observation_id": observation_id, "project_id": project_id}

    async def _hydrate_observation_evidence(
        self,
        refs_by_result: list[list[dict]],
    ) -> list[list[dict]]:
        requested_by_project: dict[str, set[int]] = {}
        for refs in refs_by_result:
            for ref in refs:
                requested_by_project.setdefault(ref["project_id"], set()).add(
                    ref["observation_id"]
                )

        bundles_by_observation: dict[tuple[str, int], dict] = {}
        for project_id, observation_ids in requested_by_project.items():
            bundles = await self.knowledge_store.get_relationship_observations_evidence(
                sorted(observation_ids),
                user_name=self.user_name,
                project_id=project_id,
            )
            for bundle in bundles:
                serialized = (
                    bundle.model_dump(mode="json")
                    if hasattr(bundle, "model_dump")
                    else dict(bundle)
                )
                subject = serialized.get("subject", {})
                if subject.get("kind") != "relationship_observation":
                    raise ValueError("observation evidence returned an invalid subject")
                try:
                    observation_id = int(subject["identifier"])
                except (KeyError, TypeError, ValueError) as exc:
                    raise ValueError(
                        "observation evidence returned an invalid identifier"
                    ) from exc
                bundles_by_observation[(project_id, observation_id)] = serialized

        hydrated: list[list[dict]] = []
        for refs in refs_by_result:
            result_bundles = []
            for ref in refs:
                key = (ref["project_id"], ref["observation_id"])
                bundle = bundles_by_observation.get(key)
                if bundle is None:
                    raise ValueError("relationship observation evidence is unavailable")
                result_bundles.append(bundle)
            hydrated.append(result_bundles)
        return hydrated

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
            discoverable_only=True,
        )
        return [
            {
                "role": message["role"],
                "timestamp": (
                    datetime.fromtimestamp(
                        message["timestamp"] / 1000.0, timezone.utc
                    ).isoformat()
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
        if user_name != self.user_name:
            raise ValueError("message evidence is outside retrieval user scope")
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

    @staticmethod
    def _message_evidence_key(message: Dict) -> tuple[str, str, int]:
        return (
            str(message.get("user_name") or ""),
            str(message.get("session_id") or ""),
            KnowledgeRetrieval._parse_message_ref_id(message.get("id")),
        )

    async def _serialize_episodes(
        self,
        episodes,
        *,
        similarity_by_episode: Optional[Dict[str, float]] = None,
        metrics: Optional[Dict[str, int | float]] = None,
    ) -> List[Dict]:
        serialized = []
        for episode in episodes or []:
            sources_consulted = (
                await self.knowledge_store.get_project_episode_source_refs(
                    episode.episode_id,
                    user_name=self.user_name,
                    project_id=episode.project_id,
                )
            )
            item = {
                "episode_id": episode.episode_id,
                "summary": episode.summary,
                "new_developments": episode.new_developments,
                "updates": episode.updates,
                "unresolved": episode.unresolved,
                "source_message_count": episode.source_message_count,
                "first_message_at": (
                    episode.first_message_at.isoformat()
                    if episode.first_message_at
                    else None
                ),
                "last_message_at": (
                    episode.last_message_at.isoformat()
                    if episode.last_message_at
                    else None
                ),
                "entities": [
                    {
                        "entity_id": entity.entity_id,
                        "source_message_count": entity.source_message_count,
                        "first_seen_at": entity.first_seen_at.isoformat()
                        if entity.first_seen_at
                        else None,
                        "last_seen_at": entity.last_seen_at.isoformat()
                        if entity.last_seen_at
                        else None,
                    }
                    for entity in episode.entities
                ],
                "relationships": [
                    {
                        "relationship_id": relationship.relationship_id,
                        "source_message_count": relationship.source_message_count,
                    }
                    for relationship in episode.relationships
                ],
                # Source messages are intentionally omitted from discovery;
                # read_episode is the explicit hydration follow-up.
                "evidence": [],
                "sources_consulted": [
                    source.model_dump(mode="json")
                    if hasattr(source, "model_dump")
                    else source
                    for source in sources_consulted
                ],
            }
            if similarity_by_episode and episode.episode_id in similarity_by_episode:
                item["similarity"] = similarity_by_episode[episode.episode_id]
            serialized.append(item)
        if metrics is not None:
            metrics["source_message_expansion_skipped_count"] = int(
                metrics.get("source_message_expansion_skipped_count", 0)
            ) + len(serialized)
        return serialized

    async def _emit_episode_retrieval(
        self,
        *,
        session_id: str,
        strategy: str,
        started_at: float,
        episode_count: int,
        matched_entity_episode_count: int,
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
                "matched_entity_episode_count": matched_entity_episode_count,
                "entity_retrieval": strategy == "exact_entity",
                "retrieval_latency_ms": round((perf_counter() - started_at) * 1000, 3),
                **metrics,
            },
        )

    @staticmethod
    def _format_message_id(message_id: Any) -> str:
        return message_id if isinstance(message_id, str) else f"msg_{message_id}"

    @staticmethod
    def _positive_int(value: Any, name: str, *, default: Any = None) -> int:
        value = default if value is None else value
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
        return value

    @staticmethod
    def _require_query(value: Any, operation: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{operation} query must be a non-blank string")
        return value.strip()

    @staticmethod
    def _parse_message_ref_id(raw_id: Any) -> int:
        if isinstance(raw_id, str):
            if raw_id.startswith("msg_"):
                return int(raw_id.split("_", 1)[1])
            if raw_id.startswith("turn_"):
                raise ValueError("Conversation turn IDs are not canonical message IDs")
        return int(raw_id)

    @staticmethod
    def _matched_entity_episode_count(episodes, entity_id: int) -> int:
        return sum(
            any(entity.entity_id == entity_id for entity in episode.entities)
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
            "attached_at": (
                source["attached_at"].isoformat()
                if source.get("attached_at")
                and hasattr(source["attached_at"], "isoformat")
                else source.get("attached_at")
            ),
            "score": 1.0,
            "context": [
                {
                    "role": source.get("role", "assistant"),
                    "timestamp": source.get("timestamp_ms", ""),
                    "content": source.get("content", ""),
                    "is_hit": True,
                }
            ],
        }
