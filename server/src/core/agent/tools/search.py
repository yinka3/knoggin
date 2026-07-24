from __future__ import annotations

import asyncio
import re
from datetime import datetime, timezone
from functools import partial
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

if TYPE_CHECKING:
    import redis.asyncio as aioredis

    from core.knowledge.documents import DocumentService
    from core.knowledge.entity.resolver import EntityResolver
    from core.knowledge.services.embedding_service import EmbeddingService
    from infrastructure.knowledge_store import KnowledgeStore
    from infrastructure.postgres_client import PostgresClient

import httpx
from loguru import logger

from common.utils.json_utils import safe_json_loads
from infrastructure.redis_client import RedisKeys

try:
    from duckduckgo_search import DDGS
except ImportError:
    DDGS = None


class SearchTools:
    redis: aioredis.Redis
    knowledge_store: KnowledgeStore
    postgres: PostgresClient
    embedding_service: EmbeddingService
    search_cfg: Dict
    document_service: Optional[DocumentService]
    document_focus: Optional[Dict] = None
    user_name: str
    session_id: str
    active_topics: Optional[List[str]]
    entities: EntityResolver
    readable_project_ids: Optional[List[str]]

    async def list_documents(
        self,
        folder_root_id: str = None,
        path_prefix: str = None,
        visibility_scope: str = None,
        limit: int = 50,
        use_focus: bool = True,
    ) -> List[Dict]:
        """List documents visible to the current project/session."""
        if not self.document_service:
            return [{"error": "No project document service available"}]
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 100
        ):
            raise ValueError("limit must be between 1 and 100")

        if (
            use_focus
            and folder_root_id is None
            and path_prefix is None
            and self.document_focus
        ):
            if self.document_focus["target_type"] == "document":
                document = await self.document_service.get_document_info(
                    session_id=self.session_id,
                    document_id=self.document_focus["document_id"],
                )
                if (
                    visibility_scope is None
                    or document["visibility_scope"] == visibility_scope
                ):
                    return [document]
                return []
            folder_root_id = self.document_focus.get("folder_root_id")
            path_prefix = self.document_focus.get("path_prefix")

        documents = await self.document_service.list_documents(
            session_id=self.session_id,
            folder_root_id=folder_root_id,
            path_prefix=path_prefix,
            visibility_scope=visibility_scope,
            limit=limit,
        )
        return documents

    async def list_folder_uploads(
        self,
        visibility_scope: str = None,
        limit: int = 25,
    ) -> List[Dict]:
        """List visible folder upload batches."""
        if not self.document_service:
            return [{"error": "No project document service available"}]
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 100
        ):
            raise ValueError("limit must be between 1 and 100")
        return await self.document_service.list_folder_uploads(
            session_id=self.session_id,
            visibility_scope=visibility_scope,
            limit=limit,
        )

    async def get_folder_upload_summary(
        self,
        folder_root_id: str = None,
        use_focus: bool = True,
    ) -> Dict:
        """Get one visible folder upload summary."""
        if not self.document_service:
            return {"error": "No project document service available"}
        explicit_folder = folder_root_id is not None
        if folder_root_id is None and use_focus and self.document_focus:
            if self.document_focus["target_type"] in (
                "folder_upload",
                "subtree",
            ):
                folder_root_id = self.document_focus["folder_root_id"]
        focus_path_prefix = None
        if (
            use_focus
            and self.document_focus
            and self.document_focus["target_type"] == "subtree"
            and not explicit_folder
            and folder_root_id == self.document_focus["folder_root_id"]
        ):
            focus_path_prefix = self.document_focus["path_prefix"]
        if folder_root_id is None:
            raise ValueError("folder_root_id is required without folder focus")
        return await self.document_service.get_folder_upload_summary(
            folder_root_id=folder_root_id,
            session_id=self.session_id,
            path_prefix=focus_path_prefix,
        )

    async def list_folder_tree(
        self,
        folder_root_id: str = None,
        path_prefix: str = None,
        max_depth: int = 3,
        use_focus: bool = True,
    ) -> List[Dict]:
        """List the visible document tree for one folder upload."""
        if not self.document_service:
            return [{"error": "No project document service available"}]
        if (
            not isinstance(max_depth, int)
            or isinstance(max_depth, bool)
            or not 1 <= max_depth <= 10
        ):
            raise ValueError("max_depth must be between 1 and 10")
        if (
            folder_root_id is None
            and use_focus
            and self.document_focus
            and self.document_focus["target_type"] in (
                "folder_upload",
                "subtree",
            )
        ):
            folder_root_id = self.document_focus["folder_root_id"]
            if path_prefix is None:
                path_prefix = self.document_focus.get("path_prefix")
        if folder_root_id is None:
            raise ValueError("folder_root_id is required without folder focus")
        return await self.document_service.list_folder_tree(
            folder_root_id=folder_root_id,
            session_id=self.session_id,
            path_prefix=path_prefix,
            max_depth=max_depth,
        )

    async def get_document_info(
        self,
        document_id: str = None,
        relative_path: str = None,
        use_focus: bool = True,
    ) -> Dict:
        """Get metadata for one visible document."""
        if not self.document_service:
            return {"error": "No project document service available"}
        if (
            document_id is None
            and relative_path is None
            and use_focus
            and self.document_focus
            and self.document_focus["target_type"] == "document"
        ):
            document_id = self.document_focus["document_id"]
        return await self.document_service.get_document_info(
            session_id=self.session_id,
            document_id=document_id,
            relative_path=relative_path,
        )

    async def read_document(
        self,
        document_id: str = None,
        relative_path: str = None,
        start_line: int = 1,
        end_line: int = None,
        use_focus: bool = True,
    ) -> List[Dict]:
        """Read a bounded line range from one visible document."""
        if not self.document_service:
            return [{"error": "No project document service available"}]
        if (
            document_id is None
            and relative_path is None
            and use_focus
            and self.document_focus
            and self.document_focus["target_type"] == "document"
        ):
            document_id = self.document_focus["document_id"]
        result = await self.document_service.read_document(
            session_id=self.session_id,
            document_id=document_id,
            relative_path=relative_path,
            start_line=start_line,
            end_line=end_line,
        )
        return [result]

    async def search_messages(self, query: str, limit: int = None) -> List[Dict]:
        """
        Search the user's actual messages by keyword or phrase.
        Use when you need their exact words, a direct quote, or when entity-based
        tools found nothing relevant.
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

        hits = [
            {
                "id": msg_key,
                "score": score,
                "user_name": self.user_name,
                "session_id": session_id or self.session_id,
            }
            for msg_key, score, session_id in results
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
            session_id = hit_result["session_id"]
            hit = next((m for m in context if m.get("is_hit")), None)
            if not hit:
                continue

            if msg_key.startswith("msg_"):
                content_key = RedisKeys.message_content(self.user_name, session_id)
                raw = await self.redis.hget(content_key, msg_key)
                data = safe_json_loads(raw) if raw else None
                if data and isinstance(data, dict):
                    hit = {
                        **hit,
                        "role": data.get("role", "user"),
                        "content": data.get("message", data.get("content", "")),
                        "timestamp": data.get("timestamp", hit.get("timestamp", "")),
                    }

            turn_marker = f"{session_id}:{hit['id']}"
            if turn_marker in seen_turns:
                continue
            seen_turns.add(turn_marker)

            output.append(
                {
                    "id": msg_key,
                    "user_name": self.user_name,
                    "session_id": session_id,
                    "role": hit.get("role", "user"),
                    "message": hit.get("content", ""),
                    "timestamp": hit.get("timestamp", ""),
                    "score": hit_result["score"],
                    "context": context,
                }
            )

        return output

    async def search_entity(self, query: str, limit: int = None) -> List[Dict]:
        """
        Find a person, place, or thing by name.
        Returns their full profile and their five strongest connections.
        Connections include canonical name and aliases; search a connection's name
        to retrieve its full profile.

        Args:
            query: Name or partial name to search
            limit: Max results to return (default 5)

        Returns:
            Matching entities with ID, name, summary, type, and top connections.
        """
        limit = limit or self.search_cfg.get("default_entity_limit", 5)
        results = await self.knowledge_store.search_entity(
            query,
            visible_project_ids=self.readable_project_ids,
            active_topics=self.active_topics,
            limit=limit,
        )

        if not results:
            return []

        for entity in results:
            for conn in entity.get("top_connections", []):
                evidence_refs = conn.pop("evidence_refs", conn.pop("evidence_ids", []))
                conn["evidence"] = await self._hydrate_evidence(evidence_refs)

        return results

    async def search_documents(
        self,
        query: str,
        document_name: str = None,
        relative_path: str = None,
        path_prefix: str = None,
        folder_root_id: str = None,
        limit: int = 5,
        use_focus: bool = True,
    ) -> List[Dict]:
        """
        Search indexed documents visible to the current project and session.

        Args:
            query: What to search for
            document_name: Optional document name to restrict search
            relative_path: Optional exact path to restrict search
            path_prefix: Optional subtree to restrict search
            folder_root_id: Optional folder upload batch
            limit: Max chunks to return

        Returns:
            Matching chunks with document metadata and relevance scores.
        """
        if not self.document_service:
            return [{"error": "No project document service available"}]
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 50
        ):
            raise ValueError("limit must be between 1 and 50")
        if document_name is not None and relative_path is not None:
            raise ValueError(
                "document_name and relative_path are mutually exclusive"
            )
        if path_prefix is not None and (
            document_name is not None or relative_path is not None
        ):
            raise ValueError(
                "path_prefix cannot be combined with an exact document selector"
            )

        document_filter = None
        if (
            use_focus
            and document_name is None
            and relative_path is None
            and path_prefix is None
            and folder_root_id is None
            and self.document_focus
        ):
            if self.document_focus["target_type"] == "document":
                document_filter = self.document_focus["document_id"]
            else:
                folder_root_id = self.document_focus.get("folder_root_id")
                path_prefix = self.document_focus.get("path_prefix")

        if document_filter is not None:
            focused_document = await self.document_service.get_document_info(
                session_id=self.session_id,
                document_id=document_filter,
            )
            visible_documents = [focused_document]
        else:
            visible_documents = await self.document_service.list_documents(
                session_id=self.session_id,
                folder_root_id=folder_root_id,
                path_prefix=path_prefix,
                limit=1000,
            )
        documents = [
            document
            for document in visible_documents
            if document.get("status") == "indexed"
        ]

        if not documents:
            return [{"error": "No indexed documents available in this project"}]

        if document_name:
            requested = document_name.lower()
            path_matches = [
                document
                for document in documents
                if document.get("relative_path", "").lower() == requested
            ]
            name_matches = [
                document
                for document in documents
                if document["original_name"].lower() == requested
            ]
            matches = path_matches or name_matches
            if len(matches) == 1:
                document_filter = matches[0]["document_id"]
            elif len(matches) > 1:
                paths = [document["relative_path"] for document in matches]
                return [
                    {
                        "error": (
                            f"Document name '{document_name}' is ambiguous. "
                            f"Use one of these paths: {', '.join(paths)}"
                        )
                    }
                ]
            else:
                available = [
                    document["relative_path"] for document in documents
                ]
                return [
                    {
                        "error": (
                            f"Document '{document_name}' not found. Available: "
                            f"{', '.join(available)}"
                        )
                    }
                ]

        results = await self.document_service.search(
            query,
            session_id=self.session_id,
            n_results=limit,
            document_filter=document_filter,
            folder_root_id=folder_root_id,
            relative_path=relative_path,
            path_prefix=path_prefix,
        )

        if not results:
            return [
                {"info": "No relevant content found in indexed documents"}
            ]

        return results

    async def web_search(
        self, query: str, limit: int = 5, freshness: str = None
    ) -> List[Dict]:
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

    async def news_search(
        self, query: str, limit: int = 5, freshness: str = None
    ) -> List[Dict]:
        """
        Search for news articles. Requires Brave Search API key.
        """
        brave_key = self.search_cfg.get("brave_api_key", "")
        if not brave_key:
            return [
                    {
                        "title": "Not Available",
                        "url": "",
                        "snippet": (
                            "News search requires a Brave Search API key. "
                            "Configure one in Settings → Web Search."
                        ),
                }
            ]
        return await self._news_brave(query, limit, brave_key, freshness or "pw")

    # Internal helpers

    async def _resolve_entity_name(self, entity: str) -> Optional[str]:
        """Resolve user input to canonical entity name via exact or fuzzy match."""
        return await self.entities.resolve_entity_name(entity)

    async def _search_messages(
        self, query: str, k: int
    ) -> List[Tuple[str, float, Optional[str]]]:
        """
        Asynchronous internal method executing FTS search over messages, followed by
        an optional cross-encoder reranking step if candidates exceed 1.
        """
        fts_limit = self.search_cfg.get("fts_limit", 50)
        rerank_candidates = self.search_cfg.get("rerank_candidates", 25)

        results = {}

        visible_session_ids = await self._get_visible_session_ids()
        fts_results = await self.knowledge_store.search_messages_fts(
            query,
            user_name=self.user_name,
            session_ids=visible_session_ids,
            visible_project_ids=self.readable_project_ids,
            limit=fts_limit,
        )

        max_fts = max([s for _, s, _ in fts_results], default=1.0) or 1.0

        for msg_id, raw_score, result_session_id in fts_results:
            msg_key = self._format_message_id(msg_id)
            scoped_key = (result_session_id, msg_key)

            norm_score = raw_score / max_fts if max_fts > 0 else 0

            logger.debug(
                f"FTS result: {result_session_id}:{msg_key} score={norm_score:.3f}"
            )

            results[scoped_key] = ("keyword", norm_score, result_session_id)

        if not results:
            return []

        try:
            if len(results) > 1:
                # Sort by combined score and take top candidates for reranking
                sorted_candidates = sorted(
                    results.items(), key=lambda x: x[1][1], reverse=True
                )[:rerank_candidates]
                candidate_keys = [k for k, _ in sorted_candidates]
                candidate_refs = [
                    {
                        "user_name": self.user_name,
                        "session_id": session_id or self.session_id,
                        "message_id": self._parse_message_ref_id(msg_key),
                    }
                    for session_id, msg_key in candidate_keys
                ]

                hydrated = await self._hydrate_evidence(candidate_refs)
                text_map = {
                    (h.get("session_id"), h["id"]): h.get("message", "")
                    for h in hydrated
                }
                texts = [
                    text_map.get((session_id, msg_key), "")
                    for session_id, msg_key in candidate_keys
                ]

                scores = await self.embedding_service.rerank(query, texts)
                reranked = sorted(
                    zip(candidate_keys, scores), key=lambda x: x[1], reverse=True
                )
                return [
                    (msg_key, float(score), session_id)
                    for (session_id, msg_key), score in reranked[:k]
                ]
        except Exception as e:
            logger.warning(f"Rerank failed, falling back to raw scores: {e}")

        # Fallback: single result
        sorted_results = sorted(results.items(), key=lambda x: x[1][1], reverse=True)[
            :k
        ]
        return [
            (msg_key, score, stored_session_id or key_session_id)
            for (key_session_id, msg_key), (
                _,
                score,
                stored_session_id,
            ) in sorted_results
        ]

    def _normalize_evidence_ref(self, ref) -> Optional[Dict]:
        if isinstance(ref, dict):
            raw_id = ref.get("message_id", ref.get("id"))
            user_name = ref.get("user_name") or self.user_name
            session_id = ref.get("session_id") or self.session_id
        else:
            raw_id = ref
            user_name = self.user_name
            session_id = self.session_id

        if raw_id is None or not user_name or not session_id:
            return None

        try:
            message_id = self._parse_message_ref_id(raw_id)
        except (TypeError, ValueError, IndexError):
            return None

        return {
            "user_name": user_name,
            "session_id": session_id,
            "message_id": message_id,
            "key": self._format_message_id(message_id),
        }

    @staticmethod
    def _format_message_id(msg_id) -> str:
        """Format a canonical message ID."""
        if isinstance(msg_id, str):
            return msg_id
        return f"msg_{msg_id}"

    @staticmethod
    def _parse_message_ref_id(raw_id) -> int:
        if isinstance(raw_id, str):
            if raw_id.startswith("msg_"):
                return int(raw_id.split("_", 1)[1])
            if raw_id.startswith("turn_"):
                raise ValueError("Conversation turn IDs are not canonical message IDs")
        return int(raw_id)

    async def _hydrate_evidence(
        self, evidence_refs: List, timeout: float = 5.0
    ) -> List[Dict]:
        """
        Fetch full message payloads from Redis for scoped evidence refs.
        Falls back to PostgreSQL lookup if Redis cache misses.
        """
        if not evidence_refs:
            return []

        normalized = []
        for idx, ref in enumerate(evidence_refs):
            item = self._normalize_evidence_ref(ref)
            if item:
                item["idx"] = idx
                normalized.append(item)

        if not normalized:
            return []

        raw_by_idx = {}
        grouped = {}
        for item in normalized:
            group_key = (item["user_name"], item["session_id"])
            grouped.setdefault(group_key, []).append(item)

        try:
            for (user_name, session_id), items in grouped.items():
                redis_key = RedisKeys.message_content(user_name, session_id)
                pipe = self.redis.pipeline()
                for item in items:
                    pipe.hget(redis_key, item["key"])
                raw_results = await asyncio.wait_for(pipe.execute(), timeout=timeout)
                for item, raw in zip(items, raw_results):
                    raw_by_idx[item["idx"]] = raw
        except asyncio.TimeoutError:
            logger.warning(
                f"Redis hydrate timed out for {len(evidence_refs)} evidence refs"
            )
            return []

        results_by_idx = {}
        missing_by_session = {}
        normalized_by_idx = {item["idx"]: item for item in normalized}

        for item in normalized:
            raw = raw_by_idx.get(item["idx"])
            if raw:
                data = safe_json_loads(raw)
                if data and isinstance(data, dict):
                    results_by_idx[item["idx"]] = {
                        "id": item["key"],
                        "user_name": item["user_name"],
                        "session_id": item["session_id"],
                        "message": data.get("message", data.get("content", "")),
                        "timestamp": data.get("timestamp", ""),
                    }
                else:
                    logger.warning(f"Malformed evidence data for {item['key']}")
            else:
                missing_by_session.setdefault(
                    (item["user_name"], item["session_id"]), []
                ).append(item["message_id"])

        for (user_name, session_id), message_ids in missing_by_session.items():
            fallback_msgs = await self.knowledge_store.get_messages_by_ids(
                message_ids,
                user_name=user_name,
                session_ids=[session_id],
                visible_project_ids=self.readable_project_ids,
            )
            for message in fallback_msgs:
                ts_iso = ""
                if "timestamp" in message and isinstance(
                    message["timestamp"], (int, float)
                ):
                    ts_iso = datetime.fromtimestamp(
                        message["timestamp"] / 1000.0, timezone.utc
                    ).isoformat()

                str_id = f"msg_{message['id']}"

                for idx, item in normalized_by_idx.items():
                    if (
                        item["user_name"] == user_name
                        and item["session_id"] == session_id
                        and item["message_id"] == message["id"]
                        and idx not in results_by_idx
                    ):
                        results_by_idx[idx] = {
                            "id": str_id,
                            "user_name": message.get("user_name"),
                            "session_id": message.get("session_id"),
                            "message": message["content"],
                            "timestamp": ts_iso,
                        }

        return [results_by_idx[idx] for idx in sorted(results_by_idx)]

    async def _get_visible_session_ids(self) -> List[str]:
        if not self.readable_project_ids:
            return [self.session_id]

        visible = {self.session_id}
        postgres = getattr(self, "postgres", None)
        if postgres is not None:
            rows = await postgres.fetch_all(
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

        redis = getattr(self, "redis", None)
        if redis is not None:
            for project_id in self.readable_project_ids:
                session_ids = await redis.smembers(
                    RedisKeys.project_sessions(self.user_name, project_id)
                )
                visible.update(str(session_id) for session_id in session_ids)
            return sorted(visible)

        return sorted(visible)



    async def _get_surrounding_context(
        self,
        msg_id: str,
        forward: int = 3,
        target_total: int = 10,
        session_id: Optional[str] = None,
    ) -> List[Dict]:
        """
        Given a specific message ID, retrieve the surrounding conversational
        context (previous and succeeding turns) to provide continuity in search results.
        """
        target_session_id = session_id or self.session_id
        is_prefixed_msg_id = msg_id.startswith("msg_")

        if is_prefixed_msg_id:
            try:
                numerical_msg_id = int(msg_id.split("_")[1])
                cached_context = await self._get_cached_surrounding_context(
                    numerical_msg_id,
                    target_session_id,
                    forward=forward,
                    target_total=target_total,
                )
                if cached_context:
                    return cached_context

                fallback_msgs = await self.knowledge_store.get_surrounding_messages(
                    numerical_msg_id,
                    user_name=self.user_name,
                    session_id=target_session_id,
                    visible_project_ids=self.readable_project_ids,
                    forward=forward,
                    target_total=target_total,
                )

                formatted_fallback = []
                for m in fallback_msgs:
                    ts_iso = ""
                    if "timestamp" in m and isinstance(
                        m["timestamp"], (int, float)
                    ):
                        ts_iso = datetime.fromtimestamp(
                            m["timestamp"] / 1000.0, timezone.utc
                        ).isoformat()

                    formatted_fallback.append(
                        {
                            "role": m["role"],
                            "timestamp": ts_iso,
                            "content": m["content"],
                            "id": f"msg_{m['id']}",
                            "is_hit": m["id"] == numerical_msg_id,
                        }
                    )
                return formatted_fallback
            except (ValueError, IndexError):
                pass
        return []

    async def _get_cached_surrounding_context(
        self,
        numerical_msg_id: int,
        session_id: str,
        *,
        forward: int,
        target_total: int,
    ) -> List[Dict]:
        redis = getattr(self, "redis", None)
        if redis is None:
            return []

        target_member = str(numerical_msg_id)
        recent_key = RedisKeys.recent_conversation(self.user_name, session_id)
        rank = await redis.zrank(recent_key, target_member)
        if rank is None:
            return []

        backward = max(target_total - forward - 1, 0)
        start = max(rank - backward, 0)
        end = rank + forward
        message_ids = await redis.zrange(recent_key, start, end)
        if not message_ids:
            return []

        conv_key = RedisKeys.conversation(self.user_name, session_id)
        context = []
        for raw_id in message_ids:
            item_id = str(raw_id)
            raw = await redis.hget(conv_key, item_id)
            data = safe_json_loads(raw) if raw else None
            if not isinstance(data, dict):
                continue
            entry = {
                "role": data.get("role", "user"),
                "timestamp": data.get("timestamp", ""),
                "content": data.get("message", data.get("content", "")),
                "id": f"msg_{item_id}",
            }
            if item_id == target_member:
                entry["is_hit"] = True
            context.append(entry)

        return context

    async def _search_duckduckgo(
        self, query: str, limit: int, freshness: str = None
    ) -> List[Dict]:
        """Free web search via DuckDuckGo — no API key required."""
        loop = asyncio.get_running_loop()
        try:
            if DDGS is None:
                return [
                    {
                        "title": "Search Error",
                        "url": "",
                        "snippet": "duckduckgo_search is not installed",
                    }
                ]
            ddgs = DDGS()
            timelimit = {"pd": "d", "pw": "w", "pm": "m", "py": "y"}.get(freshness)

            raw = await loop.run_in_executor(
                None,
                partial(
                    ddgs.text, query, max_results=min(limit, 10), timelimit=timelimit
                ),
            )

            if not raw:
                return [
                    {
                        "title": "No Results",
                        "url": "",
                        "snippet": f"No web results found for: {query}",
                    }
                ]

            results = []
            for r in raw:
                results.append(
                    {
                        "title": r.get("title", "Untitled"),
                        "url": r.get("href", r.get("link", "")),
                        "snippet": r.get("body", r.get("snippet", "")),
                    }
                )
            return results
        except Exception as e:
            logger.error(f"DuckDuckGo search failed: {e}")
            return [
                {
                    "title": "Search Error",
                    "url": "",
                    "snippet": f"DuckDuckGo search failed: {e}",
                }
            ]

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
                results.append(
                    {
                        "title": r.get("title", "Untitled"),
                        "url": r.get("url", ""),
                        "snippet": r.get("content", ""),
                    }
                )

            if not results:
                return [
                    {
                        "title": "No Results",
                        "url": "",
                        "snippet": f"No web results found for: {query}",
                    }
                ]
            return results
        except httpx.TimeoutException:
            logger.warning("Tavily timed out, falling back to DuckDuckGo")
            return await self._search_duckduckgo(query, limit)
        except Exception as e:
            logger.error(f"Tavily search failed: {e}")
            return await self._search_duckduckgo(query, limit)

    async def _search_brave(
        self, query: str, limit: int, api_key: str, freshness: str = None
    ) -> List[Dict]:
        """Premium web search via Brave Search API."""
        url = "https://api.search.brave.com/res/v1/web/search"
        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "X-Subscription-Token": api_key,
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
                return (
                    await self._search_tavily(
                        query, limit, self.search_cfg.get("tavily_api_key", "")
                    )
                    if self.search_cfg.get("tavily_api_key")
                    else await self._search_duckduckgo(query, limit)
                )
            if response.status_code == 429:
                logger.warning("Brave rate limit hit, falling back")
                return (
                    await self._search_tavily(
                        query, limit, self.search_cfg.get("tavily_api_key", "")
                    )
                    if self.search_cfg.get("tavily_api_key")
                    else await self._search_duckduckgo(query, limit)
                )

            response.raise_for_status()
            data = response.json()

            results = []
            for result in data.get("web", {}).get("results", []):
                snippet = result.get("description", result.get("snippet", ""))
                snippet = re.sub(r"<[^>]+>", "", snippet)
                # Append extra snippets for richer context
                extra = result.get("extra_snippets", [])
                if extra:
                    snippet += " ... " + " ... ".join(
                        re.sub(r"<[^>]+>", "", s) for s in extra[:2]
                    )
                results.append(
                    {
                        "title": result.get("title", "Untitled"),
                        "url": result.get("url", ""),
                        "snippet": snippet,
                    }
                )

            if not results:
                return [
                    {
                        "title": "No Results",
                        "url": "",
                        "snippet": f"No web results found for: {query}",
                    }
                ]
            return results
        except httpx.TimeoutException:
            logger.warning("Brave timed out, falling back to DuckDuckGo")
            return await self._search_duckduckgo(query, limit)
        except Exception as e:
            logger.error(f"Brave search failed: {e}")
            return await self._search_duckduckgo(query, limit)

    async def _news_brave(
        self, query: str, limit: int, api_key: str, freshness: str = "pw"
    ) -> List[Dict]:
        """News search via Brave News API."""
        url = "https://api.search.brave.com/res/v1/news/search"
        headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "X-Subscription-Token": api_key,
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
                return [
                    {
                        "title": "Error",
                        "url": "",
                        "snippet": (
                            f"Brave News API error ({response.status_code}). "
                            "Check your API key in Settings."
                        ),
                    }
                ]

            response.raise_for_status()
            data = response.json()

            results = []
            for article in data.get("results", []):
                snippet = article.get("description", "")
                snippet = re.sub(r"<[^>]+>", "", snippet)
                results.append(
                    {
                        "title": article.get("title", "Untitled"),
                        "url": article.get("url", ""),
                        "snippet": snippet,
                        "source": article.get("meta_url", {}).get("hostname", ""),
                        "date": article.get("age", ""),
                    }
                )

            if not results:
                return [
                    {
                        "title": "No Results",
                        "url": "",
                        "snippet": f"No news found for: {query}",
                    }
                ]
            return results
        except httpx.TimeoutException:
            logger.warning("Brave news timed out")
            return [
                {
                    "title": "Timeout",
                    "url": "",
                    "snippet": "News search timed out. Try a simpler query.",
                }
            ]
        except Exception as e:
            logger.error(f"Brave news search failed: {e}")
            return [
                {
                    "title": "Search Error",
                    "url": "",
                    "snippet": f"News search failed: {e}",
                }
            ]
