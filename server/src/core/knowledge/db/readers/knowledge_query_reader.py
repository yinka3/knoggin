"""Focused cross-table reads for canonical knowledge evidence."""

from __future__ import annotations

from typing import Any

from loguru import logger

from common.exceptions import StorageReadError
from common.scoping import IDENTITY_ENTITY_ID, require_visible_project_ids
from common.utils.time_utils import get_now_ms
from infrastructure.postgres_client import PostgresClient


class KnowledgeQueryReader:
    """Read joins that span messages, entities, and observations."""

    def __init__(self, client: PostgresClient):
        self.client = client

    @staticmethod
    def _raise_storage_read(operation: str, exc: Exception) -> None:
        logger.error("Storage read failed for {}: {}", operation, exc)
        raise StorageReadError(
            operation,
            details={"error_type": type(exc).__name__},
        ) from exc

    async def get_hot_topic_context_with_messages(
        self,
        hot_topic_names: list[str],
        *,
        visible_project_ids: list[str],
        msg_limit: int = 5,
    ) -> dict[str, dict[str, Any]]:
        """Return a compact topic context with canonical source-message refs."""

        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_hot_topic_context_with_messages",
        )
        if not hot_topic_names:
            return {}

        query = """
        SELECT
            entity.topic,
            entity.canonical_name AS name,
            COALESCE(
                array_agg(DISTINCT alias.alias) FILTER (WHERE alias.alias IS NOT NULL),
                '{}'::text[]
            ) AS aliases,
            COALESCE(
                jsonb_agg(
                    DISTINCT jsonb_build_object(
                        'user_name', observation.user_name,
                        'session_id', observation.session_id,
                        'message_id', observation.message_id
                    )
                ) FILTER (WHERE observation.observation_id IS NOT NULL),
                '[]'::jsonb
            ) AS message_refs
        FROM entities entity
        LEFT JOIN entity_aliases alias ON alias.entity_id = entity.entity_id
        LEFT JOIN relationships relationship
          ON relationship.entity_a_id = entity.entity_id
          OR relationship.entity_b_id = entity.entity_id
        LEFT JOIN relationship_observations observation
          ON observation.relationship_id = relationship.relationship_id
         AND observation.project_id = relationship.project_id
        WHERE entity.topic = ANY(%s)
          AND (entity.project_id = ANY(%s) OR entity.entity_id = %s)
          AND (relationship.project_id = ANY(%s) OR relationship.project_id IS NULL)
        GROUP BY entity.entity_id, entity.topic, entity.canonical_name,
                 entity.last_mentioned_ms
        ORDER BY entity.last_mentioned_ms DESC NULLS LAST
        """
        try:
            rows = await self.client.fetch_all(
                query,
                (
                    hot_topic_names,
                    visible_project_ids,
                    IDENTITY_ENTITY_ID,
                    visible_project_ids,
                ),
            )
        except Exception as exc:
            self._raise_storage_read("get_hot_topic_context_with_messages", exc)

        topics: dict[str, dict[str, Any]] = {}
        for row in rows:
            topic = row["topic"]
            result = topics.setdefault(topic, {"entities": [], "message_refs": []})
            if len(result["entities"]) < 3:
                result["entities"].append(
                    {"name": row["name"], "aliases": row["aliases"] or []}
                )
            known = {
                (ref["user_name"], ref["session_id"], ref["message_id"])
                for ref in result["message_refs"]
            }
            for ref in row["message_refs"] or []:
                key = (ref["user_name"], ref["session_id"], ref["message_id"])
                if key not in known and len(result["message_refs"]) < msg_limit:
                    result["message_refs"].append(ref)
                    known.add(key)
        return topics

    async def get_recent_activity(
        self,
        entity_name: str,
        *,
        visible_project_ids: list[str],
        active_topics: list[str] | None = None,
        hours: int = 24,
    ) -> list[dict[str, Any]]:
        """Return recent observed graph activity around one entity."""

        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_recent_activity",
        )
        if not entity_name or not entity_name.strip():
            return []
        cutoff_ms = get_now_ms() - (hours * 3600 * 1000)
        query = """
        SELECT
            target.canonical_name AS entity,
            MAX(observation.observed_at_ms) AS time,
            COALESCE(
                jsonb_agg(
                    jsonb_build_object(
                        'user_name', observation.user_name,
                        'session_id', observation.session_id,
                        'message_id', observation.message_id
                    )
                    ORDER BY observation.observed_at_ms DESC,
                             observation.observation_id DESC
                ),
                '[]'::jsonb
            ) AS evidence_refs
        FROM entities source
        JOIN relationships relationship
          ON relationship.entity_a_id = source.entity_id
          OR relationship.entity_b_id = source.entity_id
        JOIN entities target
          ON target.entity_id = CASE
              WHEN relationship.entity_a_id = source.entity_id
              THEN relationship.entity_b_id
              ELSE relationship.entity_a_id
          END
        JOIN relationship_observations observation
          ON observation.relationship_id = relationship.relationship_id
         AND observation.project_id = relationship.project_id
        WHERE source.canonical_name = %s
          AND observation.observed_at_ms > %s
          AND (source.project_id = ANY(%s) OR source.entity_id = %s)
          AND (target.project_id = ANY(%s) OR target.entity_id = %s)
          AND relationship.project_id = ANY(%s)
        """
        params: tuple = (
            entity_name,
            cutoff_ms,
            visible_project_ids,
            IDENTITY_ENTITY_ID,
            visible_project_ids,
            IDENTITY_ENTITY_ID,
            visible_project_ids,
        )
        if active_topics is not None:
            query += " AND target.topic = ANY(%s)"
            params = (*params, active_topics)
        query += """
        GROUP BY target.entity_id, target.canonical_name
        ORDER BY time DESC
        LIMIT 50
        """
        try:
            rows = await self.client.fetch_all(query, params)
            return [
                {
                    "entity": row["entity"],
                    "evidence_refs": row["evidence_refs"] or [],
                    "time": row["time"],
                }
                for row in rows
            ]
        except Exception as exc:
            self._raise_storage_read("get_recent_activity", exc)
