"""Focused cross-table reads for canonical knowledge evidence."""

from __future__ import annotations

from typing import Any

from loguru import logger

from common.exceptions import StorageReadError
from common.scoping import require_scope_value, require_visible_project_ids
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
        project_id: str,
        msg_limit: int = 5,
    ) -> dict[str, dict[str, Any]]:
        """Return current-project topic context with entity-mention evidence."""

        project_id = require_scope_value(
            project_id,
            "project_id",
            "get_hot_topic_context_with_messages",
        )
        if not hot_topic_names:
            return {}

        query = """
        SELECT
            context.topic,
            entity.canonical_name AS name,
            COALESCE(
                array_agg(DISTINCT alias.alias) FILTER (WHERE alias.alias IS NOT NULL),
                '{}'::text[]
            ) AS aliases,
            COALESCE(
                jsonb_agg(
                    DISTINCT jsonb_build_object(
                        'project_id', message.project_id,
                        'user_name', message.user_name,
                        'session_id', message.session_id,
                        'message_id', message.message_id
                    )
                ) FILTER (WHERE message.message_id IS NOT NULL),
                '[]'::jsonb
            ) AS message_refs
        FROM entities entity
        JOIN project_entity_contexts context
          ON context.entity_id = entity.entity_id
        LEFT JOIN entity_aliases alias ON alias.entity_id = entity.entity_id
        LEFT JOIN message_entity_refs mention
          ON mention.entity_id = entity.entity_id
        LEFT JOIN messages message
          ON message.message_id = mention.message_id
         AND message.project_id = context.project_id
        WHERE context.topic = ANY(%s)
          AND context.project_id = %s
        GROUP BY entity.entity_id, context.project_id, context.topic,
                 entity.canonical_name, context.last_mentioned_ms
        ORDER BY context.last_mentioned_ms DESC NULLS LAST
        """
        try:
            rows = await self.client.fetch_all(
                query,
                (
                    hot_topic_names,
                    project_id,
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
        entity_id: int,
        *,
        visible_project_ids: list[str],
        hours: int = 24,
    ) -> list[dict[str, Any]]:
        """Return recent message activity, optionally enriched by observations.

        ``message_entity_refs`` is the admission boundary. A message remains
        visible even if no relationship observation was extracted from it.
        """

        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_recent_activity",
        )
        if not isinstance(entity_id, int) or isinstance(entity_id, bool):
            return []
        cutoff_ms = get_now_ms() - (hours * 3600 * 1000)
        query = """
        SELECT
            entity.entity_id,
            entity.canonical_name AS entity,
            message.project_id,
            message.timestamp_ms AS time,
            COALESCE(
                jsonb_build_array(
                    jsonb_build_object(
                        'project_id', message.project_id,
                        'user_name', message.user_name,
                        'session_id', message.session_id,
                        'message_id', message.message_id
                    )
                ),
                '[]'::jsonb
            ) AS evidence_refs,
            COALESCE(
                jsonb_agg(DISTINCT jsonb_build_object(
                    'relationship_id', observation.relationship_id,
                    'project_id', observation.project_id,
                    'observation_id', observation.observation_id,
                    'observed_at_ms', observation.observed_at_ms
                )) FILTER (WHERE observation.observation_id IS NOT NULL),
                '[]'::jsonb
            ) AS observation_refs
        FROM message_entity_refs mention
        JOIN messages message ON message.message_id = mention.message_id
        JOIN entities entity ON entity.entity_id = mention.entity_id
        LEFT JOIN project_context_block_supports support
          ON support.project_id = message.project_id
         AND support.session_id = message.session_id
         AND support.message_id = message.message_id
        LEFT JOIN relationship_observation_blocks observation_block
          ON observation_block.project_id = support.project_id
         AND observation_block.block_id = support.block_id
        LEFT JOIN relationship_observations observation
          ON observation.observation_id = observation_block.observation_id
         AND observation.project_id = observation_block.project_id
         AND (
             observation.source_entity_id = mention.entity_id
             OR observation.target_entity_id = mention.entity_id
         )
        WHERE mention.entity_id = %s
          AND message.timestamp_ms > %s
          AND message.project_id = ANY(%s)
        """
        params: tuple = (
            entity_id,
            cutoff_ms,
            visible_project_ids,
        )
        query += """
        GROUP BY entity.entity_id, entity.canonical_name, message.project_id,
                 message.user_name, message.session_id, message.message_id,
                 message.timestamp_ms
        ORDER BY time DESC
        LIMIT 50
        """
        try:
            rows = await self.client.fetch_all(query, params)
            return [
                {
                    "entity_id": int(row["entity_id"]),
                    "entity": row["entity"],
                    "project_id": row["project_id"],
                    "evidence_refs": row["evidence_refs"] or [],
                    "observation_refs": row["observation_refs"] or [],
                    "time": row["time"],
                }
                for row in rows
            ]
        except Exception as exc:
            self._raise_storage_read("get_recent_activity", exc)
