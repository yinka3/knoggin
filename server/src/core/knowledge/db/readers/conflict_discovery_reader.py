"""PostgreSQL cursor and bounded relationship-evidence reads for conflict review."""

from __future__ import annotations

from typing import Any

from common.scoping import require_scope_value
from core.knowledge.conflicts import ConflictDiscoveryCursor


class ConflictDiscoveryReader:
    """Uses one durable cursor per project, without leases or continuations."""

    def __init__(self, client) -> None:
        self.client = client

    async def get_cursor(
        self,
        *,
        user_name: str,
        project_id: str,
    ) -> ConflictDiscoveryCursor:
        user_name = require_scope_value(user_name, "user_name", "get_conflict_cursor")
        project_id = require_scope_value(project_id, "project_id", "get_conflict_cursor")
        await self.client.execute(
            """
            INSERT INTO public.conflict_discovery_checkpoints (
                user_name, project_id
            )
            VALUES (%s, %s)
            ON CONFLICT (user_name, project_id) DO NOTHING
            """,
            (user_name, project_id),
        )
        row = await self.client.fetch_one(
            """
            SELECT last_reviewed_observation_id
            FROM public.conflict_discovery_checkpoints
            WHERE user_name = %s AND project_id = %s
            """,
            (user_name, project_id),
        )
        if row is None:
            raise RuntimeError("Conflict checkpoint was not created")
        return ConflictDiscoveryCursor(
            user_name=user_name,
            project_id=project_id,
            last_reviewed_observation_id=int(row["last_reviewed_observation_id"]),
        )

    async def get_seed_observations(
        self,
        cursor: ConflictDiscoveryCursor,
        *,
        max_span_days: int,
        limit: int = 128,
    ) -> list[dict[str, Any]]:
        if max_span_days < 1:
            raise ValueError("Conflict discovery max_span_days must be positive")
        if limit < 1:
            raise ValueError("Conflict discovery seed limit must be positive")
        rows = await self.client.fetch_all(
            self._observation_query(
                """
                AND observation.observation_id > %s
                ORDER BY observation.observation_id
                LIMIT %s
                """
            ),
            (
                cursor.user_name,
                cursor.project_id,
                cursor.last_reviewed_observation_id,
                limit,
            ),
        )
        if not rows:
            return []
        max_span_ms = max_span_days * 86_400_000
        anchor_ms = int(rows[0]["observed_at_ms"])
        return [
            row
            for row in rows
            if abs(int(row["observed_at_ms"]) - anchor_ms) <= max_span_ms
        ]

    async def get_direct_neighborhood(
        self,
        *,
        user_name: str,
        project_id: str,
        entity_ids: list[int],
        limit: int = 128,
    ) -> list[dict[str, Any]]:
        """Return a bounded recent evidence slice touching the supplied endpoints."""

        if not entity_ids:
            return []
        if limit < 1:
            raise ValueError("Conflict discovery neighborhood limit must be positive")
        rows = await self.client.fetch_all(
            self._observation_query(
                """
                AND (
                    observation.source_entity_id = ANY(%s)
                    OR observation.target_entity_id = ANY(%s)
                )
                ORDER BY observation.observation_id DESC
                LIMIT %s
                """
            ),
            (user_name, project_id, entity_ids, entity_ids, limit),
        )
        return list(reversed(rows))

    async def advance(
        self,
        cursor: ConflictDiscoveryCursor,
        *,
        last_reviewed_observation_id: int,
        cur=None,
    ) -> None:
        if last_reviewed_observation_id < cursor.last_reviewed_observation_id:
            raise ValueError("Conflict discovery cursor cannot move backwards")
        query = """
            UPDATE public.conflict_discovery_checkpoints
            SET last_reviewed_observation_id = GREATEST(
                    last_reviewed_observation_id, %s
                ),
                last_completed_at = now(),
                updated_at = now()
            WHERE user_name = %s AND project_id = %s
        """
        params = (
            last_reviewed_observation_id,
            cursor.user_name,
            cursor.project_id,
        )
        if cur is not None:
            await cur.execute(query, params)
            return
        await self.client.execute(query, params)

    @staticmethod
    def _observation_query(suffix: str) -> str:
        return f"""
            SELECT
                observation.observation_id,
                observation.relationship_id,
                observation.message_id,
                observation.session_id,
                observation.source_entity_id,
                source.canonical_name AS source_entity_name,
                observation.target_entity_id,
                target.canonical_name AS target_entity_name,
                observation.source_type,
                observation.target_type,
                observation.observed_relationship_label,
                observation.canonical_relationship_type,
                observation.domain_status,
                observation.confidence,
                observation.context,
                observation.observed_at_ms
            FROM public.relationship_observations observation
            JOIN public.entities source ON source.entity_id = observation.source_entity_id
            JOIN public.entities target ON target.entity_id = observation.target_entity_id
            WHERE observation.user_name = %s
              AND observation.project_id = %s
            {suffix}
        """  # noqa: S608
