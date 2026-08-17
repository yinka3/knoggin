"""Durable cursor and scoped relationship-evidence reads for conflict discovery."""

from __future__ import annotations

import uuid
from typing import Any

from common.scoping import require_scope_value
from core.knowledge.conflicts import (
    ConflictDiscoveryContinuation,
    ConflictDiscoveryLease,
)


class ConflictDiscoveryReader:
    """Reads one project at a time; cursor state is durable in Postgres."""

    def __init__(self, client) -> None:
        self.client = client

    async def claim(
        self,
        *,
        user_name: str,
        project_id: str,
        lease_seconds: int = 900,
    ) -> ConflictDiscoveryLease | None:
        user_name = require_scope_value(user_name, "user_name", "claim_conflicts")
        project_id = require_scope_value(project_id, "project_id", "claim_conflicts")
        if lease_seconds <= 0:
            raise ValueError("Conflict discovery lease_seconds must be positive")
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                INSERT INTO public.conflict_discovery_checkpoints (
                    user_name, project_id
                )
                VALUES (%s, %s)
                ON CONFLICT (user_name, project_id) DO NOTHING
                """,
                (user_name, project_id),
            )
            await cur.execute(
                """
                SELECT cursor_observed_at_ms, cursor_observation_id,
                       continuation, lease_expires_at
                FROM public.conflict_discovery_checkpoints
                WHERE user_name = %s AND project_id = %s
                FOR UPDATE
                """,
                (user_name, project_id),
            )
            row = await cur.fetchone()
            if row is None:
                raise RuntimeError("Conflict checkpoint was not created")
            if row.get("lease_expires_at") is not None:
                await cur.execute(
                    "SELECT now() >= %s AS expired",
                    (row["lease_expires_at"],),
                )
                expiry = await cur.fetchone()
                if not expiry or not expiry["expired"]:
                    return None
            token = str(uuid.uuid4())
            await cur.execute(
                """
                UPDATE public.conflict_discovery_checkpoints
                SET lease_token = %s,
                    lease_expires_at = now() + (%s * interval '1 second'),
                    updated_at = now()
                WHERE user_name = %s AND project_id = %s
                """,
                (token, lease_seconds, user_name, project_id),
            )
            return ConflictDiscoveryLease(
                user_name=user_name,
                project_id=project_id,
                cursor_observed_at_ms=int(row["cursor_observed_at_ms"]),
                cursor_observation_id=int(row["cursor_observation_id"]),
                lease_token=token,
                continuation=self._continuation_from_row(row.get("continuation")),
            )

    async def get_seed_observations(
        self,
        lease: ConflictDiscoveryLease,
        *,
        max_span_days: int,
        limit: int = 512,
    ) -> list[dict[str, Any]]:
        if max_span_days < 1:
            raise ValueError("Conflict discovery max_span_days must be positive")
        if limit < 1:
            raise ValueError("Conflict discovery seed limit must be positive")
        # Progress is based on insertion identity, not observed time.  A
        # delayed/replayed session may add an old observation after a newer
        # project-wide cursor has advanced; timestamp pagination would skip it
        # forever.  Time still bounds a single packet below.
        rows = await self.client.fetch_all(
            self._observation_query(
                """
                AND observation.observation_id > %s
                ORDER BY observation.observation_id
                LIMIT %s
                """
            ),
            (
                lease.user_name,
                lease.project_id,
                lease.cursor_observation_id,
                limit,
            ),
        )
        if not rows:
            return []
        max_span_ms = max_span_days * 86_400_000
        anchor_ms = int(rows[0]["observed_at_ms"])
        seeds: list[dict[str, Any]] = []
        for row in rows:
            if abs(int(row["observed_at_ms"]) - anchor_ms) > max_span_ms:
                break
            seeds.append(row)
        return seeds

    async def get_direct_neighborhood(
        self,
        *,
        user_name: str,
        project_id: str,
        entity_ids: list[int],
        limit: int = 512,
    ) -> list[dict[str, Any]]:
        if not entity_ids:
            return []
        if limit < 1:
            raise ValueError("Conflict discovery neighborhood limit must be positive")
        return await self.client.fetch_all(
            self._observation_query(
                """
                AND (
                    observation.source_entity_id = ANY(%s)
                    OR observation.target_entity_id = ANY(%s)
                )
                ORDER BY observation.observation_id
                LIMIT %s
                """
            ),
            (user_name, project_id, entity_ids, entity_ids, limit),
        )

    async def get_direct_neighborhood_page(
        self,
        *,
        user_name: str,
        project_id: str,
        entity_ids: list[int],
        after_observation_id: int,
        limit: int = 512,
    ) -> tuple[list[dict[str, Any]], bool]:
        """Return a stable identity-ordered neighborhood page and continuation bit."""
        if not entity_ids:
            return [], False
        if after_observation_id < 0:
            raise ValueError("Conflict discovery continuation cursor cannot be negative")
        if limit < 1:
            raise ValueError("Conflict discovery neighborhood limit must be positive")
        rows = await self.client.fetch_all(
            self._observation_query(
                """
                AND observation.observation_id > %s
                AND (
                    observation.source_entity_id = ANY(%s)
                    OR observation.target_entity_id = ANY(%s)
                )
                ORDER BY observation.observation_id
                LIMIT %s
                """
            ),
            (
                user_name,
                project_id,
                after_observation_id,
                entity_ids,
                entity_ids,
                limit + 1,
            ),
        )
        return rows[:limit], len(rows) > limit

    async def get_observations_by_ids(
        self,
        *,
        user_name: str,
        project_id: str,
        observation_ids: list[int],
    ) -> list[dict[str, Any]]:
        if not observation_ids:
            return []
        return await self.client.fetch_all(
            self._observation_query(
                """
                AND observation.observation_id = ANY(%s)
                ORDER BY observation.observation_id
                """
            ),
            (user_name, project_id, observation_ids),
        )

    async def has_continuation(self, *, user_name: str, project_id: str) -> bool:
        row = await self.client.fetch_one(
            """
            SELECT continuation
            FROM public.conflict_discovery_checkpoints
            WHERE user_name = %s AND project_id = %s
            """,
            (user_name, project_id),
        )
        return bool(row and self._continuation_from_row(row.get("continuation")))

    async def complete(
        self,
        lease: ConflictDiscoveryLease,
        *,
        cursor_observed_at_ms: int,
        cursor_observation_id: int,
        continuation: ConflictDiscoveryContinuation | None = None,
    ) -> bool:
        return bool(
            await self.client.execute(
                """
                UPDATE public.conflict_discovery_checkpoints
                SET cursor_observed_at_ms = %s,
                    cursor_observation_id = %s,
                    continuation = %s::jsonb,
                    lease_token = NULL,
                    lease_expires_at = NULL,
                    last_completed_at = now(),
                    updated_at = now()
                WHERE user_name = %s
                  AND project_id = %s
                  AND lease_token = %s
                """,
                (
                    cursor_observed_at_ms,
                    cursor_observation_id,
                    self._continuation_json(continuation),
                    lease.user_name,
                    lease.project_id,
                    lease.lease_token,
                ),
            )
        )

    @staticmethod
    def _continuation_json(
        continuation: ConflictDiscoveryContinuation | None,
    ) -> str:
        if continuation is None:
            return "{}"
        return (
            "{"
            f'"seed_observation_id":{continuation.seed_observation_id},'
            f'"source_entity_id":{continuation.source_entity_id},'
            f'"target_entity_id":{continuation.target_entity_id},'
            f'"after_observation_id":{continuation.after_observation_id},'
            '"overlap_observation_ids":['
            + ",".join(str(item) for item in continuation.overlap_observation_ids)
            + "]}"
        )

    @staticmethod
    def _continuation_from_row(value: Any) -> ConflictDiscoveryContinuation | None:
        if not value:
            return None
        if isinstance(value, str):
            # Some async Postgres drivers return JSONB as a decoded dict and
            # others as text. The checkpoint accepts either representation.
            import json

            value = json.loads(value)
        if not value or "seed_observation_id" not in value:
            return None
        return ConflictDiscoveryContinuation(
            seed_observation_id=int(value["seed_observation_id"]),
            source_entity_id=int(value["source_entity_id"]),
            target_entity_id=int(value["target_entity_id"]),
            after_observation_id=int(value.get("after_observation_id", 0)),
            overlap_observation_ids=tuple(
                int(item) for item in value.get("overlap_observation_ids", [])
            ),
        )

    async def release(self, lease: ConflictDiscoveryLease) -> None:
        await self.client.execute(
            """
            UPDATE public.conflict_discovery_checkpoints
            SET lease_token = NULL, lease_expires_at = NULL, updated_at = now()
            WHERE user_name = %s
              AND project_id = %s
              AND lease_token = %s
            """,
            (lease.user_name, lease.project_id, lease.lease_token),
        )

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
