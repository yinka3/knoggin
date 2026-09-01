"""Observation-first historical relationship normalization."""

from __future__ import annotations

from dataclasses import dataclass
from functools import wraps
from typing import Any, Iterable

from loguru import logger
from psycopg import Error as PsycopgError

from common.conf.domain_config import CompiledDomain
from common.exceptions import StorageWriteError
from common.scoping import require_scope_value
from core.knowledge.relationship_reclassification import (
    RelationshipReclassification,
    RelationshipReclassificationPlan,
    plan_relationship_reclassification,
)


def _storage_write(operation: str):
    """Translate database failures without hiding domain decisions."""

    def decorate(method):
        @wraps(method)
        async def wrapped(self, *args, **kwargs):
            try:
                return await method(self, *args, **kwargs)
            except PsycopgError as exc:
                self._raise_storage_write(operation, exc)

        return wrapped

    return decorate


@dataclass(frozen=True, slots=True)
class RelationshipReclassificationBatchResult:
    scanned: int
    updated: int
    conflicts: int


@dataclass(frozen=True, slots=True)
class HistoricalRelationshipReclassificationResult:
    domain_version: int
    scanned: int
    updated: int
    unchanged: int
    unmapped: int
    incompatible: int
    conflicts: int
    batches: int
    truncated: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "domain_version": self.domain_version,
            "scanned": self.scanned,
            "updated": self.updated,
            "unchanged": self.unchanged,
            "unmapped": self.unmapped,
            "incompatible": self.incompatible,
            "conflicts": self.conflicts,
            "batches": self.batches,
            "truncated": self.truncated,
            "projection_rebuild_required": self.updated > 0,
        }


class RelationshipReclassificationWriter:
    """Reinterpret canonical observations, then reconcile graph identities."""

    def __init__(self, client):
        self.client = client

    @staticmethod
    def _raise_storage_write(operation: str, exc: Exception) -> None:
        logger.error("Storage write failed for {}: {}", operation, exc)
        raise StorageWriteError(
            operation,
            details={"error_type": type(exc).__name__},
        ) from exc

    @staticmethod
    def _scope(user_name: str, project_id: str, operation: str) -> tuple[str, str]:
        return (
            require_scope_value(user_name, "user_name", operation),
            require_scope_value(project_id, "project_id", operation),
        )

    @staticmethod
    def _validate_limit(limit: int | None) -> None:
        if limit is not None and (
            not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0
        ):
            raise ValueError("limit must be a positive integer when provided")

    @_storage_write("fetch_relationship_reclassification_observations")
    async def fetch_observations(
        self,
        *,
        user_name: str,
        project_id: str,
        after_observation_id: int = 0,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch unrecognized observations in stable evidence order."""

        user_name, project_id = self._scope(
            user_name,
            project_id,
            "fetch relationship reclassification",
        )
        if (
            not isinstance(after_observation_id, int)
            or isinstance(after_observation_id, bool)
            or after_observation_id < 0
        ):
            raise ValueError("after_observation_id must be a non-negative integer")
        self._validate_limit(limit)

        query = """
            SELECT
                observation.observation_id,
                observation.relationship_id,
                observation.project_id,
                observation.source_entity_id AS entity_a_id,
                observation.target_entity_id AS entity_b_id,
                relationship.relationship_type,
                observation.canonical_relationship_type,
                observation.domain_status,
                observation.domain_version,
                observation."symmetric" AS symmetric,
                observation.observed_relationship_label,
                observation.source_type,
                observation.target_type
            FROM public.relationship_observations observation
            JOIN public.relationships relationship
              ON relationship.relationship_id = observation.relationship_id
             AND relationship.project_id = observation.project_id
            WHERE observation.user_name = %s
              AND observation.project_id = %s
              AND observation.domain_status = 'unrecognized'
              AND observation.observation_id > %s
            ORDER BY observation.observation_id
        """
        params: list[Any] = [user_name, project_id, after_observation_id]
        if limit is not None:
            query += " LIMIT %s"
            params.append(limit)
        return list(await self.client.fetch_all(query, tuple(params)))

    @_storage_write("preview_relationship_reclassification")
    async def preview(
        self,
        *,
        user_name: str,
        project_id: str,
        domain: CompiledDomain,
        limit: int | None = None,
    ) -> RelationshipReclassificationPlan:
        if not isinstance(domain, CompiledDomain):
            raise TypeError("domain must be a CompiledDomain")
        rows = await self.fetch_observations(
            user_name=user_name,
            project_id=project_id,
            limit=limit,
        )
        return plan_relationship_reclassification(rows, domain)

    @staticmethod
    async def _ensure_relationship_identity(
        cur,
        *,
        change: RelationshipReclassification,
        user_name: str,
    ) -> None:
        entity_a_id, entity_b_id = change.entity_a_id, change.entity_b_id
        if change.new_symmetric:
            entity_a_id, entity_b_id = sorted((entity_a_id, entity_b_id))
        await cur.execute(
            """
            INSERT INTO public.relationships (
                relationship_id,
                user_name,
                project_id,
                entity_a_id,
                entity_b_id,
                relationship_type,
                "symmetric"
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (relationship_id) DO NOTHING
            """,
            (
                change.new_relationship_id,
                user_name,
                change.project_id,
                entity_a_id,
                entity_b_id,
                change.new_relationship_type,
                change.new_symmetric,
            ),
        )

    @staticmethod
    async def _rewrite_observation(
        cur,
        *,
        change: RelationshipReclassification,
        domain_version: int,
    ) -> None:
        """Move one evidence row and retain its semantics on unique-key merge."""

        await cur.execute(
            """
            WITH moved AS (
                DELETE FROM public.relationship_observations
                WHERE observation_id = %s
                  AND relationship_id = %s
                  AND project_id = %s
                RETURNING
                    project_id,
                    user_name,
                    session_id,
                    message_id,
                    source_entity_id,
                    target_entity_id,
                    source_type,
                    target_type,
                    observed_relationship_label,
                    confidence,
                    context,
                    observed_at_ms
            )
            INSERT INTO public.relationship_observations (
                relationship_id,
                project_id,
                user_name,
                session_id,
                message_id,
                source_entity_id,
                target_entity_id,
                source_type,
                target_type,
                observed_relationship_label,
                canonical_relationship_type,
                domain_status,
                domain_version,
                "symmetric",
                confidence,
                context,
                observed_at_ms
            )
            SELECT
                %s,
                project_id,
                user_name,
                session_id,
                message_id,
                CASE WHEN %s THEN LEAST(source_entity_id, target_entity_id)
                     ELSE source_entity_id END,
                CASE WHEN %s THEN GREATEST(source_entity_id, target_entity_id)
                     ELSE target_entity_id END,
                source_type,
                target_type,
                observed_relationship_label,
                %s,
                'recognized',
                %s,
                %s,
                confidence,
                context,
                observed_at_ms
            FROM moved
            ON CONFLICT (
                project_id,
                user_name,
                session_id,
                message_id,
                source_entity_id,
                target_entity_id,
                observed_relationship_label
            ) DO UPDATE SET
                relationship_id = EXCLUDED.relationship_id,
                canonical_relationship_type = EXCLUDED.canonical_relationship_type,
                domain_status = EXCLUDED.domain_status,
                domain_version = EXCLUDED.domain_version,
                "symmetric" = EXCLUDED."symmetric",
                confidence = GREATEST(
                    public.relationship_observations.confidence,
                    EXCLUDED.confidence
                ),
                context = COALESCE(
                    EXCLUDED.context,
                    public.relationship_observations.context
                ),
                observed_at_ms = GREATEST(
                    public.relationship_observations.observed_at_ms,
                    EXCLUDED.observed_at_ms
                )
            """,
            (
                change.observation_id,
                change.relationship_id,
                change.project_id,
                change.new_relationship_id,
                change.new_symmetric,
                change.new_symmetric,
                change.new_canonical_relationship_type,
                domain_version,
                change.new_symmetric,
            ),
        )
    @staticmethod
    async def _reconcile_episode_relationships(
        cur,
        *,
        project_id: str,
        relationship_ids: list[str],
    ) -> None:
        if not relationship_ids:
            return
        await cur.execute(
            """
            INSERT INTO public.episode_relationships (
                episode_id,
                project_id,
                relationship_id,
                source_message_count
            )
            SELECT
                episode_message.episode_id,
                episode_message.project_id,
                observation.relationship_id,
                COUNT(DISTINCT episode_message.message_id)
            FROM public.episode_messages episode_message
            JOIN public.relationship_observations observation
              ON observation.project_id = episode_message.project_id
             AND observation.session_id = episode_message.session_id
             AND observation.message_id = episode_message.message_id
            WHERE episode_message.project_id = %s
              AND observation.relationship_id = ANY(%s)
            GROUP BY
                episode_message.episode_id,
                episode_message.project_id,
                observation.relationship_id
            ON CONFLICT (episode_id, relationship_id) DO UPDATE SET
                source_message_count = GREATEST(
                    public.episode_relationships.source_message_count,
                    EXCLUDED.source_message_count
                )
            """,
            (project_id, relationship_ids),
        )

    @staticmethod
    async def _remove_orphaned_relationships(
        cur,
        *,
        project_id: str,
        relationship_ids: list[str],
    ) -> None:
        if not relationship_ids:
            return
        await cur.execute(
            """
            DELETE FROM public.episode_relationships episode_relationship
            WHERE episode_relationship.project_id = %s
              AND episode_relationship.relationship_id = ANY(%s)
              AND NOT EXISTS (
                  SELECT 1
                  FROM public.relationship_observations observation
                  WHERE observation.project_id = episode_relationship.project_id
                    AND observation.relationship_id = episode_relationship.relationship_id
              )
            """,
            (project_id, relationship_ids),
        )
        await cur.execute(
            """
            DELETE FROM public.relationships relationship
            WHERE relationship.project_id = %s
              AND relationship.relationship_id = ANY(%s)
              AND NOT EXISTS (
                  SELECT 1
                  FROM public.relationship_observations observation
                  WHERE observation.project_id = relationship.project_id
                    AND observation.relationship_id = relationship.relationship_id
              )
            """,
            (project_id, relationship_ids),
        )

    @_storage_write("apply_relationship_reclassification")
    async def apply(
        self,
        *,
        user_name: str,
        project_id: str,
        domain: CompiledDomain,
        changes: Iterable[RelationshipReclassification],
    ) -> RelationshipReclassificationBatchResult:
        user_name, project_id = self._scope(
            user_name,
            project_id,
            "apply relationship reclassification",
        )
        if not isinstance(domain, CompiledDomain):
            raise TypeError("domain must be a CompiledDomain")
        planned = tuple(changes)
        if not planned:
            return RelationshipReclassificationBatchResult(0, 0, 0)
        observation_ids = [change.observation_id for change in planned]
        if len(set(observation_ids)) != len(observation_ids):
            raise ValueError("Relationship observation IDs must be unique")

        async with self.client.transaction() as cur:
            await cur.execute(
                """
                SELECT
                    observation.observation_id,
                    observation.relationship_id,
                    observation.project_id,
                    observation.source_entity_id AS entity_a_id,
                    observation.target_entity_id AS entity_b_id,
                    relationship.relationship_type,
                    observation.canonical_relationship_type,
                    observation.domain_status,
                    observation.domain_version,
                    observation."symmetric" AS symmetric,
                    observation.observed_relationship_label,
                    observation.source_type,
                    observation.target_type
                FROM public.relationship_observations observation
                JOIN public.relationships relationship
                  ON relationship.relationship_id = observation.relationship_id
                 AND relationship.project_id = observation.project_id
                WHERE observation.user_name = %s
                  AND observation.project_id = %s
                  AND observation.observation_id = ANY(%s)
                FOR UPDATE OF observation
                """,
                (user_name, project_id, observation_ids),
            )
            current_rows = {
                int(row["observation_id"]): row for row in await cur.fetchall()
            }
            conflicts = 0
            updated = 0
            old_relationship_ids: set[str] = set()
            affected_relationship_ids: set[str] = set()
            for change in planned:
                current = current_rows.get(change.observation_id)
                if current is None:
                    conflicts += 1
                    continue
                current_plan = plan_relationship_reclassification((current,), domain)
                if not current_plan.changes:
                    conflicts += 1
                    continue
                target = current_plan.changes[0]
                if target != change:
                    conflicts += 1
                    continue

                await self._ensure_relationship_identity(
                    cur,
                    change=target,
                    user_name=user_name,
                )
                await self._rewrite_observation(
                    cur,
                    change=target,
                    domain_version=domain.version,
                )
                old_relationship_ids.add(target.relationship_id)
                affected_relationship_ids.add(target.relationship_id)
                affected_relationship_ids.add(target.new_relationship_id)
                updated += 1

            await self._reconcile_episode_relationships(
                cur,
                project_id=project_id,
                relationship_ids=sorted(affected_relationship_ids),
            )
            await self._remove_orphaned_relationships(
                cur,
                project_id=project_id,
                relationship_ids=sorted(old_relationship_ids),
            )

        return RelationshipReclassificationBatchResult(
            scanned=len(planned), updated=updated, conflicts=conflicts
        )

    @_storage_write("reclassify_relationships")
    async def reclassify(
        self,
        *,
        user_name: str,
        project_id: str,
        domain: CompiledDomain,
        batch_size: int = 100,
        max_relationships: int | None = None,
    ) -> HistoricalRelationshipReclassificationResult:
        if (
            not isinstance(batch_size, int)
            or isinstance(batch_size, bool)
            or batch_size <= 0
        ):
            raise ValueError("batch_size must be a positive integer")
        self._validate_limit(max_relationships)

        after_observation_id = 0
        scanned = updated = unchanged = unmapped = incompatible = conflicts = batches = 0
        truncated = False
        while True:
            remaining = (
                max_relationships - scanned if max_relationships is not None else None
            )
            if remaining is not None and remaining <= 0:
                truncated = True
                break
            page_size = min(batch_size, remaining) if remaining else batch_size
            rows = await self.fetch_observations(
                user_name=user_name,
                project_id=project_id,
                after_observation_id=after_observation_id,
                limit=page_size,
            )
            if not rows:
                break
            plan = plan_relationship_reclassification(rows, domain)
            batch_result = await self.apply(
                user_name=user_name,
                project_id=project_id,
                domain=domain,
                changes=plan.changes,
            )
            scanned += plan.scanned
            updated += batch_result.updated
            unchanged += plan.unchanged
            unmapped += plan.unmapped
            incompatible += plan.incompatible
            conflicts += batch_result.conflicts
            batches += 1
            after_observation_id = max(int(row["observation_id"]) for row in rows)
            if len(rows) < page_size:
                break
            if max_relationships is not None and scanned >= max_relationships:
                probe = await self.fetch_observations(
                    user_name=user_name,
                    project_id=project_id,
                    after_observation_id=after_observation_id,
                    limit=1,
                )
                truncated = bool(probe)
                break

        return HistoricalRelationshipReclassificationResult(
            domain_version=domain.version,
            scanned=scanned,
            updated=updated,
            unchanged=unchanged,
            unmapped=unmapped,
            incompatible=incompatible,
            conflicts=conflicts,
            batches=batches,
            truncated=truncated,
        )
