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
from core.knowledge.db.writers.relationship_interpretation_writer import (
    RelationshipInterpretationWriter,
)
from core.knowledge.maintenance_reviews import (
    RelationshipInterpretationChange,
    RelationshipInterpretationPlan,
)
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
    """Plan historical normalization and delegate mutation to the typed primitive."""

    def __init__(self, client):
        self.client = client
        self.interpretation = RelationshipInterpretationWriter(client)

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
                observation.observed_relationship_label,
                observation.interpretation_source,
                source_context.entity_type AS source_type,
                target_context.entity_type AS target_type
            FROM public.relationship_observations observation
            JOIN public.relationships relationship
              ON relationship.relationship_id = observation.relationship_id
             AND relationship.project_id = observation.project_id
            LEFT JOIN public.project_entity_contexts source_context
              ON source_context.project_id = observation.project_id
             AND source_context.entity_id = observation.source_entity_id
            LEFT JOIN public.project_entity_contexts target_context
              ON target_context.project_id = observation.project_id
             AND target_context.entity_id = observation.target_entity_id
            WHERE observation.user_name = %s
              AND observation.project_id = %s
              AND observation.interpretation_source = 'observed'
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
        if len({change.observation_id for change in planned}) != len(planned):
            raise ValueError("Relationship observation IDs must be unique")
        typed_changes = [
            RelationshipInterpretationChange(
                observation_id=change.observation_id,
                expected_relationship_id=change.relationship_id,
                target_relationship_type=change.new_relationship_type,
                interpretation_source="domain",
            )
            for change in planned
            if change.project_id == project_id
        ]
        result = await self.interpretation.apply_plan(
            user_name=user_name,
            project_id=project_id,
            plan=RelationshipInterpretationPlan(changes=typed_changes),
            domain=domain,
        )
        conflicts = result.conflicts + sum(
            change.project_id != project_id for change in planned
        )
        return RelationshipReclassificationBatchResult(
            scanned=len(planned),
            updated=result.updated,
            conflicts=conflicts,
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
