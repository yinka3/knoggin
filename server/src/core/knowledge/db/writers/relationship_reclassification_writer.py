"""Bounded, transactional persistence for historical relationship normalization."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from common.conf.domain_config import CompiledDomain
from common.scoping import require_scope_value
from common.utils.time_utils import get_now_ms
from core.knowledge.relationship_reclassification import (
    RelationshipReclassification,
    RelationshipReclassificationPlan,
    plan_relationship_reclassification,
)


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
    """Normalize historical relationship aggregates without semantic guessing."""

    def __init__(self, client):
        self.client = client

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

    async def fetch_relationships(
        self,
        *,
        user_name: str,
        project_id: str,
        after_relationship_id: str = "",
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch unrecognized aggregates in stable identity order."""

        user_name, project_id = self._scope(
            user_name,
            project_id,
            "fetch relationship reclassification",
        )
        if not isinstance(after_relationship_id, str):
            raise ValueError("after_relationship_id must be text")
        self._validate_limit(limit)

        query = """
            SELECT
                r.relationship_id,
                r.user_name,
                r.project_id,
                r.entity_a_id,
                r.entity_b_id,
                r.relationship_type,
                r.canonical_relationship_type,
                r.observed_relationship_label,
                r.domain_status,
                r."symmetric",
                observed.source_type,
                observed.target_type
            FROM public.relationships r
            LEFT JOIN LATERAL (
                SELECT source_type, target_type
                FROM public.relationship_observations
                WHERE relationship_id = r.relationship_id
                  AND project_id = r.project_id
                ORDER BY observation_id
                LIMIT 1
            ) observed ON TRUE
            WHERE r.user_name = %s
              AND r.project_id = %s
              AND r.domain_status = 'unrecognized'
              AND r.relationship_id > %s
            ORDER BY r.relationship_id
        """
        params: list[Any] = [user_name, project_id, after_relationship_id]
        if limit is not None:
            query += " LIMIT %s"
            params.append(limit)
        return list(await self.client.fetch_all(query, tuple(params)))

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
        rows = await self.fetch_relationships(
            user_name=user_name,
            project_id=project_id,
            limit=limit,
        )
        return plan_relationship_reclassification(rows, domain)

    @staticmethod
    def _observation_sql() -> str:
        return """
            WITH rewritten AS (
                SELECT
                    project_id, user_name, session_id, message_id,
                    source_entity_id, target_entity_id, source_type, target_type,
                    observed_relationship_label, confidence, context,
                    observed_at_ms
                FROM public.relationship_observations
                WHERE relationship_id = %s AND project_id = %s
            )
            INSERT INTO public.relationship_observations (
                relationship_id, project_id, user_name, session_id, message_id,
                source_entity_id, target_entity_id, source_type, target_type,
                observed_relationship_label, canonical_relationship_type,
                domain_status, confidence, context, observed_at_ms
            )
            SELECT
                %s, project_id, user_name, session_id, message_id,
                CASE WHEN %s THEN LEAST(source_entity_id, target_entity_id)
                     ELSE source_entity_id END,
                CASE WHEN %s THEN GREATEST(source_entity_id, target_entity_id)
                     ELSE target_entity_id END,
                source_type, target_type, observed_relationship_label,
                %s, 'recognized', confidence, context, observed_at_ms
            FROM rewritten
            ON CONFLICT (
                project_id, user_name, session_id, message_id,
                source_entity_id, target_entity_id, observed_relationship_label
            ) DO UPDATE SET
                relationship_id = EXCLUDED.relationship_id,
                canonical_relationship_type = EXCLUDED.canonical_relationship_type,
                domain_status = EXCLUDED.domain_status,
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
        """

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
        relationship_ids = [change.relationship_id for change in planned]
        if any(not isinstance(item, str) or not item for item in relationship_ids):
            raise ValueError("Relationship reclassification IDs must be non-blank text")
        if len(set(relationship_ids)) != len(relationship_ids):
            raise ValueError("Relationship reclassification IDs must be unique")

        async with self.client.transaction() as cur:
            await cur.execute(
                """
                SELECT
                    r.relationship_id, r.project_id, r.entity_a_id, r.entity_b_id,
                    r.relationship_type, r.canonical_relationship_type,
                    r.observed_relationship_label, r.domain_status, r."symmetric",
                    observed.source_type, observed.target_type
                FROM public.relationships r
                LEFT JOIN LATERAL (
                    SELECT source_type, target_type
                    FROM public.relationship_observations
                    WHERE relationship_id = r.relationship_id
                      AND project_id = r.project_id
                    ORDER BY observation_id
                    LIMIT 1
                ) observed ON TRUE
                WHERE r.user_name = %s
                  AND r.project_id = %s
                  AND r.relationship_id = ANY(%s)
                FOR UPDATE
                """,
                (user_name, project_id, relationship_ids),
            )
            current_rows = {
                str(row["relationship_id"]): row for row in await cur.fetchall()
            }
            conflicts = 0
            updated = 0
            now_ms = get_now_ms()
            for change in planned:
                current = current_rows.get(change.relationship_id)
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

                await cur.execute(
                    """
                    INSERT INTO public.relationships (
                        relationship_id, user_name, project_id, entity_a_id,
                        entity_b_id, relationship_type,
                        canonical_relationship_type, observed_relationship_label,
                        domain_status, "symmetric", weight, confidence, context,
                        last_seen_ms
                    )
                    SELECT
                        %s, user_name, project_id,
                        CASE WHEN %s THEN LEAST(entity_a_id, entity_b_id)
                             ELSE entity_a_id END,
                        CASE WHEN %s THEN GREATEST(entity_a_id, entity_b_id)
                             ELSE entity_b_id END,
                        %s, %s, observed_relationship_label, 'recognized', %s,
                        weight, confidence, context,
                        GREATEST(COALESCE(last_seen_ms, 0), %s)
                    FROM public.relationships
                    WHERE relationship_id = %s AND project_id = %s
                    ON CONFLICT (relationship_id) DO UPDATE SET
                        weight = CASE
                            WHEN %s THEN public.relationships.weight
                            ELSE public.relationships.weight + EXCLUDED.weight
                        END,
                        confidence = GREATEST(
                            public.relationships.confidence, EXCLUDED.confidence
                        ),
                        relationship_type = EXCLUDED.relationship_type,
                        canonical_relationship_type = EXCLUDED.canonical_relationship_type,
                        domain_status = EXCLUDED.domain_status,
                        "symmetric" = EXCLUDED."symmetric",
                        context = COALESCE(EXCLUDED.context, public.relationships.context),
                        last_seen_ms = GREATEST(
                            COALESCE(public.relationships.last_seen_ms, 0),
                            COALESCE(EXCLUDED.last_seen_ms, 0)
                        )
                    """,
                    (
                        change.new_relationship_id,
                        change.new_symmetric,
                        change.new_symmetric,
                        change.new_relationship_type,
                        change.new_canonical_relationship_type,
                        change.new_symmetric,
                        now_ms,
                        change.relationship_id,
                        project_id,
                        change.new_relationship_id == change.relationship_id,
                    ),
                )
                await cur.execute(
                    """
                    INSERT INTO public.relationship_evidence_refs (
                        relationship_id, project_id, user_name, session_id, message_id
                    )
                    SELECT %s, project_id, user_name, session_id, message_id
                    FROM public.relationship_evidence_refs
                    WHERE relationship_id = %s AND project_id = %s
                    ON CONFLICT (
                        relationship_id, user_name, session_id, message_id
                    ) DO NOTHING
                    """,
                    (change.new_relationship_id, change.relationship_id, project_id),
                )
                await cur.execute(
                    self._observation_sql(),
                    (
                        change.relationship_id,
                        project_id,
                        change.new_relationship_id,
                        change.new_symmetric,
                        change.new_symmetric,
                        change.new_canonical_relationship_type,
                    ),
                )
                await cur.execute(
                    """
                    INSERT INTO public.episode_relationships (
                        episode_id, project_id, relationship_id,
                        prominence_weight, is_central_relationship, source_message_count
                    )
                    SELECT episode_id, project_id, %s, prominence_weight,
                           is_central_relationship, source_message_count
                    FROM public.episode_relationships
                    WHERE relationship_id = %s AND project_id = %s
                    ON CONFLICT (episode_id, relationship_id) DO UPDATE SET
                        prominence_weight = GREATEST(
                            public.episode_relationships.prominence_weight,
                            EXCLUDED.prominence_weight
                        ),
                        is_central_relationship = (
                            public.episode_relationships.is_central_relationship
                            OR EXCLUDED.is_central_relationship
                        ),
                        source_message_count = GREATEST(
                            public.episode_relationships.source_message_count,
                            EXCLUDED.source_message_count
                        )
                    """,
                    (change.new_relationship_id, change.relationship_id, project_id),
                )
                if change.new_relationship_id != change.relationship_id:
                    await cur.execute(
                        "DELETE FROM public.relationship_evidence_refs "
                        "WHERE relationship_id = %s AND project_id = %s",
                        (change.relationship_id, project_id),
                    )
                    await cur.execute(
                        "DELETE FROM public.relationship_observations "
                        "WHERE relationship_id = %s AND project_id = %s",
                        (change.relationship_id, project_id),
                    )
                    await cur.execute(
                        "DELETE FROM public.episode_relationships "
                        "WHERE relationship_id = %s AND project_id = %s",
                        (change.relationship_id, project_id),
                    )
                    await cur.execute(
                        "DELETE FROM public.relationships "
                        "WHERE relationship_id = %s AND project_id = %s",
                        (change.relationship_id, project_id),
                    )
                updated += 1

        return RelationshipReclassificationBatchResult(
            scanned=len(planned), updated=updated, conflicts=conflicts
        )

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
        if max_relationships is not None and (
            not isinstance(max_relationships, int)
            or isinstance(max_relationships, bool)
            or max_relationships <= 0
        ):
            raise ValueError(
                "max_relationships must be a positive integer when provided"
            )

        after_relationship_id = ""
        scanned = updated = unchanged = unmapped = incompatible = conflicts = batches = 0
        truncated = False
        while True:
            remaining = None
            if max_relationships is not None:
                remaining = max_relationships - scanned
                if remaining <= 0:
                    truncated = True
                    break
            page_size = min(batch_size, remaining) if remaining else batch_size
            rows = await self.fetch_relationships(
                user_name=user_name,
                project_id=project_id,
                after_relationship_id=after_relationship_id,
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
            after_relationship_id = max(
                _relationship_sort_key(row["relationship_id"]) for row in rows
            )
            if len(rows) < page_size:
                break
            if max_relationships is not None and scanned >= max_relationships:
                probe = await self.fetch_relationships(
                    user_name=user_name,
                    project_id=project_id,
                    after_relationship_id=after_relationship_id,
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


def _relationship_sort_key(value: object) -> str:
    return str(value)
