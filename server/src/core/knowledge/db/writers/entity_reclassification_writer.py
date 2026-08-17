"""Bounded, transactional persistence for historical entity reclassification."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from common.conf.domain_config import CompiledDomain
from common.scoping import IDENTITY_ENTITY_ID, require_scope_value
from common.utils.time_utils import get_now_ms
from core.knowledge.db.writers.age_projection_writer import AgeProjectionWriter
from core.knowledge.entity.reclassification import (
    EntityReclassification,
    ReclassificationPlan,
    plan_reclassification,
)
from infrastructure.postgres_client import PostgresClient


@dataclass(frozen=True, slots=True)
class ReclassificationBatchResult:
    """Outcome of one durable reclassification batch."""

    scanned: int
    updated: int
    conflicts: int


@dataclass(frozen=True, slots=True)
class HistoricalReclassificationResult:
    """Aggregate outcome for one explicit maintenance run."""

    domain_version: int
    scanned: int
    updated: int
    unchanged: int
    unmapped: int
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
            "conflicts": self.conflicts,
            "batches": self.batches,
            "truncated": self.truncated,
            "search_index_rebuild_required": self.updated > 0,
        }


class EntityReclassificationWriter:
    """Read and update entity type/topic pairs without guessing semantics."""

    def __init__(self, client: PostgresClient, graph_name: str = "knoggin_graph"):
        self.client = client
        self.projection = AgeProjectionWriter(client, graph_name=graph_name)

    @staticmethod
    def _scope(user_name: str, project_id: str, operation: str) -> tuple[str, str]:
        return (
            require_scope_value(user_name, "user_name", operation),
            require_scope_value(project_id, "project_id", operation),
        )

    async def fetch_entities(
        self,
        *,
        user_name: str,
        project_id: str,
        after_entity_id: int = 0,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch project entities in stable ID order for planning."""

        user_name, project_id = self._scope(
            user_name,
            project_id,
            "fetch reclassification entities",
        )
        if not isinstance(after_entity_id, int) or after_entity_id < 0:
            raise ValueError("after_entity_id must be a non-negative integer")
        if limit is not None and (
            not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0
        ):
            raise ValueError("limit must be a positive integer when provided")

        query = """
            SELECT entity_id, canonical_name, type, topic
            FROM public.entities
            WHERE user_name = %s
              AND project_id = %s
              AND entity_id <> %s
              AND entity_id > %s
            ORDER BY entity_id
        """
        params: list[Any] = [
            user_name,
            project_id,
            IDENTITY_ENTITY_ID,
            after_entity_id,
        ]
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
    ) -> ReclassificationPlan:
        """Plan changes without opening a write transaction."""

        rows = await self.fetch_entities(
            user_name=user_name,
            project_id=project_id,
            limit=limit,
        )
        return plan_reclassification(rows, domain)

    async def apply(
        self,
        *,
        user_name: str,
        project_id: str,
        domain: CompiledDomain,
        changes: Iterable[EntityReclassification],
    ) -> ReclassificationBatchResult:
        """Apply one pre-planned batch with compare-and-update semantics."""

        user_name, project_id = self._scope(
            user_name,
            project_id,
            "apply entity reclassification",
        )
        if not isinstance(domain, CompiledDomain):
            raise TypeError("domain must be a CompiledDomain")

        planned = tuple(changes)
        if not planned:
            return ReclassificationBatchResult(scanned=0, updated=0, conflicts=0)

        entity_ids = [change.entity_id for change in planned]
        if any(
            not isinstance(entity_id, int)
            or isinstance(entity_id, bool)
            or entity_id <= 0
            or entity_id == IDENTITY_ENTITY_ID
            for entity_id in entity_ids
        ):
            raise ValueError(
                "Reclassification entity IDs must be positive non-identity IDs"
            )
        if len(set(entity_ids)) != len(entity_ids):
            raise ValueError("Reclassification entity IDs must be unique")

        now_ms = get_now_ms()
        updated_entities: list[dict[str, Any]] = []
        conflicts = 0
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                SELECT entity_id, canonical_name, type, topic
                FROM public.entities
                WHERE user_name = %s
                  AND project_id = %s
                  AND entity_id = ANY(%s)
                  AND entity_id <> %s
                FOR UPDATE
                """,
                (user_name, project_id, entity_ids, IDENTITY_ENTITY_ID),
            )
            current_rows = {int(row["entity_id"]): row for row in await cur.fetchall()}

            for change in planned:
                current = current_rows.get(change.entity_id)
                if current is None:
                    conflicts += 1
                    continue

                # Revalidate the target against the same immutable snapshot that
                # produced the plan before touching durable state.
                current_plan = plan_reclassification((current,), domain)
                if not current_plan.changes:
                    conflicts += 1
                    continue
                target = current_plan.changes[0]
                if (
                    target.new_type != change.new_type
                    or target.new_topic != change.new_topic
                    or target.old_type != change.old_type
                    or target.old_topic != change.old_topic
                ):
                    conflicts += 1
                    continue

                await cur.execute(
                    """
                    UPDATE public.entities
                    SET type = %s,
                        topic = %s,
                        last_updated_ms = %s
                    WHERE user_name = %s
                      AND project_id = %s
                      AND entity_id = %s
                      AND type IS NOT DISTINCT FROM %s
                      AND topic IS NOT DISTINCT FROM %s
                    RETURNING entity_id
                    """,
                    (
                        change.new_type,
                        change.new_topic,
                        now_ms,
                        user_name,
                        project_id,
                        change.entity_id,
                        change.old_type,
                        change.old_topic,
                    ),
                )
                if await cur.fetchone() is None:
                    conflicts += 1
                    continue

                updated_entities.append(
                    {
                        "id": change.entity_id,
                        "project_id": project_id,
                        "type": change.new_type,
                        "topic": change.new_topic,
                        "last_updated": now_ms,
                    }
                )

            if updated_entities:
                await self.projection.project_entity_domain(cur, updated_entities)
                await self.projection.project_entity_topics(cur, updated_entities)

        return ReclassificationBatchResult(
            scanned=len(planned),
            updated=len(updated_entities),
            conflicts=conflicts,
        )

    async def reclassify(
        self,
        *,
        user_name: str,
        project_id: str,
        domain: CompiledDomain,
        batch_size: int = 100,
        max_entities: int | None = None,
    ) -> HistoricalReclassificationResult:
        """Run bounded batches until the scoped project is exhausted."""

        if (
            not isinstance(batch_size, int)
            or isinstance(batch_size, bool)
            or batch_size <= 0
        ):
            raise ValueError("batch_size must be a positive integer")
        if max_entities is not None and (
            not isinstance(max_entities, int)
            or isinstance(max_entities, bool)
            or max_entities <= 0
        ):
            raise ValueError("max_entities must be a positive integer when provided")

        after_entity_id = 0
        scanned = updated = unchanged = unmapped = conflicts = batches = 0
        truncated = False
        while True:
            remaining = None
            if max_entities is not None:
                remaining = max_entities - scanned
                if remaining <= 0:
                    truncated = True
                    break
            page_size = min(batch_size, remaining) if remaining else batch_size
            rows = await self.fetch_entities(
                user_name=user_name,
                project_id=project_id,
                after_entity_id=after_entity_id,
                limit=page_size,
            )
            if not rows:
                break

            plan = plan_reclassification(rows, domain)
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
            conflicts += batch_result.conflicts
            batches += 1
            after_entity_id = max(int(row["entity_id"]) for row in rows)

            if len(rows) < page_size:
                break
            if max_entities is not None and scanned >= max_entities:
                probe = await self.fetch_entities(
                    user_name=user_name,
                    project_id=project_id,
                    after_entity_id=after_entity_id,
                    limit=1,
                )
                truncated = bool(probe)
                break

        return HistoricalReclassificationResult(
            domain_version=domain.version,
            scanned=scanned,
            updated=updated,
            unchanged=unchanged,
            unmapped=unmapped,
            conflicts=conflicts,
            batches=batches,
            truncated=truncated,
        )
