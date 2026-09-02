"""Deterministic in-place relationship reinterpretation and reconciliation."""

from __future__ import annotations

import json
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Iterable

from common.conf.domain_config import CompiledDomain
from common.schema.ingestion.contracts import relationship_identity
from common.scoping import require_scope_value
from core.knowledge.db.projection_rebuilder import GraphBuilder
from core.knowledge.db.writers.maintenance_review_writer import MaintenanceReviewWriter
from core.knowledge.maintenance_reviews import RelationshipInterpretationPlan


@dataclass(frozen=True, slots=True)
class RelationshipInterpretationResult:
    updated: int
    detached: int
    conflicts: int
    stale_reviews: int
    audit_id: str | None


class RelationshipInterpretationWriter:
    """Apply a typed observation plan without replacing evidence identity."""

    def __init__(self, client, *, reviews=None, projection=None) -> None:
        self.client = client
        self.reviews = reviews or MaintenanceReviewWriter(client)
        self.projection = projection or GraphBuilder(client)

    @asynccontextmanager
    async def _cursor_context(self, cur=None):
        if cur is not None:
            yield cur
            return
        async with self.client.transaction() as transaction_cursor:
            yield transaction_cursor

    async def apply_plan(
        self,
        *,
        user_name: str,
        project_id: str,
        plan: RelationshipInterpretationPlan,
        domain: CompiledDomain | None = None,
        review_id: str | None = None,
        actor: str | None = None,
        cur=None,
    ) -> RelationshipInterpretationResult:
        user_name = require_scope_value(user_name, "user_name", "reinterpret_relationships")
        project_id = require_scope_value(project_id, "project_id", "reinterpret_relationships")
        if not isinstance(plan, RelationshipInterpretationPlan):
            raise TypeError("plan must be a RelationshipInterpretationPlan")
        if domain is not None and not isinstance(domain, CompiledDomain):
            raise TypeError("domain must be a CompiledDomain")
        if not plan.changes:
            return RelationshipInterpretationResult(0, 0, 0, 0, None)

        updated = detached = conflicts = 0
        changed_ids: list[int] = []
        changes_for_audit: list[dict] = []
        old_relationship_ids: set[str] = set()
        async with self._cursor_context(cur) as active_cur:
            ids = [change.observation_id for change in plan.changes]
            await active_cur.execute(
                """
                SELECT observation.observation_id, observation.relationship_id,
                       observation.project_id, observation.source_entity_id,
                       observation.target_entity_id, observation.observed_relationship_label,
                       relationship.relationship_type,
                       relationship."symmetric",
                       source_context.entity_type AS source_type,
                       target_context.entity_type AS target_type
                FROM public.relationship_observations observation
                LEFT JOIN public.relationships relationship
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
                  AND observation.observation_id = ANY(%s)
                FOR UPDATE OF observation
                """,
                (user_name, project_id, ids),
            )
            rows = {
                int(row["observation_id"]): row
                for row in await active_cur.fetchall()
            }
            prepared_changes = []
            for change in plan.changes:
                row = rows.get(change.observation_id)
                if row is None or row.get("relationship_id") != change.expected_relationship_id:
                    conflicts += 1
                    continue
                old_id = row.get("relationship_id")
                new_id = None
                relationship_type = None
                if change.target_relationship_type is not None:
                    relationship_type = change.target_relationship_type
                    symmetric = bool(row.get("symmetric", False))
                    if domain is not None:
                        definition = domain.relationship(relationship_type)
                        if definition is None or not domain.relationship_allows(
                            relationship_type,
                            row.get("source_type") or "",
                            row.get("target_type") or "",
                        ):
                            conflicts += 1
                            continue
                        symmetric = definition.symmetric
                    new_id = relationship_identity(
                        project_id,
                        int(row["source_entity_id"]),
                        int(row["target_entity_id"]),
                        relationship_type,
                        symmetric=symmetric,
                    )
                    a_id, b_id = int(row["source_entity_id"]), int(row["target_entity_id"])
                    if symmetric:
                        a_id, b_id = sorted((a_id, b_id))
                else:
                    symmetric = False
                    a_id = b_id = None
                prepared_changes.append(
                    (
                        change,
                        row,
                        old_id,
                        new_id,
                        relationship_type,
                        symmetric,
                        a_id,
                        b_id,
                    )
                )

            if review_id is not None and conflicts:
                raise ValueError(
                    "maintenance review is stale; relationship evidence changed"
                )

            for (
                change,
                row,
                old_id,
                new_id,
                relationship_type,
                symmetric,
                a_id,
                b_id,
            ) in prepared_changes:
                if old_id:
                    old_relationship_ids.add(str(old_id))
                if new_id is not None:
                    await active_cur.execute(
                        """
                        INSERT INTO public.relationships
                            (relationship_id, user_name, project_id, entity_a_id,
                             entity_b_id, relationship_type, symmetric)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (relationship_id) DO NOTHING
                        """,
                        (
                            new_id,
                            user_name,
                            project_id,
                            a_id,
                            b_id,
                            relationship_type,
                            symmetric,
                        ),
                    )
                await active_cur.execute(
                    """
                    UPDATE public.relationship_observations
                    SET relationship_id = %s,
                        interpretation_source = %s
                    WHERE observation_id = %s
                      AND project_id = %s
                    """,
                    (
                        new_id,
                        change.interpretation_source,
                        change.observation_id,
                        project_id,
                    ),
                )
                changed_ids.append(change.observation_id)
                changes_for_audit.append(
                    {
                        "observation_id": change.observation_id,
                        "old_relationship_id": old_id,
                        "new_relationship_id": new_id,
                        "interpretation_source": change.interpretation_source,
                    }
                )
                updated += 1
                if new_id is None:
                    detached += 1

            if changed_ids:
                await self._reconcile_episodes(active_cur, project_id, changed_ids)
                await self._remove_orphans(active_cur, project_id, sorted(old_relationship_ids))
                stale_reviews = await self.reviews.mark_stale_for_observations(
                    user_name=user_name,
                    project_id=project_id,
                    observation_ids=changed_ids,
                    reason="Relationship interpretation changed",
                    exclude_review_id=review_id,
                    cur=active_cur,
                )
                audit_id = str(uuid.uuid4())
                await active_cur.execute(
                    """
                    INSERT INTO public.maintenance_reinterpretation_audits
                        (audit_id, user_name, project_id, observation_ids, changes)
                    VALUES (%s, %s, %s, %s::jsonb, %s::jsonb)
                    """,
                    (
                        audit_id,
                        user_name,
                        project_id,
                        json.dumps(changed_ids),
                        json.dumps(changes_for_audit, sort_keys=True),
                    ),
                )
                await self.projection.rebuild_project_projection(
                    project_id, user_name, cur=active_cur
                )
                if review_id is not None and conflicts == 0:
                    await self.reviews.transition(
                        review_id,
                        user_name=user_name,
                        project_id=project_id,
                        status="applied",
                        actor=actor or user_name,
                        reason="Typed relationship interpretation applied",
                        cur=active_cur,
                    )
            else:
                stale_reviews = 0
                audit_id = None
        return RelationshipInterpretationResult(
            updated=updated,
            detached=detached,
            conflicts=conflicts,
            stale_reviews=stale_reviews,
            audit_id=audit_id,
        )

    # ``reinterpret`` is the semantic name used by maintenance callers; keep
    # the explicit ``apply_plan`` spelling for code that emphasizes the typed
    # input contract.
    reinterpret = apply_plan

    @staticmethod
    async def _reconcile_episodes(cur, project_id: str, observation_ids: Iterable[int]):
        await cur.execute("DROP TABLE IF EXISTS affected_maintenance_episodes")
        await cur.execute(
            """
            CREATE TEMP TABLE affected_maintenance_episodes ON COMMIT DROP AS
            SELECT DISTINCT episode_message.episode_id
            FROM public.episode_messages episode_message
            JOIN public.relationship_observations observation
              ON observation.project_id = episode_message.project_id
             AND observation.session_id = episode_message.session_id
             AND observation.message_id = episode_message.message_id
            WHERE episode_message.project_id = %s
              AND observation.observation_id = ANY(%s)
            """,
            (project_id, list(observation_ids)),
        )
        await cur.execute(
            """
            DELETE FROM public.episode_relationships
            WHERE project_id = %s
              AND episode_id IN (SELECT episode_id FROM affected_maintenance_episodes)
            """,
            (project_id,),
        )
        await cur.execute(
            """
            INSERT INTO public.episode_relationships
                (episode_id, project_id, relationship_id, source_message_count)
            SELECT episode_message.episode_id, episode_message.project_id,
                   observation.relationship_id,
                   COUNT(DISTINCT episode_message.message_id)
            FROM public.episode_messages episode_message
            JOIN public.relationship_observations observation
              ON observation.project_id = episode_message.project_id
             AND observation.session_id = episode_message.session_id
             AND observation.message_id = episode_message.message_id
            WHERE episode_message.project_id = %s
              AND episode_message.episode_id IN (
                  SELECT episode_id FROM affected_maintenance_episodes
              )
              AND observation.relationship_id IS NOT NULL
            GROUP BY episode_message.episode_id, episode_message.project_id,
                     observation.relationship_id
            """,
            (project_id,),
        )

    @staticmethod
    async def _remove_orphans(cur, project_id: str, relationship_ids: list[str]):
        if not relationship_ids:
            return
        await cur.execute(
            """
            DELETE FROM public.relationships relationship
            WHERE relationship.project_id = %s
              AND relationship.relationship_id = ANY(%s)
              AND NOT EXISTS (
                  SELECT 1 FROM public.relationship_observations observation
                  WHERE observation.project_id = relationship.project_id
                    AND observation.relationship_id = relationship.relationship_id
              )
            """,
            (project_id, relationship_ids),
        )
