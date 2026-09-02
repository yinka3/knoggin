"""Application-owned, user-global entity identity maintenance."""

from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any, Iterable

from core.knowledge.db.projection_rebuilder import GraphBuilder
from core.knowledge.db.writers.global_entity_merge_writer import (
    EntityMergeConflict,
    GlobalEntityMergeWriter,
)
from core.knowledge.db.writers.maintenance_review_writer import MaintenanceReviewWriter
from core.knowledge.maintenance_reviews import (
    EntityContextMergeChoice,
    EntityMergePlan,
    EvidenceRef,
)
from infrastructure.postgres_client import PostgresClient


class EntityMaintenanceService:
    """Repair user-global identity without requiring a project runtime.

    Candidate discovery is deliberately deterministic.  A caller may use the
    returned evidence to ask an LLM for bounded reasoning, but only a typed
    :class:`EntityMergePlan` reaches ``GlobalEntityMergeWriter``.
    """

    def __init__(
        self,
        postgres: PostgresClient,
        knowledge_store=None,
        user_name: str | None = None,
    ) -> None:
        self.postgres = postgres
        self.knowledge_store = knowledge_store
        self.user_name = user_name
        self.writer = GlobalEntityMergeWriter(postgres)
        self.review_writer = MaintenanceReviewWriter(postgres)
        self.projection_rebuilder = GraphBuilder(postgres)

    async def discover_duplicate_candidates(
        self,
        *,
        user_name: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Find deterministic global duplicate signals, without choosing a survivor."""

        actor = user_name or self.user_name
        if not actor or not actor.strip():
            raise ValueError("user_name is required for global maintenance")
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 500:
            raise ValueError("limit must be between 1 and 500")
        rows = await self.postgres.fetch_all(
            """
            WITH active AS (
                SELECT e.entity_id, e.canonical_name,
                       lower(regexp_replace(btrim(e.canonical_name), '\\s+', ' ', 'g')) AS normalized_name
                FROM public.entities e
                WHERE e.user_name = %s AND e.status = 'active'
                  AND e.entity_id <> 1
            ), pairs AS (
                SELECT a.entity_id AS entity_a_id,
                       b.entity_id AS entity_b_id,
                       a.canonical_name AS name_a,
                       b.canonical_name AS name_b,
                       CASE WHEN a.normalized_name = b.normalized_name
                            THEN 'canonical_name' ELSE 'alias' END AS signal
                FROM active a
                JOIN active b ON b.entity_id > a.entity_id
                WHERE a.normalized_name = b.normalized_name
                   OR EXISTS (
                       SELECT 1 FROM public.entity_aliases alias
                       WHERE alias.entity_id = a.entity_id
                         AND lower(alias.alias) = b.normalized_name
                   )
                   OR EXISTS (
                       SELECT 1 FROM public.entity_aliases alias
                       WHERE alias.entity_id = b.entity_id
                         AND lower(alias.alias) = a.normalized_name
                   )
            )
            SELECT p.entity_a_id, p.entity_b_id, p.name_a, p.name_b, p.signal,
                   COALESCE(array_agg(DISTINCT context.project_id)
                            FILTER (WHERE context.project_id IS NOT NULL), '{}') AS project_ids,
                   (SELECT count(*) FROM public.message_entity_refs ref
                    WHERE ref.entity_id IN (p.entity_a_id, p.entity_b_id)) AS message_ref_count
            FROM pairs p
            LEFT JOIN public.project_entity_contexts context
              ON context.entity_id IN (p.entity_a_id, p.entity_b_id)
            GROUP BY p.entity_a_id, p.entity_b_id, p.name_a, p.name_b, p.signal
            ORDER BY p.entity_a_id, p.entity_b_id
            LIMIT %s
            """,
            (actor.strip(), limit),
        )
        return [
            {
                "entity_a_id": int(row["entity_a_id"]),
                "entity_b_id": int(row["entity_b_id"]),
                "name_a": row["name_a"],
                "name_b": row["name_b"],
                "signal": row["signal"],
                "project_ids": list(row.get("project_ids") or []),
                "message_ref_count": int(row.get("message_ref_count") or 0),
                "survivor_required": True,
            }
            for row in rows
        ]

    async def preview_merge(
        self,
        *,
        user_name: str | None = None,
        survivor_entity_id: int,
        retired_entity_id: int,
        context_choices: Iterable[EntityContextMergeChoice | dict[str, Any]] = (),
    ) -> dict[str, Any]:
        """Build a typed merge plan and expose any required context decisions."""

        actor = user_name or self.user_name
        if not actor:
            raise ValueError("user_name is required for global maintenance")
        snapshot = await self.writer.snapshot(
            actor, survivor_entity_id, retired_entity_id
        )
        entities = {int(row["entity_id"]): row for row in snapshot["entities"]}
        if set(entities) != {survivor_entity_id, retired_entity_id}:
            raise ValueError("both active entities must exist in the user's scope")
        if any(row["status"] != "active" for row in entities.values()):
            raise ValueError("only active entities can be merged")
        choices: dict[str, dict[str, str]] = {}
        for raw_choice in context_choices:
            choice = (
                raw_choice
                if isinstance(raw_choice, EntityContextMergeChoice)
                else EntityContextMergeChoice.model_validate(raw_choice)
            )
            choices[choice.project_id] = {
                "entity_type": choice.entity_type,
                "topic": choice.topic,
            }
        by_project: dict[str, dict[int, dict[str, Any]]] = {}
        for context in snapshot["contexts"]:
            by_project.setdefault(context["project_id"], {})[
                int(context["entity_id"])
            ] = context
        conflicts = []
        for project_id, contexts in by_project.items():
            primary = contexts.get(survivor_entity_id)
            secondary = contexts.get(retired_entity_id)
            if primary and secondary and (
                primary["entity_type"] != secondary["entity_type"]
                or primary["topic"] != secondary["topic"]
            ) and project_id not in choices:
                conflicts.append(
                    {
                        "project_id": project_id,
                        "survivor": {"entity_type": primary["entity_type"], "topic": primary["topic"]},
                        "retired": {"entity_type": secondary["entity_type"], "topic": secondary["topic"]},
                    }
                )
        plan = EntityMergePlan(
            survivor_entity_id=survivor_entity_id,
            retired_entity_id=retired_entity_id,
            context_choices=[
                EntityContextMergeChoice(
                    project_id=project_id,
                    entity_type=value["entity_type"],
                    topic=value["topic"],
                )
                for project_id, value in sorted(choices.items())
            ],
        )
        return {
            "plan": plan,
            "snapshot": snapshot,
            "affected_project_ids": sorted(by_project),
            "context_conflicts": conflicts,
            "ready": not conflicts,
            "state_hash": self.state_hash(snapshot),
        }

    async def propose(
        self,
        *,
        user_name: str | None = None,
        survivor_entity_id: int,
        retired_entity_id: int,
        reasoning: str,
        evidence_refs: Iterable[EvidenceRef | dict[str, Any] | int | str] = (),
        context_choices: Iterable[EntityContextMergeChoice | dict[str, Any]] = (),
    ):
        """Persist an explicit global merge review; no mutation occurs."""

        actor = user_name or self.user_name
        if not actor:
            raise ValueError("user_name is required for global maintenance")
        preview = await self.preview_merge(
            user_name=actor,
            survivor_entity_id=survivor_entity_id,
            retired_entity_id=retired_entity_id,
            context_choices=context_choices,
        )
        refs = [
            value if isinstance(value, EvidenceRef) else EvidenceRef.from_value(value)
            for value in evidence_refs
        ]
        return await self.review_writer.open(
            user_name=actor,
            scope="user-global",
            project_id=None,
            kind="entity_merge",
            reasoning=reasoning,
            proposed_plan=preview["plan"],
            evidence_refs=refs,
            evidence_snapshot=preview["snapshot"],
            expected_state={"state_hash": preview["state_hash"]},
            dedupe_key=f"{survivor_entity_id}:{retired_entity_id}",
        )

    async def merge(
        self,
        plan: EntityMergePlan | dict[str, Any],
        *,
        user_name: str | None = None,
        expected_state_hash: str | None = None,
    ) -> dict[str, Any]:
        """Execute a previously inspected typed plan."""

        actor = user_name or self.user_name
        if not actor:
            raise ValueError("user_name is required for global maintenance")
        typed_plan = (
            plan if isinstance(plan, EntityMergePlan) else EntityMergePlan.model_validate(plan)
        )
        preview = await self.preview_merge(
            user_name=actor,
            survivor_entity_id=typed_plan.survivor_entity_id,
            retired_entity_id=typed_plan.retired_entity_id,
            context_choices=typed_plan.context_choices,
        )
        if preview["context_conflicts"]:
            raise EntityMergeConflict(preview["context_conflicts"])
        if expected_state_hash and expected_state_hash != preview["state_hash"]:
            raise ValueError("merge plan is stale; entity evidence changed")
        merge_id = str(uuid.uuid4())
        choices = {
            choice.project_id: {
                "entity_type": choice.entity_type,
                "topic": choice.topic,
            }
            for choice in typed_plan.context_choices
        }
        result = await self.writer.merge(
            user_name=actor,
            survivor_id=typed_plan.survivor_entity_id,
            retired_id=typed_plan.retired_entity_id,
            context_choices=choices,
            plan=typed_plan.model_dump(mode="json"),
            merge_id=merge_id,
        )
        projection_errors = []
        for project_id in result["affected_project_ids"]:
            try:
                await self.projection_rebuilder.rebuild_project_projection(
                    project_id, actor
                )
            except Exception as exc:  # relational merge remains durable/auditable
                projection_errors.append({"project_id": project_id, "error": str(exc)})
        result["projection_errors"] = projection_errors
        return result

    @staticmethod
    def state_hash(snapshot: dict[str, Any]) -> str:
        payload = json.dumps(snapshot, sort_keys=True, default=str, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

