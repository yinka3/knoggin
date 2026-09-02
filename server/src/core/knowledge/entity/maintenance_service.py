"""Application-owned, user-global entity identity maintenance."""

from __future__ import annotations

import hashlib
import json
import uuid
from contextlib import asynccontextmanager
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
    EntityMergeRollbackPlan,
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
        user_name: str,
    ) -> None:
        if postgres is None:
            raise ValueError("EntityMaintenanceService requires PostgreSQL")
        if not user_name or not user_name.strip():
            raise ValueError("EntityMaintenanceService requires user_name")
        self.postgres = postgres
        self.user_name = user_name.strip()
        self.writer = GlobalEntityMergeWriter(postgres)
        self.review_writer = MaintenanceReviewWriter(postgres)
        self.projection_rebuilder = GraphBuilder(postgres)

    @asynccontextmanager
    async def _cursor(self, cur=None):
        if cur is not None:
            yield cur
            return
        async with self.postgres.transaction() as transaction_cursor:
            yield transaction_cursor

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
        capture_frontiers: bool = True,
        cur=None,
    ) -> dict[str, Any]:
        """Build a typed merge plan and expose any required context decisions."""

        actor = user_name or self.user_name
        if not actor:
            raise ValueError("user_name is required for global maintenance")
        snapshot = await self.writer.snapshot(
            actor,
            survivor_entity_id,
            retired_entity_id,
            cur=cur,
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
        frontiers = (
            await self.capture_frontier(by_project, cur=cur)
            if capture_frontiers
            else {project_id: {"token": ""} for project_id in by_project}
        )
        definition_versions = await self._definition_versions(
            actor,
            by_project,
            cur=cur,
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
            frontier_tokens={
                project_id: frontier["token"]
                for project_id, frontier in sorted(frontiers.items())
            },
            definition_versions=definition_versions,
            expected_state_hash=self.state_hash(snapshot),
        )
        return {
            "plan": plan,
            "snapshot": snapshot,
            "affected_project_ids": sorted(by_project),
            "context_conflicts": conflicts,
            "ready": not conflicts,
            "state_hash": self.state_hash(snapshot),
            "frontiers": frontiers,
            "definition_versions": definition_versions,
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
        if not refs:
            raise ValueError("at least one merge evidence reference is required")
        valid_message_ids = {
            str(item["message_id"]) for item in preview["snapshot"]["message_refs"]
        }
        valid_episode_ids = {
            str(item["episode_id"])
            for item in preview["snapshot"]["episode_entities"]
        }
        invalid_refs = [
            ref
            for ref in refs
            if (
                ref.kind == "message" and ref.id not in valid_message_ids
            )
            or (ref.kind == "episode" and ref.id not in valid_episode_ids)
        ]
        if invalid_refs:
            raise ValueError("merge evidence must belong to one of the candidate entities")
        return await self.review_writer.open(
            user_name=actor,
            scope="user-global",
            project_id=None,
            kind="entity_merge",
            reasoning=reasoning,
            proposed_plan=preview["plan"],
            evidence_refs=refs,
            evidence_snapshot=preview["snapshot"],
            expected_state={
                "state_hash": preview["state_hash"],
                "frontiers": preview["frontiers"],
                "definition_versions": preview["definition_versions"],
            },
            dedupe_key=f"{survivor_entity_id}:{retired_entity_id}",
        )

    async def merge(
        self,
        plan: EntityMergePlan | dict[str, Any],
        *,
        user_name: str | None = None,
        expected_state_hash: str | None = None,
        review_id: str | None = None,
        review_expected_state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute a previously inspected typed plan."""

        actor = user_name or self.user_name
        if not actor:
            raise ValueError("user_name is required for global maintenance")
        typed_plan = (
            plan if isinstance(plan, EntityMergePlan) else EntityMergePlan.model_validate(plan)
        )
        merge_id = str(uuid.uuid4())
        choices = {
            choice.project_id: {
                "entity_type": choice.entity_type,
                "topic": choice.topic,
            }
            for choice in typed_plan.context_choices
        }
        async with self.postgres.transaction() as cur:
            await cur.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (f"entity-merge:{actor}",),
            )
            preview = await self.preview_merge(
                user_name=actor,
                survivor_entity_id=typed_plan.survivor_entity_id,
                retired_entity_id=typed_plan.retired_entity_id,
                context_choices=typed_plan.context_choices,
                capture_frontiers=False,
                cur=cur,
            )
            if preview["context_conflicts"]:
                raise EntityMergeConflict(preview["context_conflicts"])
            expected_hash = expected_state_hash or typed_plan.expected_state_hash
            if not expected_hash:
                raise ValueError("merge plan requires an expected state hash")
            if expected_hash != preview["state_hash"]:
                raise ValueError("merge plan is stale; entity evidence changed")
            if preview["affected_project_ids"] and set(typed_plan.frontier_tokens) != set(
                preview["affected_project_ids"]
            ):
                raise ValueError(
                    "merge plan requires a frontier token for every affected project"
                )
            expected_frontiers = {
                project_id: {"token": token}
                for project_id, token in typed_plan.frontier_tokens.items()
            }
            if not await self.revalidate_frontier(
                expected_frontiers,
                user_name=actor,
                cur=cur,
            ):
                raise ValueError("merge plan is stale; ingestion advanced after review")
            current_versions = await self._definition_versions(
                actor,
                preview["frontiers"],
                cur=cur,
            )
            if current_versions != typed_plan.definition_versions:
                raise ValueError("merge plan is stale; project definition changed")
            result = await self.writer.merge(
                user_name=actor,
                survivor_id=typed_plan.survivor_entity_id,
                retired_id=typed_plan.retired_entity_id,
                context_choices=choices,
                plan=typed_plan.model_dump(mode="json"),
                merge_id=merge_id,
                cur=cur,
            )
            if review_id is not None:
                await self.review_writer.transition(
                    review_id,
                    user_name=actor,
                    status="applied",
                    expected_state=review_expected_state,
                    actor=actor,
                    reason="Confirmed global entity merge applied",
                    cur=cur,
                )
        result["projection_errors"] = await self._rebuild_projections(
            result["affected_project_ids"],
            actor,
        )
        return result

    async def apply_merge_review(
        self,
        review_id: str,
        *,
        user_name: str | None = None,
        expected_state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute one open, user-confirmed global merge review."""

        actor = user_name or self.user_name
        if not actor:
            raise ValueError("user_name is required for global maintenance")
        review = await self.review_writer.get(review_id, user_name=actor)
        if (
            review is None
            or review.scope != "user-global"
            or review.kind != "entity_merge"
            or review.status != "open"
            or not isinstance(review.proposed_plan, EntityMergePlan)
        ):
            raise ValueError("Unknown or already-resolved global entity merge review")
        if expected_state is not None and review.expected_state != expected_state:
            raise ValueError("Maintenance review expected state no longer matches")
        result = await self.merge(
            review.proposed_plan,
            user_name=user_name,
            expected_state_hash=review.expected_state.get("state_hash"),
            review_id=review.review_id,
            review_expected_state=review.expected_state,
        )
        result["review_id"] = review.review_id
        return result

    @staticmethod
    def state_hash(snapshot: dict[str, Any]) -> str:
        payload = json.dumps(snapshot, sort_keys=True, default=str, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    async def plan_rollback(
        self,
        merge_id: str,
        *,
        user_name: str | None = None,
    ) -> dict[str, Any]:
        """Classify a merge inverse without mutating knowledge."""

        actor = user_name or self.user_name
        if not actor:
            raise ValueError("user_name is required for global maintenance")
        result = await self.writer.plan_rollback(merge_id=merge_id, user_name=actor)
        typed_plan = EntityMergeRollbackPlan(
            merge_id=merge_id,
            safe_mutation_ids=result["safe_mutation_ids"],
            conflicting_mutation_ids=[
                int(item["mutation_id"])
                for item in result["conflicting_mutations"]
            ],
            required_decisions=[
                f"Review post-merge change to {item['object_kind']}:{item['object_key']}"
                for item in result["conflicting_mutations"]
            ],
        )
        return {**result, "plan": typed_plan}

    async def rollback(
        self,
        merge_id: str,
        *,
        user_name: str | None = None,
        approved_mutation_ids: Iterable[int] = (),
    ) -> dict[str, Any]:
        """Apply safe inverse mutations and open a review for conflicts."""

        actor = user_name or self.user_name
        if not actor:
            raise ValueError("user_name is required for global maintenance")
        plan = await self.plan_rollback(merge_id, user_name=actor)
        approved_ids = sorted({int(item) for item in approved_mutation_ids})
        requested_ids = sorted(
            set(plan["safe_mutation_ids"]) | set(approved_ids)
        )
        result = await self.writer.rollback_safe(
            merge_id=merge_id,
            user_name=actor,
            safe_mutation_ids=requested_ids,
            force_mutation_ids=approved_ids,
        )
        if result["applied_mutation_ids"]:
            result["projection_errors"] = await self._rebuild_projections(
                result["affected_project_ids"],
                actor,
            )
        else:
            result["projection_errors"] = []
        conflicts = [
            item
            for item in plan["conflicting_mutations"]
            if int(item["mutation_id"]) not in set(approved_ids)
        ]
        concurrent_ids = set(result.get("concurrent_conflicts") or [])
        if concurrent_ids:
            conflicts.extend(
                {
                    "mutation_id": mutation_id,
                    "object_kind": "concurrent_change",
                    "object_key": str(mutation_id),
                    "expected": None,
                    "current": None,
                }
                for mutation_id in sorted(concurrent_ids)
            )
        if conflicts:
            review = await self.review_writer.open(
                user_name=actor,
                scope="user-global",
                project_id=None,
                kind="entity_merge_rollback_conflict",
                reasoning="Post-merge changes make part of the inverse ambiguous.",
                proposed_plan=plan["plan"],
                evidence_refs=[
                    EvidenceRef(kind="merge_mutation", id=str(item["mutation_id"]))
                    for item in conflicts
                ],
                evidence_snapshot={str(item["mutation_id"]): item for item in conflicts},
                expected_state={"merge_id": merge_id},
                dedupe_key=merge_id,
            )
            result.update(
                {
                    "policy_result": "partially_rolled_back",
                    "conflicts": conflicts,
                    "review_id": review.review_id,
                }
            )
        else:
            result["policy_result"] = "rolled_back"
        return result

    async def dismiss_review(
        self,
        review_id: str,
        *,
        user_name: str | None = None,
        expected_state: dict[str, Any] | None = None,
        reason: str | None = None,
    ):
        """Dismiss an open user-global review without mutating knowledge."""

        actor = user_name or self.user_name
        if not actor:
            raise ValueError("user_name is required for global maintenance")
        review = await self.review_writer.get(review_id, user_name=actor)
        if review is None or review.scope != "user-global" or review.status != "open":
            raise ValueError("Unknown or already-resolved global maintenance review")
        return await self.review_writer.transition(
            review_id,
            user_name=actor,
            status="dismissed",
            expected_state=expected_state,
            actor=actor,
            reason=reason,
        )

    async def list_reviews(self, *, user_name: str | None = None):
        """Return user-global maintenance history for the local user."""

        actor = user_name or self.user_name
        if not actor:
            raise ValueError("user_name is required for global maintenance")
        return await self.review_writer.list(
            user_name=actor,
            scope="user-global",
        )

    async def _rebuild_projections(
        self,
        project_ids: Iterable[str],
        user_name: str,
    ) -> list[dict[str, str]]:
        errors = []
        for project_id in sorted(set(project_ids)):
            try:
                await self.projection_rebuilder.rebuild_project_projection(
                    project_id,
                    user_name,
                )
            except Exception as exc:  # relational mutation remains durable/auditable
                errors.append({"project_id": project_id, "error": str(exc)})
        return errors

    async def capture_frontier(
        self,
        project_ids: Iterable[str],
        *,
        user_name: str | None = None,
        cur=None,
    ) -> dict[str, dict[str, Any]]:
        """Capture stable ingestion boundaries for the affected projects."""

        actor = user_name or self.user_name
        if not actor:
            raise ValueError("user_name is required for global maintenance")
        normalized_projects = sorted({str(project_id).strip() for project_id in project_ids if str(project_id).strip()})
        result: dict[str, dict[str, Any]] = {}
        async with self._cursor(cur) as active_cur:
            for project_id in normalized_projects:
                await active_cur.execute(
                    """
                    SELECT
                        count(*) FILTER (WHERE ingestion_state IN
                            ('waiting_for_seal', 'ready', 'claimed')) AS pending_count,
                        COALESCE(max(message_id) FILTER (WHERE ingestion_state IN
                            ('processed', 'failed', 'excluded')), 0) AS frontier_message_id,
                        max(timestamp_ms) FILTER (WHERE ingestion_state IN
                            ('processed', 'failed', 'excluded')) AS frontier_timestamp_ms
                    FROM public.messages
                    WHERE user_name = %s AND project_id = %s
                      AND role = 'user' AND lifecycle_state <> 'superseded'
                    """,
                    (actor, project_id),
                )
                row = await active_cur.fetchone()
                pending = int(row["pending_count"] or 0)
                if pending:
                    raise RuntimeError(
                        f"project {project_id} has {pending} pending ingestion messages"
                    )
                message_id = int(row["frontier_message_id"] or 0)
                timestamp_ms = row["frontier_timestamp_ms"]
                token = self._frontier_token(message_id, timestamp_ms)
                await active_cur.execute(
                    """
                    INSERT INTO public.maintenance_frontiers
                        (user_name, project_id, frontier_message_id,
                         frontier_timestamp_ms, frontier_token)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (user_name, project_id) DO UPDATE SET
                        frontier_message_id = EXCLUDED.frontier_message_id,
                        frontier_timestamp_ms = EXCLUDED.frontier_timestamp_ms,
                        frontier_token = EXCLUDED.frontier_token,
                        updated_at = now()
                    """,
                    (actor, project_id, message_id, timestamp_ms, token),
                )
                result[project_id] = {
                    "project_id": project_id,
                    "message_id": message_id,
                    "timestamp_ms": timestamp_ms,
                    "token": token,
                }
        return result

    async def revalidate_frontier(
        self,
        frontiers: dict[str, dict[str, Any]],
        *,
        user_name: str | None = None,
        cur=None,
    ) -> bool:
        """Return false when ingestion advanced or live work appeared."""

        actor = user_name or self.user_name
        if not actor:
            raise ValueError("user_name is required for global maintenance")
        for project_id, frontier in frontiers.items():
            query = """
                SELECT
                    count(*) FILTER (WHERE ingestion_state IN
                        ('waiting_for_seal', 'ready', 'claimed')) AS pending_count,
                    COALESCE(max(message_id) FILTER (WHERE ingestion_state IN
                        ('processed', 'failed', 'excluded')), 0) AS frontier_message_id,
                    max(timestamp_ms) FILTER (WHERE ingestion_state IN
                        ('processed', 'failed', 'excluded')) AS frontier_timestamp_ms
                FROM public.messages
                WHERE user_name = %s AND project_id = %s
                  AND role = 'user' AND lifecycle_state <> 'superseded'
                """
            if cur is None:
                row = await self.postgres.fetch_one(query, (actor, project_id))
            else:
                await cur.execute(query, (actor, project_id))
                row = await cur.fetchone()
            if int(row["pending_count"] or 0):
                return False
            token = self._frontier_token(
                int(row["frontier_message_id"] or 0), row["frontier_timestamp_ms"]
            )
            if token != frontier.get("token"):
                return False
        return True

    async def preflight(self, *, user_name: str | None = None) -> dict[str, Any]:
        """Run cheap signals before any optional maintenance model call."""

        candidates = await self.discover_duplicate_candidates(user_name=user_name, limit=100)
        return {
            "candidate_count": len(candidates),
            "candidates": candidates,
            "llm_required": bool(candidates),
            "reason": "deterministic duplicate signals found"
            if candidates
            else "no deterministic maintenance candidates",
        }

    @staticmethod
    def _frontier_token(message_id: int, timestamp_ms: int | None) -> str:
        return hashlib.sha256(
            f"{int(message_id)}:{timestamp_ms if timestamp_ms is not None else ''}".encode()
        ).hexdigest()

    async def _definition_versions(
        self,
        user_name: str,
        project_ids: Iterable[str] | dict[str, Any],
        *,
        cur=None,
    ) -> dict[str, int]:
        ids = project_ids.keys() if isinstance(project_ids, dict) else project_ids
        normalized = sorted({str(project_id) for project_id in ids})
        if not normalized:
            return {}
        query = """
            SELECT project_id, COALESCE((domain_config->>'version')::integer, 0) AS version
            FROM public.projects
            WHERE user_name = %s AND project_id = ANY(%s)
            """
        if cur is None:
            rows = await self.postgres.fetch_all(query, (user_name, normalized))
        else:
            await cur.execute(query, (user_name, normalized))
            rows = await cur.fetchall()
        return {str(row["project_id"]): int(row["version"] or 0) for row in rows}
