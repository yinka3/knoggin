"""Persistence for typed semantic-maintenance reviews."""

from __future__ import annotations

import hashlib
import json
import uuid
from contextlib import asynccontextmanager
from typing import Any, Iterable

from common.scoping import require_scope_value
from core.knowledge.maintenance_reviews import (
    EvidenceRef,
    MaintenancePlan,
    MaintenanceReview,
    ReviewScope,
    ReviewStatus,
    review_from_row,
    validate_plan,
)


class MaintenanceReviewWriter:
    """Create and transition review proposals with append-only audit events."""

    _NAMESPACE = uuid.UUID("6cce6f7e-9a95-4e58-9e0b-4f7f1a7e6f34")

    def __init__(self, client) -> None:
        self.client = client

    @asynccontextmanager
    async def _cursor_context(self, cur=None):
        if cur is not None:
            yield cur
            return
        async with self.client.transaction() as transaction_cursor:
            yield transaction_cursor

    @classmethod
    def signature(
        cls,
        *,
        user_name: str,
        scope: ReviewScope,
        project_id: str | None,
        kind: str,
        dedupe_key: str | None,
        evidence_refs: Iterable[EvidenceRef],
        plan: MaintenancePlan,
        expected_state: dict[str, Any] | None = None,
    ) -> str:
        payload = {
            "user_name": user_name,
            "scope": scope,
            "project_id": project_id,
            "kind": kind,
            "dedupe_key": dedupe_key,
            "evidence_refs": [ref.model_dump(mode="json") for ref in evidence_refs],
            "plan": plan.model_dump(mode="json"),
            "expected_state": expected_state or {},
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    async def open(
        self,
        *,
        user_name: str,
        scope: ReviewScope,
        project_id: str | None,
        kind: str,
        reasoning: str,
        proposed_plan: MaintenancePlan | dict[str, Any],
        evidence_refs: Iterable[EvidenceRef | dict[str, Any] | int | str] = (),
        evidence_snapshot: dict[str, Any] | None = None,
        expected_state: dict[str, Any] | None = None,
        dedupe_key: str | None = None,
        cur=None,
    ) -> MaintenanceReview:
        user_name = require_scope_value(user_name, "user_name", "open_maintenance_review")
        kind = require_scope_value(kind, "kind", "open_maintenance_review")
        if scope == "project":
            project_id = require_scope_value(
                project_id, "project_id", "open_maintenance_review"
            )
        elif project_id is not None:
            project_id = require_scope_value(
                project_id, "project_id", "open_maintenance_review"
            )
        if not isinstance(reasoning, str) or not reasoning.strip():
            raise ValueError("reasoning must not be blank")
        plan = validate_plan(proposed_plan)
        refs = [
            item
            if isinstance(item, EvidenceRef)
            else EvidenceRef.from_value(item)
            for item in evidence_refs
        ]
        signature = self.signature(
            user_name=user_name,
            scope=scope,
            project_id=project_id,
            kind=kind,
            dedupe_key=dedupe_key,
            evidence_refs=refs,
            plan=plan,
            expected_state=expected_state,
        )
        review_id = str(uuid.uuid5(self._NAMESPACE, signature))
        review = MaintenanceReview(
            review_id=review_id,
            user_name=user_name,
            scope=scope,
            project_id=project_id,
            kind=kind,
            dedupe_key=dedupe_key,
            evidence_refs=refs,
            evidence_snapshot=evidence_snapshot or {},
            reasoning=" ".join(reasoning.split()),
            proposed_plan=plan,
            expected_state=expected_state or {},
            status="open",
        )
        params = (
            review.review_id,
            review.user_name,
            review.scope,
            review.project_id,
            review.kind,
            review.dedupe_key,
            json.dumps([ref.model_dump(mode="json") for ref in refs], sort_keys=True),
            json.dumps(review.evidence_snapshot, sort_keys=True, default=str),
            review.reasoning,
            json.dumps(plan.model_dump(mode="json"), sort_keys=True, default=str),
            json.dumps(review.expected_state, sort_keys=True, default=str),
            signature,
        )
        query = """
            INSERT INTO public.maintenance_reviews (
                review_id, user_name, scope, project_id, kind, dedupe_key,
                evidence_refs, evidence_snapshot, reasoning, proposed_plan,
                expected_state, status, signature, created_at, resolved_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s,
                    %s::jsonb, %s::jsonb, 'open', %s, now(), NULL)
            ON CONFLICT (signature) DO UPDATE SET
                updated_at = now()
            RETURNING review_id, user_name, scope, project_id, kind, dedupe_key,
                      evidence_refs, evidence_snapshot, reasoning, proposed_plan,
                      expected_state, status, created_at, resolved_at
        """
        async with self._cursor_context(cur) as active_cur:
            row = await self._fetchone(active_cur, query, params)
            if row is None:
                row = await self._fetchone(
                    active_cur,
                    """
                    SELECT review_id, user_name, scope, project_id, kind, dedupe_key,
                           evidence_refs, evidence_snapshot, reasoning, proposed_plan,
                           expected_state, status, created_at, resolved_at
                    FROM public.maintenance_reviews WHERE signature = %s
                    """,
                    (signature,),
                )
            if row is None:
                raise RuntimeError("Maintenance review disappeared during deduplication")
            hydrated = review_from_row(dict(row))
            for ref in refs:
                observation_id = None
                if ref.kind == "observation":
                    try:
                        observation_id = int(ref.id)
                    except ValueError:
                        observation_id = None
                await self._execute(
                    active_cur,
                    """
                    INSERT INTO public.maintenance_review_evidence
                        (review_id, evidence_kind, evidence_id, observation_id, snapshot)
                    VALUES (%s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (review_id, evidence_kind, evidence_id) DO NOTHING
                    """,
                    (
                        hydrated.review_id,
                        ref.kind,
                        ref.id,
                        observation_id,
                        json.dumps(
                            (evidence_snapshot or {}).get(ref.id, {}),
                            sort_keys=True,
                            default=str,
                        ),
                    ),
                )
            if hydrated.status == "open":
                await self._event(
                    active_cur,
                    review_id=hydrated.review_id,
                    status="open",
                    actor=user_name,
                    reason="review created or reaffirmed",
                )
            return hydrated

    async def get(
        self,
        review_id: str,
        *,
        user_name: str,
        project_id: str | None = None,
        cur=None,
    ):
        review_id = require_scope_value(review_id, "review_id", "get_maintenance_review")
        user_name = require_scope_value(user_name, "user_name", "get_maintenance_review")
        query = """
            SELECT review_id, user_name, scope, project_id, kind, dedupe_key,
                   evidence_refs, evidence_snapshot, reasoning, proposed_plan,
                   expected_state, status, created_at, resolved_at
            FROM public.maintenance_reviews
            WHERE review_id = %s AND user_name = %s
        """
        params: tuple[Any, ...] = (review_id, user_name)
        if project_id is not None:
            query += " AND project_id = %s"
            params += (project_id,)
        return self._hydrate(await self._fetchone(cur, query, params))

    async def get_by_key(
        self,
        *,
        user_name: str,
        project_id: str | None,
        kind: str,
        dedupe_key: str,
        cur=None,
    ) -> MaintenanceReview | None:
        row = await self._fetchone(
            cur,
            """
            SELECT review_id, user_name, scope, project_id, kind, dedupe_key,
                   evidence_refs, evidence_snapshot, reasoning, proposed_plan,
                   expected_state, status, created_at, resolved_at
            FROM public.maintenance_reviews
            WHERE user_name = %s AND project_id IS NOT DISTINCT FROM %s
              AND kind = %s AND dedupe_key = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (user_name, project_id, kind, dedupe_key),
        )
        return self._hydrate(row)

    async def list_open(
        self, *, user_name: str, project_id: str | None = None
    ) -> list[MaintenanceReview]:
        query = """
            SELECT review_id, user_name, scope, project_id, kind, dedupe_key,
                   evidence_refs, evidence_snapshot, reasoning, proposed_plan,
                   expected_state, status, created_at, resolved_at
            FROM public.maintenance_reviews
            WHERE user_name = %s AND status = 'open'
        """
        params: tuple[Any, ...] = (user_name,)
        if project_id is not None:
            query += " AND project_id = %s"
            params += (project_id,)
        query += " ORDER BY created_at DESC, review_id"
        rows = await self.client.fetch_all(query, params)
        return [review_from_row(dict(row)) for row in rows]

    async def list(
        self,
        *,
        user_name: str,
        project_id: str | None = None,
        scope: ReviewScope | None = None,
    ) -> list[MaintenanceReview]:
        """Return review history in deterministic creation order."""

        query = """
            SELECT review_id, user_name, scope, project_id, kind, dedupe_key,
                   evidence_refs, evidence_snapshot, reasoning, proposed_plan,
                   expected_state, status, created_at, resolved_at
            FROM public.maintenance_reviews
            WHERE user_name = %s
        """
        params: tuple[Any, ...] = (user_name,)
        if project_id is not None:
            query += " AND project_id = %s"
            params += (project_id,)
        if scope is not None:
            query += " AND scope = %s"
            params += (scope,)
        query += " ORDER BY created_at, review_id"
        rows = await self.client.fetch_all(query, params)
        return [review_from_row(dict(row)) for row in rows]

    async def transition(
        self,
        review_id: str,
        *,
        user_name: str,
        status: ReviewStatus,
        project_id: str | None = None,
        expected_state: dict[str, Any] | None = None,
        actor: str | None = None,
        reason: str | None = None,
        cur=None,
    ) -> MaintenanceReview:
        if status not in {"applied", "dismissed", "stale"}:
            raise ValueError("transition status must be applied, dismissed, or stale")
        review_id = require_scope_value(review_id, "review_id", "transition_maintenance_review")
        user_name = require_scope_value(user_name, "user_name", "transition_maintenance_review")
        select_query = """
            SELECT review_id, user_name, scope, project_id, kind, dedupe_key,
                   evidence_refs, evidence_snapshot, reasoning, proposed_plan,
                   expected_state, status, created_at, resolved_at
            FROM public.maintenance_reviews
            WHERE review_id = %s AND user_name = %s
        """
        select_params: tuple[Any, ...] = (review_id, user_name)
        if project_id is not None:
            select_query += " AND project_id = %s"
            select_params += (project_id,)

        # Keep the compare, state transition, event, and returned snapshot in
        # one transaction.  In particular, callers that already hold a
        # transaction cursor must not compare through a second pooled
        # connection.
        async with self._cursor_context(cur) as active_cur:
            current_row = await self._fetchone(active_cur, select_query, select_params)
            current = self._hydrate(current_row)
            if current is None or current.status != "open":
                raise ValueError("Unknown or already-resolved maintenance review")
            if expected_state is not None and current.expected_state != expected_state:
                raise ValueError("Maintenance review expected state no longer matches")

            query = """
                UPDATE public.maintenance_reviews
                SET status = %s, resolved_at = COALESCE(resolved_at, now()), updated_at = now()
                WHERE review_id = %s AND user_name = %s AND status = 'open'
            """
            params = (status, review_id, user_name)
            if project_id is not None:
                query += " AND project_id = %s"
                params += (project_id,)
            await self._execute(active_cur, query, params)
            if getattr(active_cur, "rowcount", 1) == 0:
                raise ValueError("Unknown or already-resolved maintenance review")
            await self._event(
                active_cur,
                review_id=review_id,
                status=status,
                actor=actor or user_name,
                reason=reason,
            )
            result = self._hydrate(
                await self._fetchone(active_cur, select_query, select_params)
            )
            if result is None:
                raise ValueError("Unknown or already-resolved maintenance review")
            return result

    async def mark_stale_for_observations(
        self,
        *,
        user_name: str,
        project_id: str,
        observation_ids: Iterable[int],
        reason: str = "Referenced observation interpretation changed",
        exclude_review_id: str | None = None,
        cur=None,
    ) -> int:
        ids = sorted({int(value) for value in observation_ids if int(value) > 0})
        if not ids:
            return 0
        query = """
            UPDATE public.maintenance_reviews review
            SET status = 'stale', resolved_at = COALESCE(resolved_at, now()), updated_at = now()
            WHERE review.user_name = %s AND review.project_id = %s
              AND review.status = 'open'
              AND (%s::text IS NULL OR review.review_id <> %s)
              AND EXISTS (
                  SELECT 1 FROM public.maintenance_review_evidence evidence
                  WHERE evidence.review_id = review.review_id
                    AND evidence.observation_id = ANY(%s)
              )
            RETURNING review.review_id
        """
        async with self._cursor_context(cur) as active_cur:
            rows = await self._fetchall(
                active_cur,
                query,
                (user_name, project_id, exclude_review_id, exclude_review_id, ids),
            )
            for row in rows:
                await self._event(
                    active_cur,
                    review_id=row["review_id"],
                    status="stale",
                    actor=user_name,
                    reason=reason,
                )
            return len(rows)

    async def mark_stale_for_definition(
        self,
        *,
        user_name: str,
        project_id: str,
        definition_version: int,
        reason: str = "Relationship definition version changed",
        cur=None,
    ) -> int:
        """Invalidate open relationship reviews built against an older definition."""

        if definition_version < 0:
            raise ValueError("definition_version must be non-negative")
        query = """
            UPDATE public.maintenance_reviews
            SET status = 'stale', resolved_at = COALESCE(resolved_at, now()), updated_at = now()
            WHERE user_name = %s AND project_id = %s
              AND status = 'open'
              AND kind IN ('relationship_advisory', 'relationship_interpretation',
                           'relationship_domain_change')
              AND NULLIF(expected_state ->> 'domain_version', '') IS NOT NULL
              AND (expected_state ->> 'domain_version') ~ '^[0-9]+$'
              AND (expected_state ->> 'domain_version')::BIGINT < %s
            RETURNING review_id
        """
        async with self._cursor_context(cur) as active_cur:
            rows = await self._fetchall(
                active_cur,
                query,
                (user_name, project_id, definition_version),
            )
            for row in rows:
                await self._event(
                    active_cur,
                    review_id=row["review_id"],
                    status="stale",
                    actor=user_name,
                    reason=reason,
                )
            return len(rows)

    @staticmethod
    def _hydrate(row):
        return review_from_row(dict(row)) if row is not None else None

    async def _fetchone(self, cur, query: str, params: tuple[Any, ...]):
        if cur is not None:
            await cur.execute(query, params)
            return await cur.fetchone()
        return await self.client.fetch_one(query, params)

    async def _fetchall(self, cur, query: str, params: tuple[Any, ...]):
        if cur is not None:
            await cur.execute(query, params)
            return await cur.fetchall()
        return await self.client.fetch_all(query, params)

    async def _execute(self, cur, query: str, params: tuple[Any, ...]):
        if cur is not None:
            await cur.execute(query, params)
        else:
            await self.client.execute(query, params)

    async def _event(self, cur, *, review_id: str, status: str, actor: str, reason: str | None):
        await self._execute(
            cur,
            """
            INSERT INTO public.maintenance_review_events
                (review_id, status, actor, reason, created_at)
            VALUES (%s, %s, %s, %s, now())
            """,
            (review_id, status, actor, reason),
        )
