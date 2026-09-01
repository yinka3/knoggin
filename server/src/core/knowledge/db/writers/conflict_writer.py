"""Conflict detection adapter backed by typed MaintenanceReviews."""

from __future__ import annotations

import hashlib
from typing import Any, Iterable

from common.scoping import require_scope_value
from core.knowledge.conflicts import (
    ConflictGroup,
    ConflictOrigin,
    ConflictResolutionKind,
    ConflictWriteResult,
)
from core.knowledge.db.writers.maintenance_review_writer import MaintenanceReviewWriter
from core.knowledge.maintenance_reviews import ConflictResolutionPlan


class ConflictWriter:
    """Record unresolved ambiguity without a conflict-specific state machine."""

    def __init__(self, client, reviews: MaintenanceReviewWriter | None = None) -> None:
        self.client = client
        self.reviews = reviews or MaintenanceReviewWriter(client)

    async def record_detection(
        self,
        *,
        user_name: str,
        project_id: str,
        origin: ConflictOrigin,
        kind: str,
        rationale: str,
        confidence: float | None,
        evidence_ids: Iterable[int],
        metadata: dict[str, Any] | None = None,
        existing_conflict_id: str | None = None,
        cur=None,
    ) -> ConflictWriteResult:
        user_name = require_scope_value(user_name, "user_name", "record_conflict")
        project_id = require_scope_value(project_id, "project_id", "record_conflict")
        if not rationale or not rationale.strip():
            raise ValueError("rationale must not be blank")
        ids = sorted({int(value) for value in evidence_ids})
        if len(ids) < 2 or any(value <= 0 for value in ids):
            raise ValueError("A conflict requires at least two observation IDs")
        if confidence is not None and not 0.0 <= confidence <= 1.0:
            raise ValueError("Conflict confidence must be between zero and one")
        signature = self._evidence_signature(ids)
        existing = await self.reviews.get_by_key(
            user_name=user_name,
            project_id=project_id,
            kind="relationship_conflict",
            dedupe_key=signature,
        )
        await self._validate_evidence(
            user_name=user_name,
            project_id=project_id,
            evidence_ids=ids,
            cur=cur,
        )
        review = await self.reviews.open(
            user_name=user_name,
            scope="project",
            project_id=project_id,
            kind="relationship_conflict",
            dedupe_key=signature,
            evidence_refs=[{"kind": "observation", "id": str(item)} for item in ids],
            evidence_snapshot={
                "origin": origin,
                "kind": kind,
                "confidence": confidence,
                "metadata": metadata or {},
                "evidence_ids": ids,
            },
            reasoning=rationale,
            proposed_plan=ConflictResolutionPlan(conflict_kind=kind),
            cur=cur,
        )
        created = existing is None
        group = ConflictGroup(
            conflict_id=review.review_id,
            user_name=user_name,
            project_id=project_id,
            status="open" if review.status == "open" else "resolved",
            origin=origin,
            kind=kind,
            rationale=review.reasoning,
            confidence=confidence,
            evidence_signature=signature,
            metadata=metadata or {},
        )
        return ConflictWriteResult(group=group, created=created, evidence_added=len(ids))

    async def resolve(
        self,
        *,
        conflict_id: str,
        user_name: str,
        project_id: str,
        resolution_kind: ConflictResolutionKind,
        resolved_by: str,
        resolution_note: str | None = None,
    ) -> ConflictGroup:
        review = await self.reviews.get(
            conflict_id,
            user_name=require_scope_value(user_name, "user_name", "resolve_conflict"),
            project_id=require_scope_value(project_id, "project_id", "resolve_conflict"),
        )
        if review is None or review.kind != "relationship_conflict":
            raise ValueError("Unknown conflict review in this project")
        if review.status == "open":
            await self.reviews.transition(
                review.review_id,
                user_name=user_name,
                project_id=project_id,
                status="applied",
                actor=resolved_by,
                reason=resolution_note or resolution_kind,
            )
        plan = review.proposed_plan
        return ConflictGroup(
            conflict_id=review.review_id,
            user_name=user_name,
            project_id=project_id,
            status="resolved",
            origin=review.evidence_snapshot.get("origin", "user_created"),
            kind=getattr(plan, "conflict_kind", "possible_contradiction"),
            rationale=review.reasoning,
            confidence=review.evidence_snapshot.get("confidence"),
            evidence_signature=review.dedupe_key or "",
            resolution_kind=resolution_kind,
            resolution_note=resolution_note,
            metadata=review.evidence_snapshot.get("metadata", {}),
        )

    @staticmethod
    def _evidence_signature(evidence_ids: Iterable[int]) -> str:
        joined = ",".join(str(value) for value in sorted(set(evidence_ids)))
        return hashlib.sha256(joined.encode("utf-8")).hexdigest()

    async def _validate_evidence(
        self,
        *,
        user_name: str,
        project_id: str,
        evidence_ids: list[int],
        cur=None,
    ) -> None:
        query = """
            SELECT count(*) AS count
            FROM public.relationship_observations
            WHERE user_name = %s AND project_id = %s
              AND observation_id = ANY(%s)
        """
        if cur is not None:
            await cur.execute(query, (user_name, project_id, evidence_ids))
            row = await cur.fetchone()
        else:
            row = await self.client.fetch_one(query, (user_name, project_id, evidence_ids))
        if row is None or int(row["count"]) != len(evidence_ids):
            raise ValueError("Conflict evidence must be relationship observations in this project")
