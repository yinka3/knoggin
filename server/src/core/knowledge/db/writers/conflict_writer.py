"""Conflict detection adapter backed by typed MaintenanceReviews."""

from __future__ import annotations

import hashlib
from typing import Any, Iterable

from common.schema.evidence import EvidenceSnapshot
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
        evidence_snapshot: EvidenceSnapshot | None = None,
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
        snapshot = evidence_snapshot or EvidenceSnapshot()
        if existing_conflict_id is not None:
            existing = await self.reviews.get(
                existing_conflict_id,
                user_name=user_name,
                project_id=project_id,
                cur=cur,
            )
            if existing is None or existing.kind != "relationship_conflict":
                raise ValueError("Unknown conflict review in this project")
            if existing.dedupe_key != signature:
                raise ValueError(
                    "Conflict review evidence is immutable; record a new review"
                )
            if existing.evidence_snapshot.state_token != snapshot.state_token:
                raise ValueError(
                    "Conflict review evidence state changed; record a new review"
                )
        else:
            existing = await self.reviews.get_by_key(
                user_name=user_name,
                project_id=project_id,
                kind="relationship_conflict",
                dedupe_key=signature,
                cur=cur,
            )
            if (
                existing is not None
                and existing.status == "open"
                and existing.evidence_snapshot.state_token != snapshot.state_token
            ):
                await self.reviews.transition(
                    existing.review_id,
                    user_name=user_name,
                    project_id=project_id,
                    status="stale",
                    actor=user_name,
                    reason="New conflict evidence superseded this review",
                    cur=cur,
                )
                existing = None
        review = await self.reviews.open(
            user_name=user_name,
            scope="project",
            project_id=project_id,
            kind="relationship_conflict",
            dedupe_key=signature,
            evidence_refs=[
                {"kind": "relationship_observation", "identifier": str(item)}
                for item in ids
            ],
            evidence_snapshot=snapshot,
            reasoning=rationale,
            proposed_plan=ConflictResolutionPlan(
                conflict_kind=kind,
                origin=origin,
                confidence=confidence,
                discovery_packet_tokens=(metadata or {}).get("discovery_packet_tokens"),
                packet_compacted=bool((metadata or {}).get("packet_compacted", False)),
            ),
            cur=cur,
        )
        created = existing is None
        existing_evidence = (
            set(existing.evidence_ids("relationship_observation"))
            if existing
            else set()
        )
        evidence_added = len(set(ids) - existing_evidence)
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
        return ConflictWriteResult(
            group=group,
            created=created,
            evidence_added=evidence_added,
        )

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
            origin=getattr(plan, "origin", "user_created"),
            kind=getattr(plan, "conflict_kind", "possible_contradiction"),
            rationale=review.reasoning,
            confidence=getattr(plan, "confidence", None),
            evidence_signature=review.dedupe_key or "",
            resolution_kind=resolution_kind,
            resolution_note=resolution_note,
            metadata={
                "discovery_packet_tokens": getattr(
                    plan, "discovery_packet_tokens", None
                ),
                "packet_compacted": getattr(plan, "packet_compacted", False),
            },
        )

    @staticmethod
    def _evidence_signature(evidence_ids: Iterable[int]) -> str:
        joined = ",".join(str(value) for value in sorted(set(evidence_ids)))
        return hashlib.sha256(joined.encode("utf-8")).hexdigest()
