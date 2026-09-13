"""Conflict detection adapter backed by typed MaintenanceReviews."""

from __future__ import annotations

import hashlib
from contextlib import asynccontextmanager
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

    @asynccontextmanager
    async def _cursor_context(self, cur=None):
        if cur is not None:
            yield cur
            return
        async with self.client.transaction() as transaction_cursor:
            yield transaction_cursor

    @staticmethod
    async def _lock_subject(cur, *, user_name: str, project_id: str, subject: str) -> None:
        """Serialize decisions for one conflict subject across discovery origins."""

        await cur.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f"relationship-conflict:{user_name}:{project_id}:{subject}",),
        )

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
        evidence_snapshot: EvidenceSnapshot,
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
        signature = self._evidence_signature(kind, ids)
        snapshot = EvidenceSnapshot.model_validate(evidence_snapshot)
        snapshot_observation_ids = {
            pointer.identifier
            for pointer in snapshot.pointers
            if pointer.kind == "relationship_observation"
        }
        if snapshot_observation_ids != {str(value) for value in ids}:
            raise ValueError(
                "Conflict evidence snapshot must cover exactly its cited observations"
            )
        async with self._cursor_context(cur) as active_cur:
            await self._lock_subject(
                active_cur,
                user_name=user_name,
                project_id=project_id,
                subject=signature,
            )
            if existing_conflict_id is not None:
                existing = await self.reviews.get(
                    existing_conflict_id,
                    user_name=user_name,
                    project_id=project_id,
                    cur=active_cur,
                )
                if existing is None or existing.kind != "relationship_conflict":
                    raise ValueError("Unknown conflict review in this project")
                if existing.dedupe_key != signature:
                    raise ValueError(
                        "Conflict review evidence is immutable; record a new review"
                    )
            else:
                existing = await self.reviews.get_by_key(
                    user_name=user_name,
                    project_id=project_id,
                    kind="relationship_conflict",
                    dedupe_key=signature,
                    cur=active_cur,
                )
            if (
                existing is not None
                and existing.evidence_snapshot.state_token == snapshot.state_token
            ):
                return self._write_result(existing, created=False)
            if existing is not None and existing.status == "open":
                await self.reviews.transition(
                    existing.review_id,
                    user_name=user_name,
                    project_id=project_id,
                    status="stale",
                    actor=user_name,
                    reason="New conflict evidence superseded this review",
                    cur=active_cur,
                )
            supersedes_review_id = existing.review_id if existing is not None else None
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
                    discovery_packet_tokens=(metadata or {}).get(
                        "discovery_packet_tokens"
                    ),
                    packet_compacted=bool(
                        (metadata or {}).get("packet_compacted", False)
                    ),
                    supersedes_review_id=supersedes_review_id,
                ),
                cur=active_cur,
            )
        return self._write_result(review, created=True, evidence_added=len(ids))

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
        user_name = require_scope_value(user_name, "user_name", "resolve_conflict")
        project_id = require_scope_value(project_id, "project_id", "resolve_conflict")
        async with self._cursor_context() as cur:
            review = await self.reviews.get(
                conflict_id,
                user_name=user_name,
                project_id=project_id,
                cur=cur,
            )
            if review is None or review.kind != "relationship_conflict":
                raise ValueError("Unknown conflict review in this project")
            await self._lock_subject(
                cur,
                user_name=user_name,
                project_id=project_id,
                subject=review.dedupe_key or review.review_id,
            )
            review, resolution = await self.reviews.resolve_conflict_review(
                review.review_id,
                user_name=user_name,
                project_id=project_id,
                resolution_kind=resolution_kind,
                resolution_note=resolution_note,
                resolved_by=resolved_by,
                cur=cur,
            )
        return self._group_from_review(
            review,
            resolution_kind=resolution.resolution_kind,
            resolution_note=resolution.resolution_note,
        )

    @staticmethod
    def _group_from_review(
        review,
        *,
        resolution_kind: ConflictResolutionKind | None = None,
        resolution_note: str | None = None,
    ) -> ConflictGroup:
        plan = review.proposed_plan
        if not isinstance(plan, ConflictResolutionPlan):
            raise RuntimeError("Relationship conflict review has an invalid proposal")
        return ConflictGroup(
            conflict_id=review.review_id,
            user_name=review.user_name,
            project_id=review.project_id or "",
            status="open" if review.status == "open" else "resolved",
            origin=plan.origin,
            kind=plan.conflict_kind,
            rationale=review.reasoning,
            confidence=plan.confidence,
            evidence_signature=review.dedupe_key or "",
            resolution_kind=resolution_kind,
            resolution_note=resolution_note,
            metadata={
                "discovery_packet_tokens": plan.discovery_packet_tokens,
                "packet_compacted": plan.packet_compacted,
                "supersedes_review_id": plan.supersedes_review_id,
            },
        )

    @classmethod
    def _write_result(
        cls,
        review,
        *,
        created: bool,
        evidence_added: int = 0,
    ) -> ConflictWriteResult:
        return ConflictWriteResult(
            group=cls._group_from_review(review),
            created=created,
            evidence_added=evidence_added,
        )

    @staticmethod
    def _evidence_signature(kind: str, evidence_ids: Iterable[int]) -> str:
        joined = ",".join(str(value) for value in sorted(set(evidence_ids)))
        return hashlib.sha256(f"{kind}:{joined}".encode("utf-8")).hexdigest()
