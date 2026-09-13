"""One application service for background, agent, and user conflict discovery."""

from __future__ import annotations

from typing import Any, Iterable

from common.schema.evidence import EvidenceBundle, EvidenceSnapshot
from common.utils.events import emit
from core.knowledge.conflicts import (
    ConflictGroup,
    ConflictOrigin,
    ConflictResolutionKind,
    ConflictWriteResult,
)
from core.knowledge.db.writers.conflict_writer import ConflictWriter


async def load_conflict_evidence(
    knowledge_store: Any,
    *,
    observation_ids: Iterable[int],
    user_name: str,
    project_id: str,
) -> tuple[EvidenceBundle, ...]:
    """Load one conflict's cited observations in the review-detail order."""

    ids = sorted({int(observation_id) for observation_id in observation_ids})
    return tuple(
        await knowledge_store.get_relationship_observations_evidence(
            ids,
            user_name=user_name,
            project_id=project_id,
        )
    )


async def snapshot_conflict_evidence(
    knowledge_store: Any,
    *,
    observation_ids: Iterable[int],
    user_name: str,
    project_id: str,
) -> EvidenceSnapshot:
    """Capture the bounded evidence state for exactly one conflict subject."""

    from core.knowledge.evidence_service import EvidenceService

    return EvidenceService.snapshot(
        await load_conflict_evidence(
            knowledge_store,
            observation_ids=observation_ids,
            user_name=user_name,
            project_id=project_id,
        )
    )


class ConflictService:
    """Applies identical scope, review, and notification rules to every origin."""

    def __init__(self, writer: ConflictWriter) -> None:
        self.writer = writer

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
    ) -> ConflictWriteResult:
        result = await self.writer.record_detection(
            user_name=user_name,
            project_id=project_id,
            origin=origin,
            kind=kind,
            rationale=rationale,
            confidence=confidence,
            evidence_ids=evidence_ids,
            metadata=metadata,
            evidence_snapshot=evidence_snapshot,
            existing_conflict_id=existing_conflict_id,
        )
        await self.notify_detection(
            user_name=user_name,
            project_id=project_id,
            origin=origin,
            result=result,
        )
        return result

    @staticmethod
    async def notify_detection(
        *,
        user_name: str,
        project_id: str,
        origin: ConflictOrigin,
        result: ConflictWriteResult,
    ) -> None:
        if result.should_notify:
            await emit(
                project_id,
                "conflict",
                "group_opened" if result.created else "group_evidence_added",
                {
                    "user_name": user_name,
                    "project_id": project_id,
                    "conflict_id": result.group.conflict_id,
                    "origin": origin,
                    "kind": result.group.kind,
                    "evidence_added": result.evidence_added,
                },
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
        group = await self.writer.resolve(
            conflict_id=conflict_id,
            user_name=user_name,
            project_id=project_id,
            resolution_kind=resolution_kind,
            resolved_by=resolved_by,
            resolution_note=resolution_note,
        )
        await emit(
            project_id,
            "conflict",
            "group_resolved",
            {
                "user_name": user_name,
                "project_id": project_id,
                "conflict_id": group.conflict_id,
                "resolution_kind": group.resolution_kind,
            },
        )
        return group
