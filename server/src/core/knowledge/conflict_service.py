"""One application service for background, agent, and user conflict discovery."""

from __future__ import annotations

from typing import Any, Iterable

from common.utils.events import emit
from core.knowledge.conflicts import (
    ConflictGroup,
    ConflictOrigin,
    ConflictResolutionKind,
    ConflictWriteResult,
)
from core.knowledge.db.writers.conflict_writer import ConflictWriter


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
            existing_conflict_id=existing_conflict_id,
        )
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
        return result

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
                "resolution_kind": resolution_kind,
            },
        )
        return group
