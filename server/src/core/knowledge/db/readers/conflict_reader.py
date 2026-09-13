"""Reads relationship-conflict MaintenanceReviews."""

from __future__ import annotations

from common.scoping import require_scope_value
from core.knowledge.maintenance_reviews import (
    ConflictResolutionRecord,
    review_from_row,
)


class ConflictReader:
    """Return the typed review and immutable evidence snapshot for a conflict."""

    def __init__(self, client) -> None:
        self.client = client

    async def get_detail(
        self,
        *,
        conflict_id: str,
        user_name: str,
        project_id: str,
    ) -> dict | None:
        row = await self.client.fetch_one(
            """
            SELECT review.review_id, review.user_name, review.scope,
                   review.project_id, review.kind, review.dedupe_key,
                   review.evidence_refs, review.evidence_snapshot, review.reasoning,
                   review.proposed_plan, review.expected_state, review.status,
                   review.created_at, review.resolved_at,
                   resolution.resolution_kind, resolution.resolution_note,
                   resolution.resolved_by, resolution.resolved_at AS resolution_resolved_at
            FROM public.maintenance_reviews AS review
            LEFT JOIN public.maintenance_review_resolutions AS resolution
              ON resolution.review_id = review.review_id
            WHERE review.review_id = %s AND review.user_name = %s
              AND review.project_id = %s AND review.kind = 'relationship_conflict'
            """,
            (
                require_scope_value(conflict_id, "conflict_id", "get_conflict"),
                require_scope_value(user_name, "user_name", "get_conflict"),
                require_scope_value(project_id, "project_id", "get_conflict"),
            ),
        )
        if row is None:
            return None
        payload = dict(row)
        resolution_payload = {
            "resolution_kind": payload.pop("resolution_kind", None),
            "resolution_note": payload.pop("resolution_note", None),
            "resolved_by": payload.pop("resolved_by", None),
            "resolved_at": payload.pop("resolution_resolved_at", None),
        }
        review = review_from_row(payload)
        detail = review.model_dump(mode="json")
        detail["resolution"] = (
            ConflictResolutionRecord.model_validate(resolution_payload).model_dump(
                mode="json"
            )
            if resolution_payload["resolution_kind"] is not None
            else None
        )
        return detail
