"""Reads relationship-conflict MaintenanceReviews."""

from __future__ import annotations

from common.scoping import require_scope_value
from core.knowledge.maintenance_reviews import review_from_row


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
            SELECT review_id, user_name, scope, project_id, kind, dedupe_key,
                   evidence_refs, evidence_snapshot, reasoning, proposed_plan,
                   expected_state, status, created_at, resolved_at
            FROM public.maintenance_reviews
            WHERE review_id = %s AND user_name = %s AND project_id = %s
              AND kind = 'relationship_conflict'
            """,
            (
                require_scope_value(conflict_id, "conflict_id", "get_conflict"),
                require_scope_value(user_name, "user_name", "get_conflict"),
                require_scope_value(project_id, "project_id", "get_conflict"),
            ),
        )
        if row is None:
            return None
        review = review_from_row(dict(row))
        return review.model_dump(mode="json")
