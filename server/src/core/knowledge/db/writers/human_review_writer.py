"""Compatibility-shaped inbox adapter backed by MaintenanceReviewWriter."""

from __future__ import annotations

from typing import Any

from common.scoping import require_scope_value
from core.knowledge.db.writers.maintenance_review_writer import (
    MaintenanceReviewWriter,
)
from core.knowledge.human_reviews import HumanReview
from core.knowledge.maintenance_reviews import ConflictResolutionPlan


class HumanReviewWriter:
    """Expose the former inbox shape without a second durable workflow table."""

    def __init__(self, client) -> None:
        self.client = client
        self.reviews = MaintenanceReviewWriter(client)

    async def open(
        self,
        *,
        user_name: str,
        project_id: str,
        kind: str,
        subject_type: str,
        subject_id: str,
        title: str,
        summary: str | None = None,
        priority: str = "normal",
        metadata: dict[str, Any] | None = None,
        cur=None,
    ) -> str:
        if priority not in {"low", "normal", "high"}:
            raise ValueError("Human review priority must be low, normal, or high")
        review = await self.reviews.open(
            user_name=require_scope_value(user_name, "user_name", "open_human_review"),
            scope="project",
            project_id=require_scope_value(project_id, "project_id", "open_human_review"),
            kind=kind,
            dedupe_key=subject_id,
            reasoning=summary or title,
            proposed_plan=ConflictResolutionPlan(
                conflict_kind=subject_type,
                note=summary,
            ),
            evidence_snapshot={
                "title": title,
                "priority": priority,
                "metadata": metadata or {},
            },
            cur=cur,
        )
        return review.review_id

    async def resolve(
        self,
        *,
        user_name: str,
        project_id: str,
        kind: str,
        subject_id: str,
        cur=None,
    ) -> None:
        review = await self.reviews.get_by_key(
            user_name=user_name,
            project_id=project_id,
            kind=kind,
            dedupe_key=subject_id,
        )
        if review is None or review.status != "open":
            return
        await self.reviews.transition(
            review.review_id,
            user_name=user_name,
            project_id=project_id,
            status="dismissed",
            actor=user_name,
            cur=cur,
        )

    async def list_open(
        self, *, user_name: str, project_id: str
    ) -> list[HumanReview]:
        reviews = await self.reviews.list_open(
            user_name=require_scope_value(user_name, "user_name", "list_human_reviews"),
            project_id=require_scope_value(project_id, "project_id", "list_human_reviews"),
        )
        result = []
        for review in reviews:
            snapshot = review.evidence_snapshot
            result.append(
                HumanReview(
                    review_id=review.review_id,
                    user_name=review.user_name,
                    project_id=review.project_id or project_id,
                    kind=review.kind,
                    subject_type=getattr(review.proposed_plan, "conflict_kind", review.kind),
                    subject_id=review.dedupe_key or review.review_id,
                    status="open",
                    priority=snapshot.get("priority", "normal"),
                    title=snapshot.get("title", review.kind),
                    summary=review.reasoning,
                    metadata=snapshot.get("metadata", {}),
                )
            )
        return result
