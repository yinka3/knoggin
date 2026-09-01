"""Evidence-backed relationship advisories stored as typed reviews."""

from __future__ import annotations

from common.scoping import require_scope_value
from core.knowledge.db.writers.maintenance_review_writer import (
    MaintenanceReviewWriter,
)
from core.knowledge.maintenance_reviews import RelationshipAdvisoryPlan
from core.knowledge.relationship_advisories import (
    RelationshipAdvisory,
    RelationshipAdvisoryDecision,
    apply_advisory_action,
)


class RelationshipAdvisoryWriter:
    """Persist advisory proposals and decisions in the common review envelope."""

    def __init__(self, client, reviews: MaintenanceReviewWriter | None = None):
        self.client = client
        self.reviews = reviews or MaintenanceReviewWriter(client)

    async def materialize_pending(
        self,
        *,
        user_name: str,
        project_id: str,
        advisory: RelationshipAdvisory,
    ) -> None:
        if advisory.disposition != "pending":
            return
        await self.reviews.open(
            user_name=user_name,
            scope="project",
            project_id=project_id,
            kind="relationship_advisory",
            dedupe_key=advisory.pattern_key,
            evidence_refs=[str(item) for item in advisory.message_ids],
            evidence_snapshot={
                "observed_label": advisory.observed_label,
                "source_type": advisory.source_type,
                "target_type": advisory.target_type,
                "occurrence_count": advisory.occurrence_count,
                "message_ids": list(advisory.message_ids),
            },
            reasoning=(
                f"{advisory.occurrence_count} observations between "
                f"{advisory.source_type or 'unknown'} and "
                f"{advisory.target_type or 'unknown'} entities."
            ),
            proposed_plan=RelationshipAdvisoryPlan(
                pattern_key=advisory.pattern_key,
                observed_label=advisory.observed_label,
            ),
        )
    async def apply_action(
        self,
        *,
        user_name: str,
        project_id: str,
        pattern_key: str,
        action: str,
        relationship_type: str | None = None,
        note: str | None = None,
        decided_by: str | None = None,
    ) -> RelationshipAdvisoryDecision:
        user_name = require_scope_value(user_name, "user_name", "apply_relationship_advisory_action")
        project_id = require_scope_value(project_id, "project_id", "apply_relationship_advisory_action")
        pattern_key = require_scope_value(pattern_key, "pattern_key", "apply_relationship_advisory_action")
        current_review = await self.reviews.get_by_key(
            user_name=user_name,
            project_id=project_id,
            kind="relationship_advisory",
            dedupe_key=pattern_key,
        )
        current = self._decision_from_review(current_review, pattern_key)
        decision = apply_advisory_action(
            current,
            pattern_key=pattern_key,
            action=action,
            relationship_type=relationship_type,
            note=note,
            decided_by=decided_by,
        )
        status = "open" if decision.disposition == "pending" else (
            "dismissed" if decision.disposition in {"dismissed", "suppressed"} else "applied"
        )
        review = await self.reviews.open(
            user_name=user_name,
            scope="project",
            project_id=project_id,
            kind="relationship_advisory",
            dedupe_key=pattern_key,
            evidence_refs=(current_review.evidence_refs if current_review else ()),
            evidence_snapshot=(current_review.evidence_snapshot if current_review else {}),
            reasoning=(current_review.reasoning if current_review else pattern_key),
            proposed_plan=RelationshipAdvisoryPlan(
                pattern_key=pattern_key,
                observed_label=(
                    current_review.proposed_plan.observed_label
                    if current_review and isinstance(current_review.proposed_plan, RelationshipAdvisoryPlan)
                    else None
                ),
                proposed_relationship_type=decision.proposed_relationship_type,
                action=decision.last_action,
                note=decision.decision_note,
            ),
        )
        if status != "open":
            await self.reviews.transition(
                review.review_id,
                user_name=user_name,
                project_id=project_id,
                status=status,
                actor=decided_by or user_name,
                reason=note,
            )
        return decision

    @staticmethod
    def _decision_from_review(review, pattern_key: str):
        if review is None:
            return None
        plan = review.proposed_plan
        proposed = getattr(plan, "proposed_relationship_type", None)
        action = getattr(plan, "action", None)
        disposition = "pending"
        if review.status == "applied":
            disposition = "accepted"
        elif review.status == "dismissed":
            disposition = "suppressed" if action == "suppress" else "dismissed"
        return RelationshipAdvisoryDecision(
            pattern_key=pattern_key,
            disposition=disposition,
            proposed_relationship_type=proposed,
            last_action=action,
            decision_note=getattr(plan, "note", None),
            revision=1,
        )
