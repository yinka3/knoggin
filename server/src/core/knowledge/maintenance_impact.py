"""Typed, read-only impact projections for maintenance review plans."""

from __future__ import annotations

from common.schema.maintenance import (
    ImpactKind,
    ImpactMode,
    MaintenanceImpactItem,
    MaintenanceImpactPreview,
)
from core.knowledge.maintenance_reviews import (
    ConflictResolutionPlan,
    EntityContextChangePlan,
    EntityMergePlan,
    EntityMergeRollbackPlan,
    MaintenanceReview,
    RelationshipAdvisoryPlan,
    RelationshipDomainChangePlan,
    RelationshipInterpretationPlan,
)


class MaintenanceImpactPlanner:
    """Project typed plan consequences without reading or mutating storage."""

    @classmethod
    def preview(cls, review: MaintenanceReview) -> MaintenanceImpactPreview:
        plan = review.proposed_plan
        impacts: list[MaintenanceImpactItem] = []
        no_impact = None

        if isinstance(plan, RelationshipInterpretationPlan):
            observation_ids = [str(change.observation_id) for change in plan.changes]
            relationship_ids = sorted(
                {
                    change.expected_relationship_id
                    for change in plan.changes
                    if change.expected_relationship_id is not None
                }
            )
            impacts.extend(
                [
                    cls._item("relationship_observation", "direct_mutation", observation_ids),
                    cls._item("relationship", "direct_mutation", relationship_ids),
                    cls._item("episode_entity_link", "direct_mutation", observation_ids),
                ]
            )
            impacts.extend(cls._project_rebuilds([review.project_id]))
        elif isinstance(plan, EntityMergePlan):
            projects = sorted(plan.frontier_tokens)
            impacts.extend(
                [
                    cls._item(
                        "entity",
                        "direct_mutation",
                        [str(plan.survivor_entity_id), str(plan.retired_entity_id)],
                    ),
                    cls._item("project_entity_context", "direct_mutation", projects),
                    cls._item("episode_entity_link", "direct_mutation", projects),
                ]
            )
            impacts.extend(cls._project_rebuilds(projects, include_cache=True))
        elif isinstance(plan, EntityMergeRollbackPlan):
            mutation_ids = [
                str(value)
                for value in plan.safe_mutation_ids + plan.conflicting_mutation_ids
            ]
            impacts.append(cls._item("merge_mutation", "direct_mutation", mutation_ids))
        elif isinstance(plan, EntityContextChangePlan):
            key = f"{plan.project_id}:{plan.entity_id}"
            impacts.append(cls._item("project_entity_context", "direct_mutation", [key]))
            impacts.extend(
                cls._project_rebuilds(
                    [plan.project_id], include_search=True, include_cache=True
                )
            )
        elif isinstance(plan, RelationshipDomainChangePlan):
            impacts.append(
                cls._item("domain_config", "direct_mutation", [plan.relationship_name])
            )
        elif isinstance(plan, (ConflictResolutionPlan, RelationshipAdvisoryPlan)):
            no_impact = (
                "This review records a judgment or disposition; its current plan does "
                "not directly mutate canonical Knowledge rows."
            )
        else:  # pragma: no cover - the discriminated union should prevent this
            raise TypeError(f"Unsupported maintenance plan: {type(plan).__name__}")

        return MaintenanceImpactPreview(
            review_id=review.review_id,
            evidence_state_token=review.evidence_snapshot.state_token,
            impacts=tuple(impacts),
            no_applicable_impact=no_impact,
        )

    @staticmethod
    def _item(kind: ImpactKind, mode: ImpactMode, identifiers) -> MaintenanceImpactItem:
        values = sorted({str(value) for value in identifiers if value is not None})
        return MaintenanceImpactItem(
            kind=kind,
            mode=mode,
            identifiers=tuple(values[:128]),
            total_count=len(values),
            truncated=len(values) > 128,
        )

    @classmethod
    def _project_rebuilds(
        cls,
        project_ids,
        *,
        include_search: bool = False,
        include_cache: bool = False,
    ) -> list[MaintenanceImpactItem]:
        projects = [project_id for project_id in project_ids if project_id]
        impacts = [cls._item("age_projection", "derived_rebuild", projects)]
        if include_search:
            impacts.append(cls._item("search_projection", "derived_rebuild", projects))
        if include_cache:
            impacts.append(
                cls._item("live_entity_cache", "cache_invalidation", projects)
            )
        return impacts
