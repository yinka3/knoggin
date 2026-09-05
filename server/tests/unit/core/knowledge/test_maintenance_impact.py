import pytest

from common.schema.evidence import EvidenceSnapshot
from core.knowledge.maintenance_impact import MaintenanceImpactPlanner
from core.knowledge.maintenance_reviews import (
    ConflictResolutionPlan,
    EntityMergePlan,
    MaintenanceReview,
    RelationshipInterpretationChange,
    RelationshipInterpretationPlan,
)


def _review(plan, *, project_id="project-1"):
    return MaintenanceReview(
        review_id="review-1",
        user_name="ada",
        scope="project" if project_id else "user-global",
        project_id=project_id,
        kind=plan.kind,
        evidence_snapshot=EvidenceSnapshot(state_token="1" * 64),
        reasoning="Inspect the bounded impact.",
        proposed_plan=plan,
    )


@pytest.mark.unit
@pytest.mark.no_network
def test_relationship_preview_separates_direct_rows_from_age_rebuild():
    preview = MaintenanceImpactPlanner.preview(
        _review(
            RelationshipInterpretationPlan(
                changes=[
                    RelationshipInterpretationChange(
                        observation_id=7,
                        expected_relationship_id="relationship-1",
                        target_relationship_type="WORKS_FOR",
                        interpretation_source="review",
                    )
                ]
            )
        )
    )

    by_kind = {item.kind: item for item in preview.impacts}
    assert by_kind["relationship_observation"].mode == "direct_mutation"
    assert by_kind["relationship_observation"].identifiers == ("7",)
    assert by_kind["age_projection"].mode == "derived_rebuild"
    assert "search_projection" not in by_kind


@pytest.mark.unit
@pytest.mark.no_network
def test_merge_preview_lists_context_scopes_projection_and_cache_impacts():
    preview = MaintenanceImpactPlanner.preview(
        _review(
            EntityMergePlan(
                survivor_entity_id=2,
                retired_entity_id=3,
                frontier_tokens={"project-2": "token-2", "project-1": "token-1"},
            ),
            project_id=None,
        )
    )

    by_kind = {item.kind: item for item in preview.impacts}
    assert by_kind["project_entity_context"].identifiers == (
        "project-1",
        "project-2",
    )
    assert by_kind["age_projection"].mode == "derived_rebuild"
    assert by_kind["live_entity_cache"].mode == "cache_invalidation"


@pytest.mark.unit
@pytest.mark.no_network
def test_judgment_only_review_explicitly_declares_no_canonical_impact():
    preview = MaintenanceImpactPlanner.preview(
        _review(ConflictResolutionPlan(conflict_kind="possible_contradiction"))
    )

    assert preview.impacts == ()
    assert preview.no_applicable_impact is not None
