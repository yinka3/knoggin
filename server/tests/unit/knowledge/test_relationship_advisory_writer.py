import pytest

from core.knowledge.db.writers.relationship_advisory_writer import (
    RelationshipAdvisoryWriter,
)
from core.knowledge.maintenance_reviews import MaintenanceReview
from core.knowledge.relationship_advisories import RelationshipAdvisory


class ReviewStore:
    def __init__(self, current=None):
        self.current = current
        self.opened = []
        self.transitions = []

    async def get_by_key(self, **kwargs):
        return self.current

    async def open(self, **kwargs):
        self.opened.append(kwargs)
        self.current = MaintenanceReview(
            review_id=f"review-{len(self.opened)}",
            user_name=kwargs["user_name"],
            scope=kwargs["scope"],
            project_id=kwargs["project_id"],
            kind=kwargs["kind"],
            dedupe_key=kwargs["dedupe_key"],
            evidence_refs=kwargs.get("evidence_refs", []),
            evidence_snapshot=kwargs.get("evidence_snapshot", {}),
            reasoning=kwargs["reasoning"],
            proposed_plan=kwargs["proposed_plan"],
            expected_state=kwargs.get("expected_state", {}),
        )
        return self.current

    async def transition(self, review_id, **kwargs):
        self.transitions.append((review_id, kwargs))
        return self.current


@pytest.mark.unit
@pytest.mark.no_network
async def test_writer_persists_acceptance_as_a_typed_review_transition():
    reviews = ReviewStore()

    decision = await RelationshipAdvisoryWriter(
        object(), reviews=reviews
    ).apply_action(
        user_name="ada",
        project_id="project-1",
        pattern_key="deploys to|project|technology",
        action="accept",
        relationship_type="DEPLOYS_TO",
        decided_by="ada",
    )

    assert decision.disposition == "accepted"
    assert decision.revision == 1
    assert reviews.opened[0]["kind"] == "relationship_advisory"
    assert reviews.opened[0]["proposed_plan"].proposed_relationship_type == (
        "DEPLOYS_TO"
    )
    assert reviews.transitions[-1][1]["status"] == "applied"


@pytest.mark.unit
@pytest.mark.no_network
async def test_materializing_pending_advisory_opens_observation_backed_review():
    reviews = ReviewStore()
    advisory = RelationshipAdvisory(
        pattern_key="deploys to|project|technology",
        observed_label="deploys to",
        source_type="Project",
        target_type="Technology",
        occurrence_count=3,
        distinct_source_entities=2,
        distinct_target_entities=2,
        semantic_window_ids=("window-1", "window-2"),
        observation_ids=(11, 12, 13),
        first_observed_ms=1,
        last_observed_ms=3,
    )

    await RelationshipAdvisoryWriter(object(), reviews=reviews).materialize_pending(
        user_name="ada",
        project_id="project-1",
        advisory=advisory,
        domain_version=4,
    )

    opened = reviews.opened[0]
    assert opened["evidence_refs"] == [
        {"kind": "observation", "id": "11"},
        {"kind": "observation", "id": "12"},
        {"kind": "observation", "id": "13"},
    ]
    assert opened["expected_state"] == {"domain_version": 4}
    assert opened["evidence_snapshot"]["semantic_window_ids"] == [
        "window-1",
        "window-2",
    ]
