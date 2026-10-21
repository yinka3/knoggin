from contextlib import asynccontextmanager
from dataclasses import replace

import pytest

from core.knowledge.db.writers.relationship_advisory_writer import (
    RelationshipAdvisoryWriter,
)
from core.knowledge.maintenance.maintenance_reviews import MaintenanceReview
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


class TransactionClient:
    def __init__(self):
        self.lock_calls = []

    @asynccontextmanager
    async def transaction(self):
        yield self

    async def execute(self, query, params):
        self.lock_calls.append((query, params))


@pytest.mark.unit
@pytest.mark.no_network
async def test_writer_persists_acceptance_as_a_typed_review_transition():
    reviews = ReviewStore(
        current=MaintenanceReview(
            review_id="proposal",
            user_name="ada",
            scope="project",
            project_id="project-1",
            kind="relationship_advisory",
            dedupe_key="deploys to|project|technology",
            reasoning="Three observations need review",
            proposed_plan={
                "kind": "relationship_advisory",
                "pattern_key": "deploys to|project|technology",
            },
        )
    )

    client = TransactionClient()
    decision = await RelationshipAdvisoryWriter(client, reviews=reviews).apply_action(
        user_name="ada",
        project_id="project-1",
        pattern_key="deploys to|project|technology",
        action="accept",
        relationship_type="DEPLOYS_TO",
        note="Approved after reviewing the evidence.",
        decided_by="ada",
    )

    assert decision.disposition == "accepted"
    assert decision.revision == 1
    assert reviews.opened[0]["kind"] == "relationship_advisory"
    plan = reviews.opened[0]["proposed_plan"]
    assert plan.proposed_relationship_type == (
        "DEPLOYS_TO"
    )
    assert plan.action == "accept"
    assert plan.note == "Approved after reviewing the evidence."
    assert reviews.opened[0]["expected_state"] == {"revision": 1}
    assert reviews.transitions[-1][1] == {
        "user_name": "ada",
        "project_id": "project-1",
        "status": "applied",
        "actor": "ada",
        "reason": "Approved after reviewing the evidence.",
        "cur": client,
    }
    assert reviews.opened[0]["cur"] is client
    assert len(client.lock_calls) == 1


@pytest.mark.unit
@pytest.mark.no_network
async def test_action_requires_an_evidence_backed_review():
    client = TransactionClient()
    reviews = ReviewStore()

    with pytest.raises(ValueError, match="review not found"):
        await RelationshipAdvisoryWriter(client, reviews=reviews).apply_action(
            user_name="ada", project_id="project-1",
            pattern_key="deploys to|project|technology", action="accept",
            relationship_type="DEPLOYS_TO",
        )
    assert reviews.opened == []


@pytest.mark.unit
@pytest.mark.no_network
async def test_action_rejects_a_superseded_review_id():
    client = TransactionClient()
    reviews = ReviewStore()
    await reviews.open(
        user_name="ada", project_id="project-1", scope="project",
        kind="relationship_advisory", dedupe_key="deploys to|project|technology",
        reasoning="Review", proposed_plan={
            "kind": "relationship_advisory",
            "pattern_key": "deploys to|project|technology",
        },
    )

    with pytest.raises(ValueError, match="superseded"):
        await RelationshipAdvisoryWriter(client, reviews=reviews).apply_action(
            user_name="ada", project_id="project-1",
            pattern_key="deploys to|project|technology", action="dismiss",
            expected_review_id="older-review",
        )
    assert reviews.transitions == []


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

    client = TransactionClient()
    await RelationshipAdvisoryWriter(client, reviews=reviews).materialize_pending(
        user_name="ada",
        project_id="project-1",
        advisory=advisory,
        domain_version=4,
    )

    opened = reviews.opened[0]
    assert opened["evidence_refs"] == [
        {"kind": "relationship_observation", "identifier": "11"},
        {"kind": "relationship_observation", "identifier": "12"},
        {"kind": "relationship_observation", "identifier": "13"},
    ]
    assert opened["expected_state"] == {"domain_version": 4, "occurrence_count": 3}
    assert [
        pointer.identifier for pointer in opened["evidence_snapshot"].pointers
    ] == []
    assert opened["cur"] is client


@pytest.mark.unit
@pytest.mark.no_network
async def test_stale_advisory_refresh_opens_successor_with_same_evidence():
    client = TransactionClient()
    reviews = ReviewStore()
    writer = RelationshipAdvisoryWriter(client, reviews=reviews)
    advisory = RelationshipAdvisory(
        pattern_key="deploys to|project|technology",
        observed_label="deploys to",
        source_type="Project",
        target_type="Technology",
        occurrence_count=2,
        distinct_source_entities=2,
        distinct_target_entities=2,
        semantic_window_ids=("window-1", "window-2"),
        observation_ids=(11, 12),
        first_observed_ms=1,
        last_observed_ms=2,
    )
    await writer.materialize_pending(
        user_name="ada", project_id="project-1", advisory=advisory, domain_version=4
    )
    reviews.current.status = "stale"

    await writer.materialize_pending(
        user_name="ada", project_id="project-1", advisory=advisory, domain_version=4
    )

    assert len(reviews.opened) == 2
    assert reviews.opened[-1]["expected_state"] == {
        "domain_version": 4,
        "occurrence_count": 2,
        "supersedes_review_id": "review-1",
    }
    await writer.materialize_pending(
        user_name="ada", project_id="project-1", advisory=advisory, domain_version=4
    )
    assert len(reviews.opened) == 2

    await writer.materialize_pending(
        user_name="ada", project_id="project-1", advisory=advisory, domain_version=5
    )
    assert reviews.transitions[-1][1]["status"] == "stale"
    assert reviews.opened[-1]["expected_state"] == {
        "domain_version": 5,
        "occurrence_count": 2,
        "supersedes_review_id": "review-2",
    }

    await writer.materialize_pending(
        user_name="ada", project_id="project-1",
        advisory=replace(advisory, occurrence_count=3), domain_version=5,
    )
    assert reviews.opened[-1]["expected_state"] == {
        "domain_version": 5,
        "occurrence_count": 3,
        "supersedes_review_id": "review-3",
    }


@pytest.mark.unit
@pytest.mark.no_network
async def test_advisory_cites_at_most_128_observations_even_for_larger_patterns():
    reviews = ReviewStore()
    client = TransactionClient()
    advisory = RelationshipAdvisory(
        pattern_key="deploys to|project|technology",
        observed_label="deploys to",
        source_type="Project",
        target_type="Technology",
        occurrence_count=129,
        distinct_source_entities=2,
        distinct_target_entities=2,
        semantic_window_ids=("window-1", "window-2"),
        observation_ids=tuple(range(1, 130)),
        first_observed_ms=1,
        last_observed_ms=129,
    )
    assert RelationshipAdvisoryWriter.evidence_ids(advisory) == tuple(range(2, 130))

    await RelationshipAdvisoryWriter(client, reviews=reviews).materialize_pending(
        user_name="ada", project_id="project-1", advisory=advisory
    )
    assert len(reviews.opened[0]["evidence_refs"]) == 128
    assert reviews.opened[0]["evidence_refs"][0]["identifier"] == "2"
    assert "129 observations" in reviews.opened[0]["reasoning"]
