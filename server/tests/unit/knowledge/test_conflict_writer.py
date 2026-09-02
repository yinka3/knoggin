import pytest

from core.knowledge.db.writers.conflict_writer import ConflictWriter
from core.knowledge.maintenance_reviews import (
    ConflictResolutionPlan,
    EvidenceRef,
    MaintenanceReview,
)
from tests.fixtures.fakes import RecordingPostgresClient


def _observation(observation_id: int) -> dict:
    return {
        "observation_id": observation_id,
        "relationship_id": f"rel-{observation_id}",
        "message_id": observation_id,
        "session_id": "session-1",
        "source_entity_id": 1,
        "target_entity_id": 2,
        "observed_relationship_label": "works at",
        "interpretation_source": "observed",
        "context": "Ada works at Acme.",
        "observed_at_ms": observation_id,
    }


def _review(evidence_ids: list[int]) -> MaintenanceReview:
    return MaintenanceReview(
        review_id="review-1",
        user_name="ada",
        scope="project",
        project_id="project-1",
        kind="relationship_conflict",
        dedupe_key=ConflictWriter._evidence_signature(evidence_ids),
        evidence_refs=[
            EvidenceRef(kind="observation", id=str(item)) for item in evidence_ids
        ],
        evidence_snapshot={"origin": "background_discovery", "confidence": 0.8},
        reasoning="The observations disagree.",
        proposed_plan=ConflictResolutionPlan(
            conflict_kind="possible_contradiction"
        ),
    )


class ReviewStore:
    def __init__(self, existing=None):
        self.existing = existing
        self.opened = []

    async def get_by_key(self, **kwargs):
        return self.existing

    async def open(self, **kwargs):
        self.opened.append(kwargs)
        return self.existing or _review(
            [int(ref["id"]) for ref in kwargs["evidence_refs"]]
        )


@pytest.mark.unit
@pytest.mark.no_network
async def test_conflict_writer_creates_one_typed_review_with_immutable_evidence():
    reviews = ReviewStore()
    client = RecordingPostgresClient(
        fetch_all_results=[[_observation(101), _observation(104)]]
    )

    result = await ConflictWriter(client, reviews=reviews).record_detection(
        user_name="ada",
        project_id="project-1",
        origin="background_discovery",
        kind="possible_contradiction",
        rationale="The two observations disagree.",
        confidence=0.8,
        evidence_ids=[101, 104],
    )

    assert result.created
    assert result.evidence_added == 2
    assert result.group.conflict_id == "review-1"
    assert reviews.opened[0]["kind"] == "relationship_conflict"
    assert reviews.opened[0]["evidence_refs"] == [
        {"kind": "observation", "id": "101"},
        {"kind": "observation", "id": "104"},
    ]


@pytest.mark.unit
@pytest.mark.no_network
async def test_conflict_writer_reaffirms_an_identical_review_without_new_evidence():
    existing = _review([101, 104])
    reviews = ReviewStore(existing)
    client = RecordingPostgresClient(
        fetch_all_results=[[_observation(101), _observation(104)]]
    )

    result = await ConflictWriter(client, reviews=reviews).record_detection(
        user_name="ada",
        project_id="project-1",
        origin="agent_discovery",
        kind="possible_contradiction",
        rationale="The same evidence remains ambiguous.",
        confidence=0.8,
        evidence_ids=[104, 101],
    )

    assert not result.created
    assert result.evidence_added == 0
    assert reviews.opened[0]["dedupe_key"] == existing.dedupe_key
