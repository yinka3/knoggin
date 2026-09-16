import pytest

from common.schema.evidence import EvidencePointer, EvidenceSnapshot
from core.knowledge.db.writers.conflict_writer import ConflictWriter
from core.knowledge.maintenance.maintenance_reviews import (
    ConflictResolutionPlan,
    MaintenanceReview,
)
from tests.fixtures.fakes import RecordingPostgresClient


def _snapshot(token: str, evidence_ids: list[int] | tuple[int, ...] = (101, 104)) -> EvidenceSnapshot:
    return EvidenceSnapshot(
        pointers=tuple(EvidencePointer.for_observation(value) for value in evidence_ids),
        total_nodes=len(evidence_ids),
        state_token=token * 64,
    )


def _review(
    evidence_ids: list[int],
    *,
    review_id: str = "review-1",
    snapshot: EvidenceSnapshot | None = None,
    status: str = "open",
) -> MaintenanceReview:
    return MaintenanceReview(
        review_id=review_id,
        user_name="ada",
        scope="project",
        project_id="project-1",
        kind="relationship_conflict",
        dedupe_key=ConflictWriter._evidence_signature(
            "possible_contradiction", evidence_ids
        ),
        evidence_refs=[
            EvidencePointer.for_observation(item) for item in evidence_ids
        ],
        evidence_snapshot=snapshot or _snapshot("0", evidence_ids),
        reasoning="The observations disagree.",
        proposed_plan=ConflictResolutionPlan(
            conflict_kind="possible_contradiction",
            origin="background_discovery",
            confidence=0.8,
        ),
        status=status,
    )


class ReviewStore:
    def __init__(self, existing: MaintenanceReview | None = None):
        self.current = existing
        self.reviews = {existing.review_id: existing} if existing else {}
        self.opened = []
        self.transitions = []

    async def get_by_key(self, **_kwargs):
        return self.current

    async def get(self, review_id, **_kwargs):
        return self.reviews.get(review_id)

    async def open(self, **kwargs):
        self.opened.append(kwargs)
        review = _review(
            [int(ref["identifier"]) for ref in kwargs["evidence_refs"]],
            review_id=f"review-{len(self.reviews) + 1}",
            snapshot=kwargs["evidence_snapshot"],
        ).model_copy(
            update={
                "dedupe_key": kwargs["dedupe_key"],
                "reasoning": kwargs["reasoning"],
                "proposed_plan": kwargs["proposed_plan"],
                "expected_state": kwargs.get("expected_state") or {},
            }
        )
        self.current = review
        self.reviews[review.review_id] = review
        return review

    async def transition(self, review_id, *, status, **_kwargs):
        self.transitions.append((review_id, status))
        review = self.reviews[review_id].model_copy(update={"status": status})
        self.reviews[review_id] = review
        if self.current and self.current.review_id == review_id:
            self.current = review
        return review


@pytest.mark.unit
@pytest.mark.no_network
async def test_conflict_writer_creates_one_typed_review_with_immutable_evidence():
    reviews = ReviewStore()
    client = RecordingPostgresClient()

    result = await ConflictWriter(client, reviews=reviews).record_detection(
        user_name="ada",
        project_id="project-1",
        origin="background_discovery",
        kind="possible_contradiction",
        rationale="The two observations disagree.",
        confidence=0.8,
        evidence_ids=[101, 104],
        evidence_snapshot=_snapshot("0"),
    )

    assert result.created
    assert result.evidence_added == 2
    assert result.group.conflict_id == "review-1"
    assert reviews.opened[0]["kind"] == "relationship_conflict"
    assert reviews.opened[0]["evidence_refs"] == [
        {"kind": "relationship_observation", "identifier": "101"},
        {"kind": "relationship_observation", "identifier": "104"},
    ]
    assert client.transaction_enters == 1


@pytest.mark.unit
@pytest.mark.no_network
async def test_equivalent_rediscovery_reuses_a_dismissed_review_despite_model_changes():
    existing = _review([101, 104], snapshot=_snapshot("1"), status="dismissed")
    reviews = ReviewStore(existing)

    result = await ConflictWriter(
        RecordingPostgresClient(), reviews=reviews
    ).record_detection(
        user_name="ada",
        project_id="project-1",
        origin="agent_discovery",
        kind="possible_contradiction",
        rationale="A different model explanation reached the same conclusion.",
        confidence=0.2,
        evidence_ids=[104, 101],
        evidence_snapshot=_snapshot("1"),
    )

    assert not result.created
    assert not result.should_notify
    assert result.evidence_added == 0
    assert result.group.conflict_id == existing.review_id
    assert result.group.status == "resolved"
    assert result.group.origin == "background_discovery"
    assert result.group.confidence == 0.8
    assert reviews.opened == []
    assert reviews.transitions == []


@pytest.mark.unit
@pytest.mark.no_network
async def test_changed_conflict_evidence_stales_the_open_review_and_creates_successor():
    existing = _review([101, 104], snapshot=_snapshot("1"))
    reviews = ReviewStore(existing)
    writer = ConflictWriter(RecordingPostgresClient(), reviews=reviews)

    successor = await writer.record_detection(
        user_name="ada",
        project_id="project-1",
        origin="background_discovery",
        kind="possible_contradiction",
        rationale="The cited evidence changed.",
        confidence=0.9,
        evidence_ids=[101, 104],
        evidence_snapshot=_snapshot("2"),
    )
    reaffirmed = await writer.record_detection(
        user_name="ada",
        project_id="project-1",
        origin="agent_discovery",
        kind="possible_contradiction",
        rationale="Confidence changed but the cited evidence did not.",
        confidence=0.1,
        evidence_ids=[104, 101],
        evidence_snapshot=_snapshot("2"),
    )

    assert successor.created
    assert successor.group.conflict_id == "review-2"
    assert reviews.transitions == [("review-1", "stale")]
    assert reviews.reviews["review-1"].status == "stale"
    assert len(reviews.opened) == 1
    assert not reaffirmed.created
    assert not reaffirmed.should_notify
    assert reaffirmed.group.conflict_id == "review-2"
    assert len(reviews.opened) == 1


@pytest.mark.unit
@pytest.mark.no_network
async def test_conflict_writer_rejects_a_snapshot_with_uncited_observations():
    reviews = ReviewStore()

    with pytest.raises(ValueError, match="exactly its cited observations"):
        await ConflictWriter(
            RecordingPostgresClient(), reviews=reviews
        ).record_detection(
            user_name="ada",
            project_id="project-1",
            origin="background_discovery",
            kind="possible_contradiction",
            rationale="The cited observations need review.",
            confidence=0.8,
            evidence_ids=[101, 104],
            evidence_snapshot=_snapshot("1", (101, 104, 999)),
        )

    assert reviews.opened == []
