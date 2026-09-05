import pytest
from pydantic import ValidationError

from common.conf.domain_config import DomainConfig
from common.conf.relationship_config import normalize_relationship
from common.schema.evidence import EvidencePointer, EvidenceSnapshot
from core.knowledge.db.writers.maintenance_review_writer import MaintenanceReviewWriter
from core.knowledge.maintenance_reviews import (
    MaintenanceReview,
    RelationshipInterpretationChange,
    RelationshipInterpretationPlan,
)
from core.knowledge.relationship_advisories import build_relationship_advisories
from tests.fixtures.fakes import RecordingPostgresClient


def test_relationship_labels_are_reusable_domain_vocabulary():
    domain = DomainConfig.from_mapping(
        {
            "version": 3,
            "topics": {"General": {}},
            "entity_types": {
                "Person": {"topic": "General", "labels": ["person"]},
                "Company": {"topic": "General", "labels": ["company"]},
            },
            "relationships": {
                "WORKS_FOR": {
                    "source_types": ["Person"],
                    "target_types": ["Company"],
                    "labels": ["works at", "employed by"],
                }
            },
        }
    ).compile()

    result = normalize_relationship(
        domain,
        "employed by",
        source_type="Person",
        target_type="Company",
    )
    assert result.canonical_type == "WORKS_FOR"
    assert result.domain_status == "recognized"


def test_review_rejects_untyped_arbitrary_patches_and_invalid_exclusion():
    with pytest.raises(ValidationError):
        MaintenanceReview(
            review_id="review-1",
            user_name="ada",
            scope="project",
            project_id="project-1",
            kind="relationship_interpretation",
            reasoning="remove unsupported evidence",
            proposed_plan={
                "kind": "relationship_interpretation",
                "changes": [
                    {
                        "observation_id": 7,
                        "expected_relationship_id": "project-1:1:2:works_at",
                        "target_relationship_type": None,
                        "interpretation_source": "domain",
                    }
                ],
            },
        )

    plan = RelationshipInterpretationPlan(
        changes=[
            {
                "observation_id": 7,
                "expected_relationship_id": "project-1:1:2:works_at",
                "target_relationship_type": None,
                "interpretation_source": "review",
            }
        ]
    )
    assert plan.changes[0].interpretation_source == "review"

    with pytest.raises(ValidationError, match="observation IDs must be unique"):
        RelationshipInterpretationPlan(
            changes=[
                {
                    "observation_id": 7,
                    "expected_relationship_id": "project-1:1:2:works_at",
                    "target_relationship_type": "WORKS_FOR",
                    "interpretation_source": "review",
                },
                {
                    "observation_id": 7,
                    "expected_relationship_id": "project-1:1:2:works_at",
                    "target_relationship_type": "WORKS_FOR",
                    "interpretation_source": "review",
                },
            ]
        )


def test_advisory_tracks_observation_ids_for_invalidation():
    advisory = build_relationship_advisories(
        [
            {
                "observation_id": 11,
                "semantic_window_id": "window-1",
                "observed_relationship_label": "works at",
                "source_type": "Person",
                "target_type": "Company",
                "source_entity_id": 1,
                "target_entity_id": 2,
                "observed_at_ms": 1,
            },
            {
                "observation_id": 12,
                "semantic_window_id": "window-1",
                "observed_relationship_label": "works at",
                "source_type": "Person",
                "target_type": "Company",
                "source_entity_id": 3,
                "target_entity_id": 4,
                "observed_at_ms": 2,
            },
            {
                "observation_id": 13,
                "semantic_window_id": "window-2",
                "observed_relationship_label": "works at",
                "source_type": "Person",
                "target_type": "Company",
                "source_entity_id": 5,
                "target_entity_id": 6,
                "observed_at_ms": 3,
            },
        ]
    )

    assert advisory[0].observation_ids == (11, 12, 13)


@pytest.mark.asyncio
async def test_transition_compares_and_records_event_in_one_transaction():
    review_row = {
        "review_id": "review-1",
        "user_name": "ada",
        "scope": "project",
        "project_id": "project-1",
        "kind": "relationship_interpretation",
        "dedupe_key": None,
        "evidence_refs": [],
        "evidence_snapshot": {},
        "reasoning": "Correct the relationship interpretation.",
        "proposed_plan": {
            "kind": "relationship_interpretation",
            "changes": [
                {
                    "observation_id": 7,
                    "expected_relationship_id": "project-1:1:2:works_at",
                    "target_relationship_type": "WORKS_FOR",
                    "interpretation_source": "review",
                }
            ],
        },
        "expected_state": {"revision": 1},
        "status": "open",
        "created_at": None,
        "resolved_at": None,
    }
    resolved_row = {**review_row, "status": "applied"}
    client = RecordingPostgresClient(fetch_one_results=[review_row, resolved_row])

    result = await MaintenanceReviewWriter(client).transition(
        "review-1",
        user_name="ada",
        project_id="project-1",
        status="applied",
        expected_state={"revision": 1},
    )

    assert result.status == "applied"
    assert client.transaction_enters == 1
    assert not any(call[0] == "fetch_one" for call in client.calls)
    assert sum("maintenance_review_events" in call[1] for call in client.calls) == 1


def test_review_signature_changes_with_typed_evidence_state():
    pointer = EvidencePointer.for_observation(7)
    plan = RelationshipInterpretationPlan(
        changes=[
            RelationshipInterpretationChange(
                observation_id=7,
                expected_relationship_id="relationship-1",
                target_relationship_type="WORKS_FOR",
                interpretation_source="review",
            )
        ]
    )
    base = {
        "user_name": "ada",
        "scope": "project",
        "project_id": "project-1",
        "kind": "relationship_interpretation",
        "dedupe_key": None,
        "evidence_refs": [pointer],
        "plan": plan,
    }

    first = MaintenanceReviewWriter.signature(
        **base,
        evidence_snapshot=EvidenceSnapshot(
            pointers=(pointer,), state_token="1" * 64
        ),
    )
    changed = MaintenanceReviewWriter.signature(
        **base,
        evidence_snapshot=EvidenceSnapshot(
            pointers=(pointer,), state_token="2" * 64
        ),
    )

    assert first != changed
