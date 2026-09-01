from unittest.mock import AsyncMock

import pytest

from core.knowledge.conflicts import (
    ConflictDiscoveryCursor,
    ConflictDiscoveryPackage,
    LLMConflictCandidate,
)
from core.knowledge.store import KnowledgeStore
from tests.fixtures.fakes import RecordingPostgresClient


def _observation(observation_id: int) -> dict:
    return {
        "observation_id": observation_id,
        "relationship_id": "project-1:2:3:knows",
        "message_id": observation_id,
        "session_id": "session-1",
        "source_entity_id": 2,
        "source_entity_name": "Ada",
        "target_entity_id": 3,
        "target_entity_name": "Knoggin",
        "observed_relationship_label": "knows",
        "interpretation_source": "observed",
        "context": "Ada knows Knoggin.",
        "observed_at_ms": 100,
    }


@pytest.mark.unit
@pytest.mark.no_network
async def test_conflict_completion_writes_groups_and_advances_cursor_in_one_transaction():
    evidence = [_observation(10), _observation(11)]
    review = {
        "review_id": "review-1",
        "user_name": "ada",
        "scope": "project",
        "project_id": "project-1",
        "kind": "relationship_conflict",
        "dedupe_key": "unused",
        "evidence_refs": [{"kind": "observation", "id": "10"}],
        "evidence_snapshot": {},
        "reasoning": "The evidence may describe incompatible states.",
        "proposed_plan": {
            "kind": "conflict_resolution",
            "conflict_kind": "possible_contradiction",
        },
        "expected_state": {},
        "status": "open",
    }
    client = RecordingPostgresClient(
        fetch_all_results=[evidence],
        fetch_one_results=[None, review],
    )
    store = KnowledgeStore(client, embedding_service=object())
    store._conflict_service.notify_detection = AsyncMock()
    package = ConflictDiscoveryPackage(
        cursor=ConflictDiscoveryCursor("ada", "project-1", 0),
        observations=tuple(evidence),
        next_observation_id=11,
        prompt="RELATIONSHIP EVIDENCE",
        estimated_tokens=12,
    )
    candidate = LLMConflictCandidate(
        evidence_ids=[10, 11],
        kind="possible_contradiction",
        rationale="The evidence may describe incompatible states.",
        confidence=0.8,
    )

    written = await store.complete_conflict_discovery(package, candidates=[candidate])

    assert written == 1
    assert client.transaction_enters == 1
    assert any("INSERT INTO public.maintenance_reviews" in call[1] for call in client.calls)
    cursor_call = next(
        call
        for call in client.calls
        if "UPDATE public.maintenance_review_checkpoints" in call[1]
    )
    assert cursor_call[2] == (11, "ada", "project-1")
    store._conflict_service.notify_detection.assert_awaited_once()
