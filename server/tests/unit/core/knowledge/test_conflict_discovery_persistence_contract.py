import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from common.schema.evidence import (
    EvidenceBundle,
    EvidenceNode,
    EvidencePointer,
    EvidenceSubject,
)
from core.knowledge.conflict.conflicts import (
    ConflictDiscoveryCursor,
    ConflictDiscoveryPackage,
    LLMConflictCandidate,
)
from core.knowledge.evidence_service import EvidenceService
from core.project.maintenance_service import ProjectMaintenanceService
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


def _bundle(observation_id: int, token: str) -> EvidenceBundle:
    pointer = EvidencePointer.for_observation(observation_id)
    return EvidenceBundle(
        subject=EvidenceSubject(
            kind="relationship_observation", identifier=str(observation_id)
        ),
        nodes=(EvidenceNode(pointer=pointer, status="active"),),
        edges=(),
        total_nodes=1,
        total_edges=0,
        state_token=token * 64,
    )


class EvidenceStore:
    def __init__(self, bundles: dict[int, EvidenceBundle]) -> None:
        self.bundles = bundles
        self.calls = []

    async def get_relationship_observations_evidence(self, observation_ids, **_kwargs):
        self.calls.append(tuple(observation_ids))
        return tuple(self.bundles[observation_id] for observation_id in observation_ids)


@pytest.mark.unit
@pytest.mark.no_network
async def test_conflict_completion_writes_groups_and_advances_cursor_in_one_transaction():
    evidence = [_observation(10), _observation(11), _observation(12)]
    bundles = (_bundle(10, "a"), _bundle(11, "b"), _bundle(12, "c"))
    expected_snapshot = EvidenceService.snapshot(bundles[:2])
    review = {
        "review_id": "review-1",
        "user_name": "ada",
        "scope": "project",
        "project_id": "project-1",
        "kind": "relationship_conflict",
        "dedupe_key": "unused",
        "evidence_refs": [
            {"kind": "relationship_observation", "identifier": "10"},
            {"kind": "relationship_observation", "identifier": "11"},
        ],
        "evidence_snapshot": expected_snapshot.model_dump(mode="json"),
        "reasoning": "The evidence may describe incompatible states.",
        "proposed_plan": {
            "kind": "conflict_resolution",
            "conflict_kind": "possible_contradiction",
        },
        "expected_state": {},
        "status": "open",
    }
    client = RecordingPostgresClient(
        fetch_one_results=[None, review],
    )
    store = EvidenceStore({10: bundles[0], 11: bundles[1], 12: bundles[2]})
    async def project_lookup(_project_id):
        return {"status": "active"}

    service = ProjectMaintenanceService(
        resources=type(
            "Resources", (), {"postgres": client, "knowledge_store": store}
        )(),
        user_name="ada",
        project_lookup=project_lookup,
        active_projects={},
        project_leases={},
    )
    service._conflict_service.notify_detection = AsyncMock()
    package = ConflictDiscoveryPackage(
        cursor=ConflictDiscoveryCursor("ada", "project-1", 0),
        observations=tuple(evidence),
        next_observation_id=11,
        prompt="RELATIONSHIP EVIDENCE",
        estimated_tokens=12,
        evidence_bundles=bundles,
    )
    candidate = LLMConflictCandidate(
        evidence_ids=[10, 11],
        kind="possible_contradiction",
        rationale="The evidence may describe incompatible states.",
        confidence=0.8,
    )

    written = await service.complete_conflict_discovery(package, candidates=[candidate])

    assert written == 1
    assert client.transaction_enters == 1
    assert any(
        "INSERT INTO public.maintenance_reviews" in call[1] for call in client.calls
    )
    review_insert = next(
        call
        for call in client.calls
        if "INSERT INTO public.maintenance_reviews" in call[1]
    )
    assert json.loads(review_insert[2][7]) == expected_snapshot.model_dump(mode="json")
    assert store.calls == [(10, 11)]
    cursor_call = next(
        call
        for call in client.calls
        if "UPDATE public.maintenance_review_checkpoints" in call[1]
    )
    assert cursor_call[2] == (11, "ada", "project-1")
    service._conflict_service.notify_detection.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.no_network
async def test_conflict_completion_keeps_committed_result_when_notification_fails():
    async def project_lookup(_project_id):
        return {"status": "active"}

    service = ProjectMaintenanceService(
        resources=SimpleNamespace(
            postgres=object(),
            knowledge_store=EvidenceStore({}),
        ),
        user_name="ada",
        project_lookup=project_lookup,
        active_projects={},
        project_leases={},
    )
    service._conflict_service.notify_detection = AsyncMock(
        side_effect=RuntimeError("event sink unavailable")
    )
    service._persist_conflict_discovery = AsyncMock(
        return_value=[
            SimpleNamespace(
                should_notify=True,
                group=SimpleNamespace(conflict_id="conflict-1"),
            )
        ]
    )
    package = ConflictDiscoveryPackage(
        cursor=ConflictDiscoveryCursor("ada", "project-1", 0),
        observations=(),
        next_observation_id=0,
        prompt="",
        estimated_tokens=0,
        evidence_bundles=(),
    )

    assert await service.complete_conflict_discovery(package, candidates=[]) == 1


@pytest.mark.unit
@pytest.mark.no_network
async def test_conflict_completion_rejects_foreign_package_scope():
    service = ProjectMaintenanceService(
        resources=SimpleNamespace(postgres=object(), knowledge_store=EvidenceStore({})),
        user_name="ada",
        project_lookup=AsyncMock(return_value={"status": "active"}),
        active_projects={},
        project_leases={},
    )
    package = ConflictDiscoveryPackage(
        cursor=ConflictDiscoveryCursor("other", "project-1", 0),
        observations=(),
        next_observation_id=0,
        prompt="",
        estimated_tokens=0,
        evidence_bundles=(),
    )

    with pytest.raises(ValueError, match="another user"):
        await service.complete_conflict_discovery(package, candidates=[])


@pytest.mark.unit
@pytest.mark.no_network
async def test_direct_conflict_report_captures_only_its_cited_evidence():
    bundles = (_bundle(10, "a"), _bundle(11, "b"), _bundle(12, "c"))
    store = EvidenceStore({10: bundles[0], 11: bundles[1], 12: bundles[2]})

    async def project_lookup(_project_id):
        return {"status": "active"}

    service = ProjectMaintenanceService(
        resources=SimpleNamespace(postgres=object(), knowledge_store=store),
        user_name="ada",
        project_lookup=project_lookup,
        active_projects={},
        project_leases={},
    )
    recorded = []

    class ConflictService:
        async def record_detection(self, **kwargs):
            recorded.append(kwargs)
            return SimpleNamespace()

    service._conflict_service = ConflictService()

    await service.record_conflict_detection(
        "project-1",
        origin="user_created",
        kind="possible_contradiction",
        rationale="These observations disagree.",
        confidence=0.7,
        evidence_ids=[11, 10],
    )

    assert recorded[0]["evidence_snapshot"] == EvidenceService.snapshot(bundles[:2])
    assert store.calls == [(10, 11)]
