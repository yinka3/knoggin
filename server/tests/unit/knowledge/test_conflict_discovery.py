from types import SimpleNamespace

import pytest

from core.knowledge.conflict_discovery import ConflictPacketBuilder
from core.knowledge.conflicts import (
    ConflictDiscoveryCursor,
    ConflictDiscoveryPackage,
    LLMConflictCandidate,
    LLMConflictDiscoveryResult,
)
from core.knowledge.jobs.conflict_discovery_job import ConflictDiscoveryJob


def _observation(
    observation_id: int,
    source_entity_id: int,
    target_entity_id: int,
    *,
    observed_at_ms: int,
    label: str = "works at",
) -> dict:
    return {
        "observation_id": observation_id,
        "relationship_id": f"rel-{observation_id}",
        "source_entity_id": source_entity_id,
        "source_entity_name": f"entity-{source_entity_id}",
        "target_entity_id": target_entity_id,
        "target_entity_name": f"entity-{target_entity_id}",
        "observed_relationship_label": label,
        "canonical_relationship_type": None,
        "confidence": 0.9,
        "context": f"Observation {observation_id}",
        "observed_at_ms": observed_at_ms,
    }


class PacketReader:
    def __init__(
        self, seeds: list[dict], neighborhoods: dict[int, list[dict]]
    ) -> None:
        self.seeds = seeds
        self.neighborhoods = neighborhoods
        self.calls = []

    async def get_seed_observations(self, cursor, *, max_span_days):
        self.calls.append(("seeds", cursor, max_span_days))
        return self.seeds

    async def get_direct_neighborhood(
        self, *, user_name, project_id, entity_ids, limit=128
    ):
        self.calls.append(("neighborhood", user_name, project_id, entity_ids, limit))
        by_id = {
            row["observation_id"]: row
            for entity_id in entity_ids
            for row in self.neighborhoods.get(entity_id, [])
        }
        return [by_id[observation_id] for observation_id in sorted(by_id)]


@pytest.mark.unit
@pytest.mark.no_network
async def test_packet_includes_bounded_direct_histories_of_both_endpoints():
    seed = _observation(101, 1, 2, observed_at_ms=100)
    ada_history = _observation(104, 1, 3, observed_at_ms=80, label="consults for")
    acme_history = _observation(224, 2, 4, observed_at_ms=60, label="owns")
    reader = PacketReader(
        [seed],
        {1: [seed, ada_history], 2: [seed, acme_history]},
    )
    cursor = ConflictDiscoveryCursor("ada", "project-1", 0)

    package = await ConflictPacketBuilder(reader).build(
        cursor,
        max_span_days=60,
        max_tokens=50_000,
    )

    assert package is not None
    assert {row["observation_id"] for row in package.observations} == {
        101,
        104,
        224,
    }
    assert package.next_observation_id == 101
    assert "104" in package.prompt
    assert "224" in package.prompt
    assert reader.calls[1][3] == [1, 2]


@pytest.mark.unit
@pytest.mark.no_network
async def test_packet_stops_at_token_ceiling_and_advances_only_reviewed_seed():
    first = _observation(101, 1, 2, observed_at_ms=100)
    second = _observation(102, 3, 4, observed_at_ms=110)
    reader = PacketReader(
        [first, second],
        {1: [first], 2: [first], 3: [second], 4: [second]},
    )
    cursor = ConflictDiscoveryCursor("ada", "project-1", 0)

    package = await ConflictPacketBuilder(
        reader,
        token_counter=lambda prompt: 11 if '"observation_id":102' in prompt else 10,
    ).build(cursor, max_span_days=60, max_tokens=10)

    assert package is not None
    assert package.next_observation_id == 101
    assert {row["observation_id"] for row in package.observations} == {101}


class JobStore:
    def __init__(self, package: ConflictDiscoveryPackage) -> None:
        self.package = package
        self.completed = []

    async def build_conflict_discovery_package(self, project_id=None, **kwargs):
        self.project_id = project_id
        self.build_args = kwargs
        return self.package

    async def complete_conflict_discovery(self, package, *, candidates):
        self.completed.append((package, candidates))
        return 1


class JobLLM:
    def count_tokens(self, text: str) -> int:
        return len(text)

    async def generate_structured(self, **kwargs):
        self.kwargs = kwargs
        return LLMConflictDiscoveryResult(
            candidates=[
                LLMConflictCandidate(
                    evidence_ids=[101, 104],
                    kind="possible_contradiction",
                    rationale="The two observations cannot both describe the same period.",
                    confidence=0.8,
                ),
                LLMConflictCandidate(
                    evidence_ids=[101, 999],
                    kind="temporal_ambiguity",
                    rationale="This cites an observation outside the packet.",
                    confidence=0.5,
                ),
            ]
        )


@pytest.mark.unit
@pytest.mark.no_network
async def test_job_persists_grounded_candidates_and_advances_cursor_together():
    cursor = ConflictDiscoveryCursor("ada", "project-1", 0)
    package = ConflictDiscoveryPackage(
        cursor=cursor,
        observations=(
            _observation(101, 1, 2, observed_at_ms=100),
            _observation(104, 1, 3, observed_at_ms=80),
        ),
        next_observation_id=101,
        prompt="RELATIONSHIP EVIDENCE",
        estimated_tokens=12,
    )
    store = JobStore(package)
    llm = JobLLM()
    job = ConflictDiscoveryJob(
        store,
        SimpleNamespace(
            enabled=True,
            interval_hours=48,
            max_seed_span_days=60,
            max_package_tokens=50_000,
        ),
        llm=llm,
    )

    result = await job.execute(
        SimpleNamespace(user_name="ada", project_id="project-1")
    )

    assert result.success
    assert len(store.completed) == 1
    completed_package, candidates = store.completed[0]
    assert completed_package is package
    assert [candidate.evidence_ids for candidate in candidates] == [[101, 104]]


@pytest.mark.unit
@pytest.mark.no_network
async def test_job_uses_normal_cadence_without_a_continuation_trigger():
    job = ConflictDiscoveryJob(
        SimpleNamespace(),
        SimpleNamespace(
            enabled=True,
            interval_hours=48,
            max_seed_span_days=60,
            max_package_tokens=50_000,
        ),
        llm=object(),
    )

    assert not await job.should_run(SimpleNamespace(user_name="ada", project_id="project-1"))
