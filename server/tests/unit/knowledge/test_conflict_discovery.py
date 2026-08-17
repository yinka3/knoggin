from types import SimpleNamespace

import pytest

from core.knowledge.conflict_discovery import ConflictPacketBuilder
from core.knowledge.conflicts import (
    ConflictDiscoveryContinuation,
    ConflictDiscoveryLease,
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

    async def get_seed_observations(self, lease, *, max_span_days):
        self.calls.append(("seeds", lease, max_span_days))
        return self.seeds

    async def get_direct_neighborhood(
        self, *, user_name, project_id, entity_ids, limit=512
    ):
        self.calls.append(("neighborhood", user_name, project_id, entity_ids, limit))
        return [
            row
            for entity_id in entity_ids
            for row in self.neighborhoods.get(entity_id, [])
        ]

    async def get_direct_neighborhood_page(
        self,
        *,
        user_name,
        project_id,
        entity_ids,
        after_observation_id,
        limit=512,
    ):
        rows = [
            row
            for entity_id in entity_ids
            for row in self.neighborhoods.get(entity_id, [])
            if row["observation_id"] > after_observation_id
        ]
        by_id = {row["observation_id"]: row for row in rows}
        rows = [by_id[key] for key in sorted(by_id)]
        self.calls.append(("neighborhood_page", entity_ids, after_observation_id, limit))
        return rows[:limit], len(rows) > limit

    async def get_observations_by_ids(self, *, observation_ids, **_kwargs):
        all_rows = {
            row["observation_id"]: row
            for rows in self.neighborhoods.values()
            for row in rows
        }
        return [all_rows[observation_id] for observation_id in observation_ids if observation_id in all_rows]


@pytest.mark.unit
@pytest.mark.no_network
async def test_packet_includes_the_direct_histories_of_both_endpoints():
    seed = _observation(101, 1, 2, observed_at_ms=100)
    ada_history = _observation(104, 1, 3, observed_at_ms=80, label="consults for")
    acme_history = _observation(224, 2, 4, observed_at_ms=60, label="owns")
    reader = PacketReader(
        [seed],
        {
            1: [seed, ada_history],
            2: [seed, acme_history],
        },
    )
    lease = ConflictDiscoveryLease("ada", "project-1", 0, 0, "lease-1")

    package = await ConflictPacketBuilder(reader).build(
        lease,
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
    assert reader.calls[1][1] == [1, 2]


@pytest.mark.unit
@pytest.mark.no_network
async def test_packet_stops_at_token_ceiling_and_leaves_a_continuation_cursor():
    first = _observation(101, 1, 2, observed_at_ms=100)
    second = _observation(102, 3, 4, observed_at_ms=110)
    reader = PacketReader(
        [first, second],
        {1: [first], 2: [first], 3: [second], 4: [second]},
    )
    lease = ConflictDiscoveryLease("ada", "project-1", 0, 0, "lease-1")

    package = await ConflictPacketBuilder(
        reader,
        token_counter=lambda prompt: 11 if '"observation_id":102' in prompt else 10,
    ).build(lease, max_span_days=60, max_tokens=10)

    assert package is not None
    assert package.next_observation_id == 101
    assert {row["observation_id"] for row in package.observations} == {101}


@pytest.mark.unit
@pytest.mark.no_network
async def test_oversized_neighborhood_persists_a_continuation_with_overlap():
    seed = _observation(10, 1, 2, observed_at_ms=100)
    nearby = [
        _observation(index, 1, 100 + index, observed_at_ms=100 + index)
        for index in range(11, 600)
    ]
    reader = PacketReader([seed], {1: [seed, *nearby], 2: [seed]})
    lease = ConflictDiscoveryLease("ada", "project-1", 0, 0, "lease-1")

    package = await ConflictPacketBuilder(
        reader,
        token_counter=lambda prompt: prompt.count('"observation_id"') * 10,
    ).build(lease, max_span_days=60, max_tokens=40)

    assert package is not None
    assert package.continuation is not None
    assert package.next_observation_id == 0
    assert package.continuation.after_observation_id > 0
    assert package.continuation.overlap_observation_ids

    continuation_lease = ConflictDiscoveryLease(
        "ada",
        "project-1",
        0,
        0,
        "lease-2",
        continuation=package.continuation,
    )
    next_package = await ConflictPacketBuilder(
        reader,
        token_counter=lambda prompt: prompt.count('"observation_id"') * 10,
    ).build(continuation_lease, max_span_days=60, max_tokens=40)

    assert next_package is not None
    assert {seed["observation_id"], *package.continuation.overlap_observation_ids}.issubset(
        {row["observation_id"] for row in next_package.observations}
    )


class JobStore:
    def __init__(self, package: ConflictDiscoveryPackage) -> None:
        self.package = package
        self.recorded = []
        self.completed = []
        self.released = []

    async def claim_conflict_discovery(self, **kwargs):
        self.claim_args = kwargs
        return self.package.lease

    async def build_conflict_discovery_package(self, lease, **kwargs):
        self.build_args = (lease, kwargs)
        return self.package

    async def record_conflict_detection(self, **kwargs):
        self.recorded.append(kwargs)
        return SimpleNamespace(should_notify=True)

    async def complete_conflict_discovery(self, package):
        self.completed.append(package)
        return True

    async def release_conflict_discovery(self, lease):
        self.released.append(lease)


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
async def test_job_records_only_model_candidates_grounded_in_its_packet():
    lease = ConflictDiscoveryLease("ada", "project-1", 0, 0, "lease-1")
    package = ConflictDiscoveryPackage(
        lease=lease,
        observations=(
            _observation(101, 1, 2, observed_at_ms=100),
            _observation(104, 1, 3, observed_at_ms=80),
        ),
        next_observed_at_ms=100,
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
    assert len(store.recorded) == 1
    assert store.recorded[0]["evidence_ids"] == [101, 104]
    assert store.recorded[0]["origin"] == "background_discovery"
    assert store.completed == [package]
    assert store.released == []
