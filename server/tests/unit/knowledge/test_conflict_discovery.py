import asyncio
from types import SimpleNamespace

import pytest

from common.schema.evidence import (
    EvidenceBundle,
    EvidenceNode,
    EvidencePointer,
    EvidenceSubject,
)
from common.schema.settings import ConflictDiscoverySettings
from core.knowledge.conflict.conflict_discovery import ConflictPacketBuilder
from core.knowledge.conflict.conflicts import (
    ConflictDiscoveryCursor,
    ConflictDiscoveryPackage,
    LLMConflictCandidate,
    LLMConflictDiscoveryResult,
)
from core.knowledge.db.readers.conflict_discovery_reader import (
    ConflictDiscoveryReader,
)
from core.knowledge.jobs.conflict_discovery_job import ConflictDiscoveryJob
from infrastructure.job.base import JobContext
from infrastructure.job.scheduler import Scheduler


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


def _bundle(
    observation_id: int,
    *,
    status: str = "active",
    nodes_truncated: bool = False,
) -> EvidenceBundle:
    pointer = EvidencePointer.for_observation(observation_id)
    return EvidenceBundle(
        subject=EvidenceSubject(
            kind="relationship_observation",
            identifier=str(observation_id),
        ),
        nodes=(EvidenceNode(pointer=pointer, status=status),),
        edges=(),
        total_nodes=2 if nodes_truncated else 1,
        total_edges=0,
        nodes_truncated=nodes_truncated,
        state_token=f"{observation_id:064x}",
    )


class PacketReader:
    def __init__(self, seeds: list[dict], neighborhoods: dict[int, list[dict]]) -> None:
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


class PacketEvidenceLoader:
    def __init__(self, bundles: dict[int, EvidenceBundle] | None = None) -> None:
        self.bundles = bundles or {}
        self.calls: list[tuple[int, ...]] = []

    async def __call__(self, observation_ids: list[int]) -> tuple[EvidenceBundle, ...]:
        self.calls.append(tuple(observation_ids))
        return tuple(
            self.bundles.setdefault(observation_id, _bundle(observation_id))
            for observation_id in observation_ids
        )


class SeedReaderClient:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows

    async def fetch_all(self, _query, params):
        after_observation_id = params[2]
        limit = params[3]
        return [
            row
            for row in self.rows
            if row["observation_id"] > after_observation_id
        ][:limit]


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

    package = await ConflictPacketBuilder(
        reader,
        evidence_loader=PacketEvidenceLoader(),
    ).build(
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
        evidence_loader=PacketEvidenceLoader(),
    ).build(cursor, max_span_days=60, max_tokens=10)

    assert package is not None
    assert package.next_observation_id == 101
    assert {row["observation_id"] for row in package.observations} == {101}


@pytest.mark.unit
@pytest.mark.no_network
async def test_packet_includes_current_context_backed_seeds_for_model_review():
    seeds = [
        {**_observation(101, 1, 2, observed_at_ms=100), "evidence_origin": "context"},
        {**_observation(102, 2, 3, observed_at_ms=110), "evidence_origin": "context"},
    ]
    package = await ConflictPacketBuilder(
        PacketReader(seeds, {}),
        evidence_loader=PacketEvidenceLoader(),
    ).build(
        ConflictDiscoveryCursor("ada", "project-1", 0),
        max_span_days=60,
        max_tokens=1_000,
    )

    assert package is not None
    assert [row["observation_id"] for row in package.observations] == [101, 102]
    assert package.next_observation_id == 102
    assert package.estimated_tokens > 0
    assert '"evidence_origin":"context"' in package.prompt


@pytest.mark.unit
@pytest.mark.no_network
async def test_seed_reader_stops_at_the_first_out_of_span_id_and_revisits_it():
    span_ms = 60 * 86_400_000
    rows = [
        _observation(1, 1, 2, observed_at_ms=0),
        _observation(3, 1, 2, observed_at_ms=span_ms),
        _observation(5, 1, 2, observed_at_ms=span_ms + 1),
        _observation(8, 1, 2, observed_at_ms=86_400_000),
    ]
    reader = ConflictDiscoveryReader(SeedReaderClient(rows))
    cursor = ConflictDiscoveryCursor("ada", "project-1", 0)
    reviewed_ids: list[int] = []

    while True:
        seeds = await reader.get_seed_observations(cursor, max_span_days=60)
        if not seeds:
            break
        reviewed_ids.extend(row["observation_id"] for row in seeds)
        cursor = ConflictDiscoveryCursor(
            "ada",
            "project-1",
            seeds[-1]["observation_id"],
        )

    assert reviewed_ids == [1, 3, 5, 8]


@pytest.mark.unit
@pytest.mark.no_network
async def test_packet_measures_provenance_and_trims_lowest_id_neighbor_first():
    seed = _observation(101, 1, 2, observed_at_ms=100)
    old_neighbor = _observation(102, 1, 3, observed_at_ms=80)
    recent_neighbor = _observation(103, 1, 4, observed_at_ms=90)
    bundles = {
        101: _bundle(101, nodes_truncated=True),
        102: _bundle(102),
        103: _bundle(103),
    }
    reader = PacketReader(
        [seed],
        {1: [seed, old_neighbor, recent_neighbor], 2: [seed]},
    )
    loader = PacketEvidenceLoader(bundles)

    def count_tokens(prompt: str) -> int:
        return 11 if '"observation_id":"102"' in prompt else 10

    package = await ConflictPacketBuilder(
        reader,
        token_counter=count_tokens,
        evidence_loader=loader,
    ).build(
        ConflictDiscoveryCursor("ada", "project-1", 0),
        max_span_days=60,
        max_tokens=10,
    )

    assert package is not None
    assert {row["observation_id"] for row in package.observations} == {101, 103}
    assert package.next_observation_id == 101
    assert '"observation_id":102' not in package.prompt
    assert '"observation_id":"102"' not in package.prompt
    assert '"nodes_truncated":true' in package.prompt
    assert [bundle.subject.identifier for bundle in package.evidence_bundles] == [
        "101",
        "103",
    ]
    assert loader.calls == [(101, 102, 103)]

    repeated = await ConflictPacketBuilder(
        PacketReader(
            [seed],
            {1: [seed, old_neighbor, recent_neighbor], 2: [seed]},
        ),
        token_counter=count_tokens,
        evidence_loader=PacketEvidenceLoader(bundles),
    ).build(
        ConflictDiscoveryCursor("ada", "project-1", 0),
        max_span_days=60,
        max_tokens=10,
    )
    assert repeated is not None
    assert repeated.observations == package.observations
    assert repeated.prompt == package.prompt


@pytest.mark.unit
@pytest.mark.no_network
async def test_packet_chunks_bounded_provenance_loads_and_reuses_them_for_rendering():
    seed = _observation(101, 1, 2, observed_at_ms=100)
    neighbors = [
        _observation(observation_id, 1, observation_id, observed_at_ms=observation_id)
        for observation_id in range(102, 107)
    ]
    loader = PacketEvidenceLoader()
    package = await ConflictPacketBuilder(
        PacketReader([seed], {1: [seed, *neighbors], 2: [seed]}),
        token_counter=lambda _prompt: 1,
        evidence_loader=loader,
        evidence_batch_size=2,
    ).build(
        ConflictDiscoveryCursor("ada", "project-1", 0),
        max_span_days=60,
        max_tokens=1,
    )

    assert package is not None
    assert {row["observation_id"] for row in package.observations} == set(
        range(101, 107)
    )
    assert loader.calls == [(101, 102), (103, 104), (105, 106)]


@pytest.mark.unit
@pytest.mark.no_network
async def test_packet_fails_when_one_seed_and_its_provenance_cannot_fit():
    seed = _observation(101, 1, 2, observed_at_ms=100)
    loader = PacketEvidenceLoader()

    with pytest.raises(
        ValueError,
        match=r"seed 101 with required provenance requires 42 tokens; ceiling is 10",
    ):
        await ConflictPacketBuilder(
            PacketReader([seed], {1: [seed], 2: [seed]}),
            token_counter=lambda _prompt: 42,
            evidence_loader=loader,
        ).build(
            ConflictDiscoveryCursor("ada", "project-1", 0),
            max_span_days=60,
            max_tokens=10,
        )

    assert loader.calls == [(101,)]


class JobStore:
    def __init__(self, package: ConflictDiscoveryPackage) -> None:
        self.package = package
        self.completed = []
        self.build_calls = 0

    async def build_conflict_discovery_package(self, project_id=None, **kwargs):
        self.build_calls += 1
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
        ConflictDiscoverySettings(),
        llm=llm,
    )

    result = await job.execute(SimpleNamespace(user_name="ada", project_id="project-1"))

    assert result.success
    assert len(store.completed) == 1
    completed_package, candidates = store.completed[0]
    assert completed_package is package
    assert [candidate.evidence_ids for candidate in candidates] == [[101, 104]]
    assert job.snapshot_for_health() == {
        "mode": "assisted",
        "scheduler_enabled": True,
        "interval_hours": 48,
        "llm_available": True,
        "last_run": {
            "reviewed_observation_count": 2,
            "opened_review_count": 1,
            "ignored_candidate_count": 1,
        },
    }


@pytest.mark.unit
@pytest.mark.no_network
async def test_job_rejects_a_candidate_for_a_neighbor_trimmed_from_the_packet():
    package = ConflictDiscoveryPackage(
        cursor=ConflictDiscoveryCursor("ada", "project-1", 0),
        observations=(_observation(101, 1, 2, observed_at_ms=100),),
        next_observation_id=101,
        prompt="RELATIONSHIP EVIDENCE\n{\"observation_id\":101}",
        estimated_tokens=12,
    )

    class TrimmedNeighborLLM:
        def count_tokens(self, text: str) -> int:
            return len(text)

        async def generate_structured(self, **_kwargs):
            return LLMConflictDiscoveryResult(
                candidates=[
                    LLMConflictCandidate(
                        evidence_ids=[101, 102],
                        kind="possible_contradiction",
                        rationale="This cites a trimmed neighborhood observation.",
                        confidence=0.8,
                    )
                ]
            )

    store = JobStore(package)
    result = await ConflictDiscoveryJob(
        store,
        ConflictDiscoverySettings(),
        llm=TrimmedNeighborLLM(),
    ).execute(SimpleNamespace(user_name="ada", project_id="project-1"))

    assert result.success
    assert store.completed == [(package, [])]


@pytest.mark.unit
@pytest.mark.no_network
async def test_job_advances_an_empty_response_but_not_a_provider_failure():
    package = ConflictDiscoveryPackage(
        cursor=ConflictDiscoveryCursor("ada", "project-1", 0),
        observations=(_observation(101, 1, 2, observed_at_ms=100),),
        next_observation_id=101,
        prompt="RELATIONSHIP EVIDENCE\n{\"observation_id\":101}",
        estimated_tokens=12,
    )

    class EmptyLLM:
        def count_tokens(self, text: str) -> int:
            return len(text)

        async def generate_structured(self, **_kwargs):
            return LLMConflictDiscoveryResult()

    empty_store = JobStore(package)
    result = await ConflictDiscoveryJob(
        empty_store,
        ConflictDiscoverySettings(),
        llm=EmptyLLM(),
    ).execute(SimpleNamespace(user_name="ada", project_id="project-1"))
    assert result.success
    assert empty_store.completed == [(package, [])]

    class FailingLLM:
        def count_tokens(self, text: str) -> int:
            return len(text)

        async def generate_structured(self, **_kwargs):
            raise RuntimeError("provider unavailable")

    failing_store = JobStore(package)
    with pytest.raises(RuntimeError, match="provider unavailable"):
        await ConflictDiscoveryJob(
            failing_store,
            ConflictDiscoverySettings(),
            llm=FailingLLM(),
        ).execute(SimpleNamespace(user_name="ada", project_id="project-1"))
    assert failing_store.completed == []


@pytest.mark.unit
@pytest.mark.no_network
async def test_job_defers_assisted_mode_to_the_scheduler_cadence():
    job = ConflictDiscoveryJob(
        SimpleNamespace(),
        ConflictDiscoverySettings(),
        llm=object(),
    )

    assert job.enabled is True
    assert not await job.should_run(
        SimpleNamespace(user_name="ada", project_id="project-1")
    )


@pytest.mark.unit
@pytest.mark.no_network
def test_conflict_discovery_settings_reject_the_retired_trusted_mode():
    with pytest.raises(ValueError):
        ConflictDiscoverySettings(mode="trusted")


@pytest.mark.unit
@pytest.mark.no_network
async def test_manual_mode_skips_background_model_work_but_allows_requested_run():
    store = JobStore(None)
    job = ConflictDiscoveryJob(
        store,
        ConflictDiscoverySettings(mode="manual"),
        llm=JobLLM(),
    )

    assert job.enabled is False
    assert not await job.should_run(
        SimpleNamespace(user_name="ada", project_id="project-1")
    )

    result = await job.execute(SimpleNamespace(user_name="ada", project_id="project-1"))

    assert result.success is True
    assert store.project_id == "project-1"
    assert result.summary.startswith("[manual]")


@pytest.mark.unit
@pytest.mark.no_network
async def test_disabled_discovery_is_not_scheduled():
    context = JobContext(user_name="ada", project_id="project-1")
    store = JobStore(None)
    job = ConflictDiscoveryJob(
        store,
        ConflictDiscoverySettings(enabled=False),
        llm=JobLLM(),
    )
    scheduler = Scheduler("ada", "project-1")
    scheduler.register(job)

    assert job.enabled is False
    await scheduler._schedule_if_due(job.name, job, context)

    assert store.build_calls == 0


@pytest.mark.unit
@pytest.mark.no_network
async def test_scheduler_admits_assisted_mode_by_cadence_but_never_manual_mode(
    monkeypatch,
):
    context = JobContext(user_name="ada", project_id="project-1")
    manual_store = JobStore(None)
    manual_job = ConflictDiscoveryJob(
        manual_store,
        ConflictDiscoverySettings(mode="manual"),
        llm=JobLLM(),
    )
    manual_scheduler = Scheduler("ada", "project-1")
    manual_scheduler.register(manual_job)
    await manual_scheduler._schedule_if_due(manual_job.name, manual_job, context)
    assert manual_store.build_calls == 0

    assisted_store = JobStore(None)
    assisted_job = ConflictDiscoveryJob(
        assisted_store,
        ConflictDiscoverySettings(mode="assisted"),
        llm=JobLLM(),
    )
    scheduler = Scheduler("ada", "project-1")
    scheduler.register(assisted_job)
    monkeypatch.setattr("infrastructure.job.scheduler.get_now_unix", lambda: 1000)

    await scheduler._schedule_if_due(assisted_job.name, assisted_job, context)
    await scheduler._running_tasks[assisted_job.name]
    await asyncio.sleep(0)
    await scheduler._schedule_if_due(assisted_job.name, assisted_job, context)

    assert assisted_store.build_calls == 1
    assert scheduler._last_successful_runs == {"conflict_discovery": 1000}
