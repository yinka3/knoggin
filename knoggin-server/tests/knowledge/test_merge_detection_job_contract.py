import pytest

from infrastructure.redis_client import RedisKeys
from knoggin_server.agent.tools.maintenance import MaintenanceTools
from knoggin_server.knowledge.entity.merge_service import EntityMergeService
from tests.fixtures.fakes import FakeRedis
from tests.knowledge.test_entity_merge_classification_contract import (
    RecordingKnowledgeStore,
    RecordingPostgres,
    snapshot_results,
)


class MaintenanceHarness(MaintenanceTools):
    def __init__(self, *, redis, entities, knowledge_store=None, postgres=None):
        self.user_name = "ada"
        self.project_id = "project-1"
        self.redis = redis
        self.knowledge_store = knowledge_store or RecordingKnowledgeStore()
        self.postgres = postgres or RecordingPostgres()
        self.entities = entities


class CandidateEntities:
    def __init__(self, candidates):
        self.candidates = list(candidates)
        self.calls = []

    async def detect_merge_entity_candidates(self, dirty_ids=None):
        self.calls.append(dirty_ids)
        return list(self.candidates)


def merge_candidate(
    primary_id,
    secondary_id,
    *,
    fuzz_score=92,
    cosine_score=None,
    fact_support=None,
    fact_support_pairs=None,
    shared_neighbor_count=0,
    facts_a=None,
    reasons=None,
):
    return {
        "primary_id": primary_id,
        "secondary_id": secondary_id,
        "primary_name": f"Entity {primary_id}",
        "secondary_name": f"Entity {secondary_id}",
        "primary_type": "person",
        "secondary_type": "person",
        "topic_a": "People",
        "topic_b": "People",
        "facts_a": facts_a or [],
        "facts_b": [],
        "fuzz_score": fuzz_score,
        "cosine_score": cosine_score,
        "fact_support": fact_support,
        "fact_support_pairs": fact_support_pairs or [],
        "shared_neighbor_count": shared_neighbor_count,
        "reasons": reasons or ["name_similarity"],
    }


@pytest.mark.no_network
async def test_graph_health_reports_ranked_project_scoped_candidates():
    redis = FakeRedis()
    await redis.sadd(RedisKeys.merge_queue("ada", "project-1"), "2", "4")
    entities = CandidateEntities(
        [
            merge_candidate(4, 5, fuzz_score=88),
            merge_candidate(2, 3, fuzz_score=96),
        ]
    )
    tools = MaintenanceHarness(redis=redis, entities=entities)

    result = await tools.check_graph_health()

    assert result["suggestions"][0] == {
        "primary_id": 2,
        "primary_name": "Entity 2",
        "primary_type": "person",
        "secondary_id": 3,
        "secondary_name": "Entity 3",
        "secondary_type": "person",
        "topic_a": "People",
        "topic_b": "People",
        "fuzz_score": 96,
        "cosine_score": None,
        "fact_support": None,
        "fact_support_pairs": [],
        "shared_neighbor_count": 0,
        "reasons": ["name_similarity"],
        "evidence_facts": [],
    }
    assert entities.calls == [{2, 4}]


@pytest.mark.no_network
async def test_graph_health_does_not_mutate_merge_queue():
    redis = FakeRedis()
    key = RedisKeys.merge_queue("ada", "project-1")
    await redis.sadd(key, "2")
    tools = MaintenanceHarness(
        redis=redis,
        entities=CandidateEntities([]),
    )

    result = await tools.check_graph_health()

    assert "healthy" in result["message"].lower()
    assert await redis.smembers(key) == {"2"}


@pytest.mark.no_network
async def test_graph_health_scans_resolver_cache_without_merge_queue():
    redis = FakeRedis()
    entities = CandidateEntities([merge_candidate(2, 3)])
    tools = MaintenanceHarness(redis=redis, entities=entities)

    result = await tools.check_graph_health()

    assert result["suggestions"][0]["primary_id"] == 2
    assert entities.calls == [None]


@pytest.mark.no_network
async def test_graph_health_exposes_merge_candidate_diagnostics():
    fact = {"fact_id": "fact-2", "content": "Entity 2 works at Acme."}
    support_pairs = [
        {
            "fact_a_id": "fact-2",
            "fact_b_id": "fact-3",
            "label": "entailment",
            "scores": {"entailment": 0.91},
        }
    ]
    entities = CandidateEntities(
        [
            merge_candidate(
                2,
                3,
                fuzz_score=0,
                cosine_score=0.98,
                fact_support="entailment",
                fact_support_pairs=support_pairs,
                facts_a=[fact],
                reasons=["vector_similarity"],
            )
        ]
    )
    tools = MaintenanceHarness(redis=FakeRedis(), entities=entities)

    result = await tools.check_graph_health()

    suggestion = result["suggestions"][0]
    assert suggestion["cosine_score"] == pytest.approx(0.98)
    assert suggestion["fact_support"] == "entailment"
    assert suggestion["fact_support_pairs"] == support_pairs
    assert suggestion["evidence_facts"] == [
        {
            "side": "primary",
            "fact_id": "fact-2",
            "content": "Entity 2 works at Acme.",
        }
    ]


@pytest.mark.no_network
async def test_graph_health_ranking_uses_entailment_then_name_then_vector_strength():
    entities = CandidateEntities(
        [
            merge_candidate(
                10,
                11,
                fuzz_score=0,
                cosine_score=0.99,
                fact_support="insufficient_facts",
                reasons=["vector_similarity"],
            ),
            merge_candidate(20, 21, fuzz_score=96, cosine_score=0.70),
            merge_candidate(
                30,
                31,
                fuzz_score=80,
                cosine_score=0.82,
                fact_support="entailment",
            ),
        ]
    )
    tools = MaintenanceHarness(redis=FakeRedis(), entities=entities)

    result = await tools.check_graph_health()

    assert [item["primary_id"] for item in result["suggestions"]] == [30, 20, 10]
    assert result["suggestions"][2]["fuzz_score"] == 0
    assert result["suggestions"][2]["cosine_score"] == pytest.approx(0.99)


@pytest.mark.no_network
async def test_agent_merge_tool_only_creates_proposal(monkeypatch):
    captured = {}

    async def fake_propose(self, **kwargs):
        captured.update(kwargs)
        return {"policy_result": "confirmation_required"}

    monkeypatch.setattr(EntityMergeService, "propose", fake_propose)
    tools = MaintenanceHarness(
        redis=FakeRedis(),
        entities=CandidateEntities([]),
        knowledge_store=RecordingKnowledgeStore(),
    )

    result = await tools.propose_entity_merge(
        2,
        3,
        ["fact-2"],
        "Same person.",
        confidence=0.9,
    )

    assert result == {"policy_result": "confirmation_required"}
    assert captured["user_name"] == "ada"
    assert captured["project_id"] == "project-1"
    assert captured["model_confidence"] == 0.9


@pytest.mark.no_network
async def test_confirm_rejects_invalid_token_without_merging():
    proposal = {
        "proposal_id": "proposal-1",
        "status": "confirmation_required",
        "confirmation_token_hash": EntityMergeService._token_hash("correct"),
        "user_name": "ada",
    }
    postgres = RecordingPostgres([[proposal]])
    store = RecordingKnowledgeStore()
    service = EntityMergeService(postgres, store)

    result = await service.confirm(
        proposal_id="proposal-1",
        confirmation_token="wrong",
        confirmed_by="ada",
    )

    assert result["policy_result"] == "rejected"
    assert store.merges == []


@pytest.mark.no_network
async def test_confirm_rejects_unauthorized_actor_without_merging():
    token = "confirm-me"
    proposal = {
        "proposal_id": "proposal-1",
        "status": "confirmation_required",
        "confirmation_token_hash": EntityMergeService._token_hash(token),
        "user_name": "ada",
    }
    postgres = RecordingPostgres([[proposal]])
    store = RecordingKnowledgeStore()

    result = await EntityMergeService(postgres, store).confirm(
        proposal_id="proposal-1",
        confirmation_token=token,
        confirmed_by="grace",
    )

    assert result["policy_result"] == "rejected"
    assert "not authorized" in result["reason"]
    assert store.merges == []


@pytest.mark.no_network
async def test_confirm_rejects_proposal_when_reviewed_state_changed():
    reviewed_snapshot = {
        "entities": snapshot_results()[0],
        "facts": snapshot_results()[1],
        "relationships": [],
        "hierarchy": [],
    }
    changed_entities = snapshot_results()[0]
    changed_entities[0] = {
        **changed_entities[0],
        "canonical_name": "Changed after review",
    }
    token = "confirm-me"
    proposal = {
        "proposal_id": "proposal-1",
        "status": "confirmation_required",
        "confirmation_token_hash": EntityMergeService._token_hash(token),
        "user_name": "ada",
        "project_id": "project-1",
        "primary_entity_id": 2,
        "duplicate_entity_id": 3,
        "reviewed_state_hash": EntityMergeService._state_hash(reviewed_snapshot),
    }
    postgres = RecordingPostgres(
        [[proposal], *snapshot_results(entities=changed_entities)]
    )
    store = RecordingKnowledgeStore()

    result = await EntityMergeService(postgres, store).confirm(
        proposal_id="proposal-1",
        confirmation_token=token,
        confirmed_by="ada",
    )

    assert result["policy_result"] == "rejected"
    assert "stale" in result["reason"]
    assert store.merges == []
    assert any(
        call[0] == "execute"
        and "UPDATE entity_merge_proposals" in call[1]
        for call in postgres.calls
    )


@pytest.mark.no_network
async def test_confirm_executes_canonical_merge_and_updates_runtime_queue(monkeypatch):
    events = []

    async def fake_emit(*args, **kwargs):
        events.append((args, kwargs))

    monkeypatch.setattr(
        "knoggin_server.knowledge.entity.merge_service.emit",
        fake_emit,
    )
    reviewed_snapshot = {
        "entities": snapshot_results()[0],
        "facts": snapshot_results()[1],
        "relationships": [],
        "hierarchy": [],
    }
    token = "confirm-me"
    proposal = {
        "proposal_id": "proposal-1",
        "status": "confirmation_required",
        "confirmation_token_hash": EntityMergeService._token_hash(token),
        "user_name": "ada",
        "project_id": "project-1",
        "primary_entity_id": 2,
        "duplicate_entity_id": 3,
        "evidence_fact_ids": ["fact-2"],
        "reasoning": "Same person.",
        "reviewed_state_hash": EntityMergeService._state_hash(reviewed_snapshot),
    }
    postgres = RecordingPostgres(
        [[proposal], *snapshot_results(), *snapshot_results()],
        execute_results=[1, 1, 1, 1],
    )
    store = RecordingKnowledgeStore()
    redis = FakeRedis()
    merge_key = RedisKeys.merge_queue("ada", "project-1")
    await redis.sadd(merge_key, "2", "3")
    service = EntityMergeService(postgres, store, redis=redis)

    result = await service.confirm(
        proposal_id="proposal-1",
        confirmation_token=token,
        confirmed_by="ada",
    )

    assert result["policy_result"] == "executed"
    assert store.merges == [(2, 3, "project-1")]
    assert await redis.smembers(merge_key) == set()
    assert await redis.smembers(
        RedisKeys.dirty_entities("ada", "project-1")
    ) == {"2"}
    assert [event[0][2] for event in events] == [
        "merge_queue_removed",
        "dirty_entities_marked",
    ]
    assert events[0][0] == (
        "project-1",
        "job",
        "merge_queue_removed",
        {
            "user_name": "ada",
            "project_id": "project-1",
            "merge_key": merge_key,
            "entity_ids": [2, 3],
            "cleared_count": 2,
            "reason": "merge_executed",
            "proposal_id": "proposal-1",
            "primary_id": 2,
            "duplicate_id": 3,
        },
    )
