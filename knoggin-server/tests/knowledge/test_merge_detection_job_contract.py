from types import SimpleNamespace

import pytest

from infrastructure.redis_client import RedisKeys
from knoggin_server.agent.tools.maintenance import MaintenanceTools
from knoggin_server.knowledge.services.entity_merge_service import EntityMergeService
from tests.fixtures.fakes import FakeRedis

from tests.knowledge.test_entity_merge_classification_contract import (
    RecordingKnowledgeStore,
    RecordingPostgres,
    snapshot_results,
)


class MaintenanceHarness(MaintenanceTools):
    def __init__(self, *, redis, knowledge_store, postgres=None):
        self.user_name = "ada"
        self.project_id = "project-1"
        self.redis = redis
        self.knowledge_store = knowledge_store
        self.postgres = postgres or RecordingPostgres()
        self.entities = SimpleNamespace(get_profile=self._get_profile)

    async def _get_profile(self, entity_id):
        return {"canonical_name": f"Entity {entity_id}"}


class SimilarityStore:
    def __init__(self, results):
        self.results = results
        self.calls = []

    async def search_similar_entities(
        self, entity_id, *, limit, visible_project_ids
    ):
        self.calls.append((entity_id, limit, visible_project_ids))
        return self.results.get(entity_id, [])


@pytest.mark.no_network
async def test_graph_health_reports_ranked_project_scoped_candidates():
    redis = FakeRedis()
    await redis.sadd(RedisKeys.merge_queue("ada", "project-1"), "2", "4")
    store = SimilarityStore({2: [(3, 0.92)], 4: [(5, 0.70)]})
    tools = MaintenanceHarness(redis=redis, knowledge_store=store)

    result = await tools.check_graph_health()

    assert result["suggestions"][0] == {
        "primary_id": 2,
        "primary_name": "Entity 2",
        "secondary_id": 3,
        "secondary_name": "Entity 3",
        "similarity_score": 0.92,
    }
    assert all(call[2] == ["project-1"] for call in store.calls)


@pytest.mark.no_network
async def test_graph_health_does_not_mutate_merge_queue():
    redis = FakeRedis()
    key = RedisKeys.merge_queue("ada", "project-1")
    await redis.sadd(key, "2")
    tools = MaintenanceHarness(
        redis=redis,
        knowledge_store=SimilarityStore({2: []}),
    )

    result = await tools.check_graph_health()

    assert "healthy" in result["message"].lower()
    assert await redis.smembers(key) == {"2"}


@pytest.mark.no_network
async def test_agent_merge_tool_only_creates_proposal(monkeypatch):
    captured = {}

    async def fake_propose(self, **kwargs):
        captured.update(kwargs)
        return {"policy_result": "confirmation_required"}

    monkeypatch.setattr(EntityMergeService, "propose", fake_propose)
    tools = MaintenanceHarness(
        redis=FakeRedis(),
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
async def test_confirm_executes_canonical_merge_and_updates_runtime_queue():
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
