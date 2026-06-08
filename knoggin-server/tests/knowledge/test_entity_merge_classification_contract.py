import asyncio

import pytest

from common.schema.contracts import MergeJudgment
from common.schema.primitives import FactRecord
from infrastructure.job.base import JobContext
from knoggin_server.knowledge.jobs.merge_job import MergeDetectionJob
from tests.fixtures.factories import make_topic_config
from tests.fixtures.fakes import FakeRedis


def seed_entity(
    entities,
    graph,
    entity_id,
    canonical_name,
    *,
    aliases=None,
    entity_type="person",
    topic="Identity",
    embedding=None,
):
    entity = graph.add_entity(
        entity_id,
        canonical_name,
        aliases=aliases,
        entity_type=entity_type,
        topic=topic,
        embedding=embedding,
    )
    entities._populate_cache(entity)


def make_fact(entity_id, content, *, fact_id=None, embedding=None):
    return FactRecord(
        id=fact_id or f"fact-{entity_id}",
        content=content,
        source_entity_id=entity_id,
        source_msg_id=1,
        embedding=embedding or [],
    )


def make_candidate(
    primary_id=101,
    secondary_id=202,
    *,
    primary_type="person",
    secondary_type="person",
    topic_a="Identity",
    topic_b="Identity",
    facts=True,
):
    facts_a = [make_fact(primary_id, f"Fact about {primary_id}.")] if facts else []
    facts_b = [make_fact(secondary_id, f"Fact about {secondary_id}.")] if facts else []
    return {
        "primary_id": primary_id,
        "secondary_id": secondary_id,
        "primary_name": f"Entity {primary_id}",
        "secondary_name": f"Entity {secondary_id}",
        "primary_type": primary_type,
        "secondary_type": secondary_type,
        "topic_a": topic_a,
        "topic_b": topic_b,
        "facts_a": facts_a,
        "facts_b": facts_b,
        "fuzz_score": 95,
    }


class FakeMergeLLM:
    merge_model = "fake-merge"

    def __init__(self, judgments=None, *, raise_error=False):
        self.judgments = list(judgments or [])
        self.raise_error = raise_error
        self.calls = []

    async def call_llm(self, **kwargs):
        self.calls.append(kwargs)
        if self.raise_error:
            raise RuntimeError("fake merge judgment failure")
        if self.judgments:
            return self.judgments.pop(0)
        return MergeJudgment(
            should_merge=True,
            reasoning="same entity",
            confidence=0.95,
            new_canonical_name=None,
        )


class FakeMergeGraph:
    def __init__(self):
        self.facts_for_entities = {}
        self.entity_facts = {}
        self.merge_results = {}
        self.merges = []
        self.invalidations = []
        self.updated_embeddings = []
        self.updated_names = []
        self.messages_by_ids_calls = []

    async def merge_entities(self, primary_id, secondary_id, project_id=None):
        self.merges.append(
            {
                "primary_id": primary_id,
                "secondary_id": secondary_id,
                "project_id": project_id,
            }
        )
        return self.merge_results.get((primary_id, secondary_id), True)

    async def invalidate_fact(self, fact_id, invalid_at, project_id=None):
        self.invalidations.append(
            {"fact_id": fact_id, "invalid_at": invalid_at, "project_id": project_id}
        )
        return True

    async def get_facts_for_entities(self, entity_ids, active_only=True):
        return {
            entity_id: list(self.facts_for_entities.get(entity_id, []))
            for entity_id in entity_ids
        }

    async def get_facts_for_entity(self, entity_id, active_only=True):
        return list(self.entity_facts.get(entity_id, []))

    async def update_entity_embedding(self, entity_id, embedding, project_id=None):
        self.updated_embeddings.append(
            {
                "entity_id": entity_id,
                "embedding": list(embedding),
                "project_id": project_id,
            }
        )
        return True

    async def update_entity_canonical_name(
        self, entity_id, canonical_name, project_id=None
    ):
        self.updated_names.append(
            {
                "entity_id": entity_id,
                "canonical_name": canonical_name,
                "project_id": project_id,
            }
        )
        return True

    async def get_messages_by_ids(self, message_ids, user_name=None, session_ids=None):
        self.messages_by_ids_calls.append(
            {
                "message_ids": list(message_ids),
                "user_name": user_name,
                "session_ids": session_ids,
            }
        )
        return []


def make_job(entities, graph_client, *, llm=None, redis=None, topic_config=None):
    return MergeDetectionJob(
        user_name="ada",
        entities=entities,
        graph_client=graph_client,
        llm_client=llm or FakeMergeLLM(),
        topic_config=topic_config or make_topic_config(),
        redis_client=redis or FakeRedis(),
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_classify_pair_suppresses_direct_and_hierarchy_edges(
    entity_manager_harness,
):
    entities, graph, _ = entity_manager_harness
    seed_entity(entities, graph, 101, "OpenAI", entity_type="org", topic="General")
    seed_entity(entities, graph, 202, "ChatGPT", entity_type="tool", topic="General")
    graph.direct_edges.add((101, 202))

    direct = await entities._classify_pair(101, 202, 95, {})
    graph.direct_edges.clear()
    graph.hierarchy_edges.add((101, 202))
    hierarchy = await entities._classify_pair(101, 202, 95, {})

    assert direct is None
    assert hierarchy is None


@pytest.mark.storage
@pytest.mark.no_network
async def test_classify_pair_rejects_missing_profile(entity_manager_harness):
    entities, graph, _ = entity_manager_harness
    seed_entity(entities, graph, 101, "Alice")

    result = await entities._classify_pair(101, 999, 95, {})

    assert result is None


@pytest.mark.storage
@pytest.mark.no_network
async def test_classify_pair_cross_topic_requires_high_fuzzy_same_type(
    entity_manager_harness,
):
    entities, graph, _ = entity_manager_harness
    seed_entity(entities, graph, 101, "Alice Chen", topic="Identity")
    seed_entity(entities, graph, 202, "Alice Chen", topic="General")

    weak = await entities._classify_pair(101, 202, 80, {})
    strong = await entities._classify_pair(101, 202, 90, {})

    assert weak is None
    assert strong["primary_id"] == 101
    assert strong["secondary_id"] == 202


@pytest.mark.storage
@pytest.mark.no_network
async def test_classify_pair_shared_neighbors_require_very_high_confidence(
    entity_manager_harness,
):
    entities, graph, _ = entity_manager_harness
    seed_entity(entities, graph, 101, "Robert Chen", embedding=[1.0, 0.0])
    seed_entity(entities, graph, 202, "Robert Chen Jr", embedding=[1.0, 0.1])
    graph.neighbor_ids_by_entity[101] = {303}
    graph.neighbor_ids_by_entity[202] = {303}

    weak = await entities._classify_pair(101, 202, 94, {})
    strong = await entities._classify_pair(101, 202, 96, {})

    assert weak is None
    assert strong["shared_neighbor_count"] == 1


@pytest.mark.storage
@pytest.mark.no_network
async def test_classify_pair_ignores_user_root_neighbor_and_includes_facts(
    entity_manager_harness,
):
    entities, graph, _ = entity_manager_harness
    fact_a = make_fact(101, "Alice works on design systems.")
    fact_b = make_fact(202, "Alicia works on design systems.")
    seed_entity(entities, graph, 101, "Alice")
    seed_entity(entities, graph, 202, "Alicia")
    graph.neighbor_ids_by_entity[101] = {1}
    graph.neighbor_ids_by_entity[202] = {1}

    result = await entities._classify_pair(
        101,
        202,
        95,
        {101: [fact_a], 202: [fact_b]},
    )

    assert result["shared_neighbor_count"] == 0
    assert result["facts_a"] == [fact_a]
    assert result["facts_b"] == [fact_b]


@pytest.mark.storage
@pytest.mark.no_network
async def test_judgement_alias_collision_same_topic_auto_merges_without_llm(
    entity_manager_harness,
):
    entities, graph, _ = entity_manager_harness
    llm = FakeMergeLLM()
    job = make_job(entities, graph, llm=llm)
    seed_entity(entities, graph, 101, "Robert Chen", aliases=["Bob"])
    seed_entity(entities, graph, 202, "Bob Smith")
    entities._id_to_names[202].add("bob")

    auto, hitl = await job._judgement([make_candidate()], JobContext("ada", "p1"))

    assert auto[0]["primary_id"] == 101
    assert hitl == []
    assert llm.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_judgement_alias_collision_cross_topic_uses_fake_llm(
    entity_manager_harness,
):
    entities, graph, _ = entity_manager_harness
    llm = FakeMergeLLM(
        [
            MergeJudgment(
                should_merge=True,
                reasoning="alias collision is real",
                confidence=0.94,
                new_canonical_name="Robert Chen",
            )
        ]
    )
    job = make_job(entities, graph, llm=llm)
    seed_entity(entities, graph, 101, "Robert Chen", aliases=["Bob"])
    seed_entity(entities, graph, 202, "Bob Smith")
    entities._id_to_names[202].add("bob")
    candidate = make_candidate(topic_a="Identity", topic_b="General")

    auto, hitl = await job._judgement([candidate], JobContext("ada", "project-1"))

    assert auto[0]["suggested_name"] == "Robert Chen"
    assert hitl == []
    assert len(llm.calls) == 1


@pytest.mark.storage
@pytest.mark.no_network
async def test_judgement_rejects_type_mismatch_before_cosine_or_llm(
    entity_manager_harness,
):
    entities, graph, _ = entity_manager_harness
    llm = FakeMergeLLM()
    job = make_job(entities, graph, llm=llm)
    seed_entity(entities, graph, 101, "Alice", entity_type="person", embedding=[1, 0])
    seed_entity(
        entities,
        graph,
        202,
        "Alice Project",
        entity_type="project",
        embedding=[1, 0],
    )
    candidate = make_candidate(primary_type="person", secondary_type="project")

    auto, hitl = await job._judgement([candidate], JobContext("ada", "project-1"))

    assert auto == []
    assert hitl == []
    assert llm.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_judgement_rejects_insufficient_facts_without_llm(
    entity_manager_harness,
):
    entities, graph, _ = entity_manager_harness
    llm = FakeMergeLLM()
    job = make_job(entities, graph, llm=llm)

    auto, hitl = await job._judgement(
        [make_candidate(facts=False)],
        JobContext("ada", "project-1"),
    )

    assert auto == []
    assert hitl == []
    assert llm.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_judgement_routes_by_cosine_and_topic(entity_manager_harness):
    entities, graph, _ = entity_manager_harness
    llm = FakeMergeLLM()
    job = make_job(entities, graph, llm=llm)
    seed_entity(entities, graph, 101, "Robert Chen", embedding=[1.0, 0.0])
    seed_entity(entities, graph, 202, "Rob Chen", embedding=[0.95, 0.05])

    auto, hitl = await job._judgement([make_candidate()], JobContext("ada", "p1"))

    assert len(auto) == 1
    assert hitl == []
    assert llm.calls == []

    cross_topic = make_candidate(topic_a="Identity", topic_b="General")
    auto, hitl = await job._judgement([cross_topic], JobContext("ada", "p1"))

    assert len(auto) == 1
    assert hitl == []
    assert len(llm.calls) == 1


@pytest.mark.storage
@pytest.mark.no_network
async def test_judgement_low_cosine_rejects_candidate(entity_manager_harness):
    entities, graph, _ = entity_manager_harness
    llm = FakeMergeLLM()
    job = make_job(entities, graph, llm=llm)
    seed_entity(entities, graph, 101, "Alice", embedding=[1.0, 0.0])
    seed_entity(entities, graph, 202, "Bob", embedding=[0.0, 1.0])

    auto, hitl = await job._judgement([make_candidate()], JobContext("ada", "p1"))

    assert auto == []
    assert hitl == []
    assert llm.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_judgement_fake_llm_score_routes_auto_hitl_and_reject(
    entity_manager_harness,
):
    entities, graph, _ = entity_manager_harness
    llm = FakeMergeLLM(
        [
            MergeJudgment(
                should_merge=True,
                reasoning="same",
                confidence=0.94,
                new_canonical_name="Same Entity",
            ),
            MergeJudgment(
                should_merge=True,
                reasoning="possible",
                confidence=0.8,
                new_canonical_name=None,
            ),
            MergeJudgment(
                should_merge=False,
                reasoning="different",
                confidence=0.9,
                new_canonical_name=None,
            ),
        ]
    )
    job = make_job(entities, graph, llm=llm)
    seed_entity(entities, graph, 101, "Alice", embedding=[0.8, 0.2])
    seed_entity(entities, graph, 202, "Alicia", embedding=[0.8, 0.7])
    seed_entity(entities, graph, 303, "Bob", embedding=[0.8, 0.2])
    seed_entity(entities, graph, 404, "Bobby", embedding=[0.8, 0.7])
    seed_entity(entities, graph, 505, "Chris", embedding=[0.8, 0.2])
    seed_entity(entities, graph, 606, "Christine", embedding=[0.8, 0.7])
    candidates = [
        make_candidate(101, 202),
        make_candidate(303, 404),
        make_candidate(505, 606),
    ]

    auto, hitl = await job._judgement(candidates, JobContext("ada", "project-1"))

    assert [c["primary_id"] for c in auto] == [101]
    assert [c["primary_id"] for c in hitl] == [303]
    assert len(llm.calls) == 3


@pytest.mark.storage
@pytest.mark.no_network
async def test_judge_with_sem_handles_fake_llm_failure(entity_manager_harness):
    entities, graph, _ = entity_manager_harness
    job = make_job(entities, graph, llm=FakeMergeLLM(raise_error=True))

    result = await job._judge_with_sem(
        make_candidate(),
        "project-1",
        asyncio.Semaphore(1),
    )

    assert result[1] == (None, None)


@pytest.mark.storage
@pytest.mark.no_network
async def test_get_merge_judgment_payload_includes_aliases_types_and_facts(
    entity_manager_harness,
):
    entities, graph, _ = entity_manager_harness
    llm = FakeMergeLLM()
    job = make_job(entities, graph, llm=llm)
    seed_entity(entities, graph, 101, "Robert Chen", aliases=["Bob"])
    seed_entity(entities, graph, 202, "Rob Chen", aliases=["Robbie"])
    candidate = make_candidate()
    candidate["primary_name"] = "Robert Chen"
    candidate["secondary_name"] = "Rob Chen"
    candidate["facts_a"] = [make_fact(101, "Robert works on memory graphs.")]
    candidate["facts_b"] = [make_fact(202, "Rob works on memory graphs.")]

    score, new_name = await job._get_merge_judgment(candidate, "project-1")

    payload = llm.calls[0]["user"]
    assert score == pytest.approx(0.95)
    assert new_name is None
    assert "Robert Chen" in payload
    assert "Rob Chen" in payload
    assert "Bob" in payload or "bob" in payload
    assert "person" in payload
    assert "Robert works on memory graphs." in payload
    assert "Rob works on memory graphs." in payload


@pytest.mark.storage
@pytest.mark.no_network
async def test_process_merges_flips_secondary_user_root_before_merge(
    entity_manager_harness,
):
    entities, entity_graph, _ = entity_manager_harness
    merge_graph = FakeMergeGraph()
    redis = FakeRedis()
    job = make_job(entities, merge_graph, redis=redis)
    seed_entity(entities, entity_graph, 1, "ada", embedding=[1.0, 0.0])
    seed_entity(entities, entity_graph, 202, "Ada Lovelace", embedding=[1.0, 0.0])
    fact_a = make_fact(202, "Ada Lovelace is the user.", embedding=[1.0, 0.0])
    fact_b = make_fact(1, "Ada is the user.", embedding=[1.0, 0.0])
    merge_graph.facts_for_entities[1] = [fact_b]
    merge_graph.facts_for_entities[202] = [fact_a]
    candidate = make_candidate(202, 1)
    candidate["primary_name"] = "Ada Lovelace"
    candidate["secondary_name"] = "ada"

    summary = await job._process_merges(
        JobContext("ada", "project-1"),
        [candidate],
    )

    assert summary.startswith("1 merged")
    assert merge_graph.merges[0]["primary_id"] == 1
    assert merge_graph.merges[0]["secondary_id"] == 202


@pytest.mark.storage
@pytest.mark.no_network
async def test_execute_merge_db_only_refuses_to_delete_user_root(
    entity_manager_harness,
):
    entities, graph, _ = entity_manager_harness
    merge_graph = FakeMergeGraph()
    job = make_job(entities, merge_graph)
    seed_entity(entities, graph, 1, "ada")

    success = await job._execute_merge_db_only(
        202,
        1,
        [],
        project_id="project-1",
    )

    assert success is False
    assert merge_graph.merges == []
