import asyncio
import json

import pytest

from common.conf.topics_config import TopicConfig
from common.schema.primitives import FactRecord
from common.schema.settings import TopicSchema
from infrastructure.job.base import JobContext
from infrastructure.redis_client import RedisKeys
from knoggin_server.knowledge.jobs.merge_job import MergeDetectionJob
from tests.fixtures.factories import make_topic_config
from tests.fixtures.fakes import FakeRedis


def make_fact(entity_id, content, *, fact_id=None, embedding=None):
    return FactRecord(
        id=fact_id or f"fact-{entity_id}",
        content=content,
        source_entity_id=entity_id,
        source_msg_id=1,
        embedding=embedding or [],
    )


def make_candidate(primary_id=101, secondary_id=202, *, llm_score=0.8):
    return {
        "primary_id": primary_id,
        "secondary_id": secondary_id,
        "primary_name": f"Entity {primary_id}",
        "secondary_name": f"Entity {secondary_id}",
        "primary_type": "person",
        "secondary_type": "person",
        "topic_a": "Identity",
        "topic_b": "Identity",
        "facts_a": [make_fact(primary_id, f"Fact about {primary_id}.")],
        "facts_b": [make_fact(secondary_id, f"Fact about {secondary_id}.")],
        "llm_score": llm_score,
    }


class FakeJobEntities:
    def __init__(self, *, candidates=None, raise_detection=False):
        self.candidates = list(candidates or [])
        self.raise_detection = raise_detection
        self.detect_calls = []
        self.resolution_lock = asyncio.Lock()
        self.ids_by_name = {}
        self.merge_calls = []
        self.embedding_calls = []
        self.profiles = {}

    async def detect_merge_entity_candidates(self, dirty_ids=None):
        self.detect_calls.append(set(dirty_ids or set()))
        if self.raise_detection:
            raise RuntimeError("candidate detection failed")
        return list(self.candidates)

    async def get_id(self, name):
        return self.ids_by_name.get(name)

    async def get_profile(self, entity_id):
        if entity_id in self.profiles:
            return self.profiles[entity_id]
        return {"canonical_name": f"Entity {entity_id}", "type": "concept"}

    def merge_into(
        self,
        primary_id,
        secondary_id,
        primary_profile_updates=None,
    ):
        self.merge_calls.append(
            {
                "primary_id": primary_id,
                "secondary_id": secondary_id,
                "primary_profile_updates": dict(primary_profile_updates or {}),
            }
        )
        if primary_profile_updates:
            self.profiles.setdefault(
                primary_id,
                {"canonical_name": f"Entity {primary_id}", "type": "concept"},
            ).update(primary_profile_updates)
        self.profiles[secondary_id] = None

    async def compute_embedding(self, entity_id, resolution_text):
        self.embedding_calls.append(
            {"entity_id": entity_id, "resolution_text": resolution_text}
        )
        return [float(entity_id), 0.1]


class FakeMergeGraph:
    def __init__(self):
        self.facts_for_entities = {}
        self.entity_facts = {}
        self.merge_results = {}
        self.merges = []
        self.invalidations = []
        self.updated_embeddings = []
        self.entities_by_id = {}
        self.entity_reads = []
        self.topic_strength = {}
        self.topic_strength_reads = []
        self.hierarchy_candidates = {}
        self.hierarchy_edges = []

    async def merge_entities(
        self,
        primary_id,
        secondary_id,
        project_id=None,
        final_topic=None,
    ):
        self.merges.append(
            {
                "primary_id": primary_id,
                "secondary_id": secondary_id,
                "project_id": project_id,
                "final_topic": final_topic,
            }
        )
        return self.merge_results.get((primary_id, secondary_id), True)

    async def get_merge_topic_strength(self, primary_id, secondary_id, project_id):
        self.topic_strength_reads.append(
            {
                "primary_id": primary_id,
                "secondary_id": secondary_id,
                "project_id": project_id,
            }
        )
        return dict(self.topic_strength.get((primary_id, secondary_id), {}))

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

    async def get_entity_by_id(self, entity_id, visible_project_ids=None):
        self.entity_reads.append(
            {
                "entity_id": entity_id,
                "visible_project_ids": visible_project_ids,
            }
        )
        return self.entities_by_id.get(entity_id)

    async def update_entity_embedding(self, entity_id, embedding, project_id=None):
        self.updated_embeddings.append(
            {
                "entity_id": entity_id,
                "embedding": list(embedding),
                "project_id": project_id,
            }
        )
        return True

    async def get_hierarchy_candidates(
        self,
        project_id,
        topic,
        parent_type,
        child_types,
        min_weight=2,
    ):
        return list(
            self.hierarchy_candidates.get(
                (
                    project_id,
                    topic,
                    parent_type,
                    tuple(child_types),
                    min_weight,
                ),
                [],
            )
        )

    async def create_hierarchy_edge(self, parent_id, child_id, project_id=None):
        self.hierarchy_edges.append(
            {"parent_id": parent_id, "child_id": child_id, "project_id": project_id}
        )
        return parent_id > 0


class FakeLLM:
    merge_model = "fake-merge"


class RecordingMergeJob(MergeDetectionJob):
    def __init__(
        self,
        *args,
        process_summary="1 merged",
        hierarchy_summary="0 hierarchy edges",
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.process_summary = process_summary
        self.hierarchy_summary = hierarchy_summary
        self.process_calls = []
        self.hierarchy_calls = []

    async def _process_merges(self, ctx, candidates):
        self.process_calls.append({"ctx": ctx, "candidates": list(candidates)})
        return self.process_summary

    async def _detect_hierarchy(self, ctx):
        self.hierarchy_calls.append(ctx)
        return self.hierarchy_summary


class AutoMergeJob(MergeDetectionJob):
    def __init__(self, *args, auto=None, hitl=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.auto = list(auto or [])
        self.hitl = list(hitl or [])
        self.finalized = []

    async def _judgement(self, candidates, ctx):
        return list(self.auto), list(self.hitl)

    async def _finalize_merge(self, ctx, merge_info):
        self.finalized.append(dict(merge_info))


def make_job(
    entities=None,
    graph=None,
    redis=None,
    *,
    topic_config=None,
    job_cls=MergeDetectionJob,
    **kwargs,
):
    return job_cls(
        user_name="ada",
        entities=entities or FakeJobEntities(),
        graph_client=graph or FakeMergeGraph(),
        llm_client=FakeLLM(),
        topic_config=topic_config or make_topic_config(),
        redis_client=redis or FakeRedis(),
        **kwargs,
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_should_run_reflects_merge_queue_contents():
    redis = FakeRedis()
    ctx = JobContext(user_name="ada", project_id="project-1")
    job = make_job(redis=redis)

    assert await job.should_run(ctx) is False

    await redis.sadd(RedisKeys.merge_queue("ada", "project-1"), "101")

    assert await job.should_run(ctx) is True


@pytest.mark.storage
@pytest.mark.no_network
async def test_execute_no_dirty_ids_skips_candidate_detection():
    entities = FakeJobEntities()
    job = make_job(entities=entities)

    result = await job.execute(JobContext("ada", "project-1"))

    assert result.success is True
    assert result.summary == "No dirty entities to merge"
    assert entities.detect_calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_execute_no_candidates_clears_dirty_queue():
    redis = FakeRedis()
    entities = FakeJobEntities(candidates=[])
    ctx = JobContext("ada", "project-1")
    await redis.sadd(RedisKeys.merge_queue("ada", "project-1"), "101", "202")
    job = make_job(entities=entities, redis=redis)

    result = await job.execute(ctx)

    assert result.success is True
    assert result.summary == "No candidates found"
    assert entities.detect_calls == [{101, 202}]
    assert await redis.smembers(RedisKeys.merge_queue("ada", "project-1")) == set()


@pytest.mark.storage
@pytest.mark.no_network
async def test_execute_candidates_processes_merges_hierarchy_and_clears_dirty_ids():
    redis = FakeRedis()
    candidate = make_candidate()
    entities = FakeJobEntities(candidates=[candidate])
    ctx = JobContext("ada", "project-1")
    await redis.sadd(RedisKeys.merge_queue("ada", "project-1"), "101", "202")
    job = make_job(
        entities=entities,
        redis=redis,
        job_cls=RecordingMergeJob,
        process_summary="2 merged",
        hierarchy_summary="1 hierarchy edges",
    )

    result = await job.execute(ctx)

    assert result.success is True
    assert result.summary == "2 merged; 1 hierarchy edges"
    assert job.process_calls[0]["candidates"] == [candidate]
    assert job.hierarchy_calls == [ctx]
    assert await redis.smembers(RedisKeys.merge_queue("ada", "project-1")) == set()


@pytest.mark.storage
@pytest.mark.no_network
async def test_execute_candidate_detection_failure_keeps_dirty_ids():
    redis = FakeRedis()
    entities = FakeJobEntities(raise_detection=True)
    ctx = JobContext("ada", "project-1")
    await redis.sadd(RedisKeys.merge_queue("ada", "project-1"), "101")
    job = make_job(entities=entities, redis=redis)

    with pytest.raises(RuntimeError, match="candidate detection failed"):
        await job.execute(ctx)

    assert await redis.smembers(RedisKeys.merge_queue("ada", "project-1")) == {"101"}


@pytest.mark.storage
@pytest.mark.no_network
async def test_execute_merge_db_only_merges_and_invalidates_duplicate_facts():
    graph = FakeMergeGraph()
    job = make_job(graph=graph)

    success = await job._execute_merge_db_only(
        101,
        202,
        ["dup-1", "dup-2"],
        project_id="project-1",
    )

    assert success is True
    assert graph.merges == [
        {
            "primary_id": 101,
            "secondary_id": 202,
            "project_id": "project-1",
            "final_topic": None,
        }
    ]
    assert graph.topic_strength_reads == [
        {"primary_id": 101, "secondary_id": 202, "project_id": "project-1"}
    ]
    assert [item["fact_id"] for item in graph.invalidations] == ["dup-1", "dup-2"]
    assert {item["project_id"] for item in graph.invalidations} == {"project-1"}


@pytest.mark.storage
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("strength", "expected_topic"),
    [
        (
            {
                "p_topic": "People",
                "s_topic": "Projects",
                "p_fact_count": 1,
                "s_fact_count": 2,
            },
            "Projects",
        ),
        (
            {
                "p_topic": "People",
                "s_topic": "Projects",
                "p_fact_count": 2,
                "s_fact_count": 2,
                "p_relationship_count": 3,
                "s_relationship_count": 1,
            },
            "People",
        ),
        (
            {
                "p_topic": "People",
                "s_topic": "Projects",
                "p_fact_count": 2,
                "s_fact_count": 2,
                "p_relationship_count": 1,
                "s_relationship_count": 1,
                "p_last": 100,
                "s_last": 200,
                "p_conf": 0.9,
                "s_conf": 0.1,
            },
            "Projects",
        ),
        (
            {
                "p_topic": "People",
                "s_topic": "Projects",
                "p_fact_count": 2,
                "s_fact_count": 2,
                "p_relationship_count": 1,
                "s_relationship_count": 1,
                "p_last": 100,
                "s_last": 100,
                "p_conf": 0.7,
                "s_conf": 0.8,
            },
            "Projects",
        ),
        (
            {
                "p_topic": "People",
                "s_topic": "Projects",
                "p_fact_count": 2,
                "s_fact_count": 2,
                "p_relationship_count": 1,
                "s_relationship_count": 1,
                "p_last": 100,
                "s_last": 100,
                "p_conf": 0.8,
                "s_conf": 0.8,
            },
            "People",
        ),
    ],
)
def test_select_merge_topic_uses_deterministic_strength_order(
    strength,
    expected_topic,
):
    assert MergeDetectionJob._select_merge_topic(strength) == expected_topic


@pytest.mark.storage
@pytest.mark.no_network
async def test_execute_merge_db_only_passes_selected_topic_to_writer():
    graph = FakeMergeGraph()
    graph.topic_strength[(101, 202)] = {
        "p_topic": "People",
        "s_topic": "Projects",
        "p_fact_count": 1,
        "s_fact_count": 2,
    }
    job = make_job(graph=graph)

    success = await job._execute_merge_db_only(
        101,
        202,
        [],
        project_id="project-1",
    )

    assert success is True
    assert graph.merges == [
        {
            "primary_id": 101,
            "secondary_id": 202,
            "project_id": "project-1",
            "final_topic": "Projects",
        }
    ]


@pytest.mark.storage
@pytest.mark.no_network
async def test_process_merges_auto_merge_uses_project_scope_and_cleans_intents():
    redis = FakeRedis()
    graph = FakeMergeGraph()
    entities = FakeJobEntities()
    candidate = make_candidate()
    graph.facts_for_entities[101] = [
        make_fact(101, "Same fact.", fact_id="a", embedding=[1.0, 0.0])
    ]
    graph.facts_for_entities[202] = [
        make_fact(202, "Same fact.", fact_id="b", embedding=[1.0, 0.0])
    ]
    job = make_job(
        entities=entities,
        graph=graph,
        redis=redis,
        job_cls=AutoMergeJob,
        auto=[candidate],
    )

    summary = await job._process_merges(JobContext("ada", "project-1"), [candidate])

    assert summary == "1 merged, 0 failed, 0 HITL"
    assert graph.merges == [
        {
            "primary_id": 101,
            "secondary_id": 202,
            "project_id": "project-1",
            "final_topic": None,
        }
    ]
    assert [item["fact_id"] for item in graph.invalidations] == ["b"]
    assert job.finalized[0]["primary_id"] == 101
    assert redis.strings == {}
    assert redis.sets[RedisKeys.merge_intents_index("ada", "project-1")] == set()


@pytest.mark.storage
@pytest.mark.no_network
async def test_process_merges_failed_graph_merge_counts_failure_and_cleans_intent():
    redis = FakeRedis()
    graph = FakeMergeGraph()
    candidate = make_candidate()
    graph.merge_results[(101, 202)] = False
    job = make_job(
        graph=graph,
        redis=redis,
        job_cls=AutoMergeJob,
        auto=[candidate],
    )

    summary = await job._process_merges(JobContext("ada", "project-1"), [candidate])

    assert summary == "0 merged, 1 failed, 0 HITL"
    assert redis.strings == {}
    assert redis.sets[RedisKeys.merge_intents_index("ada", "project-1")] == set()


@pytest.mark.storage
@pytest.mark.no_network
async def test_process_merges_stores_hitl_proposals_with_expiry():
    redis = FakeRedis()
    hitl_candidate = make_candidate(llm_score=0.75)
    job = make_job(
        redis=redis,
        job_cls=AutoMergeJob,
        hitl=[hitl_candidate],
    )

    summary = await job._process_merges(
        JobContext("ada", "project-1"),
        [hitl_candidate],
    )

    proposal_key = RedisKeys.merge_proposals("ada", "project-1")
    stored = json.loads(redis.lists[proposal_key][0])
    assert summary == "0 merged, 0 failed, 1 HITL"
    assert stored["primary_id"] == 101
    assert stored["secondary_id"] == 202
    assert stored["llm_score"] == 0.75
    assert redis.expirations == [(proposal_key, 7 * 24 * 3600)]


@pytest.mark.storage
@pytest.mark.no_network
async def test_finalize_merge_refreshes_primary_topic_in_runtime_cache():
    redis = FakeRedis()
    graph = FakeMergeGraph()
    entities = FakeJobEntities()
    entities.profiles[101] = {
        "canonical_name": "Entity 101",
        "type": "concept",
        "topic": "People",
    }
    entities.profiles[202] = {
        "canonical_name": "Entity 202",
        "type": "concept",
        "topic": "Projects",
    }
    graph.entities_by_id[101] = {
        "id": 101,
        "canonical_name": "Entity 101",
        "type": "concept",
        "topic": "Projects",
    }
    graph.entity_facts[101] = [make_fact(101, "Merged entity fact.")]
    job = make_job(entities=entities, graph=graph, redis=redis)

    await job._finalize_merge(
        JobContext("ada", "project-1"),
        make_candidate(101, 202),
    )

    assert graph.entity_reads == [
        {"entity_id": 101, "visible_project_ids": None}
    ]
    assert entities.merge_calls == [
        {
            "primary_id": 101,
            "secondary_id": 202,
            "primary_profile_updates": {"topic": "Projects"},
        }
    ]
    assert (await entities.get_profile(101))["topic"] == "Projects"
    assert await entities.get_profile(202) is None
    assert entities.embedding_calls[0]["resolution_text"] == (
        "Entity 101 (concept). Merged entity fact."
    )
    assert graph.updated_embeddings == [
        {
            "entity_id": 101,
            "embedding": [101.0, 0.1],
            "project_id": "project-1",
        }
    ]
    assert await redis.smembers(RedisKeys.dirty_entities("ada", "project-1")) == {
        "101"
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_detect_hierarchy_without_config_returns_zero_edges():
    job = make_job()

    summary = await job._detect_hierarchy(JobContext("ada", "project-1"))

    assert summary == "0 hierarchy edges"


@pytest.mark.storage
@pytest.mark.no_network
async def test_detect_hierarchy_creates_project_scoped_edges_and_counts_successes():
    graph = FakeMergeGraph()
    topic_config = TopicConfig(
        {
            "Work": TopicSchema(
                active=True,
                labels=["area", "task"],
                hierarchy={"area": ["task"]},
            )
        }
    )
    graph.hierarchy_candidates[
        ("project-1", "Work", "area", ("task",), 2)
    ] = [
        {
            "parent_id": 101,
            "child_id": 202,
            "parent_name": "Knoggin",
            "child_name": "Graph tests",
            "parent_type": "area",
            "child_type": "task",
        },
        {
            "parent_id": -1,
            "child_id": 303,
            "parent_name": "Broken",
            "child_name": "Skipped",
            "parent_type": "area",
            "child_type": "task",
        },
    ]
    job = make_job(graph=graph, topic_config=topic_config)

    summary = await job._detect_hierarchy(JobContext("ada", "project-1"))

    assert summary == "1 hierarchy edges"
    assert graph.hierarchy_edges == [
        {"parent_id": 101, "child_id": 202, "project_id": "project-1"},
        {"parent_id": -1, "child_id": 303, "project_id": "project-1"},
    ]
