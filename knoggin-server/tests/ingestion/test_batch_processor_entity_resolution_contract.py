import asyncio

import pytest

from common.schema.contracts import BulkRelevanceResult, RelevanceResult
from common.schema.primitives import FactRecord
from knoggin_server.ingestion.services.pipeline_service import BatchProcessor
from knoggin_server.knowledge.services.entity_service import EntityManager
from tests.fixtures.factories import make_topic_config

MESSAGES = [
    {
        "id": 1,
        "message": "Alice is working with Bob on the Knoggin project.",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "role": "user",
    },
    {
        "id": 2,
        "message": "Alice uses Linear to organize the project work.",
        "timestamp": "2026-01-01T00:01:00+00:00",
        "role": "user",
    },
]


class FakeEmbeddingService:
    def __init__(self):
        self.batch_calls = []
        self.single_calls = []
        self.fail_single_prefixes = set()

    async def encode(self, texts):
        self.batch_calls.append(list(texts))
        return [self._vector_for(text) for text in texts]

    async def encode_single(self, text):
        self.single_calls.append(text)
        if any(text.startswith(prefix) for prefix in self.fail_single_prefixes):
            raise RuntimeError(f"embedding failed for {text}")
        return self._vector_for(text)

    def _vector_for(self, text):
        total = sum(ord(ch) for ch in text)
        return [float(total % 97), float(len(text)), float(total % 13)]


class FakeGraphClient:
    def __init__(self):
        self.vector_results = {}
        self.neighbors_by_entity = {}
        self.relevant_facts_by_entity = {}
        self.fact_fail_entity_ids = set()
        self.vector_searches = []
        self.neighbor_calls = []
        self.fact_searches = []

    async def get_entity_by_id(self, entity_id, visible_project_ids=None):
        return None

    async def get_entities_by_names(self, names, visible_project_ids=None):
        return []

    async def get_entity_embedding(self, entity_id):
        return []

    async def search_entities_by_embedding(
        self,
        vector,
        limit=5,
        score_threshold=0.85,
        visible_project_ids=None,
    ):
        self.vector_searches.append(
            {
                "vector": list(vector),
                "limit": limit,
                "score_threshold": score_threshold,
                "visible_project_ids": visible_project_ids,
            }
        )
        return list(self.vector_results.get(tuple(vector), []))

    async def get_neighbor_ids_batch(self, candidate_ids):
        self.neighbor_calls.append(list(candidate_ids))
        return {
            candidate_id: set(self.neighbors_by_entity.get(candidate_id, set()))
            for candidate_id in candidate_ids
        }

    async def search_relevant_facts(self, entity_id, embedding, limit=5):
        self.fact_searches.append(
            {"entity_id": entity_id, "embedding": list(embedding), "limit": limit}
        )
        if entity_id in self.fact_fail_entity_ids:
            raise RuntimeError(f"fact search failed for {entity_id}")
        return list(self.relevant_facts_by_entity.get(entity_id, []))


class FakeLLM:
    extraction_model = "fake-llm"

    def __init__(self, relevance=True, *, empty_response=False, raise_error=False):
        self.relevance = relevance
        self.empty_response = empty_response
        self.raise_error = raise_error
        self.calls = []

    async def call_llm(self, **kwargs):
        self.calls.append(kwargs)
        if self.raise_error:
            raise RuntimeError("fake llm failure")
        if self.empty_response:
            return BulkRelevanceResult(judgments=[])
        if isinstance(self.relevance, list):
            return BulkRelevanceResult(
                judgments=[
                    RelevanceResult(index=index, is_relevant=is_relevant)
                    for index, is_relevant in enumerate(self.relevance, start=1)
                ]
            )
        return BulkRelevanceResult(
            judgments=[RelevanceResult(index=1, is_relevant=self.relevance)]
        )


def make_harness():
    embedding = FakeEmbeddingService()
    graph = FakeGraphClient()
    entities = EntityManager(
        graph_client=graph,
        embedding_service=embedding,
        project_id="project-1",
        readable_project_ids=["project-1"],
    )
    next_ids = iter(range(1001, 1100))

    async def get_next_ent_id():
        return next(next_ids)

    processor = BatchProcessor(
        scope_id="session-1",
        redis_client=None,
        llm=FakeLLM(),
        entities=entities,
        processor=None,
        cpu_executor=None,
        user_name="ada",
        topic_config=make_topic_config(),
        get_next_ent_id=get_next_ent_id,
    )
    return processor, entities, graph, embedding


async def seed_entity(
    entities,
    entity_id,
    canonical_name,
    *,
    aliases=None,
    entity_type="person",
    topic="Identity",
):
    await entities.register_entity(
        entity_id,
        canonical_name,
        [canonical_name, *(aliases or [])],
        entity_type,
        topic,
        session_id="seed-session",
        source_context=f"Seeded profile for {canonical_name}.",
    )


def vector_for(embedding, text):
    return tuple(embedding._vector_for(text))


def make_fact(entity_id, content="The entity is relevant to this message."):
    return FactRecord(
        id=f"fact-{entity_id}",
        content=content,
        source_entity_id=entity_id,
        source_msg_id=1,
    )


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_boost_candidates_empty_input_returns_empty_scores():
    processor, _, graph, embedding = make_harness()

    scores = await processor._boost_candidates(
        [],
        {1: "Alice is working on Knoggin."},
        batch_matched_ids=set(),
    )

    assert scores == {}
    assert processor.llm.calls == []
    assert graph.neighbor_calls == []
    assert graph.fact_searches == []
    assert embedding.batch_calls == []


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_boost_candidates_without_facts_preserves_base_score():
    processor, _, graph, _ = make_harness()

    scores = await processor._boost_candidates(
        [(101, 0.8, 1)],
        {1: "Alice is working on Knoggin."},
        batch_matched_ids=set(),
    )

    assert scores == {101: pytest.approx(0.8)}
    assert len(graph.fact_searches) == 1
    assert processor.llm.calls == []


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_boost_candidates_relevant_fact_adds_llm_boost():
    processor, _, graph, _ = make_harness()
    graph.relevant_facts_by_entity[101] = [
        make_fact(101, "Knoggin is a memory graph project.")
    ]

    scores = await processor._boost_candidates(
        [(101, 0.8, 1)],
        {1: "Alice is working on the memory graph project."},
        batch_matched_ids=set(),
    )

    assert scores == {101: pytest.approx(0.85)}
    assert len(processor.llm.calls) == 1


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_boost_candidates_irrelevant_fact_keeps_base_score():
    processor, _, graph, _ = make_harness()
    processor.llm = FakeLLM(relevance=False)
    graph.relevant_facts_by_entity[101] = [
        make_fact(101, "Knoggin is a memory graph project.")
    ]

    scores = await processor._boost_candidates(
        [(101, 0.8, 1)],
        {1: "Alice is buying lunch."},
        batch_matched_ids=set(),
    )

    assert scores == {101: pytest.approx(0.8)}
    assert len(processor.llm.calls) == 1


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_boost_candidates_llm_failure_falls_back_to_base_score():
    processor, _, graph, _ = make_harness()
    processor.llm = FakeLLM(raise_error=True)
    graph.relevant_facts_by_entity[101] = [
        make_fact(101, "Knoggin is a memory graph project.")
    ]

    scores = await processor._boost_candidates(
        [(101, 0.8, 1)],
        {1: "Alice is working on the memory graph project."},
        batch_matched_ids=set(),
    )

    assert scores == {101: pytest.approx(0.8)}
    assert len(processor.llm.calls) == 1


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_boost_candidates_empty_llm_response_falls_back_to_base_score():
    processor, _, graph, _ = make_harness()
    processor.llm = FakeLLM(empty_response=True)
    graph.relevant_facts_by_entity[101] = [
        make_fact(101, "Knoggin is a memory graph project.")
    ]

    scores = await processor._boost_candidates(
        [(101, 0.8, 1)],
        {1: "Alice is working on the memory graph project."},
        batch_matched_ids=set(),
    )

    assert scores == {101: pytest.approx(0.8)}
    assert len(processor.llm.calls) == 1


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_boost_candidates_graph_neighbor_overlap_adds_boost():
    processor, _, graph, _ = make_harness()
    graph.neighbors_by_entity[101] = {201}

    scores = await processor._boost_candidates(
        [(101, 0.8, 1)],
        {1: "Alice is working on Knoggin."},
        batch_matched_ids={201},
    )

    assert scores == {101: pytest.approx(0.83)}
    assert processor.llm.calls == []


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_boost_candidates_graph_neighbor_boost_caps_at_point_zero_five():
    processor, _, graph, _ = make_harness()
    graph.neighbors_by_entity[101] = {201, 202, 203}

    scores = await processor._boost_candidates(
        [(101, 0.7, 1)],
        {1: "Alice is working on Knoggin."},
        batch_matched_ids={201, 202, 203},
    )

    assert scores == {101: pytest.approx(0.75)}


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_boost_candidates_fact_and_neighbor_boosts_combine():
    processor, _, graph, _ = make_harness()
    graph.relevant_facts_by_entity[101] = [
        make_fact(101, "Knoggin is a memory graph project.")
    ]
    graph.neighbors_by_entity[101] = {201}

    scores = await processor._boost_candidates(
        [(101, 0.8, 1)],
        {1: "Alice is working on the memory graph project."},
        batch_matched_ids={201},
    )

    assert scores == {101: pytest.approx(0.88)}


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_boost_candidates_duplicate_candidate_rows_keep_max_score():
    processor, _, _, _ = make_harness()
    reversed_processor, _, _, _ = make_harness()

    scores = await processor._boost_candidates(
        [(101, 0.7, 1), (101, 0.9, 2)],
        {
            1: "First mention of the candidate.",
            2: "Second mention of the candidate.",
        },
        batch_matched_ids=set(),
    )
    reversed_scores = await reversed_processor._boost_candidates(
        [(101, 0.9, 2), (101, 0.7, 1)],
        {
            1: "First mention of the candidate.",
            2: "Second mention of the candidate.",
        },
        batch_matched_ids=set(),
    )

    assert scores == {101: pytest.approx(0.9)}
    assert reversed_scores == {101: pytest.approx(0.9)}


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_boost_candidates_missing_message_text_keeps_base_score():
    processor, _, graph, embedding = make_harness()

    scores = await processor._boost_candidates(
        [(101, 0.8, 99)],
        {1: "Known message text."},
        batch_matched_ids=set(),
    )

    assert scores == {101: pytest.approx(0.8)}
    assert processor.llm.calls == []
    assert graph.fact_searches == []
    assert embedding.batch_calls == []


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_boost_candidates_fact_search_failure_falls_back_to_base_score():
    processor, _, graph, _ = make_harness()
    graph.fact_fail_entity_ids.add(101)

    scores = await processor._boost_candidates(
        [(101, 0.8, 1)],
        {1: "Alice is working on the memory graph project."},
        batch_matched_ids=set(),
    )

    assert scores == {101: pytest.approx(0.8)}
    assert len(graph.fact_searches) == 1
    assert processor.llm.calls == []


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolve_mentions_registers_new_entities_when_no_candidates():
    processor, entities, _, _ = make_harness()

    result = await processor._resolve_mentions(
        [
            (1, "Alice", "person", "Identity"),
            (1, "Knoggin", "project", "General"),
        ],
        MESSAGES,
    )

    assert result.entity_ids == [1001, 1002]
    assert result.new_ids == {1001, 1002}
    assert result.alias_ids == set()
    assert result.entity_msg_map == {1001: [1], 1002: [1]}

    alice_profile = await entities.get_profile(1001)
    knoggin_profile = await entities.get_profile(1002)
    assert alice_profile["canonical_name"] == "Alice"
    assert alice_profile["topic"] == "Identity"
    assert knoggin_profile["canonical_name"] == "Knoggin"
    assert knoggin_profile["topic"] == "General"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolve_mentions_reuses_exact_known_alias_candidate():
    processor, entities, _, _ = make_harness()
    await seed_entity(entities, 102, "Robert Chen", aliases=["Bob"])

    result = await processor._resolve_mentions(
        [(1, "Bob", "person", "Identity")],
        MESSAGES,
    )

    assert result.entity_ids == [102]
    assert result.new_ids == set()
    assert result.alias_ids == set()
    assert result.entity_msg_map == {102: [1]}


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolve_mentions_reuses_high_confidence_fuzzy_candidate():
    processor, entities, _, _ = make_harness()
    await seed_entity(entities, 102, "Robert Chen")

    result = await processor._resolve_mentions(
        [(1, "Robert Chn", "person", "Identity")],
        MESSAGES,
    )

    assert result.entity_ids == [102]
    assert result.new_ids == set()
    assert result.entity_msg_map == {102: [1]}


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolve_mentions_uses_vector_candidate_when_cache_name_does_not_match():
    processor, entities, graph, embedding = make_harness()
    await seed_entity(entities, 202, "Linear", entity_type="tool", topic="General")
    graph.vector_results[vector_for(embedding, "project planning app")] = [(202, 0.92)]

    result = await processor._resolve_mentions(
        [(2, "project planning app", "tool", "General")],
        MESSAGES,
    )

    assert result.entity_ids == [202]
    assert result.new_ids == set()
    assert result.entity_msg_map == {202: [2]}


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolve_mentions_creates_new_entity_when_candidate_below_threshold():
    processor, entities, graph, embedding = make_harness()
    await seed_entity(entities, 202, "Linear", entity_type="tool", topic="General")
    graph.vector_results[vector_for(embedding, "work tracker")] = [(202, 0.4)]

    result = await processor._resolve_mentions(
        [(2, "work tracker", "tool", "General")],
        MESSAGES,
    )

    assert result.entity_ids == [1001]
    assert result.new_ids == {1001}
    assert result.entity_msg_map == {1001: [2]}
    assert await entities.get_profile(202) is not None


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolve_mentions_deduplicates_repeated_new_name_within_batch():
    processor, entities, _, _ = make_harness()

    result = await processor._resolve_mentions(
        [
            (1, "Alice", "person", "Identity"),
            (2, "alice", "person", "Identity"),
        ],
        MESSAGES,
    )

    assert result.entity_ids == [1001]
    assert result.new_ids == {1001}
    assert result.entity_msg_map == {1001: [1, 2]}
    assert (await entities.get_profile(1001))["canonical_name"] == "Alice"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolve_mentions_embeds_unique_nonblank_names_only():
    processor, _, _, embedding = make_harness()

    await processor._resolve_mentions(
        [
            (1, "Alice", "person", "Identity"),
            (1, "", "person", "Identity"),
            (2, "Alice", "person", "Identity"),
            (2, "Linear", "tool", "General"),
        ],
        MESSAGES,
    )

    assert len(embedding.batch_calls) == 1
    assert set(embedding.batch_calls[0]) == {"Alice", "Linear"}


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolve_mentions_records_alias_updates_for_existing_match():
    processor, entities, graph, embedding = make_harness()
    await seed_entity(entities, 102, "Robert Chen", aliases=["Bob"])
    graph.vector_results[vector_for(embedding, "Bobby")] = [(102, 0.92)]

    result = await processor._resolve_mentions(
        [(1, "Bobby", "person", "Identity")],
        MESSAGES,
    )

    assert result.entity_ids == [102]
    assert result.new_ids == set()
    assert result.alias_ids == {102}
    assert result.alias_updates == {102: ["Bobby"]}
    assert entities.get_known_aliases()["bobby"] == 102


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolve_mentions_graph_neighbor_boost_can_cross_threshold():
    processor, entities, graph, embedding = make_harness()
    await seed_entity(entities, 301, "Knoggin", entity_type="project", topic="General")
    await seed_entity(entities, 401, "Ada Lovelace")
    graph.vector_results[vector_for(embedding, "memory graph project")] = [(301, 0.83)]
    graph.neighbors_by_entity[301] = {401}

    result = await processor._resolve_mentions(
        [
            (1, "Ada Lovelace", "person", "Identity"),
            (1, "memory graph project", "project", "General"),
        ],
        MESSAGES,
    )

    assert result.entity_ids == [401, 301]
    assert result.new_ids == set()
    assert result.entity_msg_map == {401: [1], 301: [1]}


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolve_mentions_fact_relevance_boost_crosses_threshold_with_fake_llm():
    processor, entities, graph, embedding = make_harness()
    processor.llm = FakeLLM(relevance=True)
    await seed_entity(entities, 501, "Notion", entity_type="tool", topic="General")
    graph.vector_results[vector_for(embedding, "workspace notes tool")] = [(501, 0.82)]
    graph.relevant_facts_by_entity[501] = [
        make_fact(501, "Notion is used to organize workspace notes.")
    ]

    result = await processor._resolve_mentions(
        [(2, "workspace notes tool", "tool", "General")],
        MESSAGES,
    )

    assert result.entity_ids == [501]
    assert result.new_ids == set()
    assert result.entity_msg_map == {501: [2]}
    assert len(processor.llm.calls) == 1


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolve_mentions_registration_failure_skips_entity_without_crashing():
    processor, _, _, embedding = make_harness()
    embedding.fail_single_prefixes.add("Alice (")

    result = await processor._resolve_mentions(
        [(1, "Alice", "person", "Identity")],
        MESSAGES,
    )

    assert result.entity_ids == []
    assert result.new_ids == set()
    assert result.alias_ids == set()
    assert result.entity_msg_map == {}


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolve_mentions_serializes_resolution_with_entity_lock():
    processor, entities, _, _ = make_harness()
    events = []
    original_register = entities.register_entity

    async def slow_register(*args, **kwargs):
        events.append(("start", args[0]))
        await asyncio.sleep(0)
        result = await original_register(*args, **kwargs)
        events.append(("end", args[0]))
        return result

    entities.register_entity = slow_register

    first, second = await asyncio.gather(
        processor._resolve_mentions([(1, "Alice", "person", "Identity")], MESSAGES),
        processor._resolve_mentions([(2, "Bob", "person", "Identity")], MESSAGES),
    )

    assert first.entity_ids == [1001]
    assert second.entity_ids == [1002]
    assert events == [("start", 1001), ("end", 1001), ("start", 1002), ("end", 1002)]
