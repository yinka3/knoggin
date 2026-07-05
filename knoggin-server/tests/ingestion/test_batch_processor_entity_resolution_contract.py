import asyncio

import pytest

from common.exceptions import LLMProviderError
from common.schema.contracts import BulkRelevanceResult, RelevanceResult
from common.schema.primitives import FactRecord
from knoggin_server.ingestion.services.pipeline_service import (
    CANDIDATE_RELEVANCE_LLM_BATCH_SIZE,
    IngestionPipeline,
)
from knoggin_server.knowledge.entity.resolver import EntityResolver
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


class FakeKnowledgeStore:
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

    async def get_entity_embedding(self, entity_id, *, visible_project_ids):
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

    async def get_neighbor_ids_batch(self, candidate_ids, *, visible_project_ids):
        self.neighbor_calls.append(list(candidate_ids))
        return {
            candidate_id: set(self.neighbors_by_entity.get(candidate_id, set()))
            for candidate_id in candidate_ids
        }

    async def search_relevant_facts(
        self,
        entity_id,
        embedding,
        *,
        visible_project_ids,
        limit=5,
    ):
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

    async def generate_structured(self, **kwargs):
        self.calls.append(kwargs)
        if self.raise_error:
            raise LLMProviderError("fake llm failure")
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
    knowledge_store = FakeKnowledgeStore()
    entities = EntityResolver(
        knowledge_store=knowledge_store,
        embedding_service=embedding,
        project_id="project-1",
        readable_project_ids=["project-1"],
    )
    next_ids = iter(range(1001, 1100))

    async def get_next_ent_id():
        return next(next_ids)

    processor = IngestionPipeline(
        project_id="project-1",
        redis_client=None,
        llm=FakeLLM(),
        entities=entities,
        processor=None,
        cpu_executor=None,
        user_name="ada",
        topic_config=make_topic_config(),
        get_next_ent_id=get_next_ent_id,
    )
    return processor, entities, knowledge_store, embedding


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
async def test_collect_candidate_support_scores_empty_input_returns_empty_scores():
    processor, _, knowledge_store, embedding = make_harness()

    scores = await processor._collect_candidate_support_scores(
        [],
        {1: "Alice is working on Knoggin."},
    )

    assert scores == {}
    assert processor.llm.calls == []
    assert knowledge_store.neighbor_calls == []
    assert knowledge_store.fact_searches == []
    assert embedding.batch_calls == []


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_collect_candidate_support_scores_without_facts_preserves_base_score():
    processor, _, knowledge_store, _ = make_harness()

    scores = await processor._collect_candidate_support_scores(
        [(101, 0.8, 1)],
        {1: "Alice is working on Knoggin."},
    )

    assert scores == {101: pytest.approx(0.8)}
    assert len(knowledge_store.fact_searches) == 1
    assert processor.llm.calls == []


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_collect_candidate_support_scores_relevant_fact_adds_llm_support():
    processor, _, knowledge_store, _ = make_harness()
    knowledge_store.relevant_facts_by_entity[101] = [
        make_fact(101, "Knoggin is a memory graph project.")
    ]

    scores = await processor._collect_candidate_support_scores(
        [(101, 0.8, 1)],
        {1: "Alice is working on the memory graph project."},
    )

    assert scores == {101: pytest.approx(0.85)}
    assert len(processor.llm.calls) == 1


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_collect_candidate_support_scores_irrelevant_fact_keeps_base_score():
    processor, _, knowledge_store, _ = make_harness()
    processor.llm = FakeLLM(relevance=False)
    knowledge_store.relevant_facts_by_entity[101] = [
        make_fact(101, "Knoggin is a memory graph project.")
    ]

    scores = await processor._collect_candidate_support_scores(
        [(101, 0.8, 1)],
        {1: "Alice is buying lunch."},
    )

    assert scores == {101: pytest.approx(0.8)}
    assert len(processor.llm.calls) == 1


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_collect_candidate_support_scores_llm_failure_falls_back_to_base_score():
    processor, _, knowledge_store, _ = make_harness()
    processor.llm = FakeLLM(raise_error=True)
    knowledge_store.relevant_facts_by_entity[101] = [
        make_fact(101, "Knoggin is a memory graph project.")
    ]

    scores = await processor._collect_candidate_support_scores(
        [(101, 0.8, 1)],
        {1: "Alice is working on the memory graph project."},
    )

    assert scores == {101: pytest.approx(0.8)}
    assert len(processor.llm.calls) == 1


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_candidate_support_empty_llm_response_uses_base_score():
    processor, _, knowledge_store, _ = make_harness()
    processor.llm = FakeLLM(empty_response=True)
    knowledge_store.relevant_facts_by_entity[101] = [
        make_fact(101, "Knoggin is a memory graph project.")
    ]

    scores = await processor._collect_candidate_support_scores(
        [(101, 0.8, 1)],
        {1: "Alice is working on the memory graph project."},
    )

    assert scores == {101: pytest.approx(0.8)}
    assert len(processor.llm.calls) == 1


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_collect_candidate_support_scores_ignores_graph_neighbor_overlap():
    processor, _, knowledge_store, _ = make_harness()
    knowledge_store.neighbors_by_entity[101] = {201}

    scores = await processor._collect_candidate_support_scores(
        [(101, 0.8, 1)], {1: "Alice is working on Knoggin."}
    )

    assert scores == {101: pytest.approx(0.8)}
    assert knowledge_store.neighbor_calls == []
    assert processor.llm.calls == []


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_candidate_support_does_not_query_graph_neighbors():
    processor, _, knowledge_store, _ = make_harness()
    knowledge_store.neighbors_by_entity[101] = {201, 202, 203}

    scores = await processor._collect_candidate_support_scores(
        [(101, 0.7, 1)], {1: "Alice is working on Knoggin."}
    )

    assert scores == {101: pytest.approx(0.7)}
    assert knowledge_store.neighbor_calls == []


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_candidate_support_uses_fact_signal_without_neighbor_signal():
    processor, _, knowledge_store, _ = make_harness()
    knowledge_store.relevant_facts_by_entity[101] = [
        make_fact(101, "Knoggin is a memory graph project.")
    ]
    knowledge_store.neighbors_by_entity[101] = {201}

    scores = await processor._collect_candidate_support_scores(
        [(101, 0.8, 1)], {1: "Alice is working on the memory graph project."}
    )

    assert scores == {101: pytest.approx(0.85)}
    assert knowledge_store.neighbor_calls == []


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_candidate_support_duplicate_candidate_rows_keep_max_score():
    processor, _, knowledge_store, _ = make_harness()
    reversed_processor, _, reversed_knowledge_store, _ = make_harness()

    scores = await processor._collect_candidate_support_scores(
        [(101, 0.7, 1), (101, 0.9, 2)],
        {
            1: "First mention of the candidate.",
            2: "Second mention of the candidate.",
        },
    )
    reversed_scores = await reversed_processor._collect_candidate_support_scores(
        [(101, 0.9, 2), (101, 0.7, 1)],
        {
            1: "First mention of the candidate.",
            2: "Second mention of the candidate.",
        },
    )

    assert scores == {101: pytest.approx(0.9)}
    assert reversed_scores == {101: pytest.approx(0.9)}
    assert knowledge_store.neighbor_calls == []
    assert reversed_knowledge_store.neighbor_calls == []


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_collect_candidate_support_scores_missing_message_text_keeps_base_score():
    processor, _, knowledge_store, embedding = make_harness()

    scores = await processor._collect_candidate_support_scores(
        [(101, 0.8, 99)],
        {1: "Known message text."},
    )

    assert scores == {101: pytest.approx(0.8)}
    assert processor.llm.calls == []
    assert knowledge_store.fact_searches == []
    assert embedding.batch_calls == []


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_candidate_support_fact_search_failure_uses_base_score():
    processor, _, knowledge_store, _ = make_harness()
    knowledge_store.fact_fail_entity_ids.add(101)

    scores = await processor._collect_candidate_support_scores(
        [(101, 0.8, 1)],
        {1: "Alice is working on the memory graph project."},
    )

    assert scores == {101: pytest.approx(0.8)}
    assert len(knowledge_store.fact_searches) == 1
    assert processor.llm.calls == []


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_collect_candidate_support_scores_batches_large_fact_relevance_requests():
    processor, _, knowledge_store, _ = make_harness()
    processor.llm = FakeLLM(relevance=[True] * CANDIDATE_RELEVANCE_LLM_BATCH_SIZE)
    candidate_pairs = [
        (1000 + index, 0.8, 1)
        for index in range(CANDIDATE_RELEVANCE_LLM_BATCH_SIZE + 1)
    ]
    for candidate_id, _, _ in candidate_pairs:
        knowledge_store.relevant_facts_by_entity[candidate_id] = [
            make_fact(candidate_id, f"Candidate {candidate_id} is relevant.")
        ]

    scores = await processor._collect_candidate_support_scores(
        candidate_pairs,
        {1: "The message is related to every candidate fact."},
    )

    assert len(processor.llm.calls) == 2
    assert len(knowledge_store.fact_searches) == CANDIDATE_RELEVANCE_LLM_BATCH_SIZE + 1
    assert scores == {
        candidate_id: pytest.approx(0.85) for candidate_id, _, _ in candidate_pairs
    }


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_collect_candidate_support_scores_embeds_each_unique_message_once():
    processor, _, _, embedding = make_harness()

    scores = await processor._collect_candidate_support_scores(
        [(101, 0.8, 1), (202, 0.7, 1), (303, 0.6, 2)],
        {
            1: "Shared message text.",
            2: "Different message text.",
        },
    )

    assert scores == {
        101: pytest.approx(0.8),
        202: pytest.approx(0.7),
        303: pytest.approx(0.6),
    }
    assert len(embedding.batch_calls) == 1
    assert set(embedding.batch_calls[0]) == {
        "Shared message text.",
        "Different message text.",
    }


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
        "session-1",
    )

    assert result.entity_ids == [1001, 1002]
    assert result.new_ids == {1001, 1002}
    assert result.alias_ids == set()
    assert result.entity_msg_map == {1001: [1], 1002: [1]}

    alice_profile = await entities.get_profile(1001)
    knoggin_profile = await entities.get_profile(1002)
    assert alice_profile.canonical_name == "Alice"
    assert alice_profile.topic == "Identity"
    assert knoggin_profile.canonical_name == "Knoggin"
    assert knoggin_profile.topic == "General"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolve_mentions_reuses_exact_known_alias_candidate():
    processor, entities, _, _ = make_harness()
    await seed_entity(entities, 102, "Robert Chen", aliases=["Bob"])

    result = await processor._resolve_mentions(
        [(1, "Bob", "person", "Identity")],
        MESSAGES,
        "session-1",
    )

    assert result.entity_ids == [102]
    assert result.new_ids == set()
    assert result.alias_ids == set()
    assert result.entity_msg_map == {102: [1]}


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolve_mentions_does_not_auto_reuse_ambiguous_exact_alias():
    processor, entities, _, _ = make_harness()
    await seed_entity(entities, 102, "Robert Chen", aliases=["Bob"])
    await seed_entity(entities, 202, "Bob Smith", aliases=["Bob"])

    result = await processor._resolve_mentions(
        [(1, "Bob", "person", "Identity")],
        {1: "Bob."},
        "session-1",
    )

    assert result.entity_ids == [1001]
    assert result.new_ids == {1001}
    assert result.alias_ids == set()
    assert result.entity_msg_map == {1001: [1]}
    assert {suggestion.candidate_id for suggestion in result.candidate_suggestions} == {
        102,
        202,
    }


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolve_mentions_does_not_reuse_entity_from_readable_project():
    processor, entities, _, _ = make_harness()
    await entities.register_entity(
        102,
        "Robert Chen",
        ["Robert Chen", "Bob"],
        "person",
        "Identity",
        session_id="archived-session",
        project_id="archived-project",
    )

    result = await processor._resolve_mentions(
        [(1, "Bob", "person", "Identity")],
        MESSAGES,
        "active-session",
    )

    assert result.entity_ids == [1001]
    assert result.new_ids == {1001}
    assert result.alias_ids == set()
    assert result.alias_updates == {}
    assert entities.get_cached_profile(102).project_id == "archived-project"
    assert entities.get_cached_profile(1001).project_id == "project-1"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolve_mentions_reused_entity_preserves_multiple_source_messages():
    processor, entities, _, _ = make_harness()
    await seed_entity(entities, 102, "Alice")

    result = await processor._resolve_mentions(
        [
            (1, "Alice", "person", "Identity"),
            (2, "Alice", "person", "Identity"),
        ],
        MESSAGES,
        "session-1",
    )

    assert result.entity_ids == [102]
    assert result.new_ids == set()
    assert result.alias_ids == set()
    assert result.entity_msg_map == {102: [1, 2]}


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolve_mentions_reuses_high_confidence_fuzzy_candidate():
    processor, entities, _, _ = make_harness()
    await seed_entity(entities, 102, "Robert Chen")

    result = await processor._resolve_mentions(
        [(1, "Robert Chn", "person", "Identity")],
        MESSAGES,
        "session-1",
    )

    assert result.entity_ids == [102]
    assert result.new_ids == set()
    assert result.entity_msg_map == {102: [1]}


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolve_mentions_uses_vector_candidate_when_cache_name_does_not_match():
    processor, entities, knowledge_store, embedding = make_harness()
    await seed_entity(entities, 202, "Linear", entity_type="tool", topic="General")
    knowledge_store.vector_results[vector_for(embedding, "project planning app")] = [
        (202, 0.92)
    ]

    result = await processor._resolve_mentions(
        [(2, "project planning app", "tool", "General")],
        MESSAGES,
        "session-1",
    )

    assert result.entity_ids == [202]
    assert result.new_ids == set()
    assert result.entity_msg_map == {202: [2]}
    assert knowledge_store.vector_searches[-1]["visible_project_ids"] == ["project-1"]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolve_mentions_creates_new_entity_when_candidate_below_threshold():
    processor, entities, knowledge_store, embedding = make_harness()
    await seed_entity(entities, 202, "Linear", entity_type="tool", topic="General")
    knowledge_store.vector_results[vector_for(embedding, "work tracker")] = [(202, 0.4)]

    result = await processor._resolve_mentions(
        [(2, "work tracker", "tool", "General")],
        MESSAGES,
        "session-1",
    )

    assert result.entity_ids == [1001]
    assert result.new_ids == {1001}
    assert result.entity_msg_map == {1001: [2]}
    assert await entities.get_profile(202) is not None


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolve_mentions_does_not_auto_reuse_descriptive_vector_candidate():
    processor, entities, knowledge_store, embedding = make_harness()
    await seed_entity(entities, 202, "Linear", entity_type="tool", topic="General")
    knowledge_store.vector_results[vector_for(embedding, "work tracker")] = [
        (202, 0.92)
    ]
    messages = [
        {
            "id": 3,
            "message": "Work tracker.",
            "timestamp": "2026-01-01T00:02:00+00:00",
            "role": "user",
        }
    ]

    result = await processor._resolve_mentions(
        [(3, "work tracker", "tool", "General")],
        messages,
        "session-1",
    )

    assert result.entity_ids == [1001]
    assert result.new_ids == {1001}
    assert result.entity_msg_map == {1001: [3]}
    assert len(result.candidate_suggestions) == 1
    suggestion = result.candidate_suggestions[0]
    assert suggestion.candidate_id == 202
    assert suggestion.base_score == pytest.approx(0.92)
    assert "below_resolution_threshold" not in suggestion.reasons


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
        "session-1",
    )

    assert result.entity_ids == [1001]
    assert result.new_ids == {1001}
    assert result.entity_msg_map == {1001: [1, 2]}
    assert (await entities.get_profile(1001)).canonical_name == "Alice"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolve_mentions_does_not_dedupe_same_name_with_different_type():
    processor, entities, _, _ = make_harness()

    result = await processor._resolve_mentions(
        [
            (1, "Apple", "organization", "General"),
            (2, "apple", "object", "General"),
        ],
        MESSAGES,
        "session-1",
    )

    assert result.entity_ids == [1001, 1002]
    assert result.new_ids == {1001, 1002}
    assert result.entity_msg_map == {1001: [1], 1002: [2]}
    assert (await entities.get_profile(1001)).canonical_name == "Apple"
    assert (await entities.get_profile(1002)).canonical_name == "apple"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolve_mentions_does_not_dedupe_same_name_with_different_topic():
    processor, entities, _, _ = make_harness()

    result = await processor._resolve_mentions(
        [
            (1, "Ada", "person", "Identity"),
            (2, "ada", "person", "General"),
        ],
        MESSAGES,
        "session-1",
    )

    assert result.entity_ids == [1001, 1002]
    assert result.new_ids == {1001, 1002}
    assert result.entity_msg_map == {1001: [1], 1002: [2]}
    assert (await entities.get_profile(1001)).canonical_name == "Ada"
    assert (await entities.get_profile(1002)).canonical_name == "ada"


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
        "session-1",
    )

    assert len(embedding.batch_calls) == 1
    assert set(embedding.batch_calls[0]) == {"Alice", "Linear"}


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolve_mentions_records_alias_updates_for_existing_match():
    processor, entities, knowledge_store, embedding = make_harness()
    await seed_entity(entities, 102, "Robert Chen", aliases=["Bob"])
    knowledge_store.vector_results[vector_for(embedding, "Bobby")] = [(102, 0.92)]

    result = await processor._resolve_mentions(
        [(1, "Bobby", "person", "Identity")],
        MESSAGES,
        "session-1",
    )

    assert result.entity_ids == [102]
    assert result.new_ids == set()
    assert result.alias_ids == {102}
    assert result.alias_updates == {102: ["Bobby"]}
    assert entities.get_known_aliases()["bobby"] == 102


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolve_mentions_batch_neighbor_overlap_does_not_affect_support():
    processor, entities, knowledge_store, embedding = make_harness()
    await seed_entity(entities, 301, "Knoggin", entity_type="project", topic="General")
    await seed_entity(entities, 401, "Ada Lovelace")
    knowledge_store.vector_results[vector_for(embedding, "memory graph project")] = [
        (301, 0.83)
    ]
    knowledge_store.neighbors_by_entity[301] = {401}

    result = await processor._resolve_mentions(
        [
            (1, "Ada Lovelace", "person", "Identity"),
            (1, "memory graph project", "project", "General"),
        ],
        MESSAGES,
        "session-1",
    )

    assert result.entity_ids == [401, 1001]
    assert result.new_ids == {1001}
    assert result.entity_msg_map == {401: [1], 1001: [1]}
    assert (await entities.get_profile(301)).canonical_name == "Knoggin"
    assert knowledge_store.neighbor_calls == []
    assert len(result.candidate_suggestions) == 1
    suggestion = result.candidate_suggestions[0]
    assert suggestion.candidate_id == 301
    assert suggestion.base_score == pytest.approx(0.83)
    assert suggestion.support_score == pytest.approx(0.83)
    assert "advisory_context_support" not in suggestion.reasons


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolve_mentions_fact_relevance_support_does_not_authorize_reuse():
    processor, entities, knowledge_store, embedding = make_harness()
    processor.llm = FakeLLM(relevance=True)
    await seed_entity(entities, 501, "Notion", entity_type="tool", topic="General")
    knowledge_store.vector_results[vector_for(embedding, "workspace notes tool")] = [
        (501, 0.82)
    ]
    knowledge_store.relevant_facts_by_entity[501] = [
        make_fact(501, "Notion is used to organize workspace notes.")
    ]

    result = await processor._resolve_mentions(
        [(2, "workspace notes tool", "tool", "General")],
        MESSAGES,
        "session-1",
    )

    assert result.entity_ids == [1001]
    assert result.new_ids == {1001}
    assert result.entity_msg_map == {1001: [2]}
    assert (await entities.get_profile(501)).canonical_name == "Notion"
    assert len(processor.llm.calls) == 1
    assert len(result.candidate_suggestions) == 1
    suggestion = result.candidate_suggestions[0]
    assert suggestion.msg_id == 2
    assert suggestion.mention == "workspace notes tool"
    assert suggestion.candidate_id == 501
    assert suggestion.candidate_name == "Notion"
    assert suggestion.base_score == pytest.approx(0.82)
    assert suggestion.support_score == pytest.approx(0.87)
    assert suggestion.created_entity_id == 1001
    assert suggestion.reasons == [
        "candidate_rejected",
        "below_resolution_threshold",
        "advisory_context_support",
        "schema_compatible",
    ]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolve_mentions_irrelevant_facts_keep_weak_ambiguous_match_separate():
    processor, entities, knowledge_store, embedding = make_harness()
    processor.llm = FakeLLM(relevance=False)
    await seed_entity(entities, 601, "OpenAI Alice")
    knowledge_store.vector_results[vector_for(embedding, "Design Alice")] = [
        (601, 0.83)
    ]
    knowledge_store.relevant_facts_by_entity[601] = [
        make_fact(601, "OpenAI Alice works on research partnerships.")
    ]
    messages = [
        {
            "id": 3,
            "message": "Design Alice finalized the onboarding mockups.",
            "timestamp": "2026-01-01T00:02:00+00:00",
            "role": "user",
        }
    ]

    result = await processor._resolve_mentions(
        [(3, "Design Alice", "person", "Identity")],
        messages,
        "session-1",
    )

    assert result.entity_ids == [1001]
    assert result.new_ids == {1001}
    assert result.alias_ids == set()
    assert result.entity_msg_map == {1001: [3]}
    assert (await entities.get_profile(601)).canonical_name == "OpenAI Alice"
    assert len(processor.llm.calls) == 1


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolve_mentions_registration_failure_skips_entity_without_crashing():
    processor, _, _, embedding = make_harness()
    embedding.fail_single_prefixes.add("Alice (")

    result = await processor._resolve_mentions(
        [(1, "Alice", "person", "Identity")],
        MESSAGES,
        "session-1",
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
        processor._resolve_mentions(
            [(1, "Alice", "person", "Identity")], MESSAGES, "session-1"
        ),
        processor._resolve_mentions(
            [(2, "Bob", "person", "Identity")], MESSAGES, "session-1"
        ),
    )

    assert first.entity_ids == [1001]
    assert second.entity_ids == [1002]
    assert events == [("start", 1001), ("end", 1001), ("start", 1002), ("end", 1002)]
