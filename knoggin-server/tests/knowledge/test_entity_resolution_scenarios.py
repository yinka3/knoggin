import pytest

from common.conf.topics_config import TopicConfig
from common.schema.contracts import BulkRelevanceResult, RelevanceResult
from common.schema.primitives import FactRecord
from common.schema.settings import EntityResolutionSettings, TopicSchema
from knoggin_server.ingestion.services.pipeline_service import BatchProcessor
from knoggin_server.knowledge.services.entity_service import EntityManager
from tests.fixtures.factories import make_topic_config


class FakeEmbeddingService:
    def __init__(self):
        self.batch_calls = []
        self.single_calls = []

    async def encode(self, texts):
        self.batch_calls.append(list(texts))
        return [self.vector_for(text) for text in texts]

    async def encode_single(self, text):
        self.single_calls.append(text)
        return self.vector_for(text)

    def vector_for(self, text):
        total = sum(ord(ch) for ch in text)
        return [float(total % 97), float(len(text)), float(total % 13)]


class FakeScenarioGraph:
    def __init__(self):
        self.entities = {}
        self.vector_results = {}
        self.relevant_facts_by_entity = {}
        self.neighbors_by_entity = {}
        self.vector_searches = []
        self.fact_searches = []
        self.neighbor_calls = []
        self.direct_edges = set()
        self.hierarchy_edges = set()

    def add_entity(
        self,
        entity_id,
        canonical_name,
        *,
        aliases=None,
        entity_type="person",
        topic="Identity",
        project_id="project-1",
        embedding=None,
    ):
        entity = {
            "id": entity_id,
            "canonical_name": canonical_name,
            "aliases": list(aliases or []),
            "type": entity_type,
            "topic": topic,
            "project_id": project_id,
            "embedding": embedding,
        }
        self.entities[entity_id] = entity
        return entity

    async def get_entities_by_names(self, names, visible_project_ids=None):
        wanted = {name.lower() for name in names}
        found = []
        for entity in self.entities.values():
            if not self._is_visible(entity, visible_project_ids):
                continue
            names_for_entity = {
                entity.get("canonical_name", "").lower(),
                *(alias.lower() for alias in entity.get("aliases") or []),
            }
            if wanted & names_for_entity:
                found.append(dict(entity))
        return found

    async def get_entity_by_id(self, entity_id, visible_project_ids=None):
        entity = self.entities.get(entity_id)
        if not entity or not self._is_visible(entity, visible_project_ids):
            return None
        return dict(entity)

    async def get_entity_embedding(self, entity_id):
        entity = self.entities.get(entity_id)
        return list(entity.get("embedding") or []) if entity else []

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
        visible_results = []
        for entity_id, score in self.vector_results.get(tuple(vector), []):
            entity = self.entities.get(entity_id)
            if entity and not self._is_visible(entity, visible_project_ids):
                continue
            visible_results.append((entity_id, score))
        return visible_results[:limit]

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
        return list(self.relevant_facts_by_entity.get(entity_id, []))

    async def has_direct_edge(self, id_a, id_b):
        return tuple(sorted((id_a, id_b))) in self.direct_edges

    async def has_hierarchy_edge(self, id_a, id_b):
        return tuple(sorted((id_a, id_b))) in self.hierarchy_edges

    async def get_neighbor_ids(self, entity_id):
        return set(self.neighbors_by_entity.get(entity_id, set()))

    def _is_visible(self, entity, visible_project_ids):
        if visible_project_ids is None:
            return True
        return entity.get("project_id") in visible_project_ids


class FakeLLM:
    extraction_model = "fake-llm"

    def __init__(self, relevance=True):
        self.relevance = relevance
        self.calls = []

    async def generate_structured(self, **kwargs):
        self.calls.append(kwargs)
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


def make_harness(*, llm=None, topic_config=None):
    embedding = FakeEmbeddingService()
    graph = FakeScenarioGraph()
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
        project_id="project-1",
        redis_client=None,
        llm=llm or FakeLLM(),
        entities=entities,
        processor=None,
        cpu_executor=None,
        user_name="ada",
        topic_config=topic_config or make_topic_config(),
        get_next_ent_id=get_next_ent_id,
    )
    return processor, entities, graph, embedding


def make_schema_topic_config():
    return TopicConfig(
        {
            **make_topic_config().raw,
            "Tools": TopicSchema(
                active=True,
                labels=["tool", "product"],
                hierarchy={},
                aliases=["apps"],
            ),
            "Concepts": TopicSchema(
                active=True,
                labels=["concept"],
                hierarchy={},
                aliases=["ideas"],
            ),
            "Objects": TopicSchema(
                active=True,
                labels=["object"],
                hierarchy={},
                aliases=["things"],
            ),
        }
    )


async def seed_entity(
    entities,
    graph,
    entity_id,
    canonical_name,
    *,
    aliases=None,
    entity_type="person",
    topic="Identity",
    project_id="project-1",
):
    graph.add_entity(
        entity_id,
        canonical_name,
        aliases=aliases,
        entity_type=entity_type,
        topic=topic,
        project_id=project_id,
    )
    await entities.register_entity(
        entity_id,
        canonical_name,
        [canonical_name, *(aliases or [])],
        entity_type,
        topic,
        session_id="seed-session",
        project_id=project_id,
    )


def make_message(message_id, text):
    return {
        "id": message_id,
        "message": text,
        "timestamp": "2026-01-01T00:00:00+00:00",
        "role": "user",
    }


def vector_for(embedding, text):
    return tuple(embedding.vector_for(text))


def make_fact(entity_id, content):
    return FactRecord(
        id=f"fact-{entity_id}",
        content=content,
        source_entity_id=entity_id,
        source_msg_id=1,
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_alice_openai_and_design_alice_stay_separate_with_irrelevant_facts():
    processor, entities, graph, embedding = make_harness(llm=FakeLLM(relevance=False))
    await seed_entity(entities, graph, 601, "OpenAI Alice")
    graph.vector_results[vector_for(embedding, "Design Alice")] = [(601, 0.83)]
    graph.relevant_facts_by_entity[601] = [
        make_fact(601, "OpenAI Alice works on research model evaluations.")
    ]
    messages = [
        make_message(3, "Design Alice approved the onboarding mockups.")
    ]

    result = await processor._resolve_mentions(
        [(3, "Design Alice", "person", "Identity")],
        messages,
        "session-1",
    )

    assert result.entity_ids == [1001]
    assert result.new_ids == {1001}
    assert result.alias_ids == set()
    assert (await entities.get_profile(601))["canonical_name"] == "OpenAI Alice"


@pytest.mark.storage
@pytest.mark.no_network
async def test_bobby_chen_reuses_robert_chen_when_context_supports_nickname_drift():
    processor, entities, graph, embedding = make_harness(llm=FakeLLM(relevance=True))
    await seed_entity(entities, graph, 102, "Robert Chen", aliases=["Bob"])
    graph.vector_results[vector_for(embedding, "Bobby Chen")] = [(102, 0.82)]
    graph.relevant_facts_by_entity[102] = [
        make_fact(102, "Robert Chen leads backend ingestion work.")
    ]
    messages = [
        make_message(4, "Bobby Chen fixed the backend ingestion retry path.")
    ]

    result = await processor._resolve_mentions(
        [(4, "Bobby Chen", "person", "Identity")],
        messages,
        "session-1",
    )

    assert result.entity_ids == [102]
    assert result.new_ids == set()
    assert result.alias_ids == {102}
    assert result.alias_updates == {102: ["Bobby Chen"]}
    assert entities.get_known_aliases()["bobby chen"] == 102


@pytest.mark.storage
@pytest.mark.no_network
async def test_sparse_bob_does_not_reuse_known_alias_without_supporting_context():
    processor, entities, graph, _ = make_harness(llm=FakeLLM(relevance=False))
    await seed_entity(entities, graph, 102, "Robert Chen", aliases=["Bob"])
    messages = [make_message(5, "Bob said yes.")]

    result = await processor._resolve_mentions(
        [(5, "Bob", "person", "Identity")],
        messages,
        "session-1",
    )

    assert result.entity_ids == [1001]
    assert result.new_ids == {1001}


@pytest.mark.storage
@pytest.mark.no_network
async def test_descriptive_knoggin_mentions_reuse_project_with_supporting_context():
    processor, entities, graph, embedding = make_harness(llm=FakeLLM(relevance=True))
    await seed_entity(
        entities,
        graph,
        301,
        "Knoggin",
        aliases=["memory project"],
        entity_type="project",
        topic="General",
    )
    graph.vector_results[vector_for(embedding, "the memory project")] = [(301, 0.82)]
    graph.vector_results[vector_for(embedding, "that graph thing")] = [(301, 0.82)]
    graph.relevant_facts_by_entity[301] = [
        make_fact(301, "Knoggin is a personal memory graph project.")
    ]
    messages = [
        make_message(6, "The memory project needs better merge tests."),
        make_message(7, "That graph thing should remember aliases."),
    ]

    result = await processor._resolve_mentions(
        [
            (6, "the memory project", "project", "General"),
            (7, "that graph thing", "project", "General"),
        ],
        messages,
        "session-1",
    )

    assert result.entity_ids == [301]
    assert result.new_ids == set()
    assert result.entity_msg_map == {301: [6, 7]}
    assert result.alias_updates == {301: ["the memory project", "that graph thing"]}


@pytest.mark.storage
@pytest.mark.no_network
async def test_product_name_positive_context_reuses_linear_tool():
    processor, entities, graph, _ = make_harness()
    await seed_entity(
        entities,
        graph,
        404,
        "Linear",
        entity_type="tool",
        topic="General",
    )
    messages = [make_message(8, "Linear is where we track onboarding.")]

    result = await processor._resolve_mentions(
        [(8, "Linear", "tool", "General")],
        messages,
        "session-1",
    )

    assert result.entity_ids == [404]
    assert result.new_ids == set()


@pytest.mark.storage
@pytest.mark.no_network
async def test_common_word_notion_context_does_not_reuse_product_entity():
    processor, entities, graph, _ = make_harness()
    await seed_entity(
        entities,
        graph,
        501,
        "Notion",
        entity_type="tool",
        topic="General",
    )
    messages = [make_message(9, "The notion was confusing, so I rewrote it.")]

    result = await processor._resolve_mentions(
        [(9, "Notion", "concept", "General")],
        messages,
        "session-1",
    )

    assert result.entity_ids == [1001]
    assert result.new_ids == {1001}


@pytest.mark.storage
@pytest.mark.no_network
async def test_common_word_cursor_context_does_not_reuse_product_entity():
    processor, entities, graph, _ = make_harness()
    await seed_entity(
        entities,
        graph,
        502,
        "Cursor",
        entity_type="tool",
        topic="General",
    )
    messages = [make_message(10, "The cursor moved weirdly in the editor.")]

    result = await processor._resolve_mentions(
        [(10, "Cursor", "object", "General")],
        messages,
        "session-1",
    )

    assert result.entity_ids == [1001]
    assert result.new_ids == {1001}


@pytest.mark.storage
@pytest.mark.no_network
async def test_openai_and_chatgpt_are_related_not_duplicate_entities():
    _, entities, graph, _ = make_harness()
    await seed_entity(
        entities,
        graph,
        701,
        "OpenAI",
        entity_type="organization",
        topic="General",
    )
    await seed_entity(
        entities,
        graph,
        702,
        "ChatGPT",
        entity_type="tool",
        topic="General",
    )
    graph.hierarchy_edges.add((701, 702))

    result = await entities._classify_pair(701, 702, 98, {})

    assert result is None


@pytest.mark.storage
@pytest.mark.no_network
async def test_same_name_in_inaccessible_project_is_not_reused():
    processor, entities, graph, embedding = make_harness()
    graph.add_entity(
        777,
        "Alice",
        entity_type="person",
        topic="Identity",
        project_id="private-project",
    )
    graph.vector_results[vector_for(embedding, "Alice")] = [(777, 0.95)]
    messages = [make_message(11, "Alice from this project reviewed the doc.")]

    result = await processor._resolve_mentions(
        [(11, "Alice", "person", "Identity")],
        messages,
        "session-1",
    )

    assert result.entity_ids == [1001]
    assert result.new_ids == {1001}
    assert graph.vector_searches[-1]["visible_project_ids"] == ["project-1"]
    assert await entities.get_profile(777) is None


@pytest.mark.storage
@pytest.mark.no_network
async def test_sparse_chris_does_not_pick_between_multiple_existing_people():
    processor, entities, graph, _ = make_harness()
    await seed_entity(entities, graph, 801, "Chris Walker")
    await seed_entity(entities, graph, 802, "Chris Lee")
    messages = [make_message(12, "Chris said yes.")]

    result = await processor._resolve_mentions(
        [(12, "Chris", "person", "Identity")],
        messages,
        "session-1",
    )

    assert result.entity_ids == [1001]
    assert result.new_ids == {1001}


@pytest.mark.storage
@pytest.mark.no_network
async def test_ibm_and_international_business_machines_reuse_one_entity():
    processor, entities, graph, _ = make_harness()
    await seed_entity(
        entities,
        graph,
        901,
        "International Business Machines",
        aliases=["IBM"],
        entity_type="organization",
        topic="General",
    )
    messages = [
        make_message(13, "IBM announced a research update."),
        make_message(14, "International Business Machines works in enterprise AI."),
    ]

    result = await processor._resolve_mentions(
        [
            (13, "IBM", "organization", "General"),
            (14, "International Business Machines", "organization", "General"),
        ],
        messages,
        "session-1",
    )

    assert result.entity_ids == [901]
    assert result.new_ids == set()
    assert result.entity_msg_map == {901: [13, 14]}


@pytest.mark.storage
@pytest.mark.no_network
async def test_schema_labels_under_same_topic_are_compatible_for_reuse():
    processor, entities, graph, _ = make_harness(
        topic_config=make_schema_topic_config()
    )
    await seed_entity(
        entities,
        graph,
        100,
        "Linear",
        entity_type="tool",
        topic="Tools",
    )
    messages = [make_message(15, "Linear is where we track onboarding work.")]

    result = await processor._resolve_mentions(
        [(15, "Linear", "product", "Tools")],
        messages,
        "session-1",
    )

    assert result.entity_ids == [100]
    assert result.new_ids == set()


@pytest.mark.storage
@pytest.mark.no_network
async def test_topic_aliases_normalize_for_schema_compatibility():
    processor, entities, graph, _ = make_harness(
        topic_config=make_schema_topic_config()
    )
    await seed_entity(
        entities,
        graph,
        101,
        "Linear",
        entity_type="tool",
        topic="Tools",
    )
    messages = [make_message(16, "Linear is where we track onboarding work.")]

    result = await processor._resolve_mentions(
        [(16, "Linear", "product", "apps")],
        messages,
        "session-1",
    )

    assert result.entity_ids == [101]
    assert result.new_ids == set()


@pytest.mark.storage
@pytest.mark.no_network
async def test_known_labels_from_different_topics_block_auto_reuse():
    processor, entities, graph, _ = make_harness(
        topic_config=make_schema_topic_config()
    )
    await seed_entity(
        entities,
        graph,
        102,
        "Notion",
        entity_type="tool",
        topic="Tools",
    )
    messages = [make_message(17, "The notion was confusing in the draft.")]

    result = await processor._resolve_mentions(
        [(17, "Notion", "concept", "Concepts")],
        messages,
        "session-1",
    )

    assert result.entity_ids == [1001]
    assert result.new_ids == {1001}


@pytest.mark.storage
@pytest.mark.no_network
async def test_unknown_labels_are_neutral_for_schema_compatibility():
    processor, _, _, _ = make_harness(topic_config=make_schema_topic_config())

    compatibility = processor._is_schema_compatible(
        "unknown-label",
        "General",
        {"canonical_name": "Linear", "type": "tool", "topic": "General"},
    )

    assert compatibility == "neutral"


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_resolution_settings_update_caution_gate_knobs():
    processor, _, _, _ = make_harness()

    processor.update_settings(
        EntityResolutionSettings(
            resolution_threshold=0.72,
            common_word_frequency_threshold=2e-5,
            context_support_epsilon=2e-6,
            sparse_context_verbs=["Answered", "PONGED"],
        )
    )

    assert processor.resolution_threshold == 0.72
    assert processor.common_word_frequency_threshold == 2e-5
    assert processor.context_support_epsilon == 2e-6
    assert processor.sparse_context_verbs == {"answered", "ponged"}
