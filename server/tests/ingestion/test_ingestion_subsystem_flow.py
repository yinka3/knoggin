import json

import pytest

from common.exceptions import LLMProviderError
from common.schema.contracts import (
    ConnectionMention,
    ConnectionsResult,
    NERMention,
    NERResult,
    UserConnectionMention,
)
from common.schema.settings import IngestionSettings, TextProcessorSettings
from core.ingestion.services.batch_consumer import IngestionWorker
from core.ingestion.services.pipeline_service import IngestionPipeline
from core.ingestion.services.processor import TextProcessor
from core.knowledge.entity.resolver import EntityResolver
from infrastructure.redis_client import RedisKeys
from tests.fixtures.factories import make_topic_config
from tests.fixtures.fakes import FakeRedis

BASE_MESSAGE = {
    "id": 1,
    "message": "Alice is working with Bob on the Knoggin project.",
    "timestamp": "2026-01-01T00:00:00+00:00",
    "role": "user",
}


class FakeSpan:
    def __init__(self, text):
        self.text = text


class FakeDoc:
    def __init__(self, text):
        self.text = text

    def __getitem__(self, item):
        if isinstance(item, slice) and isinstance(item.start, str):
            return FakeSpan(item.start)
        return FakeSpan("")


class FakeNLP:
    vocab = object()

    def __call__(self, text):
        return FakeDoc(text)

    def make_doc(self, text):
        return FakeDoc(text)


class FakeMatcher:
    def __init__(self, matches_by_text):
        self.matches_by_text = matches_by_text

    def __call__(self, doc):
        return [
            ("KNOWN", span_text, None)
            for span_text in self.matches_by_text.get(doc.text, [])
        ]


class FakeEmbeddingService:
    def __init__(self):
        self.batch_calls = []
        self.single_calls = []

    async def encode(self, texts):
        self.batch_calls.append(list(texts))
        return [self._vector_for(text) for text in texts]

    async def encode_single(self, text):
        self.single_calls.append(text)
        return self._vector_for(text)

    def _vector_for(self, text):
        total = sum(ord(ch) for ch in text)
        return [float(total % 97), float(len(text)), float(total % 13)]


class IntegratedKnowledgeStore:
    def __init__(self, events=None):
        self.events = events if events is not None else []
        self.saved_message_logs = []
        self.vector_results = {}
        self.neighbors_by_entity = {}
        self.relevant_facts_by_entity = {}

    async def save_message_logs(self, messages):
        self.events.append("save_message_logs")
        self.saved_message_logs.append(messages)
        return True

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
        return list(self.vector_results.get(tuple(vector), []))

    async def get_neighbor_ids_batch(
        self, candidate_ids, *, visible_project_ids
    ):
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
        return list(self.relevant_facts_by_entity.get(entity_id, []))


class RoutedLLM:
    extraction_model = "fake-routed-llm"

    def __init__(
        self,
        *,
        ner_result=None,
        connections_result=None,
        raise_for=None,
    ):
        self.ner_result = ner_result if ner_result is not None else NERResult()
        self.connections_result = (
            connections_result
            if connections_result is not None
            else ConnectionsResult()
        )
        self.raise_for = set(raise_for or [])
        self.calls = []

    async def generate_structured(self, **kwargs):
        self.calls.append(kwargs)
        response_model = kwargs["response_model"]
        if response_model in self.raise_for:
            raise LLMProviderError(f"fake {response_model.__name__} failure")
        if response_model is NERResult:
            return self.ner_result
        if response_model is ConnectionsResult:
            return self.connections_result
        raise AssertionError(f"Unexpected response model: {response_model}")


class GraphWriteRecorder:
    def __init__(self, events=None, *, response=(True, None)):
        self.events = events if events is not None else []
        self.response = response
        self.calls = []

    async def __call__(self, result):
        self.events.append("write_to_graph")
        self.calls.append(result)
        return self.response


async def empty_context(window, up_to_msg_id=None):
    return []


def make_entity(name, *, msg_id="m1", typ="project", topic="General", confidence=0.95):
    return NERMention(
        msg_id=msg_id,
        name=name,
        type=typ,
        topic=topic,
        confidence=confidence,
    )


def make_connections(*, relationship_entity_b="Bob"):
    return ConnectionsResult(
        connections=[
            ConnectionMention(
                msg_id="m1",
                entity_a="Alice",
                entity_b=relationship_entity_b,
                relationship="works_with",
                confidence=0.91,
                context="Alice is working with Bob.",
            )
        ],
        user_connections=[
            UserConnectionMention(
                msg_id="m1",
                entity_name="Knoggin",
                relationship="works_on",
                confidence=0.88,
                context="ada works on Knoggin.",
            )
        ],
    )


async def push_messages(redis, key, *messages):
    for message in messages:
        await redis.rpush(key, json.dumps(message))


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


async def make_harness(
    *,
    message=None,
    known_matches=None,
    gliner_matches=None,
    ner_result=None,
    connections_result=None,
    raise_for_llm=None,
    write_response=(True, None),
    llm_ner=False,
):
    message = message or BASE_MESSAGE
    redis = FakeRedis()
    events = []
    knowledge_store = IntegratedKnowledgeStore(events)
    embedding = FakeEmbeddingService()
    entities = EntityResolver(
        knowledge_store=knowledge_store,
        embedding_service=embedding,
        project_id="project-1",
        readable_project_ids=["project-1"],
    )
    llm = RoutedLLM(
        ner_result=ner_result,
        connections_result=connections_result,
        raise_for=raise_for_llm,
    )
    text_processor = TextProcessor(
        llm=llm,
        topic_config=make_topic_config(),
        get_known_aliases=entities.get_known_aliases,
        get_alias_version=entities.get_alias_version,
        get_profile=entities.get_profile,
        gliner=object(),
        spacy=FakeNLP(),
        settings=TextProcessorSettings(llm_ner=llm_ner),
    )
    text_processor._build_phrase_matcher = lambda: (
        FakeMatcher(known_matches or {}),
        entities.get_known_aliases(),
    )
    text_processor.run_gliner = lambda text: list((gliner_matches or {}).get(text, []))

    next_ids = iter(range(1001, 1100))

    async def get_next_ent_id():
        return next(next_ids)

    batch_processor = IngestionPipeline(
        project_id="project-1",
        redis_client=redis,
        llm=llm,
        entities=entities,
        processor=text_processor,
        cpu_executor=None,
        user_name="ada",
        topic_config=make_topic_config(),
        get_next_ent_id=get_next_ent_id,
        knowledge_store=knowledge_store,
    )
    batch_processor.dlq_calls = []

    async def record_dead_letter(messages, error, **kwargs):
        batch_processor.dlq_calls.append((messages, error, kwargs))
        return True

    batch_processor.move_to_dead_letter = record_dead_letter
    write_to_graph = GraphWriteRecorder(events, response=write_response)
    consumer = IngestionWorker(
        user_name="ada",
        session_id="session-1",
        knowledge_store=knowledge_store,
        processor=batch_processor,
        redis=redis,
        get_session_context=empty_context,
        write_to_graph=write_to_graph,
        settings=IngestionSettings(checkpoint_interval=4),
    )
    return consumer, redis, batch_processor, knowledge_store, write_to_graph, entities


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_ingestion_subsystem_happy_path_drains_buffer_to_graph_write():
    ner_result = NERResult(
        mentions=[make_entity("Linear", msg_id="m1", typ="tool", topic="General")]
    )
    consumer, redis, processor, knowledge_store, write_to_graph, entities = (
        await make_harness(
            known_matches={BASE_MESSAGE["message"]: ["Bob"]},
            gliner_matches={
                BASE_MESSAGE["message"]: [("Alice", "person"), ("Knoggin", "project")]
            },
            ner_result=ner_result,
            connections_result=make_connections(),
            llm_ner=True,
        )
    )
    await seed_entity(entities, 102, "Robert Chen", aliases=["Bob"])
    await push_messages(redis, consumer._buffer_key, BASE_MESSAGE)

    await consumer._drain_buffer(flush_partial=True)

    assert await redis.llen(consumer._buffer_key) == 0
    assert knowledge_store.saved_message_logs[0][0]["id"] == 1
    assert await redis.get(consumer._checkpoint_key) == "1"
    assert len(write_to_graph.calls) == 1
    result = write_to_graph.calls[0]
    assert result.success is True
    assert result.scope.user_name == "ada"
    assert result.scope.session_id == "session-1"
    assert result.scope.project_id == "project-1"
    assert result.trace.message_ids == [1]
    assert result.trace.llm_mentions_accepted == 1
    assert set(result.entity_ids) == {1001, 102, 1002}
    assert result.new_entity_ids == {1001, 1002}
    assert result.relationship_observations[0].message_id == 1
    assert result.relationship_observations[0].entity_a_name == "Alice"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_ingestion_subsystem_alias_resolution_survives_connection_validation():
    consumer, redis, _, _, write_to_graph, entities = await make_harness(
        known_matches={BASE_MESSAGE["message"]: ["Bob"]},
        gliner_matches={BASE_MESSAGE["message"]: [("Alice", "person")]},
        connections_result=make_connections(relationship_entity_b="Bob"),
    )
    await seed_entity(entities, 102, "Robert Chen", aliases=["Bob"])
    await push_messages(redis, consumer._buffer_key, BASE_MESSAGE)

    await consumer._drain_buffer(flush_partial=True)

    result = write_to_graph.calls[0]
    assert (
        result.relationship_observations[0].entity_b_name
        == "Robert Chen"
    )
    assert not any(issue.code == "invalid_entity_name" for issue in result.issues)


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_ingestion_subsystem_no_mentions_saves_logs_and_skips_graph_write():
    consumer, redis, _, knowledge_store, write_to_graph, _ = await make_harness(
        ner_result=NERResult(),
    )
    await push_messages(redis, consumer._buffer_key, BASE_MESSAGE)

    await consumer._drain_buffer(flush_partial=True)

    assert await redis.llen(consumer._buffer_key) == 0
    assert knowledge_store.saved_message_logs[0][0]["id"] == 1
    assert write_to_graph.calls == []
    assert await redis.get(consumer._checkpoint_key) == "1"
    assert await redis.get(RedisKeys.last_processed("ada", "session-1")) == "1"
    assert await redis.get(RedisKeys.project_last_processed("ada", "project-1")) == "1"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_ingestion_subsystem_connection_fallback_succeeds_without_dlq():
    consumer, redis, processor, _, write_to_graph, _ = await make_harness(
        gliner_matches={BASE_MESSAGE["message"]: [("Alice", "person")]},
        raise_for_llm={ConnectionsResult},
    )
    await push_messages(redis, consumer._buffer_key, BASE_MESSAGE)

    await consumer._drain_buffer(flush_partial=True)

    assert processor.dlq_calls == []
    assert len(write_to_graph.calls) == 1
    result = write_to_graph.calls[0]
    assert result.success is True
    assert result.relationship_observations == []
    assert result.trace.fallbacks == [
        {"stage": "connections", "fallback": "empty_connections"},
    ]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_ingestion_subsystem_graph_write_failure_dlqs_complete_batch_result():
    consumer, redis, processor, knowledge_store, _, _ = await make_harness(
        gliner_matches={BASE_MESSAGE["message"]: [("Alice", "person")]},
        connections_result=ConnectionsResult(),
        write_response=(False, "graph failed"),
    )
    await push_messages(redis, consumer._buffer_key, BASE_MESSAGE)

    await consumer._drain_buffer(flush_partial=True)

    assert knowledge_store.events == ["save_message_logs", "write_to_graph"]
    assert await redis.llen(consumer._buffer_key) == 0
    messages, error, kwargs = processor.dlq_calls[0]
    assert messages == [BASE_MESSAGE]
    assert error == "graph failed"
    assert kwargs["stage"] == "graph_write"
    assert kwargs["batch_result"].success is True
    assert kwargs["batch_result"].new_entity_ids == {1001}


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_ingestion_subsystem_processing_failure_goes_to_processing_dlq():
    consumer, redis, processor, knowledge_store, write_to_graph, _ = (
        await make_harness()
    )

    async def failing_extract_mentions(*args, **kwargs):
        raise RuntimeError("extract boom")

    processor.processor.extract_mentions = failing_extract_mentions
    await push_messages(redis, consumer._buffer_key, BASE_MESSAGE)

    await consumer._drain_buffer(flush_partial=True)

    assert knowledge_store.saved_message_logs == []
    assert write_to_graph.calls == []
    assert await redis.llen(consumer._buffer_key) == 0
    messages, error, kwargs = processor.dlq_calls[0]
    assert messages == [BASE_MESSAGE]
    assert error == "extract boom"
    assert kwargs["stage"] == "processing"
