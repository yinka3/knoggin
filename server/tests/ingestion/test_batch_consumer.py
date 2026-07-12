import asyncio
import json

import pytest

from common.schema.contracts import BatchResult, CandidateSuggestion
from common.schema.settings import IngestionSettings
from core.ingestion.services.batch_consumer import IngestionWorker
from infrastructure.redis_client import RedisKeys
from tests.fixtures.fakes import FakeKnowledgeStore, FakeRedis


class FakeProcessor:
    def __init__(self, result=None, *, raise_on_run=False, dlq_success=True):
        self.entities = type("Entities", (), {"project_id": "project-1"})()
        self.project_id = "project-1"
        self.result = result or BatchResult()
        self.raise_on_run = raise_on_run
        self.dlq_success = dlq_success
        self.run_calls = []
        self.dlq_calls = []

    async def run(self, messages, session_text, *, session_id):
        self.run_calls.append((messages, session_text, session_id))
        if self.raise_on_run:
            raise RuntimeError("processor boom")
        return self.result

    async def move_to_dead_letter(self, messages, error, **kwargs):
        self.dlq_calls.append((messages, error, kwargs))
        return self.dlq_success


class RecordingContext:
    def __init__(self, turns=None):
        self.turns = turns or []
        self.calls = []

    async def __call__(self, window, up_to_msg_id=None):
        self.calls.append((window, up_to_msg_id))
        return self.turns


class RecordingKnowledgeStore(FakeKnowledgeStore):
    def __init__(
        self,
        events=None,
        *,
        raise_on_save=False,
        raise_on_candidate_suggestions=False,
    ):
        super().__init__()
        self.events = events if events is not None else []
        self.raise_on_save = raise_on_save
        self.raise_on_candidate_suggestions = raise_on_candidate_suggestions

    async def save_message_logs(self, messages):
        self.events.append("save_message_logs")
        if self.raise_on_save:
            raise RuntimeError("message log down")
        return await super().save_message_logs(messages)

    async def save_candidate_suggestions(self, scope, suggestions):
        self.events.append("save_candidate_suggestions")
        if self.raise_on_candidate_suggestions:
            raise RuntimeError("candidate suggestions down")
        return await super().save_candidate_suggestions(scope, suggestions)


class RecordingWriteToGraph:
    def __init__(
        self,
        events=None,
        *,
        response=(True, None),
        raise_error=False,
        sleep_seconds=0,
    ):
        self.events = events if events is not None else []
        self.response = response
        self.raise_error = raise_error
        self.sleep_seconds = sleep_seconds
        self.calls = []

    async def __call__(self, result):
        self.events.append("write_to_graph")
        self.calls.append(result)
        if self.sleep_seconds:
            await asyncio.sleep(self.sleep_seconds)
        if self.raise_error:
            raise RuntimeError("write boom")
        return self.response


async def empty_context(window, up_to_msg_id=None):
    return []


async def successful_write_to_graph(result):
    return True, None


def make_message(msg_id, text=None):
    return {
        "id": msg_id,
        "message": text or f"message {msg_id}",
        "timestamp": f"2026-01-01T00:0{msg_id}:00+00:00",
        "role": "user",
    }


async def push_messages(redis, key, *messages):
    for message in messages:
        await redis.rpush(key, json.dumps(message))


def graph_write_result():
    return BatchResult(new_entity_ids={101})


def suggestion_result(*, graph_writes=False):
    result = BatchResult(
        candidate_suggestions=[
            CandidateSuggestion(
                msg_id=1,
                mention="workspace notes tool",
                mention_type="tool",
                mention_topic="General",
                candidate_id=501,
                candidate_name="Notion",
                base_score=0.82,
                reasons=["candidate_rejected"],
                created_entity_id=1001,
            )
        ]
    )
    if graph_writes:
        result.new_entity_ids.add(1001)
    return result


def failed_result(error="boom"):
    return BatchResult(success=False, error=error)


def make_consumer(
    redis=None,
    processor=None,
    knowledge_store=None,
    get_session_context=None,
    write_to_graph=None,
    batch_size=8,
    batch_debounce_seconds=0.75,
    checkpoint_interval=4,
    batch_timeout=360.0,
    session_window=18,
):
    redis = redis or FakeRedis()
    processor = processor or FakeProcessor()
    knowledge_store = knowledge_store or FakeKnowledgeStore()
    settings = IngestionSettings(
        batch_size=batch_size,
        batch_debounce_seconds=batch_debounce_seconds,
        checkpoint_interval=checkpoint_interval,
        batch_timeout=batch_timeout,
        session_window=session_window,
    )
    consumer = IngestionWorker(
        user_name="ada",
        session_id="session-1",
        knowledge_store=knowledge_store,
        processor=processor,
        redis=redis,
        get_session_context=get_session_context or empty_context,
        write_to_graph=write_to_graph or successful_write_to_graph,
        settings=settings,
    )
    return consumer, redis, processor, knowledge_store


async def wait_for_processor_run(processor, *, timeout=0.5):
    async def wait_until_run():
        while not processor.run_calls:
            await asyncio.sleep(0.001)

    await asyncio.wait_for(wait_until_run(), timeout=timeout)


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_consumer_waits_for_debounce_before_processing_partial_batch():
    consumer, redis, processor, _ = make_consumer(
        batch_size=2,
        batch_debounce_seconds=0.05,
    )
    consumer.start()
    await push_messages(redis, consumer._buffer_key, make_message(1))
    consumer.signal()

    await asyncio.sleep(0.01)
    assert processor.run_calls == []

    await wait_for_processor_run(processor)
    await consumer.stop()

    assert [call[0] for call in processor.run_calls] == [[make_message(1)]]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_consumer_processes_full_batch_without_waiting_for_debounce():
    consumer, redis, processor, _ = make_consumer(
        batch_size=2,
        batch_debounce_seconds=1.0,
    )
    consumer.start()
    await push_messages(redis, consumer._buffer_key, make_message(1), make_message(2))
    consumer.signal()

    await wait_for_processor_run(processor, timeout=0.1)
    await consumer.stop()

    assert [call[0] for call in processor.run_calls] == [
        [make_message(1), make_message(2)]
    ]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_consumer_flush_bypasses_debounce_for_partial_batch():
    consumer, redis, processor, _ = make_consumer(
        batch_size=2,
        batch_debounce_seconds=1.0,
    )
    consumer.start()
    await push_messages(redis, consumer._buffer_key, make_message(1))
    consumer.signal()

    await asyncio.sleep(0.01)
    assert processor.run_calls == []

    await asyncio.wait_for(consumer.flush(), timeout=0.1)
    await consumer.stop()

    assert [call[0] for call in processor.run_calls] == [[make_message(1)]]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_consumer_defers_partial_remainder_until_next_partial_drain():
    consumer, redis, processor, _ = make_consumer(batch_size=2)
    await push_messages(redis, consumer._buffer_key, make_message(1))

    await consumer._drain_buffer(flush_partial=False)

    assert processor.run_calls == []
    assert await redis.llen(consumer._buffer_key) == 1


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_consumer_debounces_partial_remainder_after_full_batch():
    consumer, redis, processor, _ = make_consumer(
        batch_size=2,
        batch_debounce_seconds=0.05,
    )
    consumer.start()
    await push_messages(
        redis,
        consumer._buffer_key,
        make_message(1),
        make_message(2),
        make_message(3),
    )
    consumer.signal()

    await wait_for_processor_run(processor)
    await asyncio.sleep(0.01)
    assert [call[0] for call in processor.run_calls] == [
        [make_message(1), make_message(2)]
    ]

    async def tail_processed():
        while len(processor.run_calls) < 2:
            await asyncio.sleep(0.001)

    await asyncio.wait_for(tail_processed(), timeout=0.5)
    await consumer.stop()

    assert [call[0] for call in processor.run_calls] == [
        [make_message(1), make_message(2)],
        [make_message(3)],
    ]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_consumer_skips_corrupt_entries_and_processes_valid_messages():
    consumer, redis, processor, knowledge_store = make_consumer()
    await redis.rpush(consumer._buffer_key, "not-json")
    await redis.rpush(
        consumer._buffer_key,
        json.dumps(
            {
                "id": 1,
                "message": "hello",
                "timestamp": "2026-01-01T00:00:00+00:00",
                "role": "user",
            }
        ),
    )

    await consumer._drain_buffer(flush_partial=True)

    assert await redis.llen(consumer._buffer_key) == 0
    assert processor.run_calls == [
        (
            [
                {
                    "id": 1,
                    "message": "hello",
                    "timestamp": "2026-01-01T00:00:00+00:00",
                    "role": "user",
                }
            ],
            "",
            "session-1",
        )
    ]
    assert knowledge_store.saved_message_logs == [
        [
            {
                "id": 1,
                "content": "hello",
                "role": "user",
                "user_name": "ada",
                "session_id": "session-1",
                "project_id": "project-1",
                "timestamp": "2026-01-01T00:00:00+00:00",
            }
        ]
    ]
    assert await redis.get(RedisKeys.last_processed("ada", "session-1")) == "1"
    assert await redis.get(RedisKeys.project_last_processed("ada", "project-1")) == "1"
    assert await redis.get(consumer._checkpoint_key) == "1"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_consumer_trims_invalid_only_buffer_without_stalling():
    consumer, redis, processor, knowledge_store = make_consumer()
    await redis.rpush(consumer._buffer_key, "not-json")
    await redis.rpush(consumer._buffer_key, json.dumps({"id": 1}))

    await consumer._drain_buffer(flush_partial=True)

    assert await redis.llen(consumer._buffer_key) == 0
    assert processor.run_calls == []
    assert knowledge_store.saved_message_logs == []


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_consumer_dlqs_message_log_failures_and_drains_processed_batch():
    class FailingKnowledgeStore(FakeKnowledgeStore):
        async def save_message_logs(self, messages):
            raise RuntimeError("graph down")

    consumer, redis, processor, _ = make_consumer(
        knowledge_store=FailingKnowledgeStore()
    )
    await redis.rpush(
        consumer._buffer_key,
        json.dumps({"id": 1, "message": "hello", "timestamp": "ts", "role": "user"}),
    )

    await consumer._drain_buffer(flush_partial=True)

    assert await redis.llen(consumer._buffer_key) == 0
    assert processor.dlq_calls
    messages, error, kwargs = processor.dlq_calls[0]
    assert messages == [
        {"id": 1, "message": "hello", "timestamp": "ts", "role": "user"}
    ]
    assert error.startswith("MESSAGE_LOG_SAVE_FAILED")
    assert kwargs["stage"] == "message_log"
    assert kwargs["session_id"] == "session-1"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_consumer_formats_session_context_and_calls_processor():
    context = RecordingContext(
        [
            {"role_label": "USER", "content": "Earlier user turn."},
            {"role_label": "ASSISTANT", "content": "Earlier assistant turn."},
        ]
    )
    consumer, redis, processor, _ = make_consumer(
        get_session_context=context,
        session_window=7,
    )
    message = make_message(5, "current message")
    await push_messages(redis, consumer._buffer_key, message)

    await consumer._drain_buffer(flush_partial=True)

    assert context.calls == [(7, 5)]
    assert processor.run_calls == [
        (
            [message],
            "[USER]: Earlier user turn.\n[ASSISTANT]: Earlier assistant turn.",
            "session-1",
        )
    ]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_consumer_success_without_graph_writes_skips_write_to_graph():
    write_to_graph = RecordingWriteToGraph()
    consumer, redis, processor, knowledge_store = make_consumer(
        write_to_graph=write_to_graph
    )
    message = make_message(1, "hello")
    await push_messages(redis, consumer._buffer_key, message)

    await consumer._drain_buffer(flush_partial=True)

    assert knowledge_store.saved_message_logs
    assert write_to_graph.calls == []
    assert await redis.get(consumer._checkpoint_key) == "1"
    assert await redis.get(RedisKeys.last_processed("ada", "session-1")) == "1"
    assert await redis.get(RedisKeys.project_last_processed("ada", "project-1")) == "1"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_consumer_success_with_graph_writes_calls_write_to_graph():
    events = []
    write_to_graph = RecordingWriteToGraph(events)
    knowledge_store = RecordingKnowledgeStore(events)
    processor = FakeProcessor(graph_write_result())
    consumer, redis, _, _ = make_consumer(
        processor=processor,
        knowledge_store=knowledge_store,
        write_to_graph=write_to_graph,
    )
    await push_messages(redis, consumer._buffer_key, make_message(1))

    await consumer._drain_buffer(flush_partial=True)

    assert write_to_graph.calls == [processor.result]
    assert events == ["save_message_logs", "write_to_graph"]
    assert await redis.get(consumer._checkpoint_key) == "1"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_consumer_persists_candidate_suggestions_before_graph_write():
    events = []
    write_to_graph = RecordingWriteToGraph(events)
    knowledge_store = RecordingKnowledgeStore(events)
    processor = FakeProcessor(suggestion_result(graph_writes=True))
    consumer, redis, _, _ = make_consumer(
        processor=processor,
        knowledge_store=knowledge_store,
        write_to_graph=write_to_graph,
    )
    await push_messages(redis, consumer._buffer_key, make_message(1))

    await consumer._drain_buffer(flush_partial=True)

    assert events == [
        "save_message_logs",
        "save_candidate_suggestions",
        "write_to_graph",
    ]
    assert write_to_graph.calls == [processor.result]
    scope, suggestions = knowledge_store.saved_candidate_suggestions[0]
    assert scope.user_name == "ada"
    assert scope.project_id == "project-1"
    assert scope.session_id == "session-1"
    assert suggestions == processor.result.candidate_suggestions


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_consumer_candidate_suggestions_do_not_trigger_graph_write():
    write_to_graph = RecordingWriteToGraph()
    processor = FakeProcessor(suggestion_result(graph_writes=False))
    consumer, redis, _, knowledge_store = make_consumer(
        processor=processor,
        write_to_graph=write_to_graph,
    )
    await push_messages(redis, consumer._buffer_key, make_message(1))

    await consumer._drain_buffer(flush_partial=True)

    assert knowledge_store.saved_candidate_suggestions
    assert write_to_graph.calls == []
    assert await redis.get(consumer._checkpoint_key) == "1"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_consumer_candidate_suggestion_failure_does_not_block_graph_write():
    events = []
    knowledge_store = RecordingKnowledgeStore(
        events, raise_on_candidate_suggestions=True
    )
    processor = FakeProcessor(suggestion_result(graph_writes=True))
    write_to_graph = RecordingWriteToGraph(events)
    consumer, redis, _, _ = make_consumer(
        processor=processor,
        knowledge_store=knowledge_store,
        write_to_graph=write_to_graph,
    )
    message = make_message(1)
    await push_messages(redis, consumer._buffer_key, message)

    await consumer._drain_buffer(flush_partial=True)

    assert write_to_graph.calls == [processor.result]
    assert await redis.llen(consumer._buffer_key) == 0
    assert processor.dlq_calls == []
    assert events == [
        "save_message_logs",
        "save_candidate_suggestions",
        "write_to_graph",
    ]
    assert await redis.get(consumer._checkpoint_key) == "1"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_consumer_processor_failure_goes_to_processing_dlq():
    processor = FakeProcessor(failed_result("boom"))
    consumer, redis, _, _ = make_consumer(processor=processor)
    message = make_message(1)
    await push_messages(redis, consumer._buffer_key, message)

    await consumer._drain_buffer(flush_partial=True)

    assert await redis.llen(consumer._buffer_key) == 0
    assert len(processor.dlq_calls) == 1
    messages, error, kwargs = processor.dlq_calls[0]
    assert messages == [message]
    assert error == "boom"
    assert kwargs["stage"] == "processing"
    assert kwargs["session_text"] == ""
    assert kwargs["session_id"] == "session-1"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_consumer_processor_exception_goes_to_processing_dlq():
    processor = FakeProcessor(raise_on_run=True)
    consumer, redis, _, _ = make_consumer(processor=processor)
    await push_messages(redis, consumer._buffer_key, make_message(1))

    await consumer._drain_buffer(flush_partial=True)

    assert await redis.llen(consumer._buffer_key) == 0
    _, error, kwargs = processor.dlq_calls[0]
    assert error.startswith("Fatal exception: processor boom")
    assert kwargs["stage"] == "processing"
    assert kwargs["session_id"] == "session-1"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_consumer_graph_write_failure_goes_to_graph_write_dlq():
    processor = FakeProcessor(graph_write_result())
    write_to_graph = RecordingWriteToGraph(response=(False, "graph failed"))
    consumer, redis, _, _ = make_consumer(
        processor=processor,
        write_to_graph=write_to_graph,
    )
    await push_messages(redis, consumer._buffer_key, make_message(1))

    await consumer._drain_buffer(flush_partial=True)

    assert await redis.llen(consumer._buffer_key) == 0
    _, error, kwargs = processor.dlq_calls[0]
    assert error == "graph failed"
    assert kwargs["stage"] == "graph_write"
    assert kwargs["batch_result"] is processor.result
    assert kwargs["session_id"] == "session-1"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_consumer_graph_write_exception_goes_to_graph_write_dlq():
    processor = FakeProcessor(graph_write_result())
    write_to_graph = RecordingWriteToGraph(raise_error=True)
    consumer, redis, _, _ = make_consumer(
        processor=processor,
        write_to_graph=write_to_graph,
    )
    await push_messages(redis, consumer._buffer_key, make_message(1))

    await consumer._drain_buffer(flush_partial=True)

    _, error, kwargs = processor.dlq_calls[0]
    assert error == "write boom"
    assert kwargs["stage"] == "graph_write"
    assert kwargs["session_id"] == "session-1"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_consumer_graph_write_timeout_goes_to_graph_write_dlq():
    processor = FakeProcessor(graph_write_result())
    write_to_graph = RecordingWriteToGraph(sleep_seconds=0.05)
    consumer, redis, _, _ = make_consumer(
        processor=processor,
        write_to_graph=write_to_graph,
    )
    consumer.batch_timeout = 0.001
    await push_messages(redis, consumer._buffer_key, make_message(1))

    await consumer._drain_buffer(flush_partial=True)

    _, error, kwargs = processor.dlq_calls[0]
    assert error == "GRAPH_WRITE_TIMEOUT"
    assert kwargs["stage"] == "graph_write"
    assert kwargs["session_id"] == "session-1"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_consumer_dlq_failure_leaves_buffer_for_retry():
    processor = FakeProcessor(failed_result("boom"), dlq_success=False)
    consumer, redis, _, _ = make_consumer(processor=processor)
    message = make_message(1)
    await push_messages(redis, consumer._buffer_key, message)

    await consumer._drain_buffer(flush_partial=True)

    assert await redis.llen(consumer._buffer_key) == 1
    assert await redis.lrange(consumer._buffer_key, 0, -1) == [json.dumps(message)]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_consumer_message_log_dlq_failure_leaves_buffer_for_retry():
    processor = FakeProcessor(dlq_success=False)
    knowledge_store = RecordingKnowledgeStore(raise_on_save=True)
    consumer, redis, _, _ = make_consumer(
        processor=processor,
        knowledge_store=knowledge_store,
    )
    message = make_message(1)
    await push_messages(redis, consumer._buffer_key, message)

    await consumer._drain_buffer(flush_partial=True)

    assert await redis.llen(consumer._buffer_key) == 1
    assert processor.dlq_calls[0][2]["stage"] == "message_log"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_consumer_checkpoint_resets_when_interval_reached():
    consumer, redis, _, _ = make_consumer(checkpoint_interval=2)
    await push_messages(redis, consumer._buffer_key, make_message(1), make_message(2))

    await consumer._drain_buffer(flush_partial=True)

    assert await redis.get(consumer._checkpoint_key) == "0"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_consumer_sets_last_processed_to_max_message_id():
    consumer, redis, _, _ = make_consumer()
    await push_messages(redis, consumer._buffer_key, make_message(3), make_message(2))

    await consumer._drain_buffer(flush_partial=True)

    assert await redis.get(RedisKeys.last_processed("ada", "session-1")) == "3"
    assert await redis.get(RedisKeys.project_last_processed("ada", "project-1")) == "3"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_consumer_processes_multiple_batches_in_one_drain():
    consumer, redis, processor, _ = make_consumer(batch_size=2)
    await push_messages(
        redis,
        consumer._buffer_key,
        make_message(1),
        make_message(2),
        make_message(3),
    )

    await consumer._drain_buffer(flush_partial=True)

    assert await redis.llen(consumer._buffer_key) == 0
    assert [call[0] for call in processor.run_calls] == [
        [make_message(1), make_message(2)],
        [make_message(3)],
    ]
    assert [call[2] for call in processor.run_calls] == ["session-1", "session-1"]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_consumer_invalid_only_buffer_does_not_call_context_or_processor():
    context = RecordingContext()
    consumer, redis, processor, knowledge_store = make_consumer(
        get_session_context=context
    )
    await redis.rpush(consumer._buffer_key, "not-json")
    await redis.rpush(consumer._buffer_key, json.dumps({"id": 1}))

    await consumer._drain_buffer(flush_partial=True)

    assert await redis.llen(consumer._buffer_key) == 0
    assert context.calls == []
    assert processor.run_calls == []
    assert knowledge_store.saved_message_logs == []
