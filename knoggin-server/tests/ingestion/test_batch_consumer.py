import json

import pytest

from common.schema.contracts import BatchResult
from infrastructure.redis_client import RedisKeys
from knoggin_server.ingestion.services.batch_consumer import BatchConsumer
from tests.fixtures.fakes import FakeGraphClient, FakeRedis


class FakeProcessor:
    def __init__(self, result=None):
        self.entities = type("Entities", (), {"project_id": "project-1"})()
        self.result = result or BatchResult()
        self.run_calls = []
        self.dlq_calls = []

    async def run(self, messages, session_text):
        self.run_calls.append((messages, session_text))
        return self.result

    async def move_to_dead_letter(self, messages, error, **kwargs):
        self.dlq_calls.append((messages, error, kwargs))
        return True


async def empty_context(window, up_to_msg_id=None):
    return []


async def successful_write_to_graph(result):
    return True, None


def make_consumer(redis=None, processor=None, graph_client=None):
    redis = redis or FakeRedis()
    processor = processor or FakeProcessor()
    graph_client = graph_client or FakeGraphClient()
    consumer = BatchConsumer(
        user_name="ada",
        session_id="session-1",
        graph_client=graph_client,
        processor=processor,
        redis=redis,
        get_session_context=empty_context,
        write_to_graph=successful_write_to_graph,
        batch_size=8,
        checkpoint_interval=4,
    )
    return consumer, redis, processor, graph_client


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_consumer_skips_corrupt_entries_and_processes_valid_messages():
    consumer, redis, processor, graph_client = make_consumer()
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
        )
    ]
    assert graph_client.saved_message_logs == [
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
    assert await redis.get(consumer._checkpoint_key) == "1"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_consumer_trims_invalid_only_buffer_without_stalling():
    consumer, redis, processor, graph_client = make_consumer()
    await redis.rpush(consumer._buffer_key, "not-json")
    await redis.rpush(consumer._buffer_key, json.dumps({"id": 1}))

    await consumer._drain_buffer(flush_partial=True)

    assert await redis.llen(consumer._buffer_key) == 0
    assert processor.run_calls == []
    assert graph_client.saved_message_logs == []


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_consumer_dlqs_message_log_failures_and_drains_processed_batch():
    class FailingGraphClient(FakeGraphClient):
        async def save_message_logs(self, messages):
            raise RuntimeError("graph down")

    consumer, redis, processor, _ = make_consumer(graph_client=FailingGraphClient())
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
