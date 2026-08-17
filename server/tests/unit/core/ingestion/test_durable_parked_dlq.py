from types import SimpleNamespace

import pytest

from core.ingestion.recovery.dlq_state import (
    DLQ_STATUS_PARKED,
    DLQ_STATUS_PROCESSING,
    DLQ_STATUS_QUEUED,
    serialize_dlq_entry,
)
from core.ingestion.recovery.replay_job import DLQReplayJob
from infrastructure.job.base import JobContext
from infrastructure.redis_client import RedisKeys
from tests.fixtures.fakes import FakeKnowledgeStore, FakeRedis


def _job(redis, knowledge_store):
    job = object.__new__(DLQReplayJob)
    job.redis = redis
    job.processor = SimpleNamespace(knowledge_store=knowledge_store)
    return job


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_parked_item_can_be_requeued_after_redis_state_is_lost():
    redis = FakeRedis()
    knowledge_store = FakeKnowledgeStore()
    job = _job(redis, knowledge_store)
    ctx = JobContext(user_name="ada", project_id="project-1")
    entry = {
        "dlq_id": "dlq-1",
        "session_id": "session-1",
        "stage": "graph_write",
        "attempt": 2,
        "error": "TimeoutError",
    }
    raw_item = serialize_dlq_entry(entry)
    processing_key = RedisKeys.dlq_processing("ada", "project-1")
    state_key = RedisKeys.dlq_state("ada", "project-1")
    await redis.rpush(processing_key, raw_item)
    await redis.hset(state_key, "dlq-1", DLQ_STATUS_PROCESSING)

    assert await job._park_claimed(ctx, raw_item, entry, "dlq-1")
    assert knowledge_store.parked_dlq_items[("ada", "project-1", "dlq-1")][
        "status"
    ] == DLQ_STATUS_PARKED

    # Simulate an eviction or restart: only the durable parked record remains.
    redis.hashes.clear()
    redis.lists.clear()

    assert await job.requeue_parked_dlq_item("ada", "project-1", "dlq-1")
    assert await redis.hget(state_key, "dlq-1") == DLQ_STATUS_QUEUED
    assert len(await redis.lrange(RedisKeys.dlq("ada", "project-1"), 0, -1)) == 1
    assert knowledge_store.parked_dlq_items[("ada", "project-1", "dlq-1")][
        "status"
    ] == "requeued"
