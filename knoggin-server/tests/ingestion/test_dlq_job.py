import json

import pytest

from common.schema.contracts import BatchResult
from infrastructure.job.base import JobContext
from infrastructure.redis_client import RedisKeys
from knoggin_server.ingestion.dlq_state import ensure_dlq_id, serialize_dlq_entry
from knoggin_server.ingestion.jobs.dlq_job import DLQReplayJob
from tests.fixtures.fakes import FakeKnowledgeStore, FakeRedis


class ProcessorWithoutKnowledgeStore:
    knowledge_store = None


class ProcessorWithKnowledgeStore:
    knowledge_store = object()


class RecordingProcessor:
    def __init__(self):
        self.knowledge_store = FakeKnowledgeStore()
        self.run_calls = []

    async def run(self, messages, session_text, *, session_id):
        self.run_calls.append((messages, session_text, session_id))
        result = BatchResult()
        result.set_scope("ada", session_id, "project-1")
        return result


async def successful_write_to_graph(result):
    return True, None


async def failing_write_to_graph(result):
    return False, "TimeoutError"


def dlq_entry(**overrides):
    entry = {
        "error": "TimeoutError",
        "attempt": 1,
        "stage": "processing",
        "messages": [{"id": 1, "message": "hello"}],
        "session_text": "[USER]: hello",
        "user_name": "ada",
        "session_id": "session-1",
        "project_id": "project-1",
    }
    entry.update(overrides)
    return entry


@pytest.mark.ingestion
@pytest.mark.no_network
def test_dlq_job_requires_processor_knowledge_store():
    with pytest.raises(
        ValueError, match="requires a IngestionPipeline with knowledge_store"
    ):
        DLQReplayJob(
            entities=object(),
            processor=ProcessorWithoutKnowledgeStore(),
            write_to_graph=lambda result: (True, None),
            redis_client=FakeRedis(),
        )


@pytest.mark.ingestion
@pytest.mark.no_network
def test_dlq_job_accepts_processor_with_knowledge_store():
    job = DLQReplayJob(
        entities=object(),
        processor=ProcessorWithKnowledgeStore(),
        write_to_graph=lambda result: (True, None),
        redis_client=FakeRedis(),
    )

    assert job.name == "dlq_auto_replay"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_dlq_job_parks_entries_missing_session_id():
    redis = FakeRedis()
    job = DLQReplayJob(
        entities=object(),
        processor=RecordingProcessor(),
        write_to_graph=successful_write_to_graph,
        redis_client=redis,
    )
    ctx = JobContext(user_name="ada", project_id="project-1")
    dlq_key = RedisKeys.dlq("ada", "project-1")
    parked_key = RedisKeys.dlq_parked("ada", "project-1")

    await redis.rpush(
        dlq_key,
        json.dumps(
            {
                "error": "TimeoutError",
                "attempt": 1,
                "stage": "processing",
                "messages": [{"id": 1, "message": "hello"}],
            }
        ),
    )

    result = await job.execute(ctx)

    assert result.summary == "Processed 1: 0 retried, 1 parked"
    assert await redis.llen(dlq_key) == 0
    assert await redis.llen(parked_key) == 1


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_dlq_processing_replay_uses_entry_session_id():
    redis = FakeRedis()
    processor = RecordingProcessor()
    job = DLQReplayJob(
        entities=object(),
        processor=processor,
        write_to_graph=successful_write_to_graph,
        redis_client=redis,
    )
    ctx = JobContext(user_name="ada", project_id="project-1")
    dlq_key = RedisKeys.dlq("ada", "project-1")
    message = {"id": 1, "message": "hello"}

    await redis.rpush(
        dlq_key,
        json.dumps(
            {
                "error": "TimeoutError",
                "attempt": 1,
                "stage": "processing",
                "messages": [message],
                "session_text": "[USER]: hello",
                "user_name": "ada",
                "session_id": "session-1",
                "project_id": "project-1",
            }
        ),
    )

    result = await job.execute(ctx)

    assert result.summary == "Processed 1: 1 retried, 0 parked"
    assert processor.run_calls == [([message], "[USER]: hello", "session-1")]


@pytest.mark.ingestion
@pytest.mark.no_network
def test_dlq_id_is_stable_for_same_durable_inputs():
    first = dlq_entry(messages=[{"id": 2}, {"id": 1}])
    second = dlq_entry(messages=[{"id": 1}, {"id": 2}], error="different error")

    assert ensure_dlq_id(first) == ensure_dlq_id(second)


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_dlq_success_claims_acknowledges_and_marks_completed():
    redis = FakeRedis()
    processor = RecordingProcessor()
    job = DLQReplayJob(
        entities=object(),
        processor=processor,
        write_to_graph=successful_write_to_graph,
        redis_client=redis,
    )
    ctx = JobContext(user_name="ada", project_id="project-1")
    entry = dlq_entry()
    dlq_id = ensure_dlq_id(entry)
    await redis.hset(RedisKeys.dlq_state("ada", "project-1"), dlq_id, "queued")
    await redis.rpush(RedisKeys.dlq("ada", "project-1"), serialize_dlq_entry(entry))

    result = await job.execute(ctx)

    assert result.summary == "Processed 1: 1 retried, 0 parked"
    assert processor.run_calls == [
        (entry["messages"], entry["session_text"], "session-1")
    ]
    assert await redis.llen(RedisKeys.dlq("ada", "project-1")) == 0
    assert await redis.llen(RedisKeys.dlq_processing("ada", "project-1")) == 0
    assert await redis.hget(RedisKeys.dlq_state("ada", "project-1"), dlq_id) == (
        "completed"
    )
    assert await redis.hget(RedisKeys.dlq_claims("ada", "project-1"), dlq_id) is None


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_dlq_retry_failure_returns_claim_to_active_queue():
    redis = FakeRedis()
    processor = RecordingProcessor()

    async def failed_run(*_args, **_kwargs):
        return BatchResult(success=False, error="boom")

    processor.run = failed_run
    job = DLQReplayJob(
        entities=object(),
        processor=processor,
        write_to_graph=successful_write_to_graph,
        redis_client=redis,
        max_attempts=3,
    )
    ctx = JobContext(user_name="ada", project_id="project-1")
    entry = dlq_entry()
    dlq_id = ensure_dlq_id(entry)
    await redis.hset(RedisKeys.dlq_state("ada", "project-1"), dlq_id, "queued")
    await redis.rpush(RedisKeys.dlq("ada", "project-1"), serialize_dlq_entry(entry))

    result = await job.execute(ctx)

    assert result.summary == "Processed 1: 0 retried, 0 parked"
    assert await redis.hget(RedisKeys.dlq_state("ada", "project-1"), dlq_id) == (
        "queued"
    )
    raw = await redis.lrange(RedisKeys.dlq("ada", "project-1"), 0, -1)
    assert json.loads(raw[0])["attempt"] == 2


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_dlq_duplicate_completed_entry_is_not_replayed():
    redis = FakeRedis()
    processor = RecordingProcessor()
    job = DLQReplayJob(
        entities=object(),
        processor=processor,
        write_to_graph=successful_write_to_graph,
        redis_client=redis,
    )
    ctx = JobContext(user_name="ada", project_id="project-1")
    entry = dlq_entry()
    dlq_id = ensure_dlq_id(entry)
    await redis.hset(RedisKeys.dlq_state("ada", "project-1"), dlq_id, "completed")
    await redis.rpush(RedisKeys.dlq("ada", "project-1"), serialize_dlq_entry(entry))

    result = await job.execute(ctx)

    assert result.summary == "Processed 0: 0 retried, 0 parked"
    assert processor.run_calls == []
    assert await redis.llen(RedisKeys.dlq("ada", "project-1")) == 0
    assert await redis.llen(RedisKeys.dlq_processing("ada", "project-1")) == 0


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_dlq_abandoned_processing_claim_is_requeued_before_work():
    redis = FakeRedis()
    processor = RecordingProcessor()
    job = DLQReplayJob(
        entities=object(),
        processor=processor,
        write_to_graph=successful_write_to_graph,
        redis_client=redis,
    )
    ctx = JobContext(user_name="ada", project_id="project-1")
    entry = dlq_entry()
    dlq_id = ensure_dlq_id(entry)
    await redis.hset(RedisKeys.dlq_state("ada", "project-1"), dlq_id, "processing")
    await redis.rpush(
        RedisKeys.dlq_processing("ada", "project-1"),
        serialize_dlq_entry(entry),
    )

    result = await job.execute(ctx)

    assert result.summary == "Processed 1: 1 retried, 0 parked"
    assert await redis.hget(RedisKeys.dlq_state("ada", "project-1"), dlq_id) == (
        "completed"
    )


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_dlq_parks_duplicate_only_once():
    redis = FakeRedis()
    job = DLQReplayJob(
        entities=object(),
        processor=RecordingProcessor(),
        write_to_graph=successful_write_to_graph,
        redis_client=redis,
        max_attempts=1,
    )
    ctx = JobContext(user_name="ada", project_id="project-1")
    entry = dlq_entry(attempt=1)
    dlq_id = ensure_dlq_id(entry)
    await redis.hset(RedisKeys.dlq_state("ada", "project-1"), dlq_id, "queued")
    await redis.rpush(RedisKeys.dlq("ada", "project-1"), serialize_dlq_entry(entry))
    await redis.rpush(RedisKeys.dlq("ada", "project-1"), serialize_dlq_entry(entry))

    result = await job.execute(ctx)

    assert result.summary == "Processed 1: 0 retried, 1 parked"
    assert await redis.llen(RedisKeys.dlq_parked("ada", "project-1")) == 1
    assert await redis.llen(RedisKeys.dlq("ada", "project-1")) == 0


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_recovery_helpers_are_duplicate_safe():
    redis = FakeRedis()
    job = DLQReplayJob(
        entities=object(),
        processor=RecordingProcessor(),
        write_to_graph=successful_write_to_graph,
        redis_client=redis,
    )

    assert await job.remark_dirty_entities("ada", "project-1", [2, 2, 3]) == 2
    assert await job.remark_dirty_entities("ada", "project-1", [2, 3]) == 0
    assert await redis.smembers(RedisKeys.dirty_entities("ada", "project-1")) == {
        "2",
        "3",
    }

    assert await job.remark_merge_candidates("ada", "project-1", [4, 4]) == 1
    assert await job.remark_merge_candidates("ada", "project-1", [4]) == 0


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_requeue_parked_dlq_item_validates_state_and_moves_to_active():
    redis = FakeRedis()
    job = DLQReplayJob(
        entities=object(),
        processor=RecordingProcessor(),
        write_to_graph=successful_write_to_graph,
        redis_client=redis,
    )
    entry = dlq_entry(attempt=3)
    dlq_id = ensure_dlq_id(entry)
    await redis.rpush(
        RedisKeys.dlq_parked("ada", "project-1"), serialize_dlq_entry(entry)
    )

    assert await job.requeue_parked_dlq_item("ada", "project-1", dlq_id) is False

    await redis.hset(RedisKeys.dlq_state("ada", "project-1"), dlq_id, "parked")
    assert await job.requeue_parked_dlq_item("ada", "project-1", dlq_id) is True
    assert await redis.llen(RedisKeys.dlq_parked("ada", "project-1")) == 0
    assert await redis.llen(RedisKeys.dlq("ada", "project-1")) == 1
    assert await redis.hget(RedisKeys.dlq_state("ada", "project-1"), dlq_id) == (
        "queued"
    )
