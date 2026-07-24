import json

import pytest

from common.schema.contracts import BatchResult
from common.schema.settings import DLQSettings
from common.utils.time_utils import frozen_time
from core.ingestion.dlq_state import ensure_dlq_id, serialize_dlq_entry
from core.ingestion.jobs.dlq_job import DLQReplayJob
from infrastructure.job.base import JobContext
from infrastructure.redis_client import RedisKeys
from tests.fixtures.fakes import FakeKnowledgeStore, FakeRedis


class ProcessorWithoutKnowledgeStore:
    knowledge_store = None


class ProcessorWithKnowledgeStore:
    knowledge_store = object()


class EntityCache:
    def __init__(self, valid_ids=None):
        self.valid_ids = set(valid_ids or [])

    def has_cached_entity(self, entity_id):
        return entity_id in self.valid_ids


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


def make_job(
    *,
    redis=None,
    entities=None,
    processor=None,
    write_to_graph=successful_write_to_graph,
    interval_seconds=60,
    batch_size=50,
    max_attempts=2,
):
    return DLQReplayJob(
        entities=entities or object(),
        processor=processor or RecordingProcessor(),
        write_to_graph=write_to_graph,
        redis_client=redis or FakeRedis(),
        settings=DLQSettings(
            interval_seconds=interval_seconds,
            batch_size=batch_size,
            max_attempts=max_attempts,
        ),
    )


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
            settings=DLQSettings(),
        )


@pytest.mark.ingestion
@pytest.mark.no_network
def test_dlq_job_accepts_processor_with_knowledge_store():
    job = make_job(processor=ProcessorWithKnowledgeStore())

    assert job.name == "dlq_auto_replay"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_dlq_job_parks_entries_missing_session_id():
    redis = FakeRedis()
    job = make_job(redis=redis)
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
    job = make_job(redis=redis, processor=processor)
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
    job = make_job(redis=redis, processor=processor)
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
async def test_dlq_prunes_only_expired_completed_dedup_markers():
    redis = FakeRedis()
    job = make_job(redis=redis)
    ctx = JobContext(user_name="ada", project_id="project-1")
    dlq_id = "completed-item"

    with frozen_time("2026-01-01T10:00:00+00:00") as clock:
        await job._ack_completed(ctx, "raw-item", dlq_id)
        assert await redis.hget(RedisKeys.dlq_state("ada", "project-1"), dlq_id) == (
            "completed"
        )

        clock.advance(hours=25)
        result = await job.execute(ctx)

    assert result.summary == "DLQ empty; pruned 1 completed markers"
    assert await redis.hget(RedisKeys.dlq_state("ada", "project-1"), dlq_id) is None
    assert await redis.zrange(
        RedisKeys.dlq_completed("ada", "project-1"), 0, -1
    ) == []


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_dlq_graph_write_retry_preserves_callback_error(monkeypatch):
    redis = FakeRedis()
    emitted = []

    async def fake_emit(*args, **kwargs):
        emitted.append((args, kwargs))

    async def graph_write_fails(result):
        return False, "database unavailable"

    monkeypatch.setattr("core.ingestion.jobs.dlq_job.emit", fake_emit)
    job = make_job(
        redis=redis,
        entities=EntityCache(valid_ids={2}),
        write_to_graph=graph_write_fails,
    )
    ctx = JobContext(user_name="ada", project_id="project-1")
    result = BatchResult(entity_ids=[2], new_entity_ids={2})
    result.set_scope("ada", "session-1", "project-1")
    entry = dlq_entry(stage="graph_write", batch_result=result.to_dict())

    assert await job._retry_graph_write(entry, ctx) is False

    work_events = [
        args[3]
        for args, _kwargs in emitted
        if args[2] == "dlq_work_unit_finished"
    ]
    assert work_events[-1]["status"] == "failed"
    assert work_events[-1]["trace"]["summary"] == "database unavailable"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_dlq_replay_graph_write_helper_skips_when_no_graph_writes():
    calls = []

    async def graph_write(result):
        calls.append(result)
        return True, None

    job = make_job(write_to_graph=graph_write)

    assert await job._write_replay_graph_if_needed(BatchResult()) is None
    assert calls == []


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_dlq_retry_failure_returns_claim_to_active_queue():
    redis = FakeRedis()
    processor = RecordingProcessor()

    async def failed_run(*_args, **_kwargs):
        return BatchResult(success=False, error="boom")

    processor.run = failed_run
    job = make_job(redis=redis, processor=processor, max_attempts=3)
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
    job = make_job(redis=redis, processor=processor)
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
    job = make_job(redis=redis, processor=processor)
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
    job = make_job(redis=redis, max_attempts=1)
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
    job = make_job(redis=redis)

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
    job = make_job(redis=redis)
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
