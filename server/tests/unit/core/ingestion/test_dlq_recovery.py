from types import SimpleNamespace

import pytest

from common.schema.settings import DLQSettings
from core.ingestion.batch import IngestionBatch
from core.ingestion.recovery import replay_job as dlq_job
from core.ingestion.recovery.dlq_payload import DLQPayload
from core.ingestion.recovery.dlq_state import (
    DLQ_STATUS_PARKED,
    DLQ_STATUS_PROCESSING,
    DLQ_STATUS_QUEUED,
    ensure_dlq_id,
    serialize_dlq_entry,
)
from core.ingestion.recovery.replay_job import DLQReplayJob
from infrastructure.job.base import JobContext
from infrastructure.redis_client import RedisKeys
from infrastructure.work_record import WorkRecord
from tests.fixtures.fakes import FakeRedis
from tests.fixtures.ingestion import ingestion_policy


def _job(redis: FakeRedis, *, max_attempts: int = 2) -> DLQReplayJob:
    processor = SimpleNamespace(knowledge_store=_ReplayStore())
    entities = SimpleNamespace(project_id="project-1")
    return DLQReplayJob(
        entities=entities,
        processor=processor,
        write_to_graph=None,
        redis_client=redis,
        settings=DLQSettings(max_attempts=max_attempts),
    )


async def _empty_requeues():
    return []


class _ReplayStore:
    async def get_requeued_dlq_items(self, **_kwargs):
        return []

    async def park_dlq_item(self, **_kwargs):
        return True


def _context() -> JobContext:
    return JobContext(user_name="ada", project_id="project-1")


def _graph_committed_payload() -> dict:
    batch = IngestionBatch.open(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        messages=[{"id": 7, "message": "Ada met Grace."}],
        session_text="[USER]: Ada met Grace.",
        policy=ingestion_policy(),
        batch_id="batch-1",
    )
    batch.validate_input()
    batch.mark_extracted()
    batch.set_resolution(
        entity_ids=[],
        new_entity_ids=set(),
        alias_updated_ids=set(),
        entity_message_map={},
        alias_updates={},
        candidate_suggestions=[],
    )
    batch.set_relationship_observations([])
    batch.complete()
    batch.mark_message_logs_handled()
    batch.mark_candidate_suggestions_handled()
    batch.set_graph_write_buffers(
        graph_work_unit=WorkRecord.for_graph_write(batch.scope),
        safe_entity_ids=set(),
        graph_alias_updates=[],
        entity_writes=[],
        relationship_writes=[],
        message_entity_refs=[],
        eligible_messages=[],
        skipped_relationships=[],
        zombie_entity_ids=set(),
        dirty_entity_ids=set(),
    )
    batch.seal_for_commit()
    batch.graph_work_unit.start()
    batch.graph_work_unit.succeed()
    batch.mark_graph_committed()
    return DLQPayload.from_ingestion_batch(batch).model_dump(mode="json")


def _entry(*, error: str, attempt: int = 1, batch_result: dict | None = None) -> dict:
    entry = {
        "error": error,
        "attempt": attempt,
        "stage": "checkpoint",
        "user_name": "ada",
        "project_id": "project-1",
        "session_id": "session-1",
        "messages": [{"id": 7, "message": "Ada met Grace."}],
    }
    if batch_result is not None:
        entry["batch_result"] = batch_result
    ensure_dlq_id(entry)
    return entry


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_unsupported_dlq_payload_is_parked(monkeypatch):
    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(dlq_job, "emit", emit_nothing)
    redis = FakeRedis()
    job = _job(redis)
    entry = _entry(
        error="unsupported DLQ schema",
        batch_result={"schema_version": 999},
    )
    dlq_key = RedisKeys.dlq("ada", "project-1")
    await redis.rpush(dlq_key, serialize_dlq_entry(entry))

    result = await job.execute(_context())

    assert result.success is True
    assert await redis.llen(dlq_key) == 0
    assert await redis.llen(RedisKeys.dlq_parked("ada", "project-1")) == 1
    assert await redis.hget(
        RedisKeys.dlq_state("ada", "project-1"), entry["dlq_id"]
    ) == DLQ_STATUS_PARKED


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_transient_poison_entry_is_requeued_once_then_parked(monkeypatch):
    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(dlq_job, "emit", emit_nothing)
    redis = FakeRedis()
    job = _job(redis, max_attempts=2)
    entry = _entry(
        error="ConnectionError while replaying",
        batch_result={"schema_version": 999},
    )
    dlq_key = RedisKeys.dlq("ada", "project-1")
    await redis.rpush(dlq_key, serialize_dlq_entry(entry))

    first = await job.execute(_context())
    assert first.summary == "Processed 1: 0 retried, 0 parked"
    queued = await redis.lrange(dlq_key, 0, -1)
    assert len(queued) == 1
    assert '"attempt": 2' in queued[0]
    assert await redis.hget(
        RedisKeys.dlq_state("ada", "project-1"), entry["dlq_id"]
    ) == DLQ_STATUS_QUEUED

    second = await job.execute(_context())
    assert second.summary == "Processed 1: 0 retried, 1 parked"
    assert await redis.llen(dlq_key) == 0
    assert await redis.llen(RedisKeys.dlq_parked("ada", "project-1")) == 1


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_abandoned_processing_claim_is_requeued(monkeypatch):
    monkeypatch.setattr(dlq_job, "get_now_unix", lambda: 10_000)
    redis = FakeRedis()
    job = _job(redis)
    entry = _entry(error="ConnectionError")
    raw = serialize_dlq_entry(entry)
    processing_key = RedisKeys.dlq_processing("ada", "project-1")
    claims_key = RedisKeys.dlq_claims("ada", "project-1")
    state_key = RedisKeys.dlq_state("ada", "project-1")
    await redis.rpush(processing_key, raw)
    await redis.hset(state_key, entry["dlq_id"], DLQ_STATUS_PROCESSING)
    await redis.hset(
        claims_key,
        entry["dlq_id"],
        '{"claimed_at": 1, "job": "dlq_auto_replay"}',
    )

    requeued = await job._requeue_abandoned_claims(_context())

    assert requeued == 1
    assert await redis.llen(processing_key) == 0
    assert await redis.llen(RedisKeys.dlq("ada", "project-1")) == 1
    assert await redis.hget(state_key, entry["dlq_id"]) == DLQ_STATUS_QUEUED
    assert await redis.hget(claims_key, entry["dlq_id"]) is None


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_checkpoint_replay_is_idempotent(monkeypatch):
    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(dlq_job, "emit", emit_nothing)
    redis = FakeRedis()
    job = _job(redis)
    entry = _entry(
        error="ConnectionError while committing checkpoint",
        batch_result=_graph_committed_payload(),
    )

    assert await job._retry_checkpoint(entry, _context()) is True
    assert await job._retry_checkpoint(entry, _context()) is True
    assert redis.strings[RedisKeys.checkpoint("ada", "session-1")] == "1"
    assert redis.strings[RedisKeys.last_processed("ada", "session-1")] == "7"
