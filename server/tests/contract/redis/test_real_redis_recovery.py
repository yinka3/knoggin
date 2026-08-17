"""Scoped real-Redis recovery contracts for the DLQ lifecycle."""

import asyncio
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from common.schema.ingestion.contracts import CandidateSuggestion
from common.schema.settings import (
    DLQSettings,
    IngestionSettings,
    RedisConnectionSettings,
)
from core.ingestion import pipeline as ingestion_pipeline
from core.ingestion.batch import IngestionBatch
from core.ingestion.pipeline import IngestionPipeline
from core.ingestion.recovery import replay_job as dlq_job
from core.ingestion.recovery.dlq_payload import DLQPayload
from core.ingestion.recovery.dlq_state import (
    DLQ_STATUS_COMPLETED,
    DLQ_STATUS_PARKED,
    DLQ_STATUS_PROCESSING,
    DLQ_STATUS_QUEUED,
    ensure_dlq_id,
    serialize_dlq_entry,
)
from core.ingestion.recovery.replay_job import DLQReplayJob
from infrastructure.job.base import JobContext
from infrastructure.redis_client import AsyncRedisClient, RedisKeys
from infrastructure.work_record import WorkRecord
from tests.fixtures.ingestion import ingestion_policy


class _FailCompletedAckRedis:
    """Delegate to Redis while failing the first completed-state ack."""

    def __init__(self, client, state_key: str):
        self._client = client
        self._state_key = state_key
        self.failed = False

    def __getattr__(self, name):
        return getattr(self._client, name)

    async def hset(self, key, field, value):
        if (
            not self.failed
            and key == self._state_key
            and str(value) == DLQ_STATUS_COMPLETED
        ):
            self.failed = True
            raise RuntimeError("simulated DLQ ack failure")
        return await self._client.hset(key, field, value)


class _FailCheckpointEvalRedis:
    """Delegate to Redis while making checkpoint commits transiently fail."""

    def __init__(self, client):
        self._client = client

    def __getattr__(self, name):
        return getattr(self._client, name)

    async def eval(self, *_args, **_kwargs):
        raise ConnectionError("simulated checkpoint connection failure")


class _ReplayStore:
    """Small durable-boundary recorder for real-Redis replay contracts."""

    def __init__(self):
        self.message_logs = []
        self.candidate_suggestions = []
        self.parked = {}

    async def get_requeued_dlq_items(self, **_kwargs):
        return []

    async def park_dlq_item(self, *, dlq_id, user_name, project_id, entry):
        self.parked[(user_name, project_id, dlq_id)] = {
            **dict(entry),
            "status": "parked",
        }

    async def get_parked_dlq_item(self, *, dlq_id, user_name, project_id):
        return self.parked.get((user_name, project_id, dlq_id))

    async def mark_parked_dlq_item_requeued(self, *, dlq_id, user_name, project_id):
        entry = self.parked.get((user_name, project_id, dlq_id))
        if entry is None or entry["status"] != "parked":
            return False
        entry["status"] = "requeued"
        return True

    async def mark_parked_dlq_item_completed_if_requeued(
        self, *, dlq_id, user_name, project_id
    ):
        entry = self.parked.get((user_name, project_id, dlq_id))
        if entry is None or entry["status"] != "requeued":
            return False
        entry["status"] = "completed"
        return True

    async def save_message_logs(self, messages):
        self.message_logs.extend(messages)
        return True

    async def save_candidate_suggestions(self, scope, suggestions):
        self.candidate_suggestions.extend(suggestions)
        return len(suggestions)


class _ReplayProcessor:
    def __init__(self, *, user_name, project_id, store):
        self.user_name = user_name
        self.project_id = project_id
        self.knowledge_store = store

    def open_batch(self, messages, session_text, *, session_id, policy):
        return IngestionBatch.open(
            user_name=self.user_name,
            project_id=self.project_id,
            session_id=session_id,
            messages=messages,
            session_text=session_text,
            policy=policy,
        )

    async def process(self, batch):
        batch.validate_input()
        batch.mark_extracted()
        batch.trace.message_ids = [int(message["id"]) for message in batch.messages]
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


def _graph_committed_payload(
    *, user_name: str, project_id: str, session_id: str, message_id: int
) -> dict:
    batch = IngestionBatch.open(
        user_name=user_name,
        project_id=project_id,
        session_id=session_id,
        messages=[{"id": message_id, "message": "Recover this checkpoint."}],
        session_text="[USER]: Recover this checkpoint.",
        policy=ingestion_policy(
            ingestion=IngestionSettings(checkpoint_interval=2)
        ),
        batch_id=f"batch-{uuid4()}",
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


async def _cleanup_redis_scope(client, user_name: str, project_id: str, session_id: str):
    keys = set(RedisKeys.project_cleanup_keys(user_name, project_id))
    keys.update(RedisKeys.session_keys(user_name, session_id))
    patterns = [
        *RedisKeys.project_cleanup_patterns(user_name, project_id),
        RedisKeys.message_dedup_pattern(user_name, session_id),
    ]
    for pattern in patterns:
        keys.update([key async for key in client.scan_iter(match=pattern)])
    if keys:
        await client.delete(*keys)


@pytest.mark.integration
@pytest.mark.requires_redis
@pytest.mark.slow
@pytest.mark.no_network
async def test_real_redis_recovers_an_abandoned_checkpoint_claim(monkeypatch):
    """A worker crash after claim is requeued and the checkpoint is committed once."""

    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(dlq_job, "emit", emit_nothing)
    user_name = f"recovery-{uuid4()}"
    project_id = f"project-{uuid4()}"
    session_id = f"session-{uuid4()}"
    message_id = int(uuid4().int % 2_000_000_000) + 1
    manager = AsyncRedisClient(RedisConnectionSettings.from_env())
    client = await manager.connect()
    job = DLQReplayJob(
        entities=SimpleNamespace(project_id=project_id),
        processor=SimpleNamespace(knowledge_store=_ReplayStore()),
        write_to_graph=None,
        redis_client=client,
        settings=DLQSettings(max_attempts=2),
    )
    context = JobContext(user_name=user_name, project_id=project_id)
    entry = {
        "error": "ConnectionError while committing checkpoint",
        "attempt": 1,
        "stage": "checkpoint",
        "user_name": user_name,
        "project_id": project_id,
        "session_id": session_id,
        "messages": [{"id": message_id, "message": "Recover this checkpoint."}],
        "batch_result": _graph_committed_payload(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            message_id=message_id,
        ),
    }
    dlq_id = ensure_dlq_id(entry)
    dlq_key = RedisKeys.dlq(user_name, project_id)
    processing_key = RedisKeys.dlq_processing(user_name, project_id)
    claims_key = RedisKeys.dlq_claims(user_name, project_id)
    state_key = RedisKeys.dlq_state(user_name, project_id)

    try:
        await client.rpush(dlq_key, serialize_dlq_entry(entry))
        claimed, raw_item, claimed_id = await job._claim_next(context)

        assert claimed is not None
        assert raw_item is not None
        assert claimed_id == dlq_id
        assert await client.llen(dlq_key) == 0
        assert await client.llen(processing_key) == 1

        await client.hset(
            claims_key,
            dlq_id,
            json.dumps(
                {
                    "claimed_at": 0,
                    "job": job.name,
                    "project_id": project_id,
                }
            ),
        )

        result = await job.execute(context)

        assert result.success is True
        assert result.summary == "Processed 1: 1 retried, 0 parked"
        assert await client.llen(dlq_key) == 0
        assert await client.llen(processing_key) == 0
        assert await client.hget(state_key, dlq_id) == DLQ_STATUS_COMPLETED
        assert await client.get(RedisKeys.last_processed(user_name, session_id)) == str(
            message_id
        )
        assert await client.get(RedisKeys.checkpoint(user_name, session_id)) == "1"
        assert await client.zscore(RedisKeys.dlq_completed(user_name, project_id), dlq_id)
    finally:
        await _cleanup_redis_scope(client, user_name, project_id, session_id)
        await manager.close()


@pytest.mark.integration
@pytest.mark.requires_redis
@pytest.mark.slow
@pytest.mark.no_network
async def test_real_redis_claim_race_has_one_owner(monkeypatch):
    """Concurrent workers cannot both claim the same queued DLQ item."""

    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(dlq_job, "emit", emit_nothing)
    user_name = f"claim-race-{uuid4()}"
    project_id = f"project-{uuid4()}"
    manager = AsyncRedisClient(RedisConnectionSettings.from_env())
    client = await manager.connect()
    job = DLQReplayJob(
        entities=SimpleNamespace(project_id=project_id),
        processor=SimpleNamespace(knowledge_store=_ReplayStore()),
        write_to_graph=None,
        redis_client=client,
        settings=DLQSettings(max_attempts=2),
    )
    context = JobContext(user_name=user_name, project_id=project_id)
    entry = {
        "error": "ConnectionError while replaying",
        "attempt": 1,
        "stage": "checkpoint",
        "user_name": user_name,
        "project_id": project_id,
        "session_id": f"session-{uuid4()}",
        "messages": [{"id": 1, "message": "claim race"}],
        "batch_result": {"schema_version": 999},
    }
    dlq_id = ensure_dlq_id(entry)
    dlq_key = RedisKeys.dlq(user_name, project_id)
    processing_key = RedisKeys.dlq_processing(user_name, project_id)
    claims_key = RedisKeys.dlq_claims(user_name, project_id)
    state_key = RedisKeys.dlq_state(user_name, project_id)

    try:
        await client.rpush(dlq_key, serialize_dlq_entry(entry))
        first, second = await asyncio.gather(
            job._claim_next(context),
            job._claim_next(context),
        )

        claims = [result for result in (first, second) if result[1] is not None]
        empty = [result for result in (first, second) if result[1] is None]
        assert len(claims) == 1
        assert len(empty) == 1
        assert claims[0][2] == dlq_id
        assert await client.llen(dlq_key) == 0
        assert await client.llen(processing_key) == 1
        assert await client.hget(state_key, dlq_id) == DLQ_STATUS_PROCESSING
        claim = json.loads(await client.hget(claims_key, dlq_id))
        assert claim["job"] == job.name
    finally:
        await _cleanup_redis_scope(client, user_name, project_id, entry["session_id"])
        await manager.close()


@pytest.mark.integration
@pytest.mark.requires_redis
@pytest.mark.slow
@pytest.mark.no_network
async def test_real_redis_live_claim_is_not_requeued_on_restart(monkeypatch):
    """Restart recovery leaves an unexpired claim owned by its current worker."""

    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(dlq_job, "emit", emit_nothing)
    now = 10_000
    monkeypatch.setattr(dlq_job, "get_now_unix", lambda: now)
    user_name = f"live-claim-{uuid4()}"
    project_id = f"project-{uuid4()}"
    session_id = f"session-{uuid4()}"
    manager = AsyncRedisClient(RedisConnectionSettings.from_env())
    client = await manager.connect()
    job = DLQReplayJob(
        entities=SimpleNamespace(project_id=project_id),
        processor=SimpleNamespace(knowledge_store=_ReplayStore()),
        write_to_graph=None,
        redis_client=client,
        settings=DLQSettings(max_attempts=2),
    )
    context = JobContext(user_name=user_name, project_id=project_id)
    entry = {
        "error": "ConnectionError while replaying",
        "attempt": 1,
        "stage": "processing",
        "user_name": user_name,
        "project_id": project_id,
        "session_id": session_id,
        "messages": [{"id": 2, "message": "live claim"}],
    }
    dlq_id = ensure_dlq_id(entry)
    raw = serialize_dlq_entry(entry)
    processing_key = RedisKeys.dlq_processing(user_name, project_id)
    claims_key = RedisKeys.dlq_claims(user_name, project_id)
    state_key = RedisKeys.dlq_state(user_name, project_id)

    try:
        await client.rpush(processing_key, raw)
        await client.hset(state_key, dlq_id, DLQ_STATUS_PROCESSING)
        await client.hset(
            claims_key,
            dlq_id,
            json.dumps({"claimed_at": now, "job": job.name}),
        )

        assert await job._requeue_abandoned_claims(context) == 0
        assert await client.llen(processing_key) == 1
        assert await client.llen(RedisKeys.dlq(user_name, project_id)) == 0
        assert await client.hget(state_key, dlq_id) == DLQ_STATUS_PROCESSING
        assert await client.hget(claims_key, dlq_id)
    finally:
        await _cleanup_redis_scope(client, user_name, project_id, session_id)
        await manager.close()


@pytest.mark.integration
@pytest.mark.requires_redis
@pytest.mark.slow
@pytest.mark.no_network
async def test_real_redis_transient_retry_reaches_max_attempts_then_parks(monkeypatch):
    """A transient stage failure is requeued once, then parked at the limit."""

    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(dlq_job, "emit", emit_nothing)
    user_name = f"retry-limit-{uuid4()}"
    project_id = f"project-{uuid4()}"
    session_id = f"session-{uuid4()}"
    manager = AsyncRedisClient(RedisConnectionSettings.from_env())
    raw_client = await manager.connect()
    client = _FailCheckpointEvalRedis(raw_client)
    job = DLQReplayJob(
        entities=SimpleNamespace(project_id=project_id),
        processor=SimpleNamespace(knowledge_store=_ReplayStore()),
        write_to_graph=None,
        redis_client=client,
        settings=DLQSettings(max_attempts=2),
    )
    context = JobContext(user_name=user_name, project_id=project_id)
    entry = {
        "error": "ConnectionError while committing checkpoint",
        "attempt": 1,
        "stage": "checkpoint",
        "user_name": user_name,
        "project_id": project_id,
        "session_id": session_id,
        "messages": [{"id": 3, "message": "retry limit"}],
        "batch_result": _graph_committed_payload(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            message_id=3,
        ),
    }
    dlq_id = ensure_dlq_id(entry)
    dlq_key = RedisKeys.dlq(user_name, project_id)
    parked_key = RedisKeys.dlq_parked(user_name, project_id)
    state_key = RedisKeys.dlq_state(user_name, project_id)

    try:
        await client.rpush(dlq_key, serialize_dlq_entry(entry))

        first = await job.execute(context)
        assert first.summary == "Processed 1: 0 retried, 0 parked"
        queued = await client.lrange(dlq_key, 0, -1)
        assert len(queued) == 1
        assert json.loads(queued[0])["attempt"] == 2
        assert await client.hget(state_key, dlq_id) == DLQ_STATUS_QUEUED

        second = await job.execute(context)
        assert second.summary == "Processed 1: 0 retried, 1 parked"
        assert await client.llen(dlq_key) == 0
        assert await client.llen(parked_key) == 1
        assert await client.hget(state_key, dlq_id) == DLQ_STATUS_PARKED
    finally:
        await _cleanup_redis_scope(raw_client, user_name, project_id, session_id)
        await manager.close()


@pytest.mark.integration
@pytest.mark.requires_redis
@pytest.mark.slow
@pytest.mark.no_network
async def test_real_redis_ack_failure_falls_back_to_parked_state(monkeypatch):
    """A completed-state write failure never leaves a processing orphan."""

    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(dlq_job, "emit", emit_nothing)
    user_name = f"ack-failure-{uuid4()}"
    project_id = f"project-{uuid4()}"
    session_id = f"session-{uuid4()}"
    manager = AsyncRedisClient(RedisConnectionSettings.from_env())
    raw_client = await manager.connect()
    state_key = RedisKeys.dlq_state(user_name, project_id)
    client = _FailCompletedAckRedis(raw_client, state_key)
    job = DLQReplayJob(
        entities=SimpleNamespace(project_id=project_id),
        processor=SimpleNamespace(knowledge_store=_ReplayStore()),
        write_to_graph=None,
        redis_client=client,
        settings=DLQSettings(max_attempts=2),
    )
    context = JobContext(user_name=user_name, project_id=project_id)
    entry = {
        "error": "ConnectionError while committing checkpoint",
        "attempt": 1,
        "stage": "checkpoint",
        "user_name": user_name,
        "project_id": project_id,
        "session_id": session_id,
        "messages": [{"id": 4, "message": "ack failure"}],
        "batch_result": _graph_committed_payload(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            message_id=4,
        ),
    }
    dlq_id = ensure_dlq_id(entry)
    dlq_key = RedisKeys.dlq(user_name, project_id)
    processing_key = RedisKeys.dlq_processing(user_name, project_id)
    parked_key = RedisKeys.dlq_parked(user_name, project_id)

    try:
        await client.rpush(dlq_key, serialize_dlq_entry(entry))
        result = await job.execute(context)

        assert client.failed is True
        # The stage replay succeeded and is counted as retried; the failed
        # completed-state ack then safely parks the item for operator review.
        assert result.summary == "Processed 1: 1 retried, 1 parked"
        assert await client.llen(dlq_key) == 0
        assert await client.llen(processing_key) == 0
        assert await client.llen(parked_key) == 1
        assert await client.hget(state_key, dlq_id) == DLQ_STATUS_PARKED
        assert await client.hget(RedisKeys.dlq_claims(user_name, project_id), dlq_id) is None
    finally:
        await _cleanup_redis_scope(raw_client, user_name, project_id, session_id)
        await manager.close()


@pytest.mark.integration
@pytest.mark.requires_redis
@pytest.mark.slow
@pytest.mark.no_network
async def test_real_redis_corrupt_json_is_parked_with_stable_id(monkeypatch):
    """Malformed JSON is retained for inspection instead of disappearing."""

    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(dlq_job, "emit", emit_nothing)
    user_name = f"corrupt-entry-{uuid4()}"
    project_id = f"project-{uuid4()}"
    manager = AsyncRedisClient(RedisConnectionSettings.from_env())
    client = await manager.connect()
    job = DLQReplayJob(
        entities=SimpleNamespace(project_id=project_id),
        processor=SimpleNamespace(knowledge_store=_ReplayStore()),
        write_to_graph=None,
        redis_client=client,
        settings=DLQSettings(max_attempts=2),
    )
    context = JobContext(user_name=user_name, project_id=project_id)
    raw_item = "{not-json"
    corrupt_id = job._corrupt_dlq_id(raw_item)
    dlq_key = RedisKeys.dlq(user_name, project_id)
    parked_key = RedisKeys.dlq_parked(user_name, project_id)
    state_key = RedisKeys.dlq_state(user_name, project_id)

    try:
        await client.rpush(dlq_key, raw_item)
        result = await job.execute(context)

        assert result.summary == "Processed 1: 0 retried, 1 parked"
        assert await client.llen(dlq_key) == 0
        assert await client.lrange(parked_key, 0, -1) == [raw_item]
        assert await client.hget(state_key, corrupt_id) == DLQ_STATUS_PARKED
    finally:
        await _cleanup_redis_scope(client, user_name, project_id, "unused")
        await manager.close()


@pytest.mark.integration
@pytest.mark.requires_redis
@pytest.mark.slow
@pytest.mark.no_network
async def test_real_redis_prunes_completed_markers_without_touching_queue(monkeypatch):
    """Completed dedupe state expires independently from queued work."""

    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(dlq_job, "emit", emit_nothing)
    monkeypatch.setattr(dlq_job, "get_now_unix", lambda: 2_000)
    user_name = f"prune-{uuid4()}"
    project_id = f"project-{uuid4()}"
    manager = AsyncRedisClient(RedisConnectionSettings.from_env())
    client = await manager.connect()
    job = DLQReplayJob(
        entities=SimpleNamespace(project_id=project_id),
        processor=SimpleNamespace(knowledge_store=_ReplayStore()),
        write_to_graph=None,
        redis_client=client,
        settings=DLQSettings(completed_state_retention_hours=0.25),
    )
    context = JobContext(user_name=user_name, project_id=project_id)
    completed_id = "completed-marker"
    state_key = RedisKeys.dlq_state(user_name, project_id)
    completed_key = RedisKeys.dlq_completed(user_name, project_id)
    dlq_key = RedisKeys.dlq(user_name, project_id)

    try:
        await client.hset(state_key, completed_id, DLQ_STATUS_COMPLETED)
        await client.zadd(completed_key, {completed_id: 100})
        await client.rpush(dlq_key, "queue-must-remain")

        assert await job._prune_completed_state(context) == 1
        assert await client.hget(state_key, completed_id) is None
        assert await client.zscore(completed_key, completed_id) is None
        assert await client.lrange(dlq_key, 0, -1) == ["queue-must-remain"]
    finally:
        await _cleanup_redis_scope(client, user_name, project_id, "unused")
        await manager.close()


@pytest.mark.integration
@pytest.mark.requires_redis
@pytest.mark.slow
@pytest.mark.no_network
async def test_real_redis_dlq_enqueue_deduplicates_logical_work(monkeypatch):
    """The pipeline's deterministic DLQ identity prevents duplicate queue work."""

    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(ingestion_pipeline, "emit", emit_nothing)
    user_name = f"enqueue-dedupe-{uuid4()}"
    project_id = f"project-{uuid4()}"
    session_id = f"session-{uuid4()}"
    manager = AsyncRedisClient(RedisConnectionSettings.from_env())
    client = await manager.connect()
    pipeline = IngestionPipeline(
        project_id=project_id,
        redis_client=client,
        llm=None,
        entities=None,
        processor=None,
        cpu_executor=None,
        user_name=user_name,
        compiled_domain=ingestion_policy().domain,
        get_next_ent_id=None,
    )
    messages = [{"id": 5, "message": "same logical DLQ work"}]
    dlq_key = RedisKeys.dlq(user_name, project_id)
    state_key = RedisKeys.dlq_state(user_name, project_id)

    try:
        assert await pipeline.move_to_dead_letter(
            messages,
            "ConnectionError while writing graph",
            stage="graph_write",
            session_id=session_id,
        )
        assert await pipeline.move_to_dead_letter(
            messages,
            "ConnectionError while writing graph",
            stage="graph_write",
            session_id=session_id,
        )

        queued = await client.lrange(dlq_key, 0, -1)
        assert len(queued) == 1
        entry = json.loads(queued[0])
        assert await client.hget(state_key, entry["dlq_id"]) == DLQ_STATUS_QUEUED
    finally:
        await _cleanup_redis_scope(client, user_name, project_id, session_id)
        await manager.close()


@pytest.mark.integration
@pytest.mark.requires_redis
@pytest.mark.slow
@pytest.mark.no_network
async def test_real_redis_scheduler_policy_is_explicitly_disabled(monkeypatch):
    """Automatic replay remains disabled until an explicit scheduling decision."""

    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(dlq_job, "emit", emit_nothing)
    user_name = f"scheduler-policy-{uuid4()}"
    project_id = f"project-{uuid4()}"
    manager = AsyncRedisClient(RedisConnectionSettings.from_env())
    client = await manager.connect()
    job = DLQReplayJob(
        entities=SimpleNamespace(project_id=project_id),
        processor=SimpleNamespace(knowledge_store=_ReplayStore()),
        write_to_graph=None,
        redis_client=client,
        settings=DLQSettings(),
    )

    try:
        assert await job.should_run(JobContext(user_name, project_id)) is False
    finally:
        await _cleanup_redis_scope(client, user_name, project_id, "unused")
        await manager.close()


def _completed_replay_payload(
    *, user_name, project_id, session_id, message_id, candidate=False
):
    batch = IngestionBatch.open(
        user_name=user_name,
        project_id=project_id,
        session_id=session_id,
        messages=[
            {
                "id": message_id,
                "message": "Replay this durable stage.",
                "timestamp": "2026-08-01T16:00:00+00:00",
            }
        ],
        session_text="[USER]: Replay this durable stage.",
        policy=ingestion_policy(
            ingestion=IngestionSettings(checkpoint_interval=10)
        ),
        batch_id=f"batch-{uuid4()}",
    )
    batch.validate_input()
    batch.mark_extracted()
    batch.trace.message_ids = [message_id]
    suggestions = (
        [
            CandidateSuggestion(
                msg_id=message_id,
                mention="Ada",
                mention_type="person",
                mention_topic="General",
                candidate_id=7,
                candidate_name="Ada Lovelace",
                base_score=0.9,
            )
        ]
        if candidate
        else []
    )
    batch.set_resolution(
        entity_ids=[],
        new_entity_ids=set(),
        alias_updated_ids=set(),
        entity_message_map={},
        alias_updates={},
        candidate_suggestions=suggestions,
    )
    batch.set_relationship_observations([])
    batch.complete()
    return DLQPayload.from_ingestion_batch(batch).model_dump(mode="json")


async def _replay_graph_callback(batch):
    """Rebuild the minimal graph boundary needed before checkpoint replay."""

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
    return True, None


@pytest.mark.integration
@pytest.mark.requires_redis
@pytest.mark.slow
@pytest.mark.no_network
async def test_real_redis_replays_processing_stage_then_checkpoint(monkeypatch):
    """Processing-stage replay rebuilds the batch before durable completion."""

    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(dlq_job, "emit", emit_nothing)
    user_name = f"stage-processing-{uuid4()}"
    project_id = f"project-{uuid4()}"
    session_id = f"session-{uuid4()}"
    manager = AsyncRedisClient(RedisConnectionSettings.from_env())
    client = await manager.connect()
    store = _ReplayStore()
    processor = _ReplayProcessor(
        user_name=user_name,
        project_id=project_id,
        store=store,
    )
    job = DLQReplayJob(
        entities=SimpleNamespace(project_id=project_id),
        processor=processor,
        write_to_graph=_replay_graph_callback,
        redis_client=client,
        settings=DLQSettings(max_attempts=2),
    )
    context = JobContext(user_name=user_name, project_id=project_id)
    entry = {
        "error": "ConnectionError during processing",
        "attempt": 1,
        "stage": "processing",
        "user_name": user_name,
        "project_id": project_id,
        "session_id": session_id,
        "messages": [{"id": 61, "message": "Replay this durable stage."}],
        "session_text": "[USER]: Replay this durable stage.",
        "batch_result": _completed_replay_payload(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            message_id=61,
        ),
    }
    dlq_id = ensure_dlq_id(entry)

    try:
        await client.rpush(
            RedisKeys.dlq(user_name, project_id), serialize_dlq_entry(entry)
        )
        result = await job.execute(context)

        assert result.summary == "Processed 1: 1 retried, 0 parked"
        assert [row["id"] for row in store.message_logs] == [61]
        assert await client.get(RedisKeys.last_processed(user_name, session_id)) == "61"
        assert await client.hget(
            RedisKeys.dlq_state(user_name, project_id), dlq_id
        ) == DLQ_STATUS_COMPLETED
    finally:
        await _cleanup_redis_scope(client, user_name, project_id, session_id)
        await manager.close()


@pytest.mark.integration
@pytest.mark.requires_redis
@pytest.mark.slow
@pytest.mark.no_network
async def test_real_redis_replays_message_log_stage(monkeypatch):
    """Message-log replay persists the log before graph/checkpoint completion."""

    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(dlq_job, "emit", emit_nothing)
    user_name = f"stage-message-log-{uuid4()}"
    project_id = f"project-{uuid4()}"
    session_id = f"session-{uuid4()}"
    manager = AsyncRedisClient(RedisConnectionSettings.from_env())
    client = await manager.connect()
    store = _ReplayStore()
    job = DLQReplayJob(
        entities=SimpleNamespace(project_id=project_id),
        processor=SimpleNamespace(knowledge_store=store),
        write_to_graph=_replay_graph_callback,
        redis_client=client,
        settings=DLQSettings(max_attempts=2),
    )
    context = JobContext(user_name=user_name, project_id=project_id)
    payload = _completed_replay_payload(
        user_name=user_name,
        project_id=project_id,
        session_id=session_id,
        message_id=62,
    )
    entry = {
        "error": "ConnectionError saving message log",
        "attempt": 1,
        "stage": "message_log",
        "user_name": user_name,
        "project_id": project_id,
        "session_id": session_id,
        "messages": payload["messages"],
        "batch_result": payload,
    }
    dlq_id = ensure_dlq_id(entry)

    try:
        await client.rpush(
            RedisKeys.dlq(user_name, project_id), serialize_dlq_entry(entry)
        )
        result = await job.execute(context)

        assert result.summary == "Processed 1: 1 retried, 0 parked"
        assert [row["id"] for row in store.message_logs] == [62]
        assert await client.get(RedisKeys.last_processed(user_name, session_id)) == "62"
        assert await client.hget(
            RedisKeys.dlq_state(user_name, project_id), dlq_id
        ) == DLQ_STATUS_COMPLETED
    finally:
        await _cleanup_redis_scope(client, user_name, project_id, session_id)
        await manager.close()


@pytest.mark.integration
@pytest.mark.requires_redis
@pytest.mark.slow
@pytest.mark.no_network
async def test_real_redis_replays_candidate_suggestion_stage(monkeypatch):
    """Candidate suggestions replay before the graph/checkpoint boundary."""

    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(dlq_job, "emit", emit_nothing)
    user_name = f"stage-candidates-{uuid4()}"
    project_id = f"project-{uuid4()}"
    session_id = f"session-{uuid4()}"
    manager = AsyncRedisClient(RedisConnectionSettings.from_env())
    client = await manager.connect()
    store = _ReplayStore()
    job = DLQReplayJob(
        entities=SimpleNamespace(project_id=project_id),
        processor=SimpleNamespace(knowledge_store=store),
        write_to_graph=_replay_graph_callback,
        redis_client=client,
        settings=DLQSettings(max_attempts=2),
    )
    context = JobContext(user_name=user_name, project_id=project_id)
    payload = _completed_replay_payload(
        user_name=user_name,
        project_id=project_id,
        session_id=session_id,
        message_id=63,
        candidate=True,
    )
    entry = {
        "error": "ConnectionError saving candidate suggestion",
        "attempt": 1,
        "stage": "candidate_suggestions",
        "user_name": user_name,
        "project_id": project_id,
        "session_id": session_id,
        "messages": payload["messages"],
        "batch_result": payload,
    }
    dlq_id = ensure_dlq_id(entry)

    try:
        await client.rpush(
            RedisKeys.dlq(user_name, project_id), serialize_dlq_entry(entry)
        )
        result = await job.execute(context)

        assert result.summary == "Processed 1: 1 retried, 0 parked"
        assert len(store.candidate_suggestions) == 1
        assert store.candidate_suggestions[0].candidate_id == 7
        assert await client.get(RedisKeys.last_processed(user_name, session_id)) == "63"
        assert await client.hget(
            RedisKeys.dlq_state(user_name, project_id), dlq_id
        ) == DLQ_STATUS_COMPLETED
    finally:
        await _cleanup_redis_scope(client, user_name, project_id, session_id)
        await manager.close()


@pytest.mark.integration
@pytest.mark.requires_redis
@pytest.mark.slow
@pytest.mark.no_network
async def test_real_redis_replays_graph_write_stage(monkeypatch):
    """Graph-write replay rebuilds the graph boundary before checkpointing."""

    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(dlq_job, "emit", emit_nothing)
    user_name = f"stage-graph-{uuid4()}"
    project_id = f"project-{uuid4()}"
    session_id = f"session-{uuid4()}"
    manager = AsyncRedisClient(RedisConnectionSettings.from_env())
    client = await manager.connect()
    store = _ReplayStore()
    job = DLQReplayJob(
        entities=SimpleNamespace(project_id=project_id),
        processor=SimpleNamespace(knowledge_store=store),
        write_to_graph=_replay_graph_callback,
        redis_client=client,
        settings=DLQSettings(max_attempts=2),
    )
    context = JobContext(user_name=user_name, project_id=project_id)
    payload = _completed_replay_payload(
        user_name=user_name,
        project_id=project_id,
        session_id=session_id,
        message_id=64,
    )
    entry = {
        "error": "ConnectionError writing graph",
        "attempt": 1,
        "stage": "graph_write",
        "user_name": user_name,
        "project_id": project_id,
        "session_id": session_id,
        "messages": payload["messages"],
        "batch_result": payload,
    }
    dlq_id = ensure_dlq_id(entry)

    try:
        await client.rpush(
            RedisKeys.dlq(user_name, project_id), serialize_dlq_entry(entry)
        )
        result = await job.execute(context)

        assert result.summary == "Processed 1: 1 retried, 0 parked"
        assert await client.get(RedisKeys.last_processed(user_name, session_id)) == "64"
        assert await client.hget(
            RedisKeys.dlq_state(user_name, project_id), dlq_id
        ) == DLQ_STATUS_COMPLETED
    finally:
        await _cleanup_redis_scope(client, user_name, project_id, session_id)
        await manager.close()
