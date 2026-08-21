import asyncio
import json
from types import MethodType

import pytest

from common.schema.ingestion.contracts import CandidateSuggestion
from common.schema.settings import IngestionSettings
from core.ingestion import worker as worker_module
from core.ingestion.batch import IngestionBatch, IngestionMilestone
from core.ingestion.recovery.replay_job import DLQReplayJob
from core.ingestion.worker import IngestionWorker
from infrastructure.job.base import JobContext
from infrastructure.redis_client import RedisKeys
from infrastructure.work_record import WorkRecord, WorkStatus
from tests.fixtures.fakes import FakeRedis
from tests.fixtures.ingestion import ingestion_policy


class _KnowledgeStore:
    async def save_message_logs(self, _rows):
        assert IngestionMilestone.MESSAGE_LOGS_HANDLED not in self.batch.milestones


class _Processor:
    project_id = "project-1"


class _SuggestionFailureStore:
    async def save_candidate_suggestions(self, _scope, _suggestions):
        raise ConnectionError("candidate store unavailable")


class _DLQProcessor(_Processor):
    def __init__(self, *, dlq_success=True):
        self.dlq_call = None
        self.dlq_success = dlq_success

    async def move_to_dead_letter(self, *args, **kwargs):
        self.dlq_call = (args, kwargs)
        return self.dlq_success


class _FailingCheckpointRedis(FakeRedis):
    async def eval(self, *_args, **_kwargs):
        raise RuntimeError("checkpoint unavailable")


class _FailingLtrimRedis(FakeRedis):
    def __init__(self):
        super().__init__()
        self.fail_ltrim = True

    async def ltrim(self, key, start, end):
        if self.fail_ltrim:
            raise RuntimeError("buffer cleanup unavailable")
        return await super().ltrim(key, start, end)


class _DrainProcessor(_Processor):
    def __init__(self, outcome: str, *, dlq_success: bool = True):
        self.outcome = outcome
        self.dlq_success = dlq_success
        self.batch = None
        self.dlq_call = None
        self.dlq_message_ids = None

    def capture_policy(self, _ingestion_settings):
        return ingestion_policy()

    def open_batch(self, messages, session_text, *, session_id, policy, batch_id=None):
        self.batch = IngestionBatch.open(
            user_name="ada",
            project_id=self.project_id,
            session_id=session_id,
            messages=messages,
            session_text=session_text,
            policy=policy,
            batch_id=batch_id,
        )
        return self.batch

    async def process(self, batch):
        batch.work_unit.mark_running()
        if self.outcome == "cancelled":
            raise asyncio.CancelledError
        if self.outcome == "failed":
            batch.fail("semantic processing failed")
            batch.work_unit.mark_failed(batch.error)
            return
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

    async def move_to_dead_letter(self, *args, **kwargs):
        self.dlq_call = (args, kwargs)
        self.dlq_message_ids = [message["id"] for message in args[0]]
        return self.dlq_success


def _drain_worker(processor: _DrainProcessor) -> IngestionWorker:
    async def get_session_context(_window, _before_message_id):
        return []

    async def write_to_graph(_batch):
        return True, None

    async def durable_stage(_batch):
        return True, False

    async def checkpoint_stage(_batch):
        return None

    worker = IngestionWorker(
        user_name="ada",
        session_id="session-1",
        knowledge_store=object(),
        processor=processor,
        redis=FakeRedis(),
        get_session_context=get_session_context,
        write_to_graph=write_to_graph,
        settings=IngestionSettings(batch_size=1, checkpoint_interval=4),
    )
    worker._save_message_logs_or_dlq = durable_stage
    worker._save_candidate_suggestions_or_dlq = durable_stage
    worker._write_graph_or_dlq = durable_stage
    worker._mark_batch_processed = checkpoint_stage
    return worker


def _checkpoint_worker(
    redis: FakeRedis,
    processor: _DrainProcessor,
) -> IngestionWorker:
    """Build a real checkpoint path while keeping extraction deterministic."""

    worker = _drain_worker(processor)
    worker.redis = redis
    worker._processed_batches = 0
    original_process = worker._process_messages

    async def process(batch):
        worker._processed_batches += 1
        await original_process(batch)

    async def save_message_logs(batch):
        batch.mark_message_logs_handled()
        return True, False

    async def save_candidate_suggestions(batch):
        batch.mark_candidate_suggestions_handled()
        return True, False

    async def write_graph(batch):
        graph_work = WorkRecord.for_graph_write(
            batch.scope, batch_id=batch.work_unit.id
        )
        batch.set_graph_write_buffers(
            graph_work_unit=graph_work,
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
        graph_work.mark_running()
        graph_work.mark_skipped("No graph writes")
        batch.mark_graph_committed()
        return True, None

    worker._process_messages = process
    worker._save_message_logs_or_dlq = save_message_logs
    worker._save_candidate_suggestions_or_dlq = save_candidate_suggestions
    worker.write_to_graph = write_graph
    worker._write_graph_or_dlq = MethodType(IngestionWorker._write_graph_or_dlq, worker)
    worker._mark_batch_processed = MethodType(
        IngestionWorker._mark_batch_processed,
        worker,
    )
    return worker


async def _queue_one_message(worker: IngestionWorker) -> None:
    await worker.redis.rpush(
        worker._buffer_key,
        json.dumps({"id": 7, "message": "Ada met Grace.", "role": "user"}),
    )


class _DurableQueueStore:
    def __init__(self):
        self.seal_calls = []
        self.claims = [
            type(
                "Claim",
                (),
                {
                    "batch_id": "claim-1",
                    "messages": [
                        {
                            "id": 7,
                            "message": "Ada met Grace.",
                            "role": "user",
                        }
                    ],
                },
            )()
        ]
        self.released = []
        self.failures = []

    async def seal_due_user_messages(self, **kwargs):
        self.seal_calls.append(kwargs)
        return [7]

    async def claim_next_ingestion_batch(self, **_kwargs):
        return self.claims.pop(0) if self.claims else None

    async def release_ingestion_claim(self, **kwargs):
        self.released.append(kwargs)

    async def fail_ingestion_claim(self, **kwargs):
        self.failures.append(kwargs)
        return True

    async def save_candidate_suggestions(self, _scope, _suggestions):
        return 0


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_consumer_uses_durable_fifo_claim_and_never_rewrites_messages(
    monkeypatch,
):
    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(worker_module, "emit", emit_nothing)
    processor = _DrainProcessor("success")
    worker = _checkpoint_worker(FakeRedis(), processor)
    store = _DurableQueueStore()
    worker.knowledge_store = store
    worker._shutdown_requested = False

    await worker._drain_durable_queue()

    assert store.seal_calls[0]["settle_delay_seconds"] == 120.0
    assert store.released == []
    assert processor.batch.batch_id == "claim-1"
    assert IngestionMilestone.MESSAGE_LOGS_HANDLED in processor.batch.milestones
    assert IngestionMilestone.CHECKPOINT_COMMITTED in processor.batch.milestones


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_consumer_records_a_retryable_durable_failure_on_the_claim(
    monkeypatch,
):
    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(worker_module, "emit", emit_nothing)
    processor = _DrainProcessor("failed")
    worker = _checkpoint_worker(FakeRedis(), processor)
    store = _DurableQueueStore()
    worker.knowledge_store = store

    await worker._drain_durable_queue()

    assert store.released == []
    assert store.failures == [
        {
            "user_name": "ada",
            "project_id": "project-1",
            "session_id": "session-1",
            "batch_id": "claim-1",
            "failure_stage": "runtime",
            "failure_code": "RuntimeError",
            "error_summary": "semantic processing failed",
            "retryable": True,
            "max_attempts": 3,
        }
    ]


def _completed_batch(*, policy=None) -> IngestionBatch:
    batch = IngestionBatch.open(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        messages=[{"id": 7, "message": "Ada met Grace."}],
        session_text="[USER]: Ada met Grace.",
        policy=policy or ingestion_policy(),
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
    return batch


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_consumer_marks_message_logs_only_after_the_store_succeeds():
    batch = _completed_batch()
    worker = object.__new__(IngestionWorker)
    worker.user_name = "ada"
    worker.session_id = "session-1"
    worker.processor = _Processor()
    worker.knowledge_store = _KnowledgeStore()
    worker.knowledge_store.batch = batch

    success, dlq_written = await worker._save_message_logs_or_dlq(batch)

    assert (success, dlq_written) == (True, False)
    assert IngestionMilestone.MESSAGE_LOGS_HANDLED in batch.milestones


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_consumer_dlqs_failed_suggestions_without_marking_them_handled(
    monkeypatch,
):
    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(worker_module, "emit", emit_nothing)
    batch = _completed_batch()
    batch.candidate_suggestions = [
        CandidateSuggestion(
            msg_id=7,
            mention="Ada",
            mention_type="person",
            mention_topic="people",
            candidate_id=2,
            candidate_name="Ada Lovelace",
            base_score=0.7,
        )
    ]
    processor = _DLQProcessor()
    worker = object.__new__(IngestionWorker)
    worker.user_name = "ada"
    worker.session_id = "session-1"
    worker.processor = processor
    worker.knowledge_store = _SuggestionFailureStore()

    success, dlq_written = await worker._save_candidate_suggestions_or_dlq(batch)

    assert (success, dlq_written) == (True, True)
    assert IngestionMilestone.CANDIDATE_SUGGESTIONS_HANDLED not in batch.milestones
    assert processor.dlq_call is not None
    assert processor.dlq_call[1]["stage"] == "candidate_suggestions"
    assert batch.work_unit.status is WorkStatus.FAILED
    assert "CANDIDATE_SUGGESTION_SAVE_FAILED" in (batch.work_unit.summary or "")


def _worker_batch(messages, session_text):
    batch = IngestionBatch.open(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        messages=messages,
        session_text=session_text,
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
    return batch


def _configure_worker(redis, processor, monkeypatch):
    class Store:
        async def save_message_logs(self, _rows):
            return True

    worker = object.__new__(IngestionWorker)
    worker.user_name = "ada"
    worker.session_id = "session-1"
    worker.processor = processor
    worker.knowledge_store = Store()
    worker.redis = redis
    worker.batch_size = 1
    worker.batch_timeout = 30
    worker.checkpoint_interval = 4
    worker.session_window = 24
    worker.settings = IngestionSettings(
        batch_size=worker.batch_size,
        batch_timeout=worker.batch_timeout,
        checkpoint_interval=worker.checkpoint_interval,
        session_window=worker.session_window,
    )
    worker._health_state = "not_started"
    worker._health_current_batch_size = 0
    worker._health_current_batch_started_at = None
    worker._health_last_success_at = None
    worker._health_last_failure_category = None
    worker._health_last_failure_at = None
    worker._health_consecutive_failures = 0
    worker._shutdown_requested = False
    worker._flush_future = None
    worker._processed_batches = 0

    async def context(_window, _before_message_id):
        return []

    def open_batch(messages, session_text, *, session_id, policy, batch_id=None):
        batch = IngestionBatch.open(
            user_name="ada",
            project_id="project-1",
            session_id=session_id,
            messages=messages,
            session_text=session_text,
            policy=policy,
            batch_id=batch_id,
        )
        return batch

    async def process(batch):
        worker._processed_batches += 1
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

    async def write_graph(batch):
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
        batch.mark_graph_committed()
        return True, None

    async def emit_nothing(*_args, **_kwargs):
        return None

    worker.get_session_context = context
    processor.open_batch = open_batch
    processor.capture_policy = lambda _settings: ingestion_policy()
    processor.process = process
    worker.write_to_graph = write_graph
    monkeypatch.setattr(worker_module, "emit", emit_nothing)
    return worker


async def _queue_one_message(worker):
    await worker.redis.rpush(
        worker._buffer_key,
        json.dumps({"id": 7, "message": "Ada met Grace.", "role": "user"}),
    )


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_consumer_marks_parent_work_succeeded_only_after_checkpoint_commit():
    batch = _completed_batch()
    batch.work_unit.mark_running()
    batch.work_unit.metadata["semantic_summary"] = "0 entities, 0 relationships"
    graph_work = WorkRecord.for_graph_write(
        batch.scope,
        batch_id=batch.work_unit.id,
    )
    batch.set_graph_write_buffers(
        graph_work_unit=graph_work,
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
    graph_work.mark_running()
    graph_work.mark_skipped("No graph writes")
    batch.mark_graph_committed()

    worker = object.__new__(IngestionWorker)
    worker.redis = FakeRedis()
    worker.session_id = "session-1"

    await worker._mark_batch_processed(batch)

    assert batch.work_unit.status is WorkStatus.SUCCEEDED
    assert batch.stage.value == "committed"
    assert "Durable ingestion commits completed" in (batch.work_unit.summary or "")


@pytest.mark.ingestion
@pytest.mark.no_network
def test_consumer_rejects_success_after_a_terminal_non_success_outcome():
    batch = _completed_batch()
    batch.work_unit.mark_running()
    batch.work_unit.mark_cancelled("Session shutdown")

    with pytest.raises(RuntimeError, match="cancelled"):
        IngestionWorker._mark_batch_work_succeeded(batch)


@pytest.mark.ingestion
@pytest.mark.no_network
def test_consumer_preserves_a_cancelled_parent_when_recording_failure():
    batch = _completed_batch()
    batch.work_unit.mark_running()
    batch.work_unit.mark_cancelled("Session shutdown")

    IngestionWorker._mark_batch_work_failed(batch, "Graph write failed")

    assert batch.work_unit.status is WorkStatus.CANCELLED
    assert batch.work_unit.summary == "Session shutdown"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_graph_write_cancellation_marks_active_work_and_propagates():
    batch = _completed_batch()
    batch.work_unit.mark_running()
    graph_work = WorkRecord.for_graph_write(
        batch.scope,
        batch_id=batch.work_unit.id,
    )
    graph_work.mark_running()
    batch.graph_work_unit = graph_work

    async def cancel_graph_write(_batch):
        raise asyncio.CancelledError

    worker = object.__new__(IngestionWorker)
    worker.write_to_graph = cancel_graph_write
    worker.batch_timeout = 30.0

    with pytest.raises(asyncio.CancelledError):
        await worker._write_graph_or_dlq(batch)

    assert batch.work_unit.status is WorkStatus.CANCELLED
    assert graph_work.status is WorkStatus.CANCELLED


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_graph_write_uses_the_timeout_captured_by_the_batch(monkeypatch):
    batch = _completed_batch(
        policy=ingestion_policy(
            ingestion=IngestionSettings(batch_timeout=12.5),
        )
    )
    observed_timeouts = []

    async def graph_write(_batch):
        return True, None

    async def record_wait_for(awaitable, *, timeout):
        observed_timeouts.append(timeout)
        return await awaitable

    monkeypatch.setattr(worker_module.asyncio, "wait_for", record_wait_for)
    worker = object.__new__(IngestionWorker)
    worker.batch_timeout = 0.01
    worker.write_to_graph = graph_write

    assert await worker._write_graph_or_dlq(batch) == (True, False)
    assert observed_timeouts == [12.5]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_drain_releases_batch_after_durable_processing(monkeypatch):
    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(worker_module, "emit", emit_nothing)
    processor = _DrainProcessor("succeeded")
    worker = _drain_worker(processor)
    worker.redis.lists[worker._buffer_key] = [
        json.dumps({"id": 7, "message": "Ada met Grace."})
    ]

    await worker._drain_buffer(flush_partial=True)

    assert processor.batch is not None
    assert processor.batch.released is True
    assert processor.batch.messages == []
    assert processor.batch.session_text == ""


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_drain_releases_batch_after_dlq_handoff(monkeypatch):
    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(worker_module, "emit", emit_nothing)
    processor = _DrainProcessor("failed")
    worker = _drain_worker(processor)
    worker.redis.lists[worker._buffer_key] = [
        json.dumps({"id": 7, "message": "Ada met Grace."})
    ]

    await worker._drain_buffer(flush_partial=True)

    assert processor.dlq_call is not None
    assert processor.dlq_message_ids == [7]
    assert processor.batch.released is True


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_drain_releases_batch_after_cancellation(monkeypatch):
    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(worker_module, "emit", emit_nothing)
    processor = _DrainProcessor("cancelled")
    worker = _drain_worker(processor)
    worker.redis.lists[worker._buffer_key] = [
        json.dumps({"id": 7, "message": "Ada met Grace."})
    ]

    with pytest.raises(asyncio.CancelledError):
        await worker._drain_buffer(flush_partial=True)

    assert processor.batch.released is True
    assert processor.batch.work_unit.status is WorkStatus.CANCELLED


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_checkpoint_failure_dlqs_and_cleans_buffer():
    redis = _FailingCheckpointRedis()
    processor = _DrainProcessor("succeeded")
    worker = _checkpoint_worker(redis, processor)
    await _queue_one_message(worker)

    assert await worker._drain_buffer(flush_partial=True) is False
    assert await redis.llen(worker._buffer_key) == 0
    assert processor.dlq_call is not None
    assert processor.dlq_call[1]["stage"] == "checkpoint"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_checkpoint_failure_leaves_buffer_when_dlq_write_fails():
    redis = _FailingCheckpointRedis()
    processor = _DrainProcessor("succeeded", dlq_success=False)
    worker = _checkpoint_worker(redis, processor)
    await _queue_one_message(worker)

    assert await worker._drain_buffer(flush_partial=True) is False
    assert await redis.llen(worker._buffer_key) == 1
    assert processor.dlq_call is not None
    assert processor.dlq_call[1]["stage"] == "checkpoint"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_buffer_cleanup_failure_retries_without_double_checkpoint():
    redis = _FailingLtrimRedis()
    processor = _DrainProcessor("succeeded")
    worker = _checkpoint_worker(redis, processor)
    await _queue_one_message(worker)

    with pytest.raises(RuntimeError, match="buffer cleanup unavailable"):
        await worker._drain_buffer(flush_partial=True)

    assert await redis.llen(worker._buffer_key) == 1
    assert redis.strings[RedisKeys.checkpoint("ada", "session-1")] == "1"

    redis.fail_ltrim = False
    assert await worker._drain_buffer(flush_partial=True) is False

    assert worker._processed_batches == 2
    assert await redis.llen(worker._buffer_key) == 0
    assert redis.strings[RedisKeys.checkpoint("ada", "session-1")] == "1"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_dlq_checkpoint_retry_releases_hydrated_batch():
    batch = _completed_batch()
    batch.milestones.add(IngestionMilestone.GRAPH_COMMITTED)
    job = object.__new__(DLQReplayJob)

    job._hydrate_replay_batch = lambda _payload: batch
    job._refresh_replay_scope = lambda *_args: None
    job._replay_work_unit = lambda *_args: WorkRecord.for_dlq_replay(
        batch.scope,
        stage="checkpoint",
        attempt=1,
    )
    job._attach_replay_unit = lambda *_args: None

    async def commit_checkpoint(replay_batch):
        assert replay_batch is batch
        assert replay_batch.released is False

    async def emit_finished(*_args):
        return None

    job._commit_replay_checkpoint = commit_checkpoint
    job._emit_replay_unit_finished = emit_finished

    success = await job._retry_checkpoint(
        {"batch_result": {}, "attempt": 1},
        JobContext(user_name="ada", project_id="project-1"),
    )

    assert success is True
    assert batch.released is True


async def test_worker_drains_sustained_input_in_bounded_batches(monkeypatch):
    redis = FakeRedis()
    processor = _DLQProcessor()
    worker = _configure_worker(redis, processor, monkeypatch)
    worker.batch_size = 4
    batch_lengths = []
    original_open_batch = processor.open_batch

    def capture_batch(messages, session_text, **kwargs):
        batch_lengths.append(len(messages))
        return original_open_batch(messages, session_text, **kwargs)

    processor.open_batch = capture_batch
    for message_id in range(10):
        await redis.rpush(
            worker._buffer_key,
            json.dumps(
                {"id": message_id, "message": f"message-{message_id}", "role": "user"}
            ),
        )

    assert await worker._drain_buffer(flush_partial=True) is False
    assert batch_lengths == [4, 4, 2]
    assert max(batch_lengths) <= worker.batch_size
    assert await redis.llen(worker._buffer_key) == 0
