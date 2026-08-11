import asyncio
import json

import pytest

from common.schema.contracts import CandidateSuggestion
from common.schema.settings import IngestionSettings
from core.ingestion.batch import IngestionBatch, IngestionMilestone
from core.ingestion.jobs.dlq_job import DLQReplayJob
from core.ingestion.services import batch_consumer
from core.ingestion.services.batch_consumer import IngestionWorker
from infrastructure.job.base import JobContext
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
    def __init__(self):
        self.dlq_call = None

    async def move_to_dead_letter(self, *args, **kwargs):
        self.dlq_call = (args, kwargs)
        return True


class _DrainProcessor(_Processor):
    def __init__(self, outcome: str):
        self.outcome = outcome
        self.batch = None
        self.dlq_call = None
        self.dlq_message_ids = None

    def capture_policy(self, _ingestion_settings):
        return ingestion_policy()

    def open_batch(self, messages, session_text, *, session_id, policy):
        self.batch = IngestionBatch.open(
            user_name="ada",
            project_id=self.project_id,
            session_id=session_id,
            messages=messages,
            session_text=session_text,
            policy=policy,
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
        return True


def _drain_worker(processor: _DrainProcessor) -> IngestionWorker:
    worker = object.__new__(IngestionWorker)
    worker.user_name = "ada"
    worker.session_id = "session-1"
    worker.processor = processor
    worker.redis = FakeRedis()
    worker.batch_size = 1
    worker.session_window = 10
    worker.checkpoint_interval = 4
    worker.settings = IngestionSettings(checkpoint_interval=4)

    async def get_session_context(_window, _before_message_id):
        return []

    async def durable_stage(_batch):
        return True, False

    async def checkpoint_stage(_batch):
        return None

    worker.get_session_context = get_session_context
    worker._save_message_logs_or_dlq = durable_stage
    worker._save_candidate_suggestions_or_dlq = durable_stage
    worker._write_graph_or_dlq = durable_stage
    worker._mark_batch_processed = checkpoint_stage
    return worker


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

    monkeypatch.setattr(batch_consumer, "emit", emit_nothing)
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

    monkeypatch.setattr(batch_consumer.asyncio, "wait_for", record_wait_for)
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

    monkeypatch.setattr(batch_consumer, "emit", emit_nothing)
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

    monkeypatch.setattr(batch_consumer, "emit", emit_nothing)
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

    monkeypatch.setattr(batch_consumer, "emit", emit_nothing)
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
