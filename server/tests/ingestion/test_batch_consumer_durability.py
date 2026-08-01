import json

import pytest

from common.schema.ingestion.contracts import CandidateSuggestion
from core.ingestion.batch import IngestionBatch, IngestionMilestone
from core.ingestion.services import batch_consumer
from core.ingestion.services.batch_consumer import IngestionWorker
from infrastructure.redis_client import RedisKeys
from infrastructure.work_record import WorkRecord
from tests.fixtures.fakes import FakeRedis


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


def _completed_batch() -> IngestionBatch:
    batch = IngestionBatch.open(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        messages=[{"id": 7, "message": "Ada met Grace."}],
        session_text="[USER]: Ada met Grace.",
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
    worker._processed_batches = 0

    async def context(_window, _before_message_id):
        return []

    async def process(messages, session_text):
        worker._processed_batches += 1
        return _worker_batch(messages, session_text)

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
    worker._process_messages = process
    worker.write_to_graph = write_graph
    monkeypatch.setattr(batch_consumer, "emit", emit_nothing)
    return worker


async def _queue_one_message(worker):
    await worker.redis.rpush(
        worker._buffer_key,
        json.dumps({"id": 7, "message": "Ada met Grace.", "role": "user"}),
    )


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_checkpoint_failure_dlqs_and_cleans_buffer(monkeypatch):
    redis = _FailingCheckpointRedis()
    processor = _DLQProcessor()
    worker = _configure_worker(redis, processor, monkeypatch)
    await _queue_one_message(worker)

    assert await worker._drain_buffer(flush_partial=True) is False
    assert await redis.llen(worker._buffer_key) == 0
    assert processor.dlq_call[1]["stage"] == "checkpoint"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_checkpoint_failure_leaves_buffer_when_dlq_write_fails(monkeypatch):
    redis = _FailingCheckpointRedis()
    processor = _DLQProcessor(dlq_success=False)
    worker = _configure_worker(redis, processor, monkeypatch)
    await _queue_one_message(worker)

    assert await worker._drain_buffer(flush_partial=True) is False
    assert await redis.llen(worker._buffer_key) == 1
    assert processor.dlq_call[1]["stage"] == "checkpoint"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_buffer_cleanup_failure_retries_without_double_checkpoint(monkeypatch):
    redis = _FailingLtrimRedis()
    processor = _DLQProcessor()
    worker = _configure_worker(redis, processor, monkeypatch)
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
