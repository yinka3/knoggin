import json

import pytest

from core.ingestion import pipeline as pipeline_service
from core.ingestion.batch import IngestionBatch, IngestionMilestone, IngestionStage
from core.ingestion.pipeline import IngestionPipeline
from core.ingestion.recovery.checkpoint import commit_ingestion_checkpoint
from core.ingestion.recovery.dlq_payload import DLQPayload
from infrastructure.redis_client import RedisKeys
from infrastructure.work_record import WorkRecord
from tests.fixtures.fakes import FakeRedis
from tests.fixtures.ingestion import ingestion_policy


def _graph_committed_batch(batch_id="batch-1") -> IngestionBatch:
    batch = IngestionBatch.open(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        messages=[{"id": 7, "message": "Ada met Grace."}],
        session_text="[USER]: Ada met Grace.",
        policy=ingestion_policy(),
        batch_id=batch_id,
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
    return batch


@pytest.mark.ingestion
@pytest.mark.no_network
def test_dlq_payload_restores_checkpoint_only_replay_state():
    batch = _graph_committed_batch()
    payload = DLQPayload.from_ingestion_batch(batch)

    replay = payload.to_ingestion_batch()

    assert payload.schema_version == 3
    assert IngestionMilestone.GRAPH_COMMITTED in replay.milestones
    assert replay.sealed is True
    assert replay.stage is IngestionStage.GRAPH_COMMITTED
    assert replay.policy == batch.policy

    with pytest.raises(ValueError, match="recorded checkpoint commit result"):
        replay.mark_checkpoint_committed()

    replay.record_checkpoint_progress(current_count=1)
    replay.mark_checkpoint_committed()

    assert replay.stage is IngestionStage.COMMITTED
    assert IngestionMilestone.CHECKPOINT_COMMITTED in replay.milestones


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_checkpoint_dlq_entry_contains_an_aggregate_snapshot(monkeypatch):
    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(pipeline_service, "emit", emit_nothing)
    redis = FakeRedis()
    pipeline = object.__new__(IngestionPipeline)
    pipeline.user_name = "ada"
    pipeline.project_id = "project-1"
    pipeline.redis = redis
    batch = _graph_committed_batch()

    success = await pipeline.move_to_dead_letter(
        batch.messages,
        "CHECKPOINT_COMMIT_FAILED: ConnectionError",
        stage="checkpoint",
        batch=batch,
        session_id="session-1",
    )

    assert success is True
    entry = json.loads(redis.lists[RedisKeys.dlq("ada", "project-1")][0])
    assert entry["stage"] == "checkpoint"
    payload = DLQPayload.model_validate(entry["batch_result"])
    assert payload.batch_id == batch.batch_id
    assert IngestionMilestone.GRAPH_COMMITTED in payload.milestones


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_checkpoint_commit_is_idempotent_and_retains_counter_progress():
    batch = _graph_committed_batch()
    redis = FakeRedis()

    first = await commit_ingestion_checkpoint(redis, batch)
    replay = DLQPayload.from_ingestion_batch(batch).to_ingestion_batch()
    second = await commit_ingestion_checkpoint(redis, replay)

    assert first.count_before_reset == 1
    assert first.current_count == 1
    assert second == first
    assert replay.checkpoint_interval == replay.policy.checkpoint_interval
    assert replay.checkpoint_count == 1
    assert redis.strings[RedisKeys.checkpoint("ada", "session-1")] == "1"
    assert redis.strings[RedisKeys.last_processed("ada", "session-1")] == "7"
    assert redis.evals[0][1][5] == batch.policy.checkpoint_interval


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_checkpoint_commit_is_idempotent_for_a_fresh_retry_batch():
    first_batch = _graph_committed_batch("batch-1")
    retry_batch = _graph_committed_batch("batch-2")
    redis = FakeRedis()

    first = await commit_ingestion_checkpoint(redis, first_batch)
    retry = await commit_ingestion_checkpoint(redis, retry_batch)

    assert retry == first
    assert redis.strings[RedisKeys.checkpoint("ada", "session-1")] == "1"
