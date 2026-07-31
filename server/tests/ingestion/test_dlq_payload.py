import pytest

from core.ingestion.batch import IngestionBatch, IngestionMilestone, IngestionStage
from core.ingestion.dlq_payload import DLQPayload
from infrastructure.work_record import WorkRecord


def _graph_committed_batch() -> IngestionBatch:
    batch = IngestionBatch.open(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        messages=[{"id": 7, "message": "Ada met Grace."}],
        session_text="[USER]: Ada met Grace.",
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
    batch.mark_graph_committed()
    return batch


@pytest.mark.ingestion
@pytest.mark.no_network
def test_dlq_payload_restores_checkpoint_only_replay_state():
    payload = DLQPayload.from_ingestion_batch(_graph_committed_batch())

    replay = payload.to_ingestion_batch()

    assert payload.schema_version == 2
    assert IngestionMilestone.GRAPH_COMMITTED in replay.milestones
    assert replay.sealed is True
    assert replay.stage is IngestionStage.GRAPH_COMMITTED

    replay.mark_checkpoint_committed()

    assert replay.stage is IngestionStage.COMMITTED
    assert IngestionMilestone.CHECKPOINT_COMMITTED in replay.milestones
