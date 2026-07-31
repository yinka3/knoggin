import pytest

from core.ingestion.batch import IngestionBatch, IngestionMilestone, IngestionStage
from core.ingestion.services import pipeline_service
from core.ingestion.services.pipeline_service import IngestionPipeline
from infrastructure.work_record import WorkRecord


def open_batch(**overrides) -> IngestionBatch:
    payload = {
        "user_name": "ada",
        "project_id": "project-1",
        "session_id": "session-1",
        "messages": [{"id": 7, "message": "Ada met Grace."}],
        "session_text": "[USER]: Ada met Grace.",
        "batch_id": "batch-1",
    }
    payload.update(overrides)
    return IngestionBatch.open(**payload)


@pytest.mark.ingestion
@pytest.mark.no_network
def test_ingestion_batch_owns_pipeline_state_without_result_adapters():
    batch = open_batch()

    batch.validate_input()
    batch.mark_extracted()
    batch.set_resolution(
        entity_ids=[2],
        new_entity_ids={2},
        alias_updated_ids=set(),
        entity_message_map={2: [7]},
        alias_updates={},
        candidate_suggestions=[],
    )
    batch.set_relationship_observations([])
    batch.complete()

    assert batch.stage is IngestionStage.COMPLETED
    assert batch.entity_ids == [2]
    assert batch.new_entity_ids == {2}
    assert batch.entity_message_map == {2: [7]}


@pytest.mark.ingestion
@pytest.mark.no_network
def test_ingestion_batch_rejects_invalid_transitions_and_released_mutation():
    batch = open_batch()

    with pytest.raises(ValueError, match="Illegal ingestion transition"):
        batch.advance_to(IngestionStage.RESOLVED)

    batch.release()

    with pytest.raises(RuntimeError, match="released"):
        batch.validate_input()


@pytest.mark.ingestion
@pytest.mark.no_network
def test_ingestion_batch_records_failure_from_any_active_stage():
    batch = open_batch()
    batch.validate_input()

    batch.fail(RuntimeError("resolver unavailable"))

    assert batch.stage is IngestionStage.FAILED
    assert batch.success is False
    assert batch.error == "resolver unavailable"


@pytest.mark.ingestion
@pytest.mark.no_network
def test_ingestion_batch_validates_graph_buffers_without_a_legacy_plan():
    batch = open_batch()
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
    batch.set_graph_write_buffers(
        graph_work_unit=WorkRecord.for_graph_write(batch.scope),
        safe_entity_ids={2},
        graph_alias_updates=[],
        entity_writes=[],
        relationship_writes=[],
        message_entity_refs=[],
        eligible_messages=[],
        skipped_relationships=[],
        zombie_entity_ids=set(),
        dirty_entity_ids={3},
    )

    with pytest.raises(ValueError, match="dirty_entity_ids"):
        batch.validate_graph_writes()


@pytest.mark.ingestion
@pytest.mark.no_network
def test_ingestion_batch_seals_and_commits_only_after_graph_preparation():
    batch = open_batch()
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
    batch.mark_checkpoint_committed()

    assert batch.stage is IngestionStage.COMMITTED
    assert batch.milestones == {
        IngestionMilestone.MESSAGE_LOGS_HANDLED,
        IngestionMilestone.CANDIDATE_SUGGESTIONS_HANDLED,
        IngestionMilestone.GRAPH_COMMITTED,
        IngestionMilestone.CHECKPOINT_COMMITTED,
    }


@pytest.mark.ingestion
@pytest.mark.no_network
def test_ingestion_batch_rejects_stale_validation_and_mutation_after_sealing():
    batch = open_batch()
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

    with pytest.raises(RuntimeError, match="sealed"):
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

    batch.validated_revision = batch.revision - 1
    with pytest.raises(ValueError, match="validation is stale"):
        batch.mark_graph_committed()


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_pipeline_process_mutates_the_supplied_ingestion_batch(monkeypatch):
    pipeline = object.__new__(IngestionPipeline)
    pipeline.user_name = "ada"
    pipeline.project_id = "project-1"

    async def extract_mentions(*_args):
        return [(7, "Ada", "person", "People")]

    async def resolve_mentions(batch, *_args):
        batch.set_resolution(
            entity_ids=[2],
            new_entity_ids={2},
            alias_updated_ids=set(),
            entity_message_map={2: [7]},
            alias_updates={},
            candidate_suggestions=[],
        )

    async def extract_connections(*_args):
        return []

    async def emit_nothing(*_args, **_kwargs):
        return None

    pipeline._extract_mentions = extract_mentions
    pipeline._resolve_mentions = resolve_mentions
    pipeline._extract_connections = extract_connections
    monkeypatch.setattr(pipeline_service, "emit", emit_nothing)
    batch = open_batch()

    await pipeline.process(batch)

    assert batch.stage is IngestionStage.COMPLETED
    assert batch.entity_ids == [2]
    assert batch.new_entity_ids == {2}
    assert batch.entity_message_map == {2: [7]}
