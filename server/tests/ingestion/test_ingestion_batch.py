import asyncio

import pytest

from common.schema.ingestion.contracts import (
    AliasUpdate,
    EntityWrite,
    SkippedRelationship,
)
from core.ingestion.batch import IngestionBatch, IngestionMilestone, IngestionStage
from core.ingestion.services import pipeline_service
from core.ingestion.services.pipeline_service import IngestionPipeline
<<<<<<< HEAD
from infrastructure.work_record import WorkRecord, WorkStatus
from tests.fixtures.ingestion import ingestion_policy
=======
from core.knowledge.entity.profile import EntityProfile
from core.knowledge.entity.resolver import EntityCandidate
from infrastructure.work_record import WorkRecord
>>>>>>> a3bae29b2bb0e50845e24f6919a397b396a54094


def open_batch(**overrides) -> IngestionBatch:
    payload = {
        "user_name": "ada",
        "project_id": "project-1",
        "session_id": "session-1",
        "messages": [{"id": 7, "message": "Ada met Grace."}],
        "session_text": "[USER]: Ada met Grace.",
        "policy": ingestion_policy(),
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
    with pytest.raises(ValueError, match="succeeded or skipped"):
        batch.mark_graph_committed()

    graph_work.mark_running()
    graph_work.mark_skipped("No graph writes")
    batch.mark_graph_committed()

    with pytest.raises(ValueError, match="recorded checkpoint commit result"):
        batch.mark_checkpoint_committed()

    batch.record_checkpoint_progress(current_count=3)
    batch.mark_checkpoint_committed()

    assert batch.stage is IngestionStage.COMMITTED
    assert graph_work.parent_id == batch.work_unit.id
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
    assert batch.work_unit.status is WorkStatus.RUNNING


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_pipeline_cancellation_marks_parent_work_and_propagates(monkeypatch):
    pipeline = object.__new__(IngestionPipeline)
    pipeline.user_name = "ada"
    pipeline.project_id = "project-1"

    async def cancel_extraction(*_args):
        raise asyncio.CancelledError

    async def emit_nothing(*_args, **_kwargs):
        return None

    pipeline._extract_mentions = cancel_extraction
    monkeypatch.setattr(pipeline_service, "emit", emit_nothing)
    batch = open_batch()

    with pytest.raises(asyncio.CancelledError):
        await pipeline.process(batch)

    assert batch.work_unit.status is WorkStatus.CANCELLED
    assert batch.work_unit.summary == "Ingestion processing cancelled"


@pytest.mark.ingestion
@pytest.mark.no_network
def test_ingestion_batch_cancellation_marks_active_child_and_parent_work():
    batch = open_batch()
    batch.work_unit.mark_running()
    graph_work = WorkRecord.for_graph_write(
        batch.scope,
        batch_id=batch.work_unit.id,
    )
    graph_work.mark_running()
    batch.graph_work_unit = graph_work

    batch.cancel_work("Session shutdown")

    assert batch.work_unit.status is WorkStatus.CANCELLED
    assert graph_work.status is WorkStatus.CANCELLED
    assert batch.work_unit.summary == "Session shutdown"
    assert graph_work.summary == "Session shutdown"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolution_handoff_uses_the_batch_contract_keyword():
    class Resolver:
        def __init__(self):
            self.resolution_lock = asyncio.Lock()
            self.registered = []

        async def register_entity(
            self,
            entity_id,
            name,
            aliases,
            entity_type,
            topic,
            **_,
        ):
            self.registered.append((entity_id, name, aliases, entity_type, topic))

    async def next_entity_id():
        return 2

    pipeline = object.__new__(IngestionPipeline)
    pipeline.entities = Resolver()
    pipeline._get_next_ent_id = next_entity_id

    async def candidate_entries(_batch, _mentions):
        return [("candidates", [])]

    pipeline._candidate_entries_for_mentions = candidate_entries
    batch = open_batch()
    batch.validate_input()
    batch.mark_extracted()

    await pipeline._resolve_mentions(batch, [(7, "Ada", "person", "People")])

    assert batch.entity_ids == [2]
    assert batch.new_entity_ids == {2}
    assert batch.entity_message_map == {2: [7]}


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolution_stages_existing_alias_until_graph_commit():
    class Resolver:
        def __init__(self):
            self.resolution_lock = asyncio.Lock()
            self.committed_aliases = []

        async def get_profile(self, _entity_id):
            return EntityProfile(
                canonical_name="Robert Chen",
                entity_type="person",
                topic="People",
                project_id="project-1",
            )

        def validate_existing(self, _canonical_name, _mentions):
            return 101, True, ["Bobby"]

        def commit_new_aliases(self, entity_id, aliases):
            self.committed_aliases.append((entity_id, aliases))

    resolver = Resolver()
    pipeline = object.__new__(IngestionPipeline)
    pipeline.entities = resolver
    pipeline.project_id = "project-1"
    pipeline.user_name = "ada"
    pipeline.resolution_threshold = 0.8
    pipeline._is_profile_visible = lambda _profile: True
    pipeline._should_accept_candidate = lambda *_args, **_kwargs: True

    async def candidate_entries(_batch, _mentions):
        return [
            (
                "candidates",
                [EntityCandidate(entity_id=101, score=1.0, signals={"exact"})],
            )
        ]

    pipeline._candidate_entries_for_mentions = candidate_entries
    batch = open_batch()
    batch.validate_input()
    batch.mark_extracted()

    await pipeline._resolve_mentions(batch, [(7, "Bobby", "person", "People")])

    assert resolver.committed_aliases == []
    assert batch.alias_updates == {101: ["Bobby"]}


@pytest.mark.ingestion
@pytest.mark.no_network
def test_sealing_snapshots_graph_buffers_into_immutable_commands():
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
    safe_entity_ids = {2}
    aliases = [AliasUpdate(entity_id=2, aliases=("Ada",))]
    embedding = [0.1, 0.2]
    skip_metadata = {"entity_a_found": False}
    skipped_relationships = [
        SkippedRelationship(
            entity_a="Ada",
            entity_b="Grace",
            message_id=7,
            reason="entity_missing_or_zombie",
            metadata=skip_metadata,
        )
    ]
    writes = [
        EntityWrite(
            entity_id=2,
            is_new=True,
            canonical_name="Ada",
            entity_type="person",
            confidence=1.0,
            topic="people",
            embedding=embedding,
        )
    ]
    batch.set_graph_write_buffers(
        graph_work_unit=WorkRecord.for_graph_write(batch.scope),
        safe_entity_ids=safe_entity_ids,
        graph_alias_updates=aliases,
        entity_writes=writes,
        relationship_writes=[],
        message_entity_refs=[],
        eligible_messages=[],
        skipped_relationships=skipped_relationships,
        zombie_entity_ids=set(),
        dirty_entity_ids={2},
    )

    safe_entity_ids.add(99)
    aliases.clear()
    embedding.append(0.3)
    skip_metadata["entity_b_found"] = False
    skipped_relationships.clear()
    batch.seal_for_commit()

    assert batch.safe_entity_ids == frozenset({2})
    assert batch.graph_alias_updates == (AliasUpdate(entity_id=2, aliases=("Ada",)),)
    assert batch.entity_writes[0].embedding == (0.1, 0.2)
    assert isinstance(batch.entity_writes, tuple)
    assert batch.skipped_relationships[0].metadata == {"entity_a_found": False}
    with pytest.raises(AttributeError):
        batch.entity_writes[0].embedding.append(0.4)
    with pytest.raises(TypeError):
        batch.skipped_relationships[0].metadata["other"] = True
