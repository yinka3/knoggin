import asyncio

import pytest

from common.schema.ingestion.contracts import EntityWrite
from core.ingestion import pipeline as pipeline_service
from core.ingestion.batch import IngestionBatch, IngestionStage
from core.ingestion.pipeline import IngestionPipeline
from core.knowledge.entity.profile import EntityProfile
from core.knowledge.entity.resolver import EntityCandidate
from infrastructure.work_record import WorkStatus
from tests.fixtures.ingestion import ingestion_policy


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


def resolve_empty(batch: IngestionBatch) -> None:
    batch.validate_input()
    batch.mark_extracted()
    batch.set_resolution(
        entity_ids=[],
        new_entity_ids=set(),
        alias_updated_ids=set(),
        entity_message_map={},
        alias_updates={},
    )


@pytest.mark.ingestion
@pytest.mark.no_network
def test_ingestion_batch_owns_only_in_memory_learning_state():
    batch = open_batch()

    resolve_empty(batch)
    batch.set_relationship_observations([])
    batch.complete()

    assert batch.stage is IngestionStage.COMPLETED
    assert batch.batch_id == "batch-1"
    assert batch.entity_message_map == {}
    assert not hasattr(batch, "milestones")
    assert not hasattr(batch, "candidate_suggestions")
    assert not hasattr(batch, "graph_work_unit")


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
async def test_resolution_handoff_keeps_new_entities_batch_local():
    class Resolver:
        def __init__(self):
            self.resolution_lock = asyncio.Lock()

        async def candidate_entries_for_mentions(self, _mentions, **_kwargs):
            return [("candidates", [])]

        @staticmethod
        def mention_dedupe_key(name, entity_type, topic, _policy):
            return name, entity_type, topic

        async def prepare_pending_entity(
            self, entity_id, name, aliases, entity_type, topic, **_
        ):
            return EntityWrite(
                entity_id=entity_id,
                is_new=True,
                canonical_name=name,
                entity_type=entity_type,
                topic=topic,
                embedding=None,
                aliases=tuple(aliases),
            )

    async def next_entity_id():
        return 2

    pipeline = object.__new__(IngestionPipeline)
    pipeline.entities = Resolver()
    pipeline._get_next_ent_id = next_entity_id
    batch = open_batch()
    batch.validate_input()
    batch.mark_extracted()

    await pipeline._resolve_mentions(batch, [(7, "Ada", "person", "People")])

    assert batch.entity_ids == [2]
    assert batch.new_entity_ids == {2}
    assert batch.pending_entity_writes[2].canonical_name == "Ada"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_resolution_stages_existing_alias_until_durable_commit():
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

        async def candidate_entries_for_mentions(self, _mentions, **_kwargs):
            return [
                (
                    "candidates",
                    [EntityCandidate(entity_id=101, score=1.0, signals={"exact"})],
                )
            ]

        @staticmethod
        def mention_dedupe_key(name, entity_type, topic, _policy):
            return name, entity_type, topic

        @staticmethod
        def schema_compatibility(*_args):
            return "compatible"

        @staticmethod
        def is_profile_visible(_profile):
            return True

        @staticmethod
        def should_accept_candidate(*_args, **_kwargs):
            return True

        def validate_existing(self, _canonical_name, _mentions):
            return 101, True, ["Bobby"]

        def commit_new_aliases(self, entity_id, aliases):
            self.committed_aliases.append((entity_id, aliases))

    resolver = Resolver()
    pipeline = object.__new__(IngestionPipeline)
    pipeline.entities = resolver
    batch = open_batch()
    batch.validate_input()
    batch.mark_extracted()

    await pipeline._resolve_mentions(batch, [(7, "Bobby", "person", "People")])

    assert resolver.committed_aliases == []
    assert batch.alias_updates == {101: ["Bobby"]}
