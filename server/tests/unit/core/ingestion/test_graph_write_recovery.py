import pytest

from common.schema.ingestion.contracts import (
    AliasUpdate,
    EntityWrite,
    ExecutionScope,
    GraphWriteSummary,
)
from core.ingestion import graph_commit as write_graph_db
from core.ingestion.batch import IngestionBatch
from infrastructure.work_record import WorkRecord
from tests.fixtures.ingestion import ingestion_policy


class _Resolver:
    def __init__(self):
        self.committed_aliases = []
        self.removed_entities = []

    def commit_new_aliases(self, entity_id, aliases):
        self.committed_aliases.append((entity_id, aliases))

    def remove_entities(self, entity_ids):
        self.removed_entities.append(list(entity_ids))

    def apply_committed_entity_writes(self, _entity_writes):
        return None


class _Store:
    def __init__(self, *, fail_alias=False, fail_graph=False):
        self.fail_alias = fail_alias
        self.fail_graph = fail_graph
        self.events = []

    async def update_entity_aliases(self, aliases, *, project_id):
        self.events.append(("aliases", aliases, project_id))
        if self.fail_alias:
            raise RuntimeError("alias persistence unavailable")

    async def write_batch(self, *args, **kwargs):
        self.events.append(("graph", args, kwargs))
        if self.fail_graph:
            raise RuntimeError("graph persistence unavailable")


class _CommitStore:
    def __init__(self, *, fail=False):
        self.fail = fail
        self.commits = []

    async def commit_ingestion(self, commit):
        self.commits.append(commit)
        if self.fail:
            raise RuntimeError("graph persistence unavailable")
        return GraphWriteSummary(aliases_updated=1)


class _Redis:
    def __init__(self, *, fail_dirty=False):
        self.fail_dirty = fail_dirty
        self.events = []

    async def sadd(self, key, *values):
        self.events.append(("sadd", key, values))
        if self.fail_dirty:
            raise RuntimeError("dirty marking unavailable")
        return len(values)

    async def delete(self, key):
        self.events.append(("delete", key))
        return 1


def _prepared_batch() -> IngestionBatch:
    batch = IngestionBatch.open(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        messages=[{"id": 7, "message": "Bobby joined the project."}],
        session_text="[USER]: Bobby joined the project.",
        policy=ingestion_policy(),
    )
    batch.validate_input()
    batch.mark_extracted()
    batch.set_resolution(
        entity_ids=[101],
        new_entity_ids=set(),
        alias_updated_ids={101},
        entity_message_map={101: [7]},
        alias_updates={101: ["Bobby"]},
        candidate_suggestions=[],
    )
    batch.set_relationship_observations([])
    batch.complete()
    batch.set_graph_write_buffers(
        graph_work_unit=WorkRecord.for_graph_write(batch.scope),
        safe_entity_ids={101},
        graph_alias_updates=[AliasUpdate(entity_id=101, aliases=("Bobby",))],
        entity_writes=[],
        relationship_writes=[],
        message_entity_refs=[],
        eligible_messages=[],
        skipped_relationships=[],
        zombie_entity_ids=set(),
        dirty_entity_ids=set(),
    )
    batch.new_entity_ids = {101}
    return batch


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_graph_write_applies_staged_aliases_after_durable_success(monkeypatch):
    async def prepare(_batch, _store, _entities):
        return None

    async def commit(**_kwargs):
        return GraphWriteSummary(aliases_updated=1)

    monkeypatch.setattr(write_graph_db, "prepare_ingestion_batch_graph_writes", prepare)
    monkeypatch.setattr(write_graph_db, "_commit_ingestion_buffers", commit)
    resolver = _Resolver()

    await write_graph_db.write_ingestion_batch_to_graph(
        _prepared_batch(),
        knowledge_store=object(),
        entities=resolver,
    )

    assert resolver.committed_aliases == [(101, ["Bobby"])]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_graph_write_does_not_apply_staged_aliases_when_persistence_fails(
    monkeypatch,
):
    async def prepare(_batch, _store, _entities):
        return None

    async def commit(**_kwargs):
        raise RuntimeError("graph unavailable")

    monkeypatch.setattr(write_graph_db, "prepare_ingestion_batch_graph_writes", prepare)
    monkeypatch.setattr(write_graph_db, "_commit_ingestion_buffers", commit)
    resolver = _Resolver()

    with pytest.raises(RuntimeError, match="graph unavailable"):
        await write_graph_db.write_ingestion_batch_to_graph(
            _prepared_batch(),
            knowledge_store=object(),
            entities=resolver,
        )

    assert resolver.committed_aliases == []


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_graph_callback_keeps_durable_success_when_redis_signaling_fails(
    monkeypatch,
):
    async def prepare(_batch, _store, _entities):
        return None

    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(write_graph_db, "prepare_ingestion_batch_graph_writes", prepare)
    monkeypatch.setattr(write_graph_db, "emit", emit_nothing)
    resolver = _Resolver()

    success, error = await write_graph_db.write_batch_callback(
        _prepared_batch(),
        knowledge_store=_CommitStore(),
        entities=resolver,
        session_id="session-1",
        project_id="project-1",
        redis_client=_Redis(fail_dirty=True),
    )

    assert (success, error) == (True, None)
    assert resolver.committed_aliases == [(101, ["Bobby"])]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_graph_callback_discards_pending_entities_after_graph_failure(
    monkeypatch,
):
    async def prepare(_batch, _store, _entities):
        return None

    monkeypatch.setattr(write_graph_db, "prepare_ingestion_batch_graph_writes", prepare)
    resolver = _Resolver()

    success, error = await write_graph_db.write_batch_callback(
        _prepared_batch(),
        knowledge_store=_CommitStore(fail=True),
        entities=resolver,
        session_id="session-1",
        project_id="project-1",
    )

    assert (success, error) == (False, "graph persistence unavailable")
    assert resolver.removed_entities == []


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_graph_write_replay_retries_alias_and_graph_boundaries(monkeypatch):
    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(write_graph_db, "emit", emit_nothing)
    store = _Store(fail_graph=True)
    scope = ExecutionScope(
        user_name="ada", project_id="project-1", session_id="session-1"
    )
    kwargs = {
        "scope": scope,
        "alias_updates": [AliasUpdate(entity_id=101, aliases=("Bobby",))],
        "entity_writes": [
            EntityWrite(
                entity_id=101,
                is_new=False,
                canonical_name="Robert Chen",
                entity_type="person",
                topic="Identity",
                embedding=None,
            )
        ],
        "relationship_writes": [],
        "message_entity_refs": [],
        "eligible_messages": [],
        "dirty_entity_ids": set(),
        "zombie_entity_ids": set(),
        "skipped_relationships": [],
        "knowledge_store": store,
    }

    with pytest.raises(RuntimeError, match="graph persistence unavailable"):
        await write_graph_db._execute_graph_write_buffers(**kwargs)

    store.fail_graph = False
    summary = await write_graph_db._execute_graph_write_buffers(**kwargs)

    assert summary.aliases_updated == 1
    assert [event[0] for event in store.events] == [
        "aliases",
        "graph",
        "aliases",
        "graph",
    ]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_graph_write_replay_retries_after_alias_persistence_failure(
    monkeypatch,
):
    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(write_graph_db, "emit", emit_nothing)
    store = _Store(fail_alias=True)
    scope = ExecutionScope(
        user_name="ada", project_id="project-1", session_id="session-1"
    )
    kwargs = {
        "scope": scope,
        "alias_updates": [AliasUpdate(entity_id=101, aliases=("Bobby",))],
        "entity_writes": [],
        "relationship_writes": [],
        "message_entity_refs": [],
        "eligible_messages": [],
        "dirty_entity_ids": set(),
        "zombie_entity_ids": set(),
        "skipped_relationships": [],
        "knowledge_store": store,
    }

    with pytest.raises(RuntimeError, match="alias persistence unavailable"):
        await write_graph_db._execute_graph_write_buffers(**kwargs)

    store.fail_alias = False
    summary = await write_graph_db._execute_graph_write_buffers(**kwargs)

    assert summary.aliases_updated == 1
    assert [event[0] for event in store.events] == ["aliases", "aliases"]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_graph_write_replay_retries_dirty_marking_after_postgres_success(
    monkeypatch,
):
    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(write_graph_db, "emit", emit_nothing)
    store = _Store()
    redis = _Redis(fail_dirty=True)
    scope = ExecutionScope(
        user_name="ada", project_id="project-1", session_id="session-1"
    )
    kwargs = {
        "scope": scope,
        "alias_updates": [],
        "entity_writes": [
            EntityWrite(
                entity_id=101,
                is_new=False,
                canonical_name="Robert Chen",
                entity_type="person",
                topic="Identity",
                embedding=None,
            )
        ],
        "relationship_writes": [],
        "message_entity_refs": [],
        "eligible_messages": [],
        "dirty_entity_ids": {101},
        "zombie_entity_ids": set(),
        "skipped_relationships": [],
        "knowledge_store": store,
        "redis_client": redis,
    }

    with pytest.raises(RuntimeError, match="dirty marking unavailable"):
        await write_graph_db._execute_graph_write_buffers(**kwargs)

    redis.fail_dirty = False
    summary = await write_graph_db._execute_graph_write_buffers(**kwargs)

    assert summary.dirty_entities_marked == 1
    assert [event[0] for event in redis.events] == [
        "sadd",
        "sadd",
        "delete",
    ]
