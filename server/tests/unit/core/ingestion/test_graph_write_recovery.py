import pytest

from common.schema.ingestion.contracts import GraphWriteSummary
from core.ingestion.batch import IngestionBatch
from core.ingestion.graph_commit import write_ingestion_batch_to_graph
from core.knowledge.entity.profile import EntityProfile
from infrastructure.work_record import WorkStatus
from tests.fixtures.ingestion import ingestion_policy


class _Resolver:
    def __init__(self):
        self.applied = []
        self.committed_aliases = []

    def get_cached_profile(self, entity_id):
        assert entity_id == 101
        return EntityProfile(
            canonical_name="Robert Chen",
            entity_type="person",
            topic="People",
            project_id="project-1",
        )

    async def get_embedding_for_id(self, _entity_id):
        return None

    def get_mentions_for_id(self, _entity_id):
        return ["Robert Chen", "Bobby"]

    def apply_committed_entity_writes(self, writes):
        self.applied.extend(writes)

    def commit_new_aliases(self, entity_id, aliases):
        self.committed_aliases.append((entity_id, aliases))


class _Store:
    def __init__(self, *, fail=False):
        self.fail = fail
        self.commits = []

    async def validate_existing_ids(self, ids, **_kwargs):
        return set(ids)

    async def commit_ingestion(self, commit):
        self.commits.append(commit)
        if self.fail:
            raise RuntimeError("graph persistence unavailable")
        return GraphWriteSummary(aliases_updated=len(commit.alias_updates))


def _completed_batch() -> IngestionBatch:
    batch = IngestionBatch.open(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        messages=[{"id": 7, "message": "Bobby joined the project."}],
        session_text="[USER]: Bobby joined the project.",
        policy=ingestion_policy(),
    )
    batch.validate_input()
    batch.set_resolution(
        entity_ids=[101],
        new_entity_ids=set(),
        alias_updated_ids={101},
        entity_message_map={101: [7]},
        alias_updates={101: ["Bobby"]},
    )
    batch.set_relationship_observations([])
    return batch


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_graph_commit_builds_one_durable_change_set_then_refreshes_cache():
    store = _Store()
    resolver = _Resolver()

    summary = await write_ingestion_batch_to_graph(
        _completed_batch(),
        knowledge_store=store,
        entities=resolver,
    )

    commit = store.commits[0]
    assert commit.message_ids == (7,)
    assert [(ref.message_id, ref.entity_id) for ref in commit.message_entity_refs] == [
        (7, 101)
    ]
    assert [(update.entity_id, update.aliases) for update in commit.alias_updates] == [
        (101, ("Bobby",))
    ]
    assert resolver.committed_aliases == [(101, ["Bobby"])]
    assert summary.aliases_updated == 1


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_graph_commit_leaves_resolver_unchanged_when_durable_write_fails():
    store = _Store(fail=True)
    resolver = _Resolver()
    batch = _completed_batch()
    batch.work_unit.mark_running()

    with pytest.raises(RuntimeError, match="graph persistence unavailable"):
        await write_ingestion_batch_to_graph(
            batch,
            knowledge_store=store,
            entities=resolver,
        )

    assert resolver.applied == []
    assert resolver.committed_aliases == []
    assert batch.work_unit.status is WorkStatus.RUNNING
