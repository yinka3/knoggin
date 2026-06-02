import pytest

from common.schema.contracts import (
    BatchResult,
    EngineScope,
    EngineWorkUnit,
    GraphMutationPlan,
    MessageConnections,
    MessageUserConnections,
)
from common.schema.primitives import ConnectionRecord
from common.schema.contracts import UserConnectionRecord
from common.scoping import IDENTITY_ENTITY_ID
from infrastructure.redis_client import RedisKeys
from knoggin_server.knowledge.db import write_graph_db
from knoggin_server.knowledge.db.write_graph_db import (
    build_graph_mutation_plan,
    execute_graph_mutation_plan,
    write_batch_callback,
    write_batch_to_graph,
)
from tests.fixtures.fakes import FakeRedis


class VectorLike:
    def __init__(self, values):
        self.values = values

    def tolist(self):
        return list(self.values)


class FakeGraphMutationClient:
    def __init__(self, validation_result="all", fail_on_write=False):
        self.validation_result = validation_result
        self.fail_on_write = fail_on_write
        self.validate_calls = []
        self.update_alias_calls = []
        self.write_batch_calls = []
        self.call_order = []

    async def validate_existing_ids(self, ids):
        self.validate_calls.append(list(ids))
        if self.validation_result == "all":
            return set(ids)
        return self.validation_result

    async def update_entity_aliases(self, alias_updates, project_id=None):
        self.call_order.append("update_entity_aliases")
        self.update_alias_calls.append((dict(alias_updates), project_id))

    async def write_batch(self, entities, relationships):
        self.call_order.append("write_batch")
        if self.fail_on_write:
            raise RuntimeError("graph write failed")
        self.write_batch_calls.append((list(entities), list(relationships)))
        return True


class FakeEntityManagerForPlan:
    def __init__(self, project_id="project-1"):
        self.project_id = project_id
        self.entity_profiles = {
            2: {
                "canonical_name": "Ada Lovelace",
                "type": "person",
                "topic": "Identity",
                "session_id": "profile-session",
                "project_id": "profile-project",
            },
            3: {
                "canonical_name": "Grace Hopper",
                "type": "person",
                "topic": "Work",
            },
            4: {
                "canonical_name": "Compiler",
                "type": "concept",
                "topic": "Work",
            },
            5: {
                "canonical_name": "Zombie",
                "type": "concept",
                "topic": "Archive",
            },
        }
        self.mentions = {
            2: ["Ada", "Analyst"],
            3: ["Amazing Grace"],
            4: ["Target Alias"],
            5: ["Zombie Alias"],
        }
        self.embeddings = {
            2: VectorLike([0.2, 0.1]),
            3: [0.3, 0.4],
            4: [0.5, 0.6],
            5: [0.7, 0.8],
        }
        self.removed_entities = []

    def get_mentions_for_id(self, entity_id):
        return list(self.mentions.get(entity_id, []))

    async def get_embedding_for_id(self, entity_id):
        return self.embeddings.get(entity_id)

    def remove_entities(self, entity_ids):
        self.removed_entities.append(list(entity_ids))


def scoped_batch(**kwargs):
    batch = BatchResult(**kwargs)
    batch.set_scope("ada", "session-1", "project-1")
    return batch


def connection(entity_a, entity_b, msg_id=7, confidence=0.8, context="related"):
    return ConnectionRecord(
        msg_id=msg_id,
        entity_a=entity_a,
        entity_b=entity_b,
        relationship="related_to",
        confidence=confidence,
        context=context,
    )


def user_connection(entity_name, msg_id=8, confidence=0.7, context="user link"):
    return UserConnectionRecord(
        msg_id=msg_id,
        entity_name=entity_name,
        relationship="cares_about",
        confidence=confidence,
        context=context,
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_mutation_plan_uses_batch_scope_over_fallback_scope():
    batch = scoped_batch(new_entity_ids={2})
    entities = FakeEntityManagerForPlan()
    graph = FakeGraphMutationClient()

    plan = await build_graph_mutation_plan(
        batch,
        graph,
        entities,
        session_id="fallback-session",
        project_id="fallback-project",
        user_name="fallback-user",
    )

    assert plan.scope.user_name == "ada"
    assert plan.scope.session_id == "session-1"
    assert plan.scope.project_id == "project-1"
    assert plan.entity_writes[0].user_name == "ada"


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_mutation_plan_uses_fallback_scope_when_batch_scope_absent():
    batch = BatchResult(new_entity_ids={2})
    entities = FakeEntityManagerForPlan(project_id=None)
    graph = FakeGraphMutationClient()

    plan = await build_graph_mutation_plan(
        batch,
        graph,
        entities,
        session_id="fallback-session",
        project_id="fallback-project",
        user_name="fallback-user",
    )

    assert plan.scope == EngineScope(
        user_name="fallback-user",
        session_id="fallback-session",
        project_id="fallback-project",
    )
    assert plan.entity_writes[0].project_id == "profile-project"


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_mutation_plan_requires_complete_scope():
    batch = BatchResult(new_entity_ids={2})

    with pytest.raises(ValueError, match="Graph batch write missing required scope"):
        await build_graph_mutation_plan(
            batch,
            FakeGraphMutationClient(),
            FakeEntityManagerForPlan(),
            session_id="session-1",
            project_id=None,
            user_name="ada",
        )


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_mutation_plan_builds_writes_and_filters_zombies():
    batch = scoped_batch(
        entity_ids=[2, 3, 4, 5],
        new_entity_ids={2},
        alias_updated_ids={3, 5},
        alias_updates={3: ["Rear Admiral"], 5: ["Do Not Persist"]},
        relationship_observations=[
            MessageConnections(
                message_id=7,
                entity_pairs=[
                    connection("analyst", "GRACE HOPPER"),
                    connection("Ada Lovelace", "Zombie", context="skip zombie"),
                    connection("Unknown", "Compiler", context="skip missing"),
                ],
            )
        ],
        user_relationship_observations=[
            MessageUserConnections(
                message_id=8,
                user_connections=[
                    user_connection("target alias"),
                    user_connection("Missing Target", context="skip user missing"),
                ],
            )
        ],
    )
    entities = FakeEntityManagerForPlan()
    graph = FakeGraphMutationClient(validation_result={3, 4})

    plan = await build_graph_mutation_plan(
        batch,
        graph,
        entities,
        session_id="fallback-session",
        project_id="fallback-project",
        user_name="fallback-user",
    )

    assert set(graph.validate_calls[0]) == {3, 4, 5}
    assert plan.safe_entity_ids == {2, 3, 4}
    assert plan.zombie_entity_ids == {5}
    assert entities.removed_entities == [[5]]

    writes_by_id = {write.id: write for write in plan.entity_writes}
    assert set(writes_by_id) == {2, 3}
    assert writes_by_id[2].canonical_name == "Ada Lovelace"
    assert writes_by_id[2].aliases == ["Ada", "Analyst"]
    assert writes_by_id[2].embedding == [0.2, 0.1]
    assert writes_by_id[2].session_id == "profile-session"
    assert writes_by_id[2].project_id == "profile-project"
    assert writes_by_id[3].session_id == "session-1"
    assert writes_by_id[3].project_id == "project-1"

    assert [(update.entity_id, update.aliases) for update in plan.alias_updates] == [
        (3, ["Rear Admiral"])
    ]

    assert len(plan.relationship_writes) == 1
    rel = plan.relationship_writes[0]
    assert rel.entity_a == "Ada Lovelace"
    assert rel.entity_b == "Grace Hopper"
    assert rel.entity_a_id == 2
    assert rel.entity_b_id == 3
    assert rel.message_id == "msg_7"
    assert rel.evidence_ref == {
        "user_name": "ada",
        "session_id": "session-1",
        "message_id": 7,
    }
    assert rel.confidence == 0.8
    assert rel.context == "related"

    assert len(plan.user_relationship_writes) == 1
    user_rel = plan.user_relationship_writes[0]
    assert user_rel.user_entity_id == IDENTITY_ENTITY_ID
    assert user_rel.entity_name == "Compiler"
    assert user_rel.entity_id == 4
    assert user_rel.message_id == "msg_8"
    assert user_rel.evidence_ref["message_id"] == 8

    assert len(plan.skipped_relationships) == 3
    assert [skip.reason for skip in plan.skipped_relationships] == [
        "entity_missing_or_zombie",
        "entity_missing_or_zombie",
        "user_target_missing_or_zombie",
    ]
    assert plan.skipped_relationships[0].metadata == {
        "entity_a_found": True,
        "entity_b_found": False,
    }
    assert plan.dirty_entity_ids == {2, 3, 4}


@pytest.mark.storage
@pytest.mark.no_network
async def test_graph_mutation_plan_treats_unknown_validation_as_valid():
    batch = scoped_batch(entity_ids=[3], alias_updated_ids={3})
    entities = FakeEntityManagerForPlan()
    graph = FakeGraphMutationClient(validation_result=None)

    plan = await build_graph_mutation_plan(
        batch,
        graph,
        entities,
        session_id="session-1",
        project_id="project-1",
        user_name="ada",
    )

    assert plan.safe_entity_ids == {3}
    assert plan.zombie_entity_ids == set()
    assert entities.removed_entities == []
    assert [write.id for write in plan.entity_writes] == [3]


@pytest.mark.storage
@pytest.mark.no_network
async def test_execute_graph_mutation_plan_orders_calls_and_marks_dirty_entities():
    batch = scoped_batch(
        entity_ids=[2, 3, 4],
        new_entity_ids={2},
        alias_updated_ids={3},
        alias_updates={3: ["Rear Admiral"]},
        relationship_observations=[
            MessageConnections(
                message_id=7,
                entity_pairs=[connection("Ada", "Grace Hopper")],
            )
        ],
        user_relationship_observations=[
            MessageUserConnections(
                message_id=8,
                user_connections=[user_connection("Compiler")],
            )
        ],
    )
    graph = FakeGraphMutationClient()
    entities = FakeEntityManagerForPlan()
    redis = FakeRedis()
    profile_key = RedisKeys.profile_complete("ada", "project-1")
    await redis.set(profile_key, "done")
    plan = await build_graph_mutation_plan(
        batch,
        graph,
        entities,
        session_id="session-1",
        project_id="project-1",
        user_name="ada",
    )

    summary = await execute_graph_mutation_plan(plan, graph, redis)

    assert graph.call_order == ["update_entity_aliases", "write_batch"]
    assert graph.update_alias_calls == [({3: ["Rear Admiral"]}, "project-1")]
    entity_payloads, relationship_payloads = graph.write_batch_calls[0]
    assert [payload["id"] for payload in entity_payloads] == [2, 3]
    assert len(relationship_payloads) == 2
    assert relationship_payloads[0]["entity_a"] == "Ada Lovelace"
    assert relationship_payloads[1]["entity_a"] == "ada"
    assert relationship_payloads[1]["entity_a_id"] == IDENTITY_ENTITY_ID

    dirty_key = RedisKeys.dirty_entities("ada", "project-1")
    assert redis.sets[dirty_key] == {"2", "3", "4"}
    assert profile_key in redis.deleted_keys
    assert summary.model_dump() == {
        "entities_written": 2,
        "relationships_written": 2,
        "user_relationships_written": 1,
        "aliases_updated": 1,
        "dirty_entities_marked": 3,
        "zombies_filtered": 0,
        "relationships_skipped": 0,
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_write_batch_to_graph_marks_skipped_work_unit_metadata():
    scope = EngineScope(user_name="ada", session_id="session-1", project_id="project-1")
    batch = BatchResult(work_unit=EngineWorkUnit.for_message_batch(scope, [7]))

    summary = await write_batch_to_graph(
        batch,
        FakeGraphMutationClient(),
        FakeEntityManagerForPlan(),
        session_id="session-1",
        project_id="project-1",
        user_name="ada",
    )

    assert summary.model_dump() == {
        "entities_written": 0,
        "relationships_written": 0,
        "user_relationships_written": 0,
        "aliases_updated": 0,
        "dirty_entities_marked": 0,
        "zombies_filtered": 0,
        "relationships_skipped": 0,
    }
    graph_work = batch.work_unit.metadata["graph_write_work_unit"]
    assert graph_work["kind"] == "graph_write"
    assert graph_work["status"] == "skipped"
    assert graph_work["trace"]["summary"] == "No graph writes"
    assert batch.work_unit.metadata["graph_write"] == summary.model_dump()


@pytest.mark.storage
@pytest.mark.no_network
async def test_write_batch_to_graph_marks_success_and_attaches_summary_metadata():
    scope = EngineScope(user_name="ada", session_id="session-1", project_id="project-1")
    batch = BatchResult(
        work_unit=EngineWorkUnit.for_message_batch(scope, [7]),
        scope=scope,
        new_entity_ids={2},
    )
    graph = FakeGraphMutationClient()

    summary = await write_batch_to_graph(
        batch,
        graph,
        FakeEntityManagerForPlan(),
        session_id="fallback-session",
        project_id="fallback-project",
        user_name="fallback-user",
    )

    assert summary.entities_written == 1
    assert graph.write_batch_calls
    graph_work = batch.work_unit.metadata["graph_write_work_unit"]
    assert graph_work["status"] == "succeeded"
    assert graph_work["metadata"]["batch_work_unit_id"] == batch.work_unit.id
    assert batch.work_unit.metadata["graph_write"]["entities_written"] == 1


@pytest.mark.storage
@pytest.mark.no_network
async def test_write_batch_to_graph_marks_failed_plan_when_execution_raises(monkeypatch):
    scope = EngineScope(user_name="ada", session_id="session-1", project_id="project-1")
    batch = BatchResult(scope=scope, new_entity_ids={2})
    observed_plan = GraphMutationPlan(
        work_unit=EngineWorkUnit.for_graph_write(scope),
        scope=scope,
        entity_writes=[
            {
                "id": 2,
                "canonical_name": "Ada Lovelace",
                "type": "person",
                "confidence": 1.0,
                "topic": "Identity",
                "embedding": [0.2, 0.1],
                "aliases": ["Ada"],
                "user_name": "ada",
                "session_id": "session-1",
                "project_id": "project-1",
            }
        ],
    )

    async def fake_build(*args, **kwargs):
        return observed_plan

    async def fail_execute(*args, **kwargs):
        raise RuntimeError("execute failed")

    monkeypatch.setattr(write_graph_db, "build_graph_mutation_plan", fake_build)
    monkeypatch.setattr(write_graph_db, "execute_graph_mutation_plan", fail_execute)

    with pytest.raises(RuntimeError, match="execute failed"):
        await write_graph_db.write_batch_to_graph(
            batch,
            FakeGraphMutationClient(),
            FakeEntityManagerForPlan(),
            session_id="session-1",
            project_id="project-1",
            user_name="ada",
        )

    assert observed_plan.work_unit.status == "failed"
    assert observed_plan.work_unit.trace.summary == "execute failed"


@pytest.mark.storage
@pytest.mark.no_network
async def test_write_batch_callback_returns_success_for_no_graph_writes():
    graph = FakeGraphMutationClient()

    assert await write_batch_callback(
        BatchResult(),
        graph,
        FakeEntityManagerForPlan(),
        session_id="session-1",
        project_id="project-1",
        user_name="ada",
    ) == (True, None)
    assert graph.write_batch_calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_write_batch_callback_removes_phantom_new_entities_on_failure():
    batch = scoped_batch(new_entity_ids={2})
    entities = FakeEntityManagerForPlan()
    graph = FakeGraphMutationClient(fail_on_write=True)

    success, error = await write_batch_callback(
        batch,
        graph,
        entities,
        session_id="session-1",
        project_id="project-1",
        user_name="ada",
    )

    assert success is False
    assert error == "graph write failed"
    assert entities.removed_entities == [[2]]
