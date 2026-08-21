import asyncio
from types import SimpleNamespace

import pytest

from common.schema.ingestion.contracts import (
    AliasUpdate,
    EntityWrite,
    EpisodeEligibility,
    ExecutionScope,
    MessageEntityRef,
    RelationshipWrite,
)
from common.schema.settings import RedisConnectionSettings
from core.ingestion import graph_commit as write_graph_db
from core.knowledge.db.writers.entity_writer import EntityWriter
from infrastructure.redis_client import AsyncRedisClient, RedisKeys


class _RedisBoundary:
    def __init__(self, client):
        self.client = client
        self.fail_dirty = True

    async def sadd(self, key, *values):
        if self.fail_dirty:
            raise RuntimeError("dirty marking unavailable")
        return await self.client.sadd(key, *values)

    async def delete(self, key):
        return await self.client.delete(key)


@pytest.mark.storage
@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.requires_redis
@pytest.mark.no_network
async def test_replaying_graph_batch_does_not_duplicate_semantic_effects(
    real_postgres_client, monkeypatch
):
    async def emit_nothing(*_args, **_kwargs):
        return None

    monkeypatch.setattr(write_graph_db, "emit", emit_nothing)
    await real_postgres_client.execute(
        """
        INSERT INTO sessions (session_id, user_name, project_id)
        VALUES ('session-1', 'ada', 'project-1')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO messages (
            user_name, session_id, message_id, project_id, role, content
        )
        VALUES ('ada', 'session-1', 7, 'project-1', 'user', 'Ada met Grace.')
        """
    )

    scope = ExecutionScope(
        user_name="ada", project_id="project-1", session_id="session-1"
    )
    entities = [
        EntityWrite(
            entity_id=101,
            is_new=True,
            canonical_name="Ada",
            entity_type="person",
            topic="People",
            embedding=None,
            aliases=("Ada",),
        ),
        EntityWrite(
            entity_id=102,
            is_new=True,
            canonical_name="Grace",
            entity_type="person",
            topic="People",
            embedding=None,
            aliases=("Grace",),
        ),
    ]
    relationships = [
        RelationshipWrite(
            entity_a_id=101,
            entity_b_id=102,
            relationship_type="met",
            message_id=7,
            confidence=1.0,
            context="Ada met Grace.",
        )
    ]
    refs = [
        MessageEntityRef(message_id=7, entity_id=101),
        MessageEntityRef(message_id=7, entity_id=102),
    ]
    writer = EntityWriter(real_postgres_client)
    knowledge_store = SimpleNamespace(
        update_entity_aliases=writer.update_entity_aliases,
        write_batch=writer.write_batch,
    )
    redis_manager = AsyncRedisClient(RedisConnectionSettings.from_env())
    redis_client = await redis_manager.connect()
    dirty_key = RedisKeys.dirty_entities("ada", "project-1")
    profile_key = RedisKeys.project_profile_complete("ada", "project-1")
    await redis_client.delete(dirty_key, profile_key)
    redis = _RedisBoundary(redis_client)
    buffers = {
        "scope": scope,
        "alias_updates": [],
        "entity_writes": entities,
        "relationship_writes": relationships,
        "message_entity_refs": refs,
        "eligible_messages": [],
        "dirty_entity_ids": {101, 102},
        "zombie_entity_ids": set(),
        "skipped_relationships": [],
        "knowledge_store": knowledge_store,
        "redis_client": redis,
    }

    try:
        with pytest.raises(RuntimeError, match="dirty marking unavailable"):
            await write_graph_db._execute_graph_write_buffers(**buffers)

        redis.fail_dirty = False
        await write_graph_db._execute_graph_write_buffers(**buffers)
    finally:
        await redis_client.delete(dirty_key, profile_key)
        await redis_manager.close()

    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM entities WHERE project_id = 'project-1'"
    ) == {"count": 2}
    assert await real_postgres_client.fetch_one(
        "SELECT weight FROM relationships WHERE relationship_id = 'project-1:101:102:met'"
    ) == {"weight": 1}
    assert await real_postgres_client.fetch_one(
        """
        SELECT count(*) AS count
        FROM relationship_evidence_refs
        WHERE relationship_id = 'project-1:101:102:met'
        """
    ) == {"count": 1}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM message_entity_refs WHERE message_id = 7"
    ) == {"count": 2}


@pytest.mark.storage
@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_new_entity_replay_rejects_cross_project_id_collision(
    real_postgres_client,
):
    await real_postgres_client.execute(
        """
        INSERT INTO entities (
            entity_id, user_name, project_id, canonical_name, type, topic
        )
        VALUES (101, 'ada', 'project-2', 'Other', 'person', 'People')
        """
    )

    writer = EntityWriter(real_postgres_client)
    with pytest.raises(RuntimeError, match="outside project project-1"):
        await writer.write_batch(
            [
                EntityWrite(
                    entity_id=101,
                    is_new=True,
                    canonical_name="Ada",
                    entity_type="person",
                    topic="People",
                    embedding=None,
                )
            ],
            [],
            scope=ExecutionScope(
                user_name="ada",
                project_id="project-1",
                session_id="session-1",
            ),
        )


@pytest.mark.storage
@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_alias_commit_survives_main_graph_write_failure(
    real_postgres_client,
):
    """An alias committed before a failed graph write converges on replay."""

    await real_postgres_client.execute(
        """
        INSERT INTO sessions (session_id, user_name, project_id)
        VALUES ('session-1', 'ada', 'project-1')
        """
    )
    scope = ExecutionScope(
        user_name="ada", project_id="project-1", session_id="session-1"
    )
    writer = EntityWriter(real_postgres_client)
    await writer.write_batch(
        [
            EntityWrite(
                entity_id=201,
                is_new=True,
                canonical_name="Ada",
                entity_type="person",
                topic="People",
                embedding=None,
            )
        ],
        [],
        scope=scope,
    )

    class FailOnceStore:
        def __init__(self):
            self.fail = True

        async def update_entity_aliases(self, aliases, *, project_id):
            return await writer.update_entity_aliases(aliases, project_id=project_id)

        async def write_batch(self, *args, **kwargs):
            if self.fail:
                self.fail = False
                raise RuntimeError("main graph transaction unavailable")
            return await writer.write_batch(*args, **kwargs)

    store = FailOnceStore()
    buffers = {
        "scope": scope,
        "alias_updates": [AliasUpdate(entity_id=201, aliases=("Augusta",))],
        "entity_writes": [
            EntityWrite(
                entity_id=201,
                is_new=False,
                canonical_name="Ada",
                entity_type="person",
                topic="People",
                embedding=None,
                aliases=("Augusta",),
            )
        ],
        "relationship_writes": [],
        "message_entity_refs": [],
        "eligible_messages": [],
        "dirty_entity_ids": set(),
        "zombie_entity_ids": set(),
        "skipped_relationships": [],
        "knowledge_store": store,
        "redis_client": None,
    }

    with pytest.raises(RuntimeError, match="main graph transaction unavailable"):
        await write_graph_db._execute_graph_write_buffers(**buffers)
    await write_graph_db._execute_graph_write_buffers(**buffers)

    aliases = await real_postgres_client.fetch_all(
        "SELECT alias FROM entity_aliases WHERE entity_id = 201 ORDER BY alias"
    )
    assert aliases == [{"alias": "Augusta"}]
    assert await real_postgres_client.fetch_one(
        "SELECT canonical_name FROM entities WHERE entity_id = 201"
    ) == {"canonical_name": "Ada"}


@pytest.mark.storage
@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_replay_does_not_resurrect_existing_entity_deleted_between_attempts(
    real_postgres_client,
):
    """A stale retry must not recreate an entity removed after its first attempt."""

    await real_postgres_client.execute(
        """
        INSERT INTO sessions (session_id, user_name, project_id)
        VALUES ('session-1', 'ada', 'project-1')
        """
    )
    scope = ExecutionScope(
        user_name="ada", project_id="project-1", session_id="session-1"
    )
    writer = EntityWriter(real_postgres_client)
    await writer.write_batch(
        [
            EntityWrite(
                entity_id=301,
                is_new=True,
                canonical_name="Ada",
                entity_type="person",
                topic="People",
                embedding=None,
            )
        ],
        [],
        scope=scope,
    )

    class FailOnceStore:
        def __init__(self):
            self.fail = True

        async def update_entity_aliases(self, aliases, *, project_id):
            return await writer.update_entity_aliases(aliases, project_id=project_id)

        async def write_batch(self, *args, **kwargs):
            if self.fail:
                self.fail = False
                raise RuntimeError("main graph transaction unavailable")
            return await writer.write_batch(*args, **kwargs)

    buffers = {
        "scope": scope,
        "alias_updates": [AliasUpdate(entity_id=301, aliases=("Augusta",))],
        "entity_writes": [
            EntityWrite(
                entity_id=301,
                is_new=False,
            canonical_name="Ada",
            entity_type="person",
            topic="People",
                embedding=None,
                aliases=("Augusta",),
            )
        ],
        "relationship_writes": [],
        "message_entity_refs": [],
        "eligible_messages": [],
        "dirty_entity_ids": set(),
        "zombie_entity_ids": set(),
        "skipped_relationships": [],
        "knowledge_store": FailOnceStore(),
        "redis_client": None,
    }

    with pytest.raises(RuntimeError, match="main graph transaction unavailable"):
        await write_graph_db._execute_graph_write_buffers(**buffers)

    # A cleanup worker can delete the entity before the DLQ retry runs.  The
    # replay must fail closed instead of turning the stale update into a new
    # entity.
    await real_postgres_client.execute("DELETE FROM entities WHERE entity_id = 301")
    with pytest.raises(RuntimeError, match="Existing entity 301 was not found"):
        await write_graph_db._execute_graph_write_buffers(**buffers)

    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM entities WHERE entity_id = 301"
    ) == {"count": 0}


@pytest.mark.storage
@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_age_projection_failure_rolls_back_relational_graph_writes(
    real_postgres_client, monkeypatch
):
    """AGE failure after SQL writes leaves no partial graph aggregate to replay."""

    await real_postgres_client.execute(
        """
        INSERT INTO sessions (session_id, user_name, project_id)
        VALUES ('session-1', 'ada', 'project-1')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO messages (
            user_name, session_id, message_id, project_id, role, content
        )
        VALUES ('ada', 'session-1', 302, 'project-1', 'user', 'Ada met Grace.')
        """
    )
    scope = ExecutionScope(
        user_name="ada", project_id="project-1", session_id="session-1"
    )
    entities = [
        EntityWrite(
            entity_id=303,
            is_new=True,
            canonical_name="Ada",
            entity_type="person",
            topic="People",
            embedding=None,
            aliases=("Augusta",),
        ),
        EntityWrite(
            entity_id=304,
            is_new=True,
            canonical_name="Grace",
            entity_type="person",
            topic="People",
            embedding=None,
            aliases=("Grace Hopper",),
        ),
    ]
    relationships = [
        RelationshipWrite(
            entity_a_id=303,
            entity_b_id=304,
            relationship_type="met",
            message_id=302,
            context="Ada met Grace.",
        )
    ]
    writer = EntityWriter(real_postgres_client)
    project_relationships = writer.projection.project_relationships

    async def fail_projection(*_args, **_kwargs):
        raise RuntimeError("AGE projection unavailable")

    monkeypatch.setattr(writer.projection, "project_relationships", fail_projection)
    with pytest.raises(RuntimeError, match="AGE projection unavailable"):
        await writer.write_batch(
            entities,
            relationships,
            message_entity_refs=[
                MessageEntityRef(message_id=302, entity_id=303),
                MessageEntityRef(message_id=302, entity_id=304),
            ],
            eligible_messages=[EpisodeEligibility(message_id=302)],
            scope=scope,
        )

    for table in (
        "entities",
        "entity_aliases",
        "relationships",
        "relationship_evidence_refs",
        "message_entity_refs",
    ):
        assert await real_postgres_client.fetch_one(
            f"SELECT count(*) AS count FROM {table}"
        ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT episode_eligible FROM messages WHERE message_id = 302"
    ) == {"episode_eligible": False}

    monkeypatch.setattr(
        writer.projection,
        "project_relationships",
        project_relationships,
    )
    await writer.write_batch(
        entities,
        relationships,
        message_entity_refs=[
            MessageEntityRef(message_id=302, entity_id=303),
            MessageEntityRef(message_id=302, entity_id=304),
        ],
        eligible_messages=[EpisodeEligibility(message_id=302)],
        scope=scope,
    )

    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM entities"
    ) == {"count": 2}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM relationships"
    ) == {"count": 1}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM relationship_evidence_refs"
    ) == {"count": 1}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM message_entity_refs"
    ) == {"count": 2}


@pytest.mark.storage
@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_replay_rejects_relationship_evidence_from_a_different_session(
    real_postgres_client,
):
    """A replay cannot attach a project entity change to another session's message."""

    await real_postgres_client.execute(
        """
        INSERT INTO sessions (session_id, user_name, project_id)
        VALUES
            ('session-1', 'ada', 'project-1'),
            ('session-2', 'ada', 'project-2')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO messages (
            user_name, session_id, message_id, project_id, role, content
        )
        VALUES ('ada', 'session-1', 305, 'project-1', 'user', 'Scoped evidence.')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO entities (
            entity_id, user_name, project_id, canonical_name, type, topic
        )
        VALUES
            (306, 'ada', 'project-1', 'Ada', 'person', 'People'),
            (307, 'ada', 'project-1', 'Grace', 'person', 'People')
        """
    )
    writer = EntityWriter(real_postgres_client)
    with pytest.raises(ValueError, match="Relationship evidence message"):
        await writer.write_batch(
            [],
            [
                RelationshipWrite(
                    entity_a_id=306,
                    entity_b_id=307,
                    relationship_type="met",
                    message_id=305,
                    confidence=1.0,
                    context="Scoped evidence.",
                )
            ],
            scope=ExecutionScope(
                user_name="ada", project_id="project-1", session_id="session-2"
            ),
        )

    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM relationships"
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM relationship_evidence_refs"
    ) == {"count": 0}


@pytest.mark.storage
@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_concurrent_identical_graph_replays_apply_evidence_once(
    real_postgres_client,
):
    """Concurrent retries converge on one entity, edge, and evidence reference."""

    await real_postgres_client.execute(
        """
        INSERT INTO sessions (session_id, user_name, project_id)
        VALUES ('session-1', 'ada', 'project-1')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO messages (
            user_name, session_id, message_id, project_id, role, content
        )
        VALUES ('ada', 'session-1', 308, 'project-1', 'user', 'Ada met Grace.')
        """
    )
    scope = ExecutionScope(
        user_name="ada", project_id="project-1", session_id="session-1"
    )
    entities = [
        EntityWrite(
            entity_id=309,
            is_new=True,
            canonical_name="Ada",
            entity_type="person",
            topic="People",
            embedding=None,
            aliases=("Augusta",),
        ),
        EntityWrite(
            entity_id=310,
            is_new=True,
            canonical_name="Grace",
            entity_type="person",
            topic="People",
            embedding=None,
            aliases=("Grace Hopper",),
        ),
    ]
    relationships = [
        RelationshipWrite(
            entity_a_id=309,
            entity_b_id=310,
            relationship_type="met",
            message_id=308,
            confidence=1.0,
            context="Ada met Grace.",
        )
    ]
    refs = [
        MessageEntityRef(message_id=308, entity_id=309),
        MessageEntityRef(message_id=308, entity_id=310),
    ]
    writer = EntityWriter(real_postgres_client)

    await asyncio.gather(
        writer.write_batch(
            entities,
            relationships,
            message_entity_refs=refs,
            eligible_messages=[EpisodeEligibility(message_id=308)],
            scope=scope,
        ),
        writer.write_batch(
            entities,
            relationships,
            message_entity_refs=refs,
            eligible_messages=[EpisodeEligibility(message_id=308)],
            scope=scope,
        ),
    )

    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM entities"
    ) == {"count": 2}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM entity_aliases"
    ) == {"count": 2}
    assert await real_postgres_client.fetch_one(
        "SELECT weight FROM relationships WHERE relationship_id = 'project-1:309:310:met'"
    ) == {"weight": 1}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM relationship_evidence_refs"
    ) == {"count": 1}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM message_entity_refs"
    ) == {"count": 2}
    assert await real_postgres_client.fetch_one(
        "SELECT episode_eligible FROM messages WHERE message_id = 308"
    ) == {"episode_eligible": False}
