"""Contracts for quiescent canonical embedding rebuilds."""

import json

import pytest
from psycopg.errors import RaiseException

from core.knowledge.db.embedding_rebuilder import EmbeddingRebuilder
from core.knowledge.entity.resolver import EntityResolver
from tests.fixtures.fakes import RecordingPostgresClient


class RecordingEmbeddingService:
    embedding_dim = 1024

    def __init__(self, fail=False):
        self.fail = fail
        self.calls = []
        self.single_calls = []

    @staticmethod
    def _vector_for(text):
        value = float(sum(ord(character) for character in text))
        return [value] * 1024

    async def encode(self, texts):
        values = list(texts)
        self.calls.append(values)
        if self.fail:
            raise RuntimeError("embedding failed")
        return [self._vector_for(text) for text in values]

    async def encode_single(self, text):
        self.single_calls.append(text)
        if self.fail:
            raise RuntimeError("embedding failed")
        return self._vector_for(text)


def make_client():
    return RecordingPostgresClient(
        fetch_one_results=[
            {
                "entity_id": 1,
                "canonical_name": "ada",
                "type": "person",
                "user_name": "ada",
                "project_id": "__identity__",
            }
        ],
        fetch_all_results=[
            [
                {
                    "entity_id": 2,
                    "canonical_name": "Widget",
                    "type": "concept",
                    "user_name": "ada",
                    "project_id": "project-1",
                }
            ],
            [
                {
                    "episode_id": "episode-1",
                    "summary": "Widget storage will use direct evidence.",
                    "new_developments": ["Episode vectors are enabled."],
                    "updates": [],
                    "unresolved": [],
                }
            ],
        ]
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_embedding_rebuilder_replaces_canonical_embeddings():
    client = make_client()
    embedding = RecordingEmbeddingService()
    rebuilder = EmbeddingRebuilder(client, embedding)

    summary = await rebuilder.rebuild_project_embeddings(
        "project-1",
        "ada",
    )

    assert summary == {
        "entities": 1,
        "identity": 1,
        "episodes": 1,
    }
    assert embedding.calls == [
        [
            "Widget",
            "ada",
        ],
        [
            "Summary:\nWidget storage will use direct evidence.\n\n"
            "New developments:\n- Episode vectors are enabled."
        ],
    ]
    entity_updates = [
        call
        for call in client.calls
        if call[0] == "execute" and "UPDATE entities" in call[1]
        and "SET embedding = %s::vector" in call[1]
    ]
    assert len(entity_updates) == 2
    assert len(json.loads(entity_updates[0][2][0])) == 1024
    identity_update = next(call for call in entity_updates if call[2][1] == 1)
    assert json.loads(identity_update[2][0]) == embedding._vector_for("ada")
    episode_update = next(
        call
        for call in client.calls
        if call[0] == "execute" and "UPDATE episodes" in call[1]
        and "SET embedding = %s::vector" in call[1]
    )
    assert episode_update[2][1:] == ("episode-1", "project-1")
    assert len(json.loads(episode_update[2][0])) == 1024


@pytest.mark.storage
@pytest.mark.no_network
async def test_entity_creation_and_rebuild_share_identity_embedding_input_and_vector():
    client = make_client()
    embedding = RecordingEmbeddingService()
    resolver = EntityResolver(
        knowledge_store=None,
        embedding_service=embedding,
        project_id="project-1",
        readable_project_ids=["project-1"],
    )

    pending = await resolver.prepare_pending_entity(
        entity_id=2,
        canonical_name=" Widget ",
        aliases=["Widget"],
        entity_type="concept",
        topic="General",
    )
    alternate_classification = await resolver.prepare_pending_entity(
        entity_id=3,
        canonical_name="Widget",
        aliases=["Widget"],
        entity_type="person",
        topic="Identity",
    )
    await EmbeddingRebuilder(client, embedding).rebuild_project_embeddings(
        "project-1",
        "ada",
    )

    assert embedding.single_calls == ["Widget", "Widget"]
    assert pending.embedding == alternate_classification.embedding
    assert embedding.calls[0] == ["Widget", "ada"]
    entity_update = next(
        call
        for call in client.calls
        if call[0] == "execute"
        and "UPDATE entities" in call[1]
        and call[2][1] == 2
    )
    assert json.loads(entity_update[2][0]) == list(pending.embedding)


@pytest.mark.storage
@pytest.mark.no_network
async def test_embedding_rebuilder_failure_preserves_existing_rows():
    client = make_client()
    rebuilder = EmbeddingRebuilder(
        client,
        RecordingEmbeddingService(fail=True),
    )

    with pytest.raises(RuntimeError, match="embedding failed"):
        await rebuilder.rebuild_project_embeddings(
            "project-1",
            "ada",
        )

    assert not any(
        call[0] == "execute"
        and ("UPDATE entities" in call[1] or "UPDATE episodes" in call[1])
        for call in client.calls
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_embedding_rebuilder_rejects_wrong_embedding_dimension():
    client = make_client()
    embedding = RecordingEmbeddingService()
    embedding.embedding_dim = 3
    rebuilder = EmbeddingRebuilder(client, embedding)

    with pytest.raises(RuntimeError, match="1024-dimensional"):
        await rebuilder.rebuild_project_embeddings(
            "project-1",
            "ada",
        )

    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("result", "match"),
    [
        (None, "result is missing"),
        ([], "count mismatch"),
        ([[0.0] * 3, [0.0] * 3], "dimension 3"),
    ],
)
async def test_embedding_rebuilder_rejects_malformed_embedding_results(
    result,
    match,
):
    class MalformedEmbeddingService:
        embedding_dim = 1024

        async def encode(self, texts):
            return result

    client = make_client()
    rebuilder = EmbeddingRebuilder(client, MalformedEmbeddingService())

    with pytest.raises(RuntimeError, match=match):
        await rebuilder.rebuild_project_embeddings(
            "project-1",
            "ada",
        )

    assert not any(
        call[0] == "execute"
        and ("UPDATE entities" in call[1] or "UPDATE episodes" in call[1])
        for call in client.calls
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_embedding_rebuilder_validates_episode_vectors_before_replacement():
    class EpisodeMalformedEmbeddingService(RecordingEmbeddingService):
        async def encode(self, texts):
            values = list(texts)
            self.calls.append(values)
            if len(self.calls) == 1:
                return [self._vector_for(text) for text in values]
            return [[0.0] * 3 for _ in values]

    client = make_client()
    rebuilder = EmbeddingRebuilder(client, EpisodeMalformedEmbeddingService())

    with pytest.raises(RuntimeError, match="episode embedding 0 has dimension 3"):
        await rebuilder.rebuild_project_embeddings(
            "project-1",
            "ada",
        )

    assert not any(
        call[0] == "execute"
        and ("UPDATE entities" in call[1] or "UPDATE episodes" in call[1])
        for call in client.calls
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_embedding_rebuilder_database_failure_exits_transaction():
    client = make_client()
    client.cursor_execute_exceptions = [
        *([None] * 4),
        RuntimeError("message insert failed"),
    ]
    rebuilder = EmbeddingRebuilder(client, RecordingEmbeddingService())

    with pytest.raises(RuntimeError, match="message insert failed"):
        await rebuilder.rebuild_project_embeddings(
            "project-1",
            "ada",
        )

    assert client.transaction_enters == 2
    assert client.transaction_exits == 2


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_embedding_rebuild_is_idempotent_and_preserves_sibling_project(
    real_postgres_client,
):
    await real_postgres_client.execute(
        """
        INSERT INTO sessions (session_id, user_name, project_id)
        VALUES ('session-1', 'ada', 'project-1'), ('session-2', 'ada', 'project-2')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO messages (
            user_name, session_id, message_id, project_id, role, content
        ) VALUES
            ('ada', 'session-1', 101, 'project-1', 'user', 'Fresh project one content'),
            ('ada', 'session-2', 201, 'project-2', 'user', 'Keep project two content')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO entities (entity_id, user_name, canonical_name) VALUES
            (1, 'ada', 'ada'),
            (2, 'ada', 'Project One'),
            (3, 'ada', 'Project Two');
        INSERT INTO project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic
        ) VALUES
            ('project-1', 2, 'ada', 'concept', 'General'),
            ('project-2', 3, 'ada', 'concept', 'General')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO episodes (
            episode_id, project_id, summary, source_message_count,
            first_message_at, last_message_at
        ) VALUES (
            'episode-1', 'project-1', 'Project one episode', 1,
            TIMESTAMPTZ '2026-01-01 00:00:01+00', TIMESTAMPTZ '2026-01-01 00:00:01+00'
        )
        """
    )
    embedding = RecordingEmbeddingService()
    rebuilder = EmbeddingRebuilder(real_postgres_client, embedding)

    expected_summary = {
        "entities": 1,
        "identity": 1,
        "episodes": 1,
    }
    assert await rebuilder.rebuild_project_embeddings("project-1", "ada") == expected_summary
    assert await rebuilder.rebuild_project_embeddings("project-1", "ada") == expected_summary

    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM messages WHERE project_id = 'project-1'"
    ) == {"count": 1}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM project_entity_contexts WHERE project_id = 'project-1'"
    ) == {"count": 1}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM entities WHERE entity_id = 2 AND embedding IS NOT NULL"
    ) == {"count": 1}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM entities WHERE entity_id = 1 AND embedding IS NOT NULL"
    ) == {"count": 1}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM episodes WHERE project_id = 'project-1' AND embedding IS NOT NULL"
    ) == {"count": 1}
    assert await real_postgres_client.fetch_one(
        """
        SELECT search_tsvector @@ plainto_tsquery('english', %s) AS matches
        FROM messages
        WHERE message_id = 101
        """,
        ("fresh",),
    ) == {"matches": True}
    assert await real_postgres_client.fetch_one(
        "SELECT canonical_name FROM entities WHERE entity_id = 2"
    ) == {"canonical_name": "Project One"}

    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM messages WHERE project_id = 'project-2'"
    ) == {"count": 1}
    assert await real_postgres_client.fetch_one(
        "SELECT canonical_name FROM entities WHERE entity_id = 3"
    ) == {"canonical_name": "Project Two"}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_embedding_rebuild_rolls_back_all_vectors_after_episode_write_failure(
    real_postgres_client,
):
    old_entity = "[" + ",".join(["0.1"] * 1024) + "]"
    old_identity = "[" + ",".join(["0.2"] * 1024) + "]"
    old_episode = "[" + ",".join(["0.3"] * 1024) + "]"
    await real_postgres_client.execute(
        """
        INSERT INTO entities (entity_id, user_name, canonical_name, embedding)
        VALUES
            (1, 'ada', 'ada', %s::vector),
            (2, 'ada', 'Widget', %s::vector)
        """,
        (old_identity, old_entity),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic
        )
        VALUES ('project-1', 2, 'ada', 'Concept', 'General')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO episodes (
            episode_id, project_id, summary, source_message_count,
            first_message_at, last_message_at, embedding
        )
        VALUES (
            'episode-1', 'project-1', 'Widget episode', 1,
            TIMESTAMPTZ '2026-01-01 00:00:01+00',
            TIMESTAMPTZ '2026-01-01 00:00:01+00',
            %s::vector
        )
        """,
        (old_episode,),
    )
    await real_postgres_client.execute(
        """
        CREATE FUNCTION fail_episode_embedding_replacement() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.embedding IS NOT NULL THEN
                RAISE EXCEPTION 'injected episode embedding failure';
            END IF;
            RETURN NEW;
        END;
        $$;
        CREATE TRIGGER fail_episode_embedding_replacement_trigger
        BEFORE UPDATE ON episodes
        FOR EACH ROW EXECUTE FUNCTION fail_episode_embedding_replacement();
        """
    )
    try:
        with pytest.raises(RaiseException, match="injected episode embedding failure"):
            await EmbeddingRebuilder(
                real_postgres_client,
                RecordingEmbeddingService(),
            ).rebuild_project_embeddings("project-1", "ada")
    finally:
        await real_postgres_client.execute(
            "DROP TRIGGER IF EXISTS fail_episode_embedding_replacement_trigger ON episodes"
        )
        await real_postgres_client.execute(
            "DROP FUNCTION IF EXISTS fail_episode_embedding_replacement()"
        )

    assert await real_postgres_client.fetch_one(
        "SELECT embedding = %s::vector AS unchanged FROM entities WHERE entity_id = 2",
        (old_entity,),
    ) == {"unchanged": True}
    assert await real_postgres_client.fetch_one(
        "SELECT embedding = %s::vector AS unchanged FROM entities WHERE entity_id = 1",
        (old_identity,),
    ) == {"unchanged": True}
    assert await real_postgres_client.fetch_one(
        "SELECT embedding = %s::vector AS unchanged FROM episodes WHERE episode_id = 'episode-1'",
        (old_episode,),
    ) == {"unchanged": True}
