import json

import pytest

from core.knowledge.db.search_index_rebuilder import SearchIndexer
from tests.fixtures.fakes import RecordingPostgresClient


class RecordingEmbeddingService:
    embedding_dim = 1024

    def __init__(self, fail=False):
        self.fail = fail
        self.calls = []

    async def encode(self, texts):
        self.calls.append(list(texts))
        if self.fail:
            raise RuntimeError("embedding failed")
        return [[float(index + 1)] * 1024 for index, _ in enumerate(texts)]


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
async def test_search_index_rebuilder_replaces_all_derived_indexes():
    client = make_client()
    embedding = RecordingEmbeddingService()
    rebuilder = SearchIndexer(client, embedding)

    summary = await rebuilder.rebuild_project_indexes(
        "project-1",
        "ada",
        ["project-1", "archive-1"],
    )

    assert summary == {
        "entities": 1,
        "identity": 1,
        "episodes": 1,
    }
    assert embedding.calls == [
        [
            "Widget (concept)",
            "ada (person)",
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
    episode_update = next(
        call
        for call in client.calls
        if call[0] == "execute" and "UPDATE episodes" in call[1]
    )
    assert episode_update[2][1:] == ("episode-1", "project-1")
    assert len(json.loads(episode_update[2][0])) == 1024


@pytest.mark.storage
@pytest.mark.no_network
async def test_search_index_rebuilder_embedding_failure_preserves_existing_rows():
    client = make_client()
    rebuilder = SearchIndexer(
        client,
        RecordingEmbeddingService(fail=True),
    )

    with pytest.raises(RuntimeError, match="embedding failed"):
        await rebuilder.rebuild_project_indexes(
            "project-1",
            "ada",
            ["project-1"],
        )

    assert not any(
        call[0] == "execute" and "DELETE FROM" in call[1]
        for call in client.calls
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_search_index_rebuilder_rejects_wrong_embedding_dimension():
    client = make_client()
    embedding = RecordingEmbeddingService()
    embedding.embedding_dim = 3
    rebuilder = SearchIndexer(client, embedding)

    with pytest.raises(RuntimeError, match="1024-dimensional"):
        await rebuilder.rebuild_project_indexes(
            "project-1",
            "ada",
            ["project-1"],
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
async def test_search_index_rebuilder_rejects_malformed_embedding_results(
    result,
    match,
):
    class MalformedEmbeddingService:
        embedding_dim = 1024

        async def encode(self, texts):
            return result

    client = make_client()
    rebuilder = SearchIndexer(client, MalformedEmbeddingService())

    with pytest.raises(RuntimeError, match=match):
        await rebuilder.rebuild_project_indexes(
            "project-1",
            "ada",
            ["project-1"],
        )

    assert not any(
        call[0] == "execute" and "DELETE FROM" in call[1]
        for call in client.calls
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_search_index_rebuilder_database_failure_exits_transaction():
    client = make_client()
    client.cursor_execute_exceptions = [
        *([None] * 13),
        RuntimeError("message insert failed"),
    ]
    rebuilder = SearchIndexer(client, RecordingEmbeddingService())

    with pytest.raises(RuntimeError, match="message insert failed"):
        await rebuilder.rebuild_project_indexes(
            "project-1",
            "ada",
            ["project-1"],
        )

    assert client.transaction_enters == 3
    assert client.transaction_exits == 3


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_search_index_rebuild_is_idempotent_and_preserves_sibling_project(
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
        INSERT INTO entities (
            entity_id, user_name, project_id, canonical_name, type, topic
        ) VALUES
            (1, 'ada', '__identity__', 'ada', 'person', 'Identity'),
            (2, 'ada', 'project-1', 'Project One', 'concept', 'General'),
            (3, 'ada', 'project-2', 'Project Two', 'concept', 'General')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO episodes (
            episode_id, project_id, session_id, summary, source_message_count,
            first_message_at, last_message_at
        ) VALUES (
            'episode-1', 'project-1', 'session-1', 'Project one episode', 1,
            TIMESTAMPTZ '2026-01-01 00:00:01+00', TIMESTAMPTZ '2026-01-01 00:00:01+00'
        )
        """
    )
    embedding = RecordingEmbeddingService()
    rebuilder = SearchIndexer(real_postgres_client, embedding)

    expected_summary = {
        "entities": 1,
        "identity": 1,
        "episodes": 1,
    }
    assert await rebuilder.rebuild_project_indexes(
        "project-1", "ada", ["project-1"]
    ) == expected_summary
    assert await rebuilder.rebuild_project_indexes(
        "project-1", "ada", ["project-1"]
    ) == expected_summary

    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM messages WHERE project_id = 'project-1'"
    ) == {"count": 1}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM entities WHERE project_id = 'project-1'"
    ) == {"count": 1}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM entities WHERE entity_id = 2 AND embedding IS NOT NULL"
    ) == {"count": 1}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM entities WHERE project_id = '__identity__' AND embedding IS NOT NULL"
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
