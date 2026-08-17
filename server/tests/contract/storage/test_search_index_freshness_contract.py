import pytest

from core.knowledge.db.search_index_rebuilder import SearchIndexer


class _MutatingEmbeddingService:
    embedding_dim = 1024

    def __init__(self, client):
        self.client = client
        self.calls = []
        self._mutated = False

    async def encode(self, texts):
        self.calls.append(list(texts))
        if not self._mutated:
            self._mutated = True
            await self.client.execute(
                "UPDATE messages SET content = %s WHERE message_id = %s",
                ("fresh canonical content", 7),
            )
        return [[float(index + 1)] * 1024 for index, _ in enumerate(texts)]


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_search_rebuild_retries_when_canonical_data_changes_during_embedding(
    real_postgres_client,
):
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
        VALUES ('ada', 'session-1', 7, 'project-1', 'user', 'stale content')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO entities (
            entity_id, user_name, project_id, canonical_name, type, topic
        )
        VALUES
            (1, 'ada', '__identity__', 'ada', 'person', 'Identity'),
            (2, 'ada', 'project-1', 'Widget', 'concept', 'General')
        """
    )

    embedding = _MutatingEmbeddingService(real_postgres_client)
    rebuilder = SearchIndexer(real_postgres_client, embedding)

    summary = await rebuilder.rebuild_project_indexes(
        "project-1",
        "ada",
        ["project-1"],
    )

    indexed = await real_postgres_client.fetch_one(
        """
        SELECT content_tsvector @@ plainto_tsquery('english', %s) AS matches
        FROM message_search
        WHERE message_id = %s
        """,
        ("fresh", 7),
    )

    assert summary == {
        "messages": 1,
        "entities": 1,
        "identity": 1,
        "episodes": 0,
    }
    assert len(embedding.calls) == 2
    assert indexed == {"matches": True}
