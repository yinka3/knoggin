import json
from datetime import datetime, timezone

import pytest

from common.exceptions import StorageWriteError
from core.knowledge.db.writers.episode_writer import EpisodeWriter
from core.knowledge.store import KnowledgeStore
from tests.fixtures.fakes import RecordingPostgresClient


class RecordingEmbeddingService:
    def __init__(self, vector=None, error=None):
        self.vector = vector if vector is not None else [0.25] * 1024
        self.error = error
        self.calls = []

    async def encode(self, texts):
        self.calls.append(list(texts))
        if self.error is not None:
            raise self.error
        return [self.vector]


_INITIAL_UPDATED_AT = datetime(2026, 1, 1, tzinfo=timezone.utc)
_EDITED_UPDATED_AT = datetime(2026, 1, 2, tzinfo=timezone.utc)


def _edit_arguments(**overrides):
    values = {
        "episode_id": "episode-1",
        "user_name": "ada",
        "project_id": "project-1",
        "summary": "Edited episode narrative.",
        "new_developments": ["The vector follows the narrative."],
        "updates": ["The writer persists both together."],
        "unresolved": [],
        "embedding": [0.25] * 1024,
        "expected_updated_at": _INITIAL_UPDATED_AT,
    }
    values.update(overrides)
    return values


def _store_edit_arguments(**overrides):
    values = _edit_arguments(**overrides)
    values.pop("embedding")
    return values


@pytest.mark.storage
@pytest.mark.no_network
def test_episode_persistence_exposes_only_project_window_writes():
    """Episodes are project aggregates; session cursors are implementation detail."""

    assert hasattr(EpisodeWriter, "write_project_semantic_window_episodes")
    assert not hasattr(EpisodeWriter, "create_episode")
    assert not hasattr(EpisodeWriter, "write_episode_window")
    assert hasattr(KnowledgeStore, "write_project_semantic_window_episodes")
    assert not hasattr(KnowledgeStore, "create_episode")
    assert not hasattr(KnowledgeStore, "write_episode_window")


@pytest.mark.storage
@pytest.mark.no_network
async def test_store_edit_generates_the_canonical_vector_before_the_atomic_write():
    client = RecordingPostgresClient(
        fetch_one_results=[{"updated_at": _EDITED_UPDATED_AT}]
    )
    embedding = RecordingEmbeddingService()
    store = KnowledgeStore(client, embedding)

    updated_at = await store.edit_episode(
        **_store_edit_arguments(
            summary="  Edited episode narrative.  ",
            new_developments=["  The vector follows the narrative.  "],
        )
    )

    assert updated_at == _EDITED_UPDATED_AT
    assert embedding.calls == [
        [
            "Summary:\nEdited episode narrative.\n\n"
            "New developments:\n- The vector follows the narrative.\n\n"
            "Updates:\n- The writer persists both together."
        ]
    ]
    assert client.transaction_enters == 1
    query, params = client.calls[0][1:]
    assert "embedding = %s::vector" in query
    assert "episode.updated_at = %s" in query
    assert params == (
        "Edited episode narrative.",
        json.dumps(["The vector follows the narrative."]),
        json.dumps(["The writer persists both together."]),
        json.dumps([]),
        json.dumps([0.25] * 1024),
        "episode-1",
        "project-1",
        _INITIAL_UPDATED_AT,
        "ada",
    )


@pytest.mark.storage
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("embedding", "error", "match"),
    [
        ([0.25] * 3, None, "1024-dimensional"),
        (None, RuntimeError("embedding unavailable"), "embedding unavailable"),
    ],
)
async def test_store_edit_embedding_failure_does_not_open_a_transaction(
    embedding,
    error,
    match,
):
    client = RecordingPostgresClient()
    store = KnowledgeStore(
        client,
        RecordingEmbeddingService(vector=embedding, error=error),
    )

    with pytest.raises(RuntimeError, match=match):
        await store.edit_episode(**_store_edit_arguments())

    assert client.transaction_enters == 0
    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_episode_writer_rejects_a_stale_edit_without_a_second_write():
    client = RecordingPostgresClient(fetch_one_results=[None])

    with pytest.raises(ValueError, match="has changed"):
        await EpisodeWriter(client).edit_episode(**_edit_arguments())

    assert client.transaction_enters == 1
    assert client.transaction_exits == 1
    assert len(client.calls) == 1
    assert "episode.updated_at = %s" in client.calls[0][1]


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_episode_writer_commits_narrative_and_vector_together_with_a_stale_guard(
    real_postgres_client,
):
    old_vector = [0.1] * 1024
    new_vector = [0.2] * 1024
    failed_vector = [0.3] * 1024
    original_updated_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    await real_postgres_client.execute(
        """
        INSERT INTO episodes (
            episode_id, project_id, summary, new_developments, updates,
            unresolved, embedding, updated_at
        ) VALUES (
            'episode-1', 'project-1', 'Original narrative',
            '["Original development"]'::jsonb, '["Original update"]'::jsonb,
            '["Original question"]'::jsonb, %s::vector, %s
        )
        """,
        (json.dumps(old_vector), original_updated_at),
    )
    writer = EpisodeWriter(real_postgres_client)

    updated_at = await writer.edit_episode(
        **_edit_arguments(
            embedding=new_vector,
            expected_updated_at=original_updated_at,
        )
    )
    current = await real_postgres_client.fetch_one(
        """
        SELECT summary, new_developments, updates, unresolved, user_modified,
               updated_at,
               embedding = %s::vector AS vector_matches
        FROM episodes
        WHERE episode_id = 'episode-1' AND project_id = 'project-1'
        """,
        (json.dumps(new_vector),),
    )
    assert current == {
        "summary": "Edited episode narrative.",
        "new_developments": ["The vector follows the narrative."],
        "updates": ["The writer persists both together."],
        "unresolved": [],
        "user_modified": True,
        "updated_at": updated_at,
        "vector_matches": True,
    }

    with pytest.raises(ValueError, match="has changed"):
        await writer.edit_episode(
            **_edit_arguments(
                summary="Stale edit must not apply.",
                embedding=failed_vector,
                expected_updated_at=original_updated_at,
            )
        )

    await real_postgres_client.execute(
        """
        CREATE FUNCTION fail_episode_edit() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.summary = 'Injected SQL failure.' THEN
                RAISE EXCEPTION 'injected episode edit failure';
            END IF;
            RETURN NEW;
        END;
        $$;
        CREATE TRIGGER fail_episode_edit_trigger
        BEFORE UPDATE ON episodes
        FOR EACH ROW EXECUTE FUNCTION fail_episode_edit();
        """
    )
    try:
        with pytest.raises(StorageWriteError, match="edit_episode"):
            await writer.edit_episode(
                **_edit_arguments(
                    summary="Injected SQL failure.",
                    embedding=failed_vector,
                    expected_updated_at=updated_at,
                )
            )
    finally:
        await real_postgres_client.execute(
            "DROP TRIGGER IF EXISTS fail_episode_edit_trigger ON episodes"
        )
        await real_postgres_client.execute("DROP FUNCTION IF EXISTS fail_episode_edit()")

    unchanged = await real_postgres_client.fetch_one(
        """
        SELECT summary, embedding = %s::vector AS vector_matches, updated_at
        FROM episodes
        WHERE episode_id = 'episode-1' AND project_id = 'project-1'
        """,
        (json.dumps(new_vector),),
    )
    assert unchanged == {
        "summary": "Edited episode narrative.",
        "vector_matches": True,
        "updated_at": updated_at,
    }
