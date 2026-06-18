import json
from datetime import datetime, timezone

import pytest

from knoggin_server.knowledge.db.search_index_rebuilder import SearchIndexRebuilder
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
    valid_at = datetime(2026, 1, 2, tzinfo=timezone.utc)
    return RecordingPostgresClient(
        execute_read_results=[
            [
                {
                    "message_id": 7,
                    "user_name": "ada",
                    "session_id": "session-1",
                    "project_id": "project-1",
                    "content": "hello",
                }
            ],
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
                    "fact_id": "fact-1",
                    "entity_id": 2,
                    "user_name": "ada",
                    "project_id": "project-1",
                    "content": "Widget uses direct evidence.",
                    "valid_at": valid_at,
                    "invalid_at": None,
                }
            ],
            [
                {
                    "entity_id": 1,
                    "canonical_name": "ada",
                    "type": "person",
                    "user_name": "ada",
                    "project_id": "__identity__",
                }
            ],
            [
                {
                    "fact_id": "identity-fact",
                    "content": "Ada prefers concise answers.",
                    "valid_at": valid_at,
                    "invalid_at": None,
                }
            ],
        ]
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_search_index_rebuilder_replaces_all_derived_indexes():
    client = make_client()
    embedding = RecordingEmbeddingService()
    rebuilder = SearchIndexRebuilder(client, embedding)

    summary = await rebuilder.rebuild_project_indexes(
        "project-1",
        "ada",
        ["project-1", "archive-1"],
    )

    assert summary == {
        "messages": 1,
        "entities": 1,
        "facts": 1,
        "identity": 1,
    }
    assert embedding.calls == [
        [
            "Widget (concept). Widget uses direct evidence.",
            "ada (person). Ada prefers concise answers.",
        ],
        ["Widget uses direct evidence."],
    ]
    assert any(
        call[0] == "execute"
        and "DELETE FROM message_search" in call[1]
        and call[2] == ("project-1", "ada")
        for call in client.calls
    )
    message_insert = next(
        call
        for call in client.calls
        if call[0] == "execute" and "INSERT INTO message_search" in call[1]
    )
    assert message_insert[2] == (
        7,
        "ada",
        "session-1",
        "project-1",
        "hello",
    )
    entity_inserts = [
        call
        for call in client.calls
        if call[0] == "execute" and "INSERT INTO entity_search" in call[1]
    ]
    assert len(entity_inserts) == 2
    assert len(json.loads(entity_inserts[0][2][4])) == 1024
    identity_fact_read = client.calls[4]
    assert identity_fact_read[0] == "execute_read"
    assert identity_fact_read[2] == (1, "ada", ["project-1", "archive-1"])


@pytest.mark.storage
@pytest.mark.no_network
async def test_search_index_rebuilder_embedding_failure_preserves_existing_rows():
    client = make_client()
    rebuilder = SearchIndexRebuilder(
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
    rebuilder = SearchIndexRebuilder(client, embedding)

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
    rebuilder = SearchIndexRebuilder(client, MalformedEmbeddingService())

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
    client.execute_exceptions = [
        None,
        None,
        None,
        RuntimeError("message insert failed"),
    ]
    rebuilder = SearchIndexRebuilder(client, RecordingEmbeddingService())

    with pytest.raises(RuntimeError, match="message insert failed"):
        await rebuilder.rebuild_project_indexes(
            "project-1",
            "ada",
            ["project-1"],
        )

    assert client.transaction_enters == 1
    assert client.transaction_exits == 1
