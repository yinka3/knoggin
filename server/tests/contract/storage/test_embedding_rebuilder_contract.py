"""Contracts for episode/document embedding configuration and rebuilds."""

import json

import pytest

from core.knowledge.db.embedding_rebuilder import EmbeddingRebuilder
from tests.fixtures.fakes import RecordingPostgresClient


class RecordingEmbeddingService:
    embedding_dim = 1024
    configuration_fingerprint = "fingerprint-v2"

    def __init__(self, *, fail: bool = False):
        self.fail = fail
        self.calls = []

    @staticmethod
    def vector_for(text: str):
        return [float(sum(ord(character) for character in text))] * 1024

    async def encode(self, texts):
        values = list(texts)
        self.calls.append(values)
        if self.fail:
            raise RuntimeError("embedding failed")
        return [self.vector_for(text) for text in values]


def rebuild_client():
    return RecordingPostgresClient(
        fetch_all_results=[
            [
                {
                    "episode_id": "episode-1",
                    "summary": "Widget storage uses durable evidence.",
                    "new_developments": ["Hybrid retrieval is enabled."],
                    "updates": [],
                    "unresolved": [],
                }
            ],
            [
                {
                    "chunk_id": "00000000-0000-0000-0000-000000000001",
                    "content": "def durable_write(): pass",
                    "relative_path": "src/store.py",
                    "language": "python",
                    "symbol_name": "durable_write",
                }
            ],
        ]
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_full_rebuild_replaces_episode_and_document_vectors_and_fingerprint():
    client = rebuild_client()
    embedding = RecordingEmbeddingService()

    summary = await EmbeddingRebuilder(client, embedding).rebuild_all_embeddings()

    assert summary == {"episodes": 1, "document_chunks": 1}
    assert embedding.calls == [
        [
            "Summary:\nWidget storage uses durable evidence.\n\n"
            "New developments:\n- Hybrid retrieval is enabled.",
            "File: src/store.py\nLanguage: python\nSymbol: durable_write\n\n"
            "def durable_write(): pass",
        ]
    ]
    updates = [
        call
        for call in client.calls
        if call[0] == "execute" and "UPDATE public." in call[1]
    ]
    assert len(updates) == 2
    assert len(json.loads(updates[0][2][0])) == 1024
    assert any(
        call[0] == "execute"
        and "INSERT INTO public.embedding_configuration" in call[1]
        and call[2] == ("fingerprint-v2",)
        for call in client.calls
    )
    assert client.transaction_enters == 2
    assert client.transaction_exits == 2


@pytest.mark.storage
@pytest.mark.no_network
async def test_encoding_failure_occurs_before_any_vector_write():
    client = rebuild_client()

    with pytest.raises(RuntimeError, match="embedding failed"):
        await EmbeddingRebuilder(
            client, RecordingEmbeddingService(fail=True)
        ).rebuild_all_embeddings()

    assert not any(
        call[0] == "execute" and "UPDATE public." in call[1] for call in client.calls
    )
    assert client.transaction_enters == 1


@pytest.mark.storage
@pytest.mark.no_network
async def test_rebuild_rejects_wrong_dimension_before_reading_storage():
    client = RecordingPostgresClient()
    embedding = RecordingEmbeddingService()
    embedding.embedding_dim = 3

    with pytest.raises(RuntimeError, match="1024-dimensional"):
        await EmbeddingRebuilder(client, embedding).rebuild_all_embeddings()

    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_matching_configuration_requires_no_corpus_scan():
    client = RecordingPostgresClient(
        fetch_one_results=[{"fingerprint": "fingerprint-v2"}]
    )

    await EmbeddingRebuilder(client, RecordingEmbeddingService()).ensure_configuration()

    assert not any(
        call[0] == "execute" and "EXISTS" in call[1] for call in client.calls
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_unknown_configuration_with_existing_vectors_requires_offline_rebuild():
    client = RecordingPostgresClient(fetch_one_results=[None, {"has_vectors": True}])

    with pytest.raises(RuntimeError, match="rebuild all episode and document vectors"):
        await EmbeddingRebuilder(
            client, RecordingEmbeddingService()
        ).ensure_configuration()

    assert not any(
        call[0] == "execute" and "INSERT INTO public.embedding_configuration" in call[1]
        for call in client.calls
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_empty_corpus_adopts_current_configuration():
    client = RecordingPostgresClient(fetch_one_results=[None, {"has_vectors": False}])

    await EmbeddingRebuilder(client, RecordingEmbeddingService()).ensure_configuration()

    assert any(
        call[0] == "execute"
        and "INSERT INTO public.embedding_configuration" in call[1]
        and call[2] == ("fingerprint-v2",)
        for call in client.calls
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_project_rebuild_is_scoped_and_does_not_change_global_fingerprint():
    client = RecordingPostgresClient(
        fetch_one_results=[{"fingerprint": "fingerprint-v2"}],
        fetch_all_results=[[], []],
    )

    summary = await EmbeddingRebuilder(
        client, RecordingEmbeddingService()
    ).rebuild_project_embeddings("project-1", "ada")

    assert summary == {"episodes": 0, "document_chunks": 0}
    reads = [
        call
        for call in client.calls
        if call[0] == "execute" and "SELECT e.episode_id" in call[1]
    ]
    assert reads[0][2] == ("project-1", "ada")
    assert not any(
        call[0] == "execute" and "INSERT INTO public.embedding_configuration" in call[1]
        for call in client.calls
    )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_configuration_fingerprint_round_trips_in_fresh_schema(
    real_postgres_client,
):
    rebuilder = EmbeddingRebuilder(
        real_postgres_client,
        RecordingEmbeddingService(),
    )

    await rebuilder.ensure_configuration()
    await rebuilder.ensure_configuration()

    row = await real_postgres_client.fetch_one(
        "SELECT fingerprint FROM public.embedding_configuration WHERE singleton = TRUE"
    )
    assert row == {"fingerprint": "fingerprint-v2"}
