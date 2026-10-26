"""Contracts for episode/document/exchange embedding configuration and rebuilds."""

import json

import pytest

from core.knowledge.db.embedding_rebuilder import EmbeddingRebuilder
from tests.fixtures.fakes import RecordingPostgresClient


class RecordingEmbeddingService:
    embedding_dim = 768
    configuration_fingerprint = "fingerprint-v2"

    def __init__(self, *, fail: bool = False, chunk_limit=None):
        self.fail = fail
        self.calls = []
        self.chunk_limit = chunk_limit

    def split_for_embedding(self, text):
        if self.chunk_limit is None:
            return [text]
        return [
            text[index : index + self.chunk_limit]
            for index in range(0, len(text), self.chunk_limit)
        ]

    @staticmethod
    def vector_for(text: str):
        return [float(sum(ord(character) for character in text))] * 768

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
            [
                {
                    "project_id": "project-1",
                    "user_message_id": 7,
                    "user_content": "Which queue?",
                    "assistant_content": "The durable one.",
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

    assert summary == {"episodes": 1, "document_chunks": 1, "exchanges": 1}
    assert embedding.calls == [
        [
            "Summary:\nWidget storage uses durable evidence.\n\n"
            "New developments:\n- Hybrid retrieval is enabled.",
        ],
        [
            "File: src/store.py\nLanguage: python\nSymbol: durable_write\n\n"
            "def durable_write(): pass",
        ],
        ["USER: Which queue?\n\nASSISTANT: The durable one."],
    ]
    updates = [
        call
        for call in client.calls
        if call[0] == "execute" and "UPDATE public." in call[1]
    ]
    assert len(updates) == 3
    assert len(json.loads(updates[0][2][0])) == 768
    assert "UPDATE public.exchange_embeddings" in updates[2][1]
    assert updates[2][2][1:] == ("project-1", 7)
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
async def test_rebuild_embeds_all_of_a_long_episode_and_keeps_documents_separate():
    client = rebuild_client()
    summary = "Episode detail remains available. " * 110
    client.fetch_all_results[0][0]["summary"] = summary
    embedding = RecordingEmbeddingService(chunk_limit=400)

    await EmbeddingRebuilder(client, embedding).rebuild_all_embeddings()

    assert len(embedding.calls) == 3
    assert len(embedding.calls[0]) > 1
    assert all(len(chunk) <= 400 for chunk in embedding.calls[0])
    assert "".join(embedding.calls[0]) == (
        "Summary:\n" + summary.strip()
        + "\n\nNew developments:\n- Hybrid retrieval is enabled."
    )
    assert len(embedding.calls[1]) == 1
    writes = [call for call in client.calls if call[0] == "execute" and "UPDATE public." in call[1]]
    assert len(writes) == 3


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

    with pytest.raises(RuntimeError, match="768-dimensional"):
        await EmbeddingRebuilder(client, embedding).rebuild_all_embeddings()

    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_configuration_rejects_wrong_dimension_before_reading_storage():
    client = RecordingPostgresClient()
    embedding = RecordingEmbeddingService()
    embedding.embedding_dim = 1024

    with pytest.raises(RuntimeError, match="database requires 768"):
        await EmbeddingRebuilder(client, embedding).ensure_configuration()

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

    with pytest.raises(RuntimeError, match="rebuild all episode, exchange, and document vectors"):
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
        fetch_all_results=[[], [], []],
    )

    summary = await EmbeddingRebuilder(
        client, RecordingEmbeddingService()
    ).rebuild_project_embeddings("project-1", "ada")

    assert summary == {"episodes": 0, "document_chunks": 0, "exchanges": 0}
    reads = [
        call
        for call in client.calls
        if call[0] == "execute"
        and ("SELECT e.episode_id" in call[1] or "FROM public.exchange_embeddings" in call[1])
    ]
    assert [read[2] for read in reads] == [("project-1", "ada")] * 2
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
