from pathlib import Path

import pytest

from knoggin_server.knowledge.services import file_rag as file_rag_module
from knoggin_server.knowledge.services.file_rag import FileRAGService


class FakeNode:
    def __init__(self, text, metadata=None):
        self._text = text
        self.metadata = metadata or {}
        self.embedding = None

    def get_content(self):
        return self._text


class FakeVectorStore:
    def __init__(self, nodes=None):
        self.nodes = list(nodes or [])
        self.added_nodes = []
        self.deleted_ref_doc_ids = []
        self.queries = []

    def get_nodes(self, filters=None):
        return list(self.nodes)

    def add(self, nodes):
        self.added_nodes.extend(nodes)
        self.nodes.extend(nodes)

    def query(self, query):
        self.queries.append(query)
        return type("VectorResult", (), {"nodes": list(self.nodes)})()

    def delete(self, ref_doc_id):
        self.deleted_ref_doc_ids.append(ref_doc_id)
        self.nodes = [
            node for node in self.nodes if node.metadata.get("file_id") != ref_doc_id
        ]


class FakeConnection:
    def __init__(self):
        self.executed = []
        self.committed = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement):
        self.executed.append(str(statement))

    def commit(self):
        self.committed += 1


class FakeEngine:
    def __init__(self):
        self.connection = FakeConnection()

    def connect(self):
        return self.connection


class FakeEmbedding:
    embedding_dim = 3

    async def encode(self, texts):
        return [[float(i), 0.0, 0.0] for i, _ in enumerate(texts, start=1)]

    async def encode_single(self, text):
        return [1.0, 0.0, 0.0]

    async def rerank(self, query, texts):
        return list(reversed(range(len(texts))))


class FakeSplitter:
    def __init__(self, chunk_size, chunk_overlap):
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap

    def get_nodes_from_documents(self, docs):
        text = docs[0].text
        midpoint = max(1, len(text) // 2)
        return [
            FakeNode(text[:midpoint], dict(docs[0].metadata)),
            FakeNode(text[midpoint:], dict(docs[0].metadata)),
        ]


@pytest.fixture
def filerag(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://example")
    service = FileRAGService("session-1", FakeEmbedding())
    store = FakeVectorStore()
    service._vector_store = store
    return service, store


@pytest.mark.storage
@pytest.mark.no_network
def test_file_rag_rebuilds_manifest_and_bm25_state_from_vector_store(filerag):
    service, store = filerag
    store.nodes = [
        FakeNode(
            "alpha chunk",
            {
                "file_id": "file_1",
                "original_name": "notes.md",
                "extension": ".md",
                "size_bytes": 12,
                "uploaded_at": "2026-01-01T00:00:00+00:00",
            },
        ),
        FakeNode(
            "beta chunk",
            {
                "file_id": "file_1",
                "original_name": "notes.md",
                "extension": ".md",
                "size_bytes": 12,
                "uploaded_at": "2026-01-01T00:00:00+00:00",
            },
        ),
        FakeNode("orphan chunk", {"original_name": "ignored.md"}),
    ]

    service._load_state_from_vector_store()

    assert service._manifest == {
        "file_1": {
            "file_id": "file_1",
            "original_name": "notes.md",
            "extension": ".md",
            "size_bytes": 12,
            "chunk_count": 2,
            "uploaded_at": "2026-01-01T00:00:00+00:00",
        }
    }
    assert service._bm25_corpus == ["alpha chunk", "beta chunk"]
    assert service._bm25_metadata == [
        {"file_id": "file_1", "file_name": "notes.md"},
        {"file_id": "file_1", "file_name": "notes.md"},
    ]
    assert service._bm25_dirty is True
    assert service._loaded_from_store is True


@pytest.mark.storage
@pytest.mark.no_network
def test_file_rag_sanitizes_llamaindex_table_names(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://example")
    service = FileRAGService('session-1"; DROP TABLE x;--', FakeEmbedding())

    assert service._session_table_name() == "file_chunks_session_1___DROP_TABLE_x"


@pytest.mark.storage
@pytest.mark.no_network
def test_file_rag_cleanup_uses_sanitized_llamaindex_table_name(filerag):
    service, store = filerag
    service.session_id = 'session-1"; DROP TABLE x;--'
    store._engine = FakeEngine()

    service.cleanup_session()

    assert store._engine.connection.executed == [
        'DROP TABLE IF EXISTS "data_file_chunks_session_1___DROP_TABLE_x"'
    ]
    assert store._engine.connection.committed == 1
    assert service._vector_store is None


@pytest.mark.storage
@pytest.mark.no_network
async def test_file_rag_ingest_file_chunks_embeds_and_updates_manifest(
    monkeypatch, tmp_path, filerag
):
    service, store = filerag
    monkeypatch.setattr(file_rag_module, "SentenceSplitter", FakeSplitter)
    file_path = tmp_path / "notes.md"
    file_path.write_text("alpha beta gamma delta", encoding="utf-8")

    meta = await service.ingest_file(str(file_path), "notes.md")

    assert meta["original_name"] == "notes.md"
    assert meta["extension"] == ".md"
    assert meta["chunk_count"] == 2
    assert meta["file_id"] in service._manifest
    assert len(store.added_nodes) == 2
    assert [node.embedding for node in store.added_nodes] == [
        [1.0, 0.0, 0.0],
        [2.0, 0.0, 0.0],
    ]


@pytest.mark.storage
@pytest.mark.no_network
async def test_file_rag_search_combines_candidates_and_applies_file_filter(filerag):
    service, store = filerag
    service._manifest = {
        "file_1": {"file_id": "file_1", "original_name": "notes.md"},
        "file_2": {"file_id": "file_2", "original_name": "other.md"},
    }
    service._loaded_from_store = True
    service._bm25_dirty = False
    service._bm25 = None
    service._bm25_corpus = []
    store.nodes = [
        FakeNode("alpha", {"file_id": "file_1", "file_name": "notes.md"}),
        FakeNode("beta", {"file_id": "file_2", "file_name": "other.md"}),
    ]

    results = await service.search("alpha", n_results=5, file_filter="file_1")

    assert results == [
        {
            "content": "alpha",
            "file_name": "notes.md",
            "file_id": "file_1",
            "score": 0.5,
            "raw_score": 0.0,
        }
    ]


@pytest.mark.storage
@pytest.mark.no_network
async def test_file_rag_delete_file_prunes_manifest_bm25_and_vector_store(filerag):
    service, store = filerag
    service._loaded_from_store = True
    service._manifest = {
        "file_1": {"file_id": "file_1", "original_name": "notes.md"},
        "file_2": {"file_id": "file_2", "original_name": "other.md"},
    }
    service._bm25_corpus = ["alpha", "beta"]
    service._bm25_metadata = [
        {"file_id": "file_1", "file_name": "notes.md"},
        {"file_id": "file_2", "file_name": "other.md"},
    ]

    assert await service.delete_file("file_1") is True

    assert store.deleted_ref_doc_ids == ["file_1"]
    assert "file_1" not in service._manifest
    assert service._bm25_corpus == ["beta"]
    assert service._bm25_metadata == [{"file_id": "file_2", "file_name": "other.md"}]
    assert service._bm25_dirty is True


@pytest.mark.storage
@pytest.mark.no_network
async def test_file_rag_rejects_unsupported_file_types(tmp_path, filerag):
    service, _ = filerag
    file_path = tmp_path / "notes.exe"
    file_path.write_text("nope", encoding="utf-8")

    with pytest.raises(ValueError, match="Unsupported file type"):
        await service.ingest_file(str(file_path), "notes.exe")
