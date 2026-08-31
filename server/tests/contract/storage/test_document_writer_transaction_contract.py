from contextlib import asynccontextmanager

import pytest

from core.knowledge.db.writers.document_writer import DocumentWriter
from core.knowledge.documents.storage import DocumentChunk

_MISMATCH_ERROR = "chunks and embeddings must have the same length"


@pytest.mark.storage
@pytest.mark.no_network
def test_chunk_copy_row_preserves_all_source_locator_fields():
    row = DocumentWriter._chunk_copy_row(
        document_id="11111111-1111-4111-8111-111111111111",
        relative_path="docs/report.md",
        chunk_index=3,
        chunk=DocumentChunk(
            content="Revenue increased.",
            page_number=2,
            start_line=8,
            end_line=9,
            start_row=4,
            end_row=5,
            section_path=("Results", "Revenue"),
            start_paragraph=11,
            end_paragraph=13,
        ),
        embedding=[0.1] * 1024,
    )

    assert row[9:17] == (2, 8, 9, 4, 5, ["Results", "Revenue"], 11, 13)


class RecordingCursor:
    def __init__(self) -> None:
        self.calls = []

    async def execute(self, query, params=None) -> None:
        self.calls.append((query, params))


class TransactionOnlyClient:
    """PostgresClient-shaped fake that intentionally exposes no pool."""

    def __init__(self) -> None:
        self.cursor = RecordingCursor()
        self.transaction_count = 0

    @asynccontextmanager
    async def transaction(self):
        self.transaction_count += 1
        yield self.cursor


@pytest.mark.storage
@pytest.mark.no_network
async def test_insert_document_uses_public_transaction_contract():
    client = TransactionOnlyClient()
    writer = DocumentWriter(client, "project-1")

    await writer.insert_document(
        document_id="11111111-1111-4111-8111-111111111111",
        original_name="notes.md",
        relative_path="notes.md",
        extension=".md",
        size_bytes=5,
        content_hash="hash",
        content=b"hello",
        created_at="2026-07-12T00:00:00+00:00",
    )

    assert client.transaction_count == 1
    assert len(client.cursor.calls) == 2
    assert "INSERT INTO public.project_documents" in client.cursor.calls[0][0]
    assert "INSERT INTO public.document_content" in client.cursor.calls[1][0]


@pytest.mark.storage
@pytest.mark.no_network
async def test_document_writer_rejects_mismatched_chunk_embedding_lists():
    client = TransactionOnlyClient()
    writer = DocumentWriter(client, "project-1")

    with pytest.raises(ValueError, match=_MISMATCH_ERROR):
        await writer.persist_indexed_chunks(
            document_id="document-1",
            chunks=["chunk"],
            embeddings=[],
            extracted_text="notes",
            indexed_at="2026-07-23T00:00:00+00:00",
        )

    assert client.transaction_count == 0
