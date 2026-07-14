from contextlib import asynccontextmanager

import pytest

from core.knowledge.db.writers.document_writer import DocumentWriter


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
        session_id=None,
        visibility_scope="project",
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
