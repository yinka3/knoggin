import hashlib
import json
import uuid

import pytest

from core.knowledge.db.readers.document_reader import DocumentReader
from core.knowledge.db.writers.document_writer import DocumentWriter
from core.knowledge.documents.storage import DocumentParseSnapshot


def _snapshot(text: str, *, parser_version: str = "test") -> DocumentParseSnapshot:
    return DocumentParseSnapshot(
        text=text,
        structure={"format": "test"},
        parser_name="test-parser",
        parser_version=parser_version,
        parser_fingerprint="a" * 64,
    )


async def _insert_document(
    client,
    *,
    document_id: str,
    project_id: str = "project-1",
    content_hash: str = "a" * 64,
    status: str = "queued",
) -> None:
    await client.execute(
        """
        INSERT INTO public.project_documents (
            document_id, project_id, original_name, relative_path,
            extension, size_bytes, content_hash, status
        )
        VALUES (%s, %s, 'notes.md', 'notes.md', '.md', 5, %s, %s)
        """,
        (document_id, project_id, content_hash, status),
    )


async def _insert_snapshot(
    client,
    *,
    document_id: str,
    content_hash: str,
    snapshot_id: str | None = None,
    text: str = "hello",
) -> str:
    snapshot_id = snapshot_id or str(uuid.uuid4())
    parse_snapshot = _snapshot(text)
    await client.execute(
        """
        INSERT INTO public.document_parse_snapshots (
            snapshot_id, document_id, source_content_hash,
            parser_name, parser_version, parser_fingerprint, snapshot
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
        """,
        (
            snapshot_id,
            document_id,
            content_hash,
            parse_snapshot.parser_name,
            parse_snapshot.parser_version,
            parse_snapshot.parser_fingerprint,
            json.dumps(parse_snapshot.to_storage_payload()),
        ),
    )
    await client.execute(
        "UPDATE public.project_documents SET current_snapshot_id = %s WHERE document_id = %s",
        (snapshot_id, document_id),
    )
    return snapshot_id


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
async def test_parse_snapshot_is_deleted_with_a_hard_deleted_parent_document(
    real_postgres_client,
):
    document_id = str(uuid.uuid4())
    await _insert_document(real_postgres_client, document_id=document_id)
    await _insert_snapshot(
        real_postgres_client,
        document_id=document_id,
        content_hash="a" * 64,
    )

    await real_postgres_client.execute(
        "DELETE FROM public.project_documents WHERE document_id = %s",
        (document_id,),
    )

    assert await real_postgres_client.fetch_all(
        "SELECT snapshot_id FROM public.document_parse_snapshots WHERE document_id = %s",
        (document_id,),
    ) == []


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
async def test_document_chunks_are_deleted_with_a_hard_deleted_parent_document(
    real_postgres_client,
):
    document_id = str(uuid.uuid4())
    snapshot_id = str(uuid.uuid4())
    chunk_id = str(uuid.uuid4())
    embedding = "[" + ",".join(["0"] * 1024) + "]"
    await _insert_document(real_postgres_client, document_id=document_id)
    await _insert_snapshot(
        real_postgres_client,
        document_id=document_id,
        content_hash="a" * 64,
        snapshot_id=snapshot_id,
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.document_chunks (
            chunk_id, document_id, snapshot_id, chunk_index,
            content, relative_path, embedding
        )
        VALUES (%s, %s, %s, 0, 'alpha', 'notes.md', %s::vector)
        """,
        (chunk_id, document_id, snapshot_id, embedding),
    )

    await real_postgres_client.execute(
        "DELETE FROM public.project_documents WHERE document_id = %s",
        (document_id,),
    )

    assert await real_postgres_client.fetch_all(
        "SELECT chunk_id FROM public.document_chunks WHERE document_id = %s",
        (document_id,),
    ) == []


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_document_reader_cannot_cross_project_catalog_scope(real_postgres_client):
    document_id = str(uuid.uuid4())
    await _insert_document(
        real_postgres_client,
        document_id=document_id,
        project_id="project-2",
        content_hash="b" * 64,
    )
    project_one = DocumentReader(real_postgres_client, "project-1")
    project_two = DocumentReader(real_postgres_client, "project-2")

    assert await project_one.fetch_documents_by_reference(
        document_id=document_id,
        relative_path=None,
    ) == []
    assert str(
        (
            await project_two.fetch_documents_by_reference(
                document_id=document_id,
                relative_path=None,
            )
        )[0]["document_id"]
    ) == document_id


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_document_catalog_has_no_folder_batch_identity(real_postgres_client):
    assert await real_postgres_client.fetch_one(
        "SELECT to_regclass('public.document_folder_uploads') IS NULL AS missing"
    ) == {"missing": True}
    assert await real_postgres_client.fetch_one(
        """
        SELECT NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'project_documents'
              AND column_name IN (
                  'folder_root_id', 'session_id', 'visibility_scope', 'source_kind'
              )
        ) AS missing
        """
    ) == {"missing": True}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
async def test_document_tombstone_keeps_the_snapshot_and_removes_current_chunks(
    real_postgres_client,
):
    document_id = str(uuid.uuid4())
    snapshot_id = str(uuid.uuid4())
    chunk_id = str(uuid.uuid4())
    embedding = "[" + ",".join(["0"] * 1024) + "]"
    await _insert_document(
        real_postgres_client,
        document_id=document_id,
        content_hash="a" * 64,
        status="indexed",
    )
    await _insert_snapshot(
        real_postgres_client,
        document_id=document_id,
        content_hash="a" * 64,
        snapshot_id=snapshot_id,
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.document_chunks (
            chunk_id, document_id, snapshot_id, chunk_index,
            content, relative_path, embedding
        )
        VALUES (%s, %s, %s, 0, 'hello', 'notes.md', %s::vector)
        """,
        (chunk_id, document_id, snapshot_id, embedding),
    )

    deleted = await DocumentWriter(real_postgres_client, "project-1").delete_document(
        document_id=document_id,
    )

    assert deleted is not None
    assert deleted["status"] == "deleted"
    assert await real_postgres_client.fetch_one(
        """
        SELECT status, current_snapshot_id = %s AS keeps_current_snapshot
        FROM public.project_documents
        WHERE document_id = %s
        """,
        (snapshot_id, document_id),
    ) == {"status": "deleted", "keeps_current_snapshot": True}
    retained = await real_postgres_client.fetch_one(
        "SELECT snapshot_id FROM public.document_parse_snapshots WHERE snapshot_id = %s",
        (snapshot_id,),
    )
    assert retained is not None
    assert str(retained["snapshot_id"]) == snapshot_id
    assert await real_postgres_client.fetch_all(
        "SELECT chunk_id FROM public.document_chunks WHERE document_id = %s",
        (document_id,),
    ) == []


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_document_publication_binds_chunks_to_the_snapshot_of_read_bytes(
    real_postgres_client,
):
    document_id = str(uuid.uuid4())
    read_content_hash = hashlib.sha256(b"alpha").hexdigest()
    await _insert_document(
        real_postgres_client,
        document_id=document_id,
        content_hash=read_content_hash,
        status="indexing",
    )
    writer = DocumentWriter(real_postgres_client, "project-1")

    rejected = await writer.persist_indexed_chunks(
        document_id=document_id,
        chunks=["alpha"],
        embeddings=[[0.0] * 1024],
        parse_snapshot=_snapshot("alpha"),
        indexed_at="2026-09-11T00:00:00+00:00",
        read_content_hash="b" * 64,
    )

    assert rejected is None
    assert await real_postgres_client.fetch_one(
        "SELECT status FROM public.project_documents WHERE document_id = %s",
        (document_id,),
    ) == {"status": "indexing"}
    assert await real_postgres_client.fetch_all(
        "SELECT chunk_id FROM public.document_chunks WHERE document_id = %s",
        (document_id,),
    ) == []
    assert await real_postgres_client.fetch_all(
        "SELECT snapshot_id FROM public.document_parse_snapshots WHERE document_id = %s",
        (document_id,),
    ) == []

    published = await writer.persist_indexed_chunks(
        document_id=document_id,
        chunks=["alpha"],
        embeddings=[[0.0] * 1024],
        parse_snapshot=_snapshot("alpha"),
        indexed_at="2026-09-11T00:00:00+00:00",
        read_content_hash=read_content_hash,
    )

    assert published is not None
    snapshot_id = published["current_snapshot_id"]
    assert published["status"] == "indexed"
    assert await real_postgres_client.fetch_one(
        """
        SELECT source_content_hash, snapshot ->> 'text' AS text
        FROM public.document_parse_snapshots
        WHERE snapshot_id = %s
        """,
        (snapshot_id,),
    ) == {"source_content_hash": read_content_hash, "text": "alpha"}
    assert await real_postgres_client.fetch_one(
        "SELECT snapshot_id FROM public.document_chunks WHERE document_id = %s",
        (document_id,),
    ) == {"snapshot_id": snapshot_id}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_transient_index_failure_is_durable_and_exhaustion_requires_explicit_retry(
    real_postgres_client,
):
    document_id = str(uuid.uuid4())
    content_hash = "a" * 64
    await _insert_document(
        real_postgres_client,
        document_id=document_id,
        content_hash=content_hash,
        status="indexing",
    )
    snapshot_id = await _insert_snapshot(
        real_postgres_client,
        document_id=document_id,
        content_hash=content_hash,
    )
    writer = DocumentWriter(real_postgres_client, "project-1")
    reader = DocumentReader(real_postgres_client, "project-1")

    first = await writer.record_index_failure(
        document_id=document_id,
        error_message="embedding provider unavailable",
        failure_kind="transient_dependency",
        retryable=True,
        max_attempts=2,
        retry_backoff_seconds=10,
        updated_at="2026-09-20T00:00:00+00:00",
    )

    assert first is not None
    assert first["status"] == "queued"
    assert first["index_attempt_count"] == 1
    assert first["last_index_failure_kind"] == "transient_dependency"
    assert first["next_index_retry_at"] is not None
    assert await reader.list_documents_for_index_recovery(
        due_at="2026-09-20T00:00:09+00:00",
        limit=10,
    ) == []
    assert [str(row["document_id"]) for row in await reader.list_documents_for_index_recovery(
        due_at="2026-09-20T00:00:10+00:00",
        limit=10,
    )] == [document_id]

    claimed = await writer.transition_index_status(
        document_id=document_id,
        status="indexing",
        allowed_statuses=("queued",),
        updated_at="2026-09-20T00:00:10+00:00",
    )
    assert claimed is not None
    exhausted = await writer.record_index_failure(
        document_id=document_id,
        error_message="embedding provider still unavailable",
        failure_kind="transient_dependency",
        retryable=True,
        max_attempts=2,
        retry_backoff_seconds=10,
        updated_at="2026-09-20T00:00:10+00:00",
    )

    assert exhausted is not None
    assert exhausted["status"] == "failed"
    assert exhausted["index_attempt_count"] == 2
    assert exhausted["next_index_retry_at"] is None
    assert await reader.list_documents_for_index_recovery(
        due_at="2026-09-20T00:10:00+00:00",
        limit=10,
    ) == []
    stored = (await reader.list_documents(limit=10))[0]
    assert str(stored["current_snapshot_id"]) == snapshot_id
    assert stored["status"] == "failed"
    assert stored["index_attempt_count"] == 2
    assert stored["last_index_failure_kind"] == "transient_dependency"

    explicit_retry = await writer.transition_index_status(
        document_id=document_id,
        status="indexing",
        allowed_statuses=("failed",),
        updated_at="2026-09-20T00:10:00+00:00",
        reset_attempts=True,
    )

    assert explicit_retry is not None
    assert explicit_retry["index_attempt_count"] == 0
    assert explicit_retry["last_index_failure_kind"] is None


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_reindex_keeps_an_earlier_snapshot_for_historical_evidence(
    real_postgres_client,
):
    document_id = str(uuid.uuid4())
    content_hash = hashlib.sha256(b"alpha").hexdigest()
    await _insert_document(
        real_postgres_client,
        document_id=document_id,
        content_hash=content_hash,
        status="indexing",
    )
    writer = DocumentWriter(real_postgres_client, "project-1")
    first = await writer.persist_indexed_chunks(
        document_id=document_id,
        chunks=["first parse"],
        embeddings=[[0.0] * 1024],
        parse_snapshot=_snapshot("first parse", parser_version="one"),
        indexed_at="2026-09-11T00:00:00+00:00",
        read_content_hash=content_hash,
    )
    assert first is not None
    first_snapshot_id = first["current_snapshot_id"]

    claimed = await writer.transition_index_status(
        document_id=document_id,
        status="indexing",
        allowed_statuses=("indexed",),
        updated_at="2026-09-11T00:01:00+00:00",
    )
    assert claimed is not None
    second = await writer.persist_indexed_chunks(
        document_id=document_id,
        chunks=["second parse"],
        embeddings=[[0.0] * 1024],
        parse_snapshot=_snapshot("second parse", parser_version="two"),
        indexed_at="2026-09-11T00:02:00+00:00",
        read_content_hash=content_hash,
    )
    assert second is not None
    assert second["current_snapshot_id"] != first_snapshot_id

    reader = DocumentReader(real_postgres_client, "project-1")
    historical = await reader.fetch_parse_snapshot(
        document_id=document_id,
        snapshot_id=first_snapshot_id,
    )
    assert historical is not None
    assert historical["snapshot"]["text"] == "first parse"
    assert await real_postgres_client.fetch_one(
        "SELECT count(*)::integer AS count FROM public.document_parse_snapshots WHERE document_id = %s",
        (document_id,),
    ) == {"count": 2}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*)::integer AS count FROM public.document_chunks WHERE document_id = %s",
        (document_id,),
    ) == {"count": 1}
