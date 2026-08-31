import uuid

import pytest

from core.knowledge.db.readers.document_reader import DocumentReader
from core.knowledge.db.writers.document_writer import DocumentWriter


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
async def test_document_content_is_deleted_with_parent_document(
    real_postgres_client,
):
    document_id = str(uuid.uuid4())

    await real_postgres_client.execute(
        """
        INSERT INTO project_documents (
            document_id, project_id, visibility_scope, original_name,
            relative_path, extension, size_bytes, content_hash
        )
        VALUES (%s, 'project-1', 'project', 'notes.md',
                'notes.md', '.md', 5, 'hash')
        """,
        (document_id,),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO document_content (document_id, content)
        VALUES (%s, %s)
        """,
        (document_id, b"hello"),
    )

    await real_postgres_client.execute(
        "DELETE FROM project_documents WHERE document_id = %s",
        (document_id,),
    )
    rows = await real_postgres_client.fetch_all(
        "SELECT document_id FROM document_content WHERE document_id = %s",
        (document_id,),
    )

    assert rows == []


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
async def test_document_chunks_are_deleted_with_parent_document(
    real_postgres_client,
):
    document_id = str(uuid.uuid4())
    chunk_id = str(uuid.uuid4())
    embedding = "[" + ",".join(["0"] * 1024) + "]"

    await real_postgres_client.execute(
        """
        INSERT INTO project_documents (
            document_id, project_id, visibility_scope, original_name,
            relative_path, extension, size_bytes, content_hash
        )
        VALUES (%s, 'project-1', 'project', 'notes.md',
                'notes.md', '.md', 5, 'hash')
        """,
        (document_id,),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO document_chunks (
            chunk_id, document_id, chunk_index, content, relative_path, embedding
        )
        VALUES (%s, %s, 0, 'alpha', 'notes.md', %s::vector)
        """,
        (chunk_id, document_id, embedding),
    )

    await real_postgres_client.execute(
        "DELETE FROM project_documents WHERE document_id = %s",
        (document_id,),
    )
    rows = await real_postgres_client.fetch_all(
        "SELECT chunk_id FROM document_chunks WHERE document_id = %s",
        (document_id,),
    )

    assert rows == []


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_document_reader_cannot_cross_project_scope(real_postgres_client):
    document_id = str(uuid.uuid4())
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_documents (
            document_id, project_id, visibility_scope, source_kind,
            original_name, relative_path, extension, size_bytes, content_hash
        )
        VALUES (%s, 'project-2', 'project', 'manual_upload',
                'private.md', 'private.md', '.md', 7, 'private-hash')
        """,
        (document_id,),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.document_content (document_id, content)
        VALUES (%s, %s)
        """,
        (document_id, b"private"),
    )

    project_one = DocumentReader(real_postgres_client, "project-1")
    project_two = DocumentReader(real_postgres_client, "project-2")

    assert await project_one.fetch_documents_by_reference(
        document_id=document_id,
        relative_path=None,
        session_id=None,
    ) == []
    assert await project_one.fetch_document_content(
        document_id=document_id,
        session_id=None,
    ) is None
    assert str((await project_two.fetch_documents_by_reference(
        document_id=document_id,
        relative_path=None,
        session_id=None,
    ))[0]["document_id"]) == document_id


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
              AND column_name = 'folder_root_id'
        ) AS missing
        """
    ) == {"missing": True}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
async def test_document_writer_tombstones_metadata_and_purges_content_and_chunks(
    real_postgres_client,
):
    document_id = str(uuid.uuid4())
    chunk_id = str(uuid.uuid4())
    embedding = "[" + ",".join(["0"] * 1024) + "]"
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_documents (
            document_id, project_id, visibility_scope, source_kind,
            original_name, relative_path, extension, size_bytes, content_hash,
            status
        )
        VALUES (%s, 'project-1', 'project', 'manual_upload',
                'notes.md', 'notes.md', '.md', 5, 'a'::text, 'indexed')
        """,
        (document_id,),
    )
    await real_postgres_client.execute(
        "INSERT INTO public.document_content (document_id, content) VALUES (%s, %s)",
        (document_id, b"hello"),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.document_chunks (
            chunk_id, document_id, chunk_index, content, relative_path, embedding
        ) VALUES (%s, %s, 0, 'hello', 'notes.md', %s::vector)
        """,
        (chunk_id, document_id, embedding),
    )

    deleted = await DocumentWriter(real_postgres_client, "project-1").delete_document(
        document_id=document_id,
        session_id=None,
    )

    assert deleted is not None
    assert deleted["status"] == "deleted"
    assert await real_postgres_client.fetch_one(
        """
        SELECT status, deleted_at IS NOT NULL AS has_deleted_at
        FROM public.project_documents
        WHERE document_id = %s
        """,
        (document_id,),
    ) == {"status": "deleted", "has_deleted_at": True}
    assert await real_postgres_client.fetch_all(
        "SELECT document_id FROM public.document_content WHERE document_id = %s",
        (document_id,),
    ) == []
    assert await real_postgres_client.fetch_all(
        "SELECT chunk_id FROM public.document_chunks WHERE document_id = %s",
        (document_id,),
    ) == []
