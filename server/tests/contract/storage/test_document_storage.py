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
    assert (await project_two.fetch_documents_by_reference(
        document_id=document_id,
        relative_path=None,
        session_id=None,
    ))[0]["document_id"] == document_id


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
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


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
async def test_folder_upload_delete_cascades_documents_and_chunks(
    real_postgres_client,
):
    folder_root_id = str(uuid.uuid4())
    document_id = str(uuid.uuid4())
    chunk_id = str(uuid.uuid4())
    embedding = "[" + ",".join(["0"] * 1024) + "]"

    await real_postgres_client.execute(
        """
        INSERT INTO document_folder_uploads (
            folder_root_id, project_id, visibility_scope, folder_name,
            candidate_count, candidate_bytes, document_count,
            total_size_bytes, excluded_count, excluded_bytes,
            excluded_directory_count, excluded_reason_counts,
            scan_settings, indexed_at
        )
        VALUES (
            %s, 'project-1', 'project', 'repo', 1, 5, 1, 5,
            0, 0, 0, '{}'::jsonb, '{}'::jsonb, NOW()
        )
        """,
        (folder_root_id,),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO project_documents (
            document_id, project_id, visibility_scope, folder_root_id,
            source_kind, original_name, relative_path, extension,
            size_bytes, content_hash, status, indexed_at
        )
        VALUES (
            %s, 'project-1', 'project', %s, 'folder_upload',
            'notes.md', 'notes.md', '.md', 5, 'hash', 'indexed', NOW()
        )
        """,
        (document_id, folder_root_id),
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
        "DELETE FROM document_folder_uploads WHERE folder_root_id = %s",
        (folder_root_id,),
    )
    documents = await real_postgres_client.fetch_all(
        "SELECT document_id FROM project_documents WHERE document_id = %s",
        (document_id,),
    )
    chunks = await real_postgres_client.fetch_all(
        "SELECT chunk_id FROM document_chunks WHERE chunk_id = %s",
        (chunk_id,),
    )

    assert documents == []
    assert chunks == []
