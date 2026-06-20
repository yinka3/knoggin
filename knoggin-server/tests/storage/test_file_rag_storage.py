import uuid

import pytest


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
async def test_file_chunks_are_deleted_with_parent_file(real_postgres_client):
    file_id = str(uuid.uuid4())
    chunk_id = str(uuid.uuid4())
    embedding = "[" + ",".join(["0"] * 1024) + "]"

    await real_postgres_client.execute_write(
        """
        INSERT INTO project_files (
            file_id, project_id, visibility_scope, original_name,
            relative_path, extension, size_bytes, content_hash, storage_key
        )
        VALUES (%s, 'project-1', 'project', 'notes.md',
                'notes.md', '.md', 5, 'hash', 'project-1/file/content')
        """,
        (file_id,),
    )
    await real_postgres_client.execute_write(
        """
        INSERT INTO file_chunks (
            chunk_id, file_id, chunk_index, content, embedding
        )
        VALUES (%s, %s, 0, 'alpha', %s::vector)
        """,
        (chunk_id, file_id, embedding),
    )

    await real_postgres_client.execute_write(
        "DELETE FROM project_files WHERE file_id = %s",
        (file_id,),
    )
    rows = await real_postgres_client.execute_read(
        "SELECT chunk_id FROM file_chunks WHERE file_id = %s",
        (file_id,),
    )

    assert rows == []
