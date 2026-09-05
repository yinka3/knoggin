"""Physical schema-reset contracts for PostgreSQL storage fixtures."""

from uuid import uuid4

import pytest
from psycopg.errors import CheckViolation


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_fresh_schema_has_no_dropped_chunk_attributes(
    real_postgres_client,
):
    """Canonical creation must not consume PostgreSQL attribute slots."""

    row = await real_postgres_client.fetch_one(
        """
        SELECT c.relnatts,
               count(a.attname) FILTER (
                   WHERE a.attnum > 0 AND NOT a.attisdropped
               ) AS live_columns,
               count(*) FILTER (
                   WHERE a.attnum > 0 AND a.attisdropped
               ) AS dropped_columns
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_attribute a ON a.attrelid = c.oid
        WHERE n.nspname = 'public'
          AND c.relname = 'document_chunks'
        GROUP BY c.relnatts
        """
    )

    assert row is not None
    assert row["dropped_columns"] == 0
    assert row["relnatts"] == row["live_columns"]


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_project_documents_rejects_an_oversized_indexed_relative_path(
    real_postgres_client,
):
    with pytest.raises(CheckViolation, match="project_documents_relative_path_size_check"):
        await real_postgres_client.execute(
            """
            INSERT INTO public.project_documents (
                document_id,
                project_id,
                original_name,
                relative_path,
                extension,
                size_bytes,
                content_hash
            )
            VALUES (%s, 'project-1', 'notes.md', %s, '.md', 1, %s)
            """,
            (str(uuid4()), "a" * 2049, "a" * 64),
        )
