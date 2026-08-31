from uuid import uuid4

import pytest

from core.knowledge.db.readers.document_reader import DocumentReader


async def _insert_document(
    client,
    *,
    document_id: str,
    project_id: str,
    content: bytes,
    extracted_text: str,
) -> None:
    content_hash = f"hash-{document_id}"
    await client.execute(
        """
        INSERT INTO project_documents (
            document_id, project_id, original_name,
            relative_path, extension, size_bytes, content_hash
        ) VALUES (%s, %s, 'notes.md', %s, '.md', %s, %s)
        """,
        (
            document_id,
            project_id,
            f"{document_id}.md",
            len(content),
            content_hash,
        ),
    )
    await client.execute(
        """
        INSERT INTO document_content (
            document_id, content, extracted_text, extracted_content_hash
        ) VALUES (%s, %s, %s, %s)
        """,
        (document_id, content, extracted_text, content_hash),
    )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_document_reader_content_and_text_follow_readable_project_scope(
    real_postgres_client,
):
    visible_document_id = str(uuid4())
    readable_cross_project_document_id = str(uuid4())
    await _insert_document(
        real_postgres_client,
        document_id=visible_document_id,
        project_id="project-1",
        content=b"visible",
        extracted_text="visible text",
    )
    await _insert_document(
        real_postgres_client,
        document_id=readable_cross_project_document_id,
        project_id="project-2",
        content=b"other project visible",
        extracted_text="other project visible text",
    )
    reader = DocumentReader(
        real_postgres_client,
        "project-1",
        readable_project_ids=["project-1", "project-2"],
    )

    assert await reader.fetch_document_content(
        document_id=visible_document_id,
    ) == b"visible"
    assert await reader.fetch_extracted_text(
        document_id=visible_document_id,
        content_hash=f"hash-{visible_document_id}",
    ) == "visible text"
    assert await reader.fetch_document_content(
        document_id=readable_cross_project_document_id,
    ) == b"other project visible"
    assert await reader.fetch_extracted_text(
        document_id=readable_cross_project_document_id,
        content_hash=f"hash-{readable_cross_project_document_id}",
    ) == "other project visible text"
