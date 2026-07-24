from uuid import uuid4

import pytest

from core.knowledge.db.readers.document_reader import DocumentReader


async def _insert_document(
    client,
    *,
    document_id: str,
    project_id: str,
    session_id: str | None,
    visibility_scope: str,
    content: bytes,
    extracted_text: str,
) -> None:
    content_hash = f"hash-{document_id}"
    await client.execute(
        """
        INSERT INTO project_documents (
            document_id, project_id, session_id, visibility_scope, original_name,
            relative_path, extension, size_bytes, content_hash
        ) VALUES (%s, %s, %s, %s, 'notes.md', %s, '.md', %s, %s)
        """,
        (
            document_id,
            project_id,
            session_id,
            visibility_scope,
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
async def test_document_reader_content_and_text_require_project_session_visibility(
    real_postgres_client,
):
    visible_document_id = str(uuid4())
    hidden_project_document_id = str(uuid4())
    hidden_session_document_id = str(uuid4())
    await _insert_document(
        real_postgres_client,
        document_id=visible_document_id,
        project_id="project-1",
        session_id=None,
        visibility_scope="project",
        content=b"visible",
        extracted_text="visible text",
    )
    await _insert_document(
        real_postgres_client,
        document_id=hidden_project_document_id,
        project_id="project-2",
        session_id=None,
        visibility_scope="project",
        content=b"other project",
        extracted_text="other project text",
    )
    await _insert_document(
        real_postgres_client,
        document_id=hidden_session_document_id,
        project_id="project-1",
        session_id="session-2",
        visibility_scope="session",
        content=b"other session",
        extracted_text="other session text",
    )
    reader = DocumentReader(real_postgres_client, "project-1")

    assert await reader.fetch_document_content(
        document_id=visible_document_id,
        session_id="session-1",
    ) == b"visible"
    assert await reader.fetch_extracted_text(
        document_id=visible_document_id,
        content_hash=f"hash-{visible_document_id}",
        session_id="session-1",
    ) == "visible text"
    assert await reader.fetch_document_content(
        document_id=hidden_project_document_id,
        session_id="session-1",
    ) is None
    assert await reader.fetch_extracted_text(
        document_id=hidden_project_document_id,
        content_hash=f"hash-{hidden_project_document_id}",
        session_id="session-1",
    ) is None
    assert await reader.fetch_document_content(
        document_id=hidden_session_document_id,
        session_id="session-1",
    ) is None
    assert await reader.fetch_extracted_text(
        document_id=hidden_session_document_id,
        content_hash=f"hash-{hidden_session_document_id}",
        session_id="session-1",
    ) is None
