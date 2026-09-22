import hashlib
import json
from uuid import uuid4

import pytest

from core.knowledge.db.readers.document_reader import DocumentReader
from core.knowledge.documents.storage import DocumentParseSnapshot


async def _insert_document_snapshot(
    client,
    *,
    document_id: str,
    project_id: str,
    text: str,
) -> tuple[str, str]:
    content_hash = hashlib.sha256(text.encode()).hexdigest()
    snapshot_id = str(uuid4())
    snapshot = DocumentParseSnapshot(
        text=text,
        structure={"format": "test"},
        parser_name="test-parser",
        parser_version="test",
        parser_fingerprint="a" * 64,
    )
    await client.execute(
        """
        INSERT INTO public.project_documents (
            document_id, project_id, original_name, relative_path,
            extension, size_bytes, content_hash, status
        )
        VALUES (%s, %s, 'notes.md', %s, '.md', %s, %s, 'indexed')
        """,
        (
            document_id,
            project_id,
            f"{document_id}.md",
            len(text.encode()),
            content_hash,
        ),
    )
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
            snapshot.parser_name,
            snapshot.parser_version,
            snapshot.parser_fingerprint,
            json.dumps(snapshot.to_storage_payload()),
        ),
    )
    await client.execute(
        "UPDATE public.project_documents SET current_snapshot_id = %s WHERE document_id = %s",
        (snapshot_id, document_id),
    )
    return content_hash, snapshot_id


@pytest.mark.storage
@pytest.mark.no_network
def test_document_snapshot_normalization_decodes_json_and_ids():
    normalized = DocumentReader._normalize_snapshot(
        {
            "snapshot_id": uuid4(),
            "document_id": uuid4(),
            "current_snapshot_id": uuid4(),
            "snapshot": '{"text":"stored"}',
        }
    )

    assert isinstance(normalized["snapshot_id"], str)
    assert isinstance(normalized["document_id"], str)
    assert isinstance(normalized["current_snapshot_id"], str)
    assert normalized["snapshot"] == {"text": "stored"}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_current_parse_snapshot_follows_readable_project_scope(
    real_postgres_client,
):
    visible_document_id = str(uuid4())
    cross_project_document_id = str(uuid4())
    visible_hash, visible_snapshot_id = await _insert_document_snapshot(
        real_postgres_client,
        document_id=visible_document_id,
        project_id="project-1",
        text="visible text",
    )
    cross_project_hash, cross_project_snapshot_id = await _insert_document_snapshot(
        real_postgres_client,
        document_id=cross_project_document_id,
        project_id="project-2",
        text="other project visible text",
    )
    reader = DocumentReader(
        real_postgres_client,
        "project-1",
        readable_project_ids=["project-1", "project-2"],
    )

    visible = await reader.fetch_current_parse_snapshot(
        document_id=visible_document_id,
        content_hash=visible_hash,
    )
    cross_project = await reader.fetch_current_parse_snapshot(
        document_id=cross_project_document_id,
        content_hash=cross_project_hash,
    )

    assert visible is not None
    assert visible["snapshot_id"] == visible_snapshot_id
    assert visible["snapshot"]["text"] == "visible text"
    assert cross_project is not None
    assert cross_project["snapshot_id"] == cross_project_snapshot_id
    assert cross_project["snapshot"]["text"] == "other project visible text"


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_retained_parse_snapshot_remains_readable_after_a_document_tombstone(
    real_postgres_client,
):
    document_id = str(uuid4())
    content_hash, snapshot_id = await _insert_document_snapshot(
        real_postgres_client,
        document_id=document_id,
        project_id="project-1",
        text="retained evidence",
    )
    await real_postgres_client.execute(
        "UPDATE public.project_documents SET status = 'deleted' WHERE document_id = %s",
        (document_id,),
    )
    reader = DocumentReader(real_postgres_client, "project-1")

    assert await reader.fetch_current_parse_snapshot(
        document_id=document_id,
        content_hash=content_hash,
    ) is None
    historical = await reader.fetch_parse_snapshot(
        document_id=document_id,
        snapshot_id=snapshot_id,
    )
    assert historical is not None
    assert historical["document_status"] == "deleted"
    assert historical["snapshot"]["text"] == "retained evidence"
