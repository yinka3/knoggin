import hashlib
import json
from uuid import uuid4

import pytest

from core.knowledge.db.readers.document_reader import DocumentReader
from core.knowledge.documents.extraction import DocumentParseSnapshot


async def _insert_document_snapshot(
    client,
    *,
    document_id: str,
    project_id: str,
    text: str,
    original_name: str = "notes.md",
    relative_path: str | None = None,
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
        VALUES (%s, %s, %s, %s, '.md', %s, %s, 'indexed')
        """,
        (
            document_id,
            project_id,
            original_name,
            relative_path or f"{document_id}.md",
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
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_indexed_name_lookup_reaches_older_rows_and_honors_subtree_scope(
    real_postgres_client,
):
    old_id = str(uuid4())
    await _insert_document_snapshot(
        real_postgres_client,
        document_id=old_id,
        project_id="project-1",
        text="old indexed document",
        original_name="history.md",
        relative_path="archive/history.md",
    )
    await real_postgres_client.execute(
        "UPDATE public.project_documents SET created_at = now() - interval '2 days' "
        "WHERE document_id = %s",
        (old_id,),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_documents (
            document_id, project_id, original_name, relative_path,
            extension, size_bytes, content_hash, status
        )
        SELECT
            ('10000000-0000-0000-0000-' || lpad(number::text, 12, '0'))::uuid,
            'project-1',
            'recent-' || number || '.md',
            'recent/recent-' || number || '.md',
            '.md', 1, repeat('a', 64), 'queued'
        FROM generate_series(1, 1000) AS generated(number)
        """
    )
    reader = DocumentReader(real_postgres_client, "project-1")

    newest_page = await reader.list_documents(limit=1000)
    assert len(newest_page) == 1000
    assert old_id not in {str(row["document_id"]) for row in newest_page}
    matches = await reader.find_indexed_documents_by_name(document_name="history.md")
    assert [str(row["document_id"]) for row in matches] == [old_id]
    path_match = await reader.find_indexed_documents_by_name(
        document_name="ARCHIVE/HISTORY.MD"
    )
    assert [str(row["document_id"]) for row in path_match] == [old_id]

    second_id = str(uuid4())
    await _insert_document_snapshot(
        real_postgres_client,
        document_id=second_id,
        project_id="project-1",
        text="second indexed document",
        original_name="history.md",
        relative_path="docs/history.md",
    )
    ambiguous = await reader.find_indexed_documents_by_name(document_name="history.md")
    assert {str(row["document_id"]) for row in ambiguous} == {old_id, second_id}
    assert len(ambiguous) == 2
    in_archive = await reader.find_indexed_documents_by_name(
        document_name="history.md", path_prefix="archive"
    )
    assert [str(row["document_id"]) for row in in_archive] == [old_id]
    in_docs = await reader.find_indexed_documents_by_name(
        document_name="history.md", path_prefix="docs"
    )
    assert [str(row["document_id"]) for row in in_docs] == [second_id]

    foreign_id = str(uuid4())
    await _insert_document_snapshot(
        real_postgres_client,
        document_id=foreign_id,
        project_id="project-2",
        text="linked project document",
        original_name="history.md",
        relative_path="external/history.md",
    )
    assert await reader.find_indexed_documents_by_name(
        document_name="history.md", path_prefix="external"
    ) == []
    linked_reader = DocumentReader(
        real_postgres_client,
        "project-1",
        readable_project_ids=["project-1", "project-2"],
    )
    linked = await linked_reader.find_indexed_documents_by_name(
        document_name="history.md", path_prefix="external"
    )
    assert [str(row["document_id"]) for row in linked] == [foreign_id]


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


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_documents_awaiting_index_are_listed_by_scope(real_postgres_client):
    for path, status in (
        ("docs/queued.md", "queued"), ("docs/running.md", "indexing"),
        ("docs/done.md", "indexed"), ("docs/broken.md", "failed"),
        ("other/queued.md", "queued"),
    ):
        await real_postgres_client.execute(
            """INSERT INTO public.project_documents (
                   document_id, project_id, original_name, relative_path,
                   extension, size_bytes, content_hash, status
               ) VALUES (%s, 'project-1', 'x.md', %s, '.md', 1, 'hash', %s)""",
            (str(uuid4()), path, status),
        )
    reader = DocumentReader(real_postgres_client, "project-1")

    assert set(await reader.list_documents_awaiting_index(path_prefix="docs")) == {
        "docs/queued.md", "docs/running.md",
    }
    assert len(await reader.list_documents_awaiting_index()) == 3
    assert await reader.list_documents_awaiting_index(path_prefix="docs/done.md") == []
