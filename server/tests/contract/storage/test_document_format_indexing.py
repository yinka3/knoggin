import hashlib

import pytest

from core.knowledge.documents import DocumentService, ProjectFilesystemFactory
from core.knowledge.documents import storage as document_storage
from core.knowledge.documents.storage import (
    DocumentParseSnapshot,
    DocumentSnapshotPage,
    LayoutRegion,
)
from tests.fixtures.documents import (
    build_docx_bytes,
    build_notebook_bytes,
    build_pdf_bytes,
    build_png_bytes,
)


class DeterministicDocumentEmbedding:
    async def encode(self, values):
        return [[float(index % 7) / 7 for index in range(1024)] for _ in values]


def _document_samples():
    return [
        (
            "notes.txt",
            b"First line\nSecond line\n",
            [
                {
                    "content": "First line\nSecond line",
                    "chunk_kind": "text",
                    "start_line": 1,
                    "end_line": 2,
                }
            ],
        ),
        (
            "launch.md",
            b"# Overview\nAlpha\n\n## Risks\nBeta\n",
            [
                {
                    "content": "# Overview\nAlpha",
                    "chunk_kind": "text",
                    "start_line": 1,
                    "end_line": 2,
                    "section_path": ["Overview"],
                },
                {
                    "content": "## Risks\nBeta",
                    "chunk_kind": "text",
                    "start_line": 4,
                    "end_line": 5,
                    "section_path": ["Overview", "Risks"],
                },
            ],
        ),
        (
            "metrics.csv",
            b"name,value\nalpha,1\nbeta,2\n",
            [
                {
                    "content": "name,value\nalpha,1\nbeta,2",
                    "chunk_kind": "csv",
                    "start_row": 1,
                    "end_row": 2,
                }
            ],
        ),
        (
            "launch.py",
            b"def alpha():\n    return 1\n\nclass Beta:\n    pass\n",
            [
                {
                    "content": "def alpha():\n    return 1",
                    "language": "python",
                    "chunk_kind": "code",
                    "symbol_name": "alpha",
                    "start_line": 1,
                    "end_line": 3,
                },
                {
                    "content": "class Beta:\n    pass",
                    "language": "python",
                    "chunk_kind": "code",
                    "symbol_name": "Beta",
                    "start_line": 4,
                    "end_line": 5,
                },
            ],
        ),
        (
            "analysis.ipynb",
            build_notebook_bytes(),
            [
                {
                    "content": "# Launch notes\nDeterministic notebook text.",
                    "language": "notebook",
                    "chunk_kind": "notebook_markdown",
                    "symbol_name": "cell 1",
                    "start_line": 1,
                    "end_line": 2,
                },
                {
                    "content": "def launch():\n    return 'ready'",
                    "language": "notebook",
                    "chunk_kind": "notebook_code",
                    "symbol_name": "cell 2",
                    "start_line": 1,
                    "end_line": 2,
                },
            ],
        ),
        (
            "brief.pdf",
            build_pdf_bytes("Alpha page.", "Beta page."),
            [
                {"content": "Alpha page.", "chunk_kind": "text", "page_number": 1},
                {"content": "Beta page.", "chunk_kind": "text", "page_number": 2},
            ],
        ),
        (
            "brief.docx",
            build_docx_bytes(
                [
                    ("Overview", 1),
                    ("Alpha paragraph.", None),
                    ("Risks", 2),
                    ("Beta paragraph.", None),
                ]
            ),
            [
                {
                    "content": "# Overview\n\nAlpha paragraph.",
                    "chunk_kind": "text",
                    "section_path": ["Overview"],
                    "start_line": 1,
                    "end_line": 3,
                },
                {
                    "content": "## Risks\n\nBeta paragraph.",
                    "chunk_kind": "text",
                    "section_path": ["Overview", "Risks"],
                    "start_line": 5,
                    "end_line": 7,
                },
            ],
        ),
        (
            "scan.png",
            build_png_bytes(),
            [
                {
                    "content": "Launch ready.\nProceed now.",
                    "chunk_kind": "text",
                    "start_line": 1,
                    "end_line": 2,
                }
            ],
        ),
    ]


def _structured_parse_snapshot(content: bytes, extension: str) -> DocumentParseSnapshot:
    if extension == ".pdf":
        return DocumentParseSnapshot(
            text="Alpha page.\n\nBeta page.",
            structure={"pages": {"1": {}, "2": {}}},
            parser_name="docling",
            parser_version="test",
            parser_fingerprint="a" * 64,
            pages=(
                DocumentSnapshotPage(
                    page_number=1,
                    text="Alpha page.",
                    regions=(
                        LayoutRegion(
                            page_number=1,
                            element_type="text",
                            extraction_method="native_text",
                        ),
                    ),
                ),
                DocumentSnapshotPage(
                    page_number=2,
                    text="Beta page.",
                    regions=(
                        LayoutRegion(
                            page_number=2,
                            element_type="text",
                            extraction_method="ocr",
                        ),
                    ),
                ),
            ),
        )
    if extension == ".docx":
        return DocumentParseSnapshot(
            text="# Overview\n\nAlpha paragraph.\n\n## Risks\n\nBeta paragraph.",
            structure={"texts": [{"label": "section_header"}]},
            parser_name="docling",
            parser_version="test",
            parser_fingerprint="a" * 64,
        )
    raise AssertionError(f"unexpected structured format {extension}")


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_representative_document_formats_publish_durable_located_chunks(
    real_postgres_client,
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(
        document_storage.pytesseract,
        "image_to_string",
        lambda _: "Launch ready.\nProceed now.\n",
    )
    monkeypatch.setattr(
        document_storage,
        "_extract_docling_snapshot",
        _structured_parse_snapshot,
    )
    service = DocumentService(
        project_id="project-1",
        postgres_client=real_postgres_client,
        embedding_service=DeterministicDocumentEmbedding(),
        document_rerank_enabled=False,
        filesystem_factory=ProjectFilesystemFactory(tmp_path / "projects"),
    )

    for original_name, content, expected_chunks in _document_samples():
        indexed = await service.submit_document(
            content=content,
            original_name=original_name,
        )

        assert indexed["status"] == "indexed"
        assert indexed["content_hash"] == hashlib.sha256(content).hexdigest()
        assert indexed["chunk_count"] == len(expected_chunks)

        snapshot_row = await real_postgres_client.fetch_one(
            """
            SELECT source_content_hash, parser_name, snapshot ->> 'text' AS text
            FROM public.document_parse_snapshots
            WHERE snapshot_id = %s
            """,
            (indexed["current_snapshot_id"],),
        )
        assert snapshot_row["text"].strip()
        assert snapshot_row["source_content_hash"] == indexed["content_hash"]
        if original_name.endswith((".pdf", ".docx")):
            assert snapshot_row["parser_name"] == "docling"

        chunk_rows = await real_postgres_client.fetch_all(
            """
            SELECT
                content, language, chunk_kind, symbol_name, page_number,
                start_line, end_line, start_row, end_row, section_path,
                start_paragraph, end_paragraph, layout_region
            FROM public.document_chunks
            WHERE document_id = %s
            ORDER BY chunk_index
            """,
            (indexed["document_id"],),
        )
        for actual, expected in zip(chunk_rows, expected_chunks, strict=True):
            assert actual == {**actual, **expected}
        if original_name.endswith(".pdf"):
            assert [row["layout_region"]["kind"] for row in chunk_rows] == [
                "layout_region",
                "layout_region",
            ]


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_failed_extraction_publishes_no_partial_derived_document_state(
    real_postgres_client,
    tmp_path,
):
    service = DocumentService(
        project_id="project-1",
        postgres_client=real_postgres_client,
        embedding_service=DeterministicDocumentEmbedding(),
        document_rerank_enabled=False,
        filesystem_factory=ProjectFilesystemFactory(tmp_path / "projects"),
    )

    with pytest.raises(RuntimeError, match="Notebook is not valid JSON"):
        await service.submit_document(
            content=b"not-json",
            original_name="broken.ipynb",
        )

    document = await real_postgres_client.fetch_one(
        """
        SELECT document_id, status, error_message
        FROM public.project_documents
        WHERE project_id = 'project-1' AND original_name = 'broken.ipynb'
        """
    )
    assert document["status"] == "failed"
    assert document["error_message"] == "Notebook is not valid JSON"
    assert (
        await real_postgres_client.fetch_all(
            "SELECT chunk_id FROM public.document_chunks WHERE document_id = %s",
            (document["document_id"],),
        )
        == []
    )
    assert await real_postgres_client.fetch_one(
        """
        SELECT snapshot_id
        FROM public.document_parse_snapshots
        WHERE document_id = %s
        """,
        (document["document_id"],),
    ) is None
