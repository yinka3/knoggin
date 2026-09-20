import pytest

from core.knowledge.documents import storage
from core.knowledge.documents.storage import (
    DocumentChunk,
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


@pytest.mark.unit
@pytest.mark.no_network
def test_pdf_extraction_preserves_captured_page_boundaries(monkeypatch):
    snapshot = DocumentParseSnapshot(
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
                        bbox=(10, 10, 50, 30),
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
                        bbox=(10, 10, 50, 30),
                    ),
                ),
            ),
        ),
    )
    monkeypatch.setattr(storage, "_extract_docling_snapshot", lambda *_: snapshot)

    extraction = storage.extract_and_split_document(
        build_pdf_bytes("Alpha page.", "Beta page."),
        ".pdf",
    )

    assert extraction.text == snapshot.text
    assert extraction.snapshot is snapshot
    assert [(chunk.page_number, chunk.content) for chunk in extraction.chunks] == [
        (1, "Alpha page."),
        (2, "Beta page."),
    ]
    assert extraction.chunks[0].layout_region == {
        "kind": "layout_region",
        "page": 1,
        "element_type": "page",
        "extraction_method": "native_text",
        "coordinate_unit": "pdf_points",
        "coordinate_origin": "bottom_left",
    }
    assert extraction.chunks[1].layout_region["extraction_method"] == "ocr"


@pytest.mark.unit
@pytest.mark.no_network
def test_docx_extraction_uses_the_captured_structured_markdown(monkeypatch):
    snapshot = DocumentParseSnapshot(
        text="# Overview\n\nAlpha paragraph.\n\n## Risks\n\nBeta paragraph.",
        structure={"texts": [{"label": "section_header"}]},
        parser_name="docling",
        parser_version="test",
        parser_fingerprint="a" * 64,
    )
    monkeypatch.setattr(storage, "_extract_docling_snapshot", lambda *_: snapshot)

    extraction = storage.extract_and_split_document(
        build_docx_bytes(
            [
                ("Overview", 1),
                ("Alpha paragraph.", None),
                ("Risks", 2),
                ("Beta paragraph.", None),
            ]
        ),
        ".docx",
    )

    assert extraction.snapshot is snapshot
    assert [(chunk.start_line, chunk.end_line, chunk.section_path) for chunk in extraction.chunks] == [
        (1, 3, ("Overview",)),
        (5, 7, ("Overview", "Risks")),
    ]
    assert "Alpha paragraph." in extraction.chunks[0].content
    assert "Beta paragraph." in extraction.chunks[1].content


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("content", "extension", "expected_chunks"),
    [
        (
            b"First line\nSecond line\n",
            ".txt",
            [
                DocumentChunk(
                    content="First line\nSecond line",
                    start_line=1,
                    end_line=2,
                )
            ],
        ),
        (
            b"# Overview\nAlpha\n\n## Risks\nBeta\n",
            ".md",
            [
                DocumentChunk(
                    content="# Overview\nAlpha",
                    start_line=1,
                    end_line=2,
                    section_path=("Overview",),
                ),
                DocumentChunk(
                    content="## Risks\nBeta",
                    start_line=4,
                    end_line=5,
                    section_path=("Overview", "Risks"),
                ),
            ],
        ),
        (
            b"name,value\nalpha,1\nbeta,2\n",
            ".csv",
            [
                DocumentChunk(
                    content="name,value\nalpha,1\nbeta,2",
                    chunk_kind="csv",
                    start_row=1,
                    end_row=2,
                )
            ],
        ),
        (
            b"def alpha():\n    return 1\n\nclass Beta:\n    pass\n",
            ".py",
            [
                DocumentChunk(
                    content="def alpha():\n    return 1",
                    language="python",
                    chunk_kind="code",
                    symbol_name="alpha",
                    start_line=1,
                    end_line=3,
                ),
                DocumentChunk(
                    content="class Beta:\n    pass",
                    language="python",
                    chunk_kind="code",
                    symbol_name="Beta",
                    start_line=4,
                    end_line=5,
                ),
            ],
        ),
    ],
)
def test_text_processing_strategies_preserve_exact_chunk_locations(
    content,
    extension,
    expected_chunks,
):
    extraction = storage.extract_and_split_document(content, extension)

    assert extraction.chunks == expected_chunks


@pytest.mark.unit
@pytest.mark.no_network
def test_notebook_extraction_preserves_cell_type_order_and_line_locations():
    extraction = storage.extract_and_split_document(
        build_notebook_bytes(),
        ".ipynb",
    )

    assert extraction.chunks == [
        DocumentChunk(
            content="# Launch notes\nDeterministic notebook text.",
            language="notebook",
            chunk_kind="notebook_markdown",
            symbol_name="cell 1",
            start_line=1,
            end_line=2,
        ),
        DocumentChunk(
            content="def launch():\n    return 'ready'",
            language="notebook",
            chunk_kind="notebook_code",
            symbol_name="cell 2",
            start_line=1,
            end_line=2,
        ),
    ]


@pytest.mark.unit
@pytest.mark.no_network
def test_image_ocr_is_indexed_as_line_located_text(monkeypatch):
    monkeypatch.setattr(
        storage.pytesseract,
        "image_to_string",
        lambda _: "Launch ready.\nProceed now.\n",
    )

    extraction = storage.extract_and_split_document(build_png_bytes(), ".png")

    assert extraction.text == "Launch ready.\nProceed now.\n"
    assert extraction.chunks == [
        DocumentChunk(
            content="Launch ready.\nProceed now.",
            start_line=1,
            end_line=2,
        )
    ]
    assert extraction.snapshot.parser_name == "knoggin-exact-text"


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("content", "extension", "error"),
    [
        (b"\x00binary", ".txt", "binary content"),
        (b"\xff", ".txt", "valid UTF-8"),
        (b"   \n", ".txt", "no extractable text"),
        (b"not-json", ".ipynb", "not valid JSON"),
    ],
)
def test_invalid_or_empty_documents_fail_before_chunk_publication(
    content,
    extension,
    error,
):
    with pytest.raises(ValueError, match=error):
        storage.extract_and_split_document(content, extension)


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize("extension", [".pdf", ".docx"])
def test_empty_structured_documents_fail_before_chunk_publication(monkeypatch, extension):
    monkeypatch.setattr(
        storage,
        "_extract_docling_snapshot",
        lambda *_: (_ for _ in ()).throw(ValueError("Document contains no extractable text")),
    )

    with pytest.raises(ValueError, match="no extractable text"):
        storage.extract_and_split_document(b"empty", extension)


@pytest.mark.unit
@pytest.mark.no_network
def test_empty_image_ocr_is_rejected(monkeypatch):
    monkeypatch.setattr(storage.pytesseract, "image_to_string", lambda _: " \n")

    with pytest.raises(ValueError, match="no readable text"):
        storage.extract_and_split_document(build_png_bytes(), ".png")
