import pytest

from core.knowledge.documents import storage
from core.knowledge.documents.storage import DocumentChunk, DocumentExtraction
from tests.fixtures.documents import (
    build_docx_bytes,
    build_notebook_bytes,
    build_pdf_bytes,
    build_png_bytes,
)


@pytest.mark.unit
@pytest.mark.no_network
def test_pdf_extraction_preserves_real_page_boundaries():
    extraction = storage.extract_and_split_document(
        build_pdf_bytes("Alpha page.", "Beta page."),
        ".pdf",
    )

    assert extraction == DocumentExtraction(
        text="Alpha page.\n\nBeta page.",
        chunks=[
            DocumentChunk(content="Alpha page.", page_number=1),
            DocumentChunk(content="Beta page.", page_number=2),
        ],
    )


@pytest.mark.unit
@pytest.mark.no_network
def test_docx_extraction_preserves_real_paragraph_and_heading_locations():
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

    assert extraction == DocumentExtraction(
        text="Overview\nAlpha paragraph.\nRisks\nBeta paragraph.",
        chunks=[
            DocumentChunk(
                content="Overview\nAlpha paragraph.",
                section_path=("Overview",),
                start_paragraph=1,
                end_paragraph=2,
            ),
            DocumentChunk(
                content="Risks\nBeta paragraph.",
                section_path=("Overview", "Risks"),
                start_paragraph=3,
                end_paragraph=4,
            ),
        ],
    )


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

    assert extraction == DocumentExtraction(
        text="Launch ready.\nProceed now.\n",
        chunks=[
            DocumentChunk(
                content="Launch ready.\nProceed now.",
                start_line=1,
                end_line=2,
            )
        ],
    )


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("content", "extension", "error"),
    [
        (b"\x00binary", ".txt", "binary content"),
        (b"\xff", ".txt", "valid UTF-8"),
        (b"   \n", ".txt", "no extractable text"),
        (b"not-json", ".ipynb", "not valid JSON"),
        (build_pdf_bytes(""), ".pdf", "no extractable text"),
        (build_docx_bytes([]), ".docx", "no extractable text"),
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
def test_empty_image_ocr_is_rejected(monkeypatch):
    monkeypatch.setattr(storage.pytesseract, "image_to_string", lambda _: " \n")

    with pytest.raises(ValueError, match="no readable text"):
        storage.extract_and_split_document(build_png_bytes(), ".png")
