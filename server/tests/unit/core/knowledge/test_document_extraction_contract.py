import pytest

from core.knowledge.documents import storage
from core.knowledge.documents.storage import (
    DocumentChunk,
    DocumentParseSnapshot,
    DocumentSnapshotPage,
    LayoutRegion,
    NativePdfTextCell,
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
def test_layout_region_locator_preserves_optional_coordinates_and_text_offsets():
    locator = LayoutRegion(
        page_number=3,
        element_type="table",
        extraction_method="ocr",
        bbox=(10.0, 20.0, 110.0, 80.0),
        text_start=4,
        text_end=24,
    ).as_locator()

    assert locator["bbox"] == {
        "left": 10.0,
        "bottom": 20.0,
        "right": 110.0,
        "top": 80.0,
    }
    assert locator["text_start"] == 4
    assert locator["text_end"] == 24


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("detail", "expected"),
    [
        ("LocalEntryNotFoundError: missing cache", "models are not installed"),
        ("malformed package", "could not parse"),
    ],
)
def test_docling_parse_failures_distinguish_missing_models_from_bad_content(
    monkeypatch, detail, expected
):
    class FailingConverter:
        def convert(self, _stream):
            raise RuntimeError(detail)

    monkeypatch.setattr(storage, "_docling_converter", lambda: FailingConverter())

    with pytest.raises((RuntimeError, ValueError), match=expected):
        storage._parse_with_docling(b"document", ".pdf")


@pytest.mark.unit
@pytest.mark.no_network
def test_docling_region_projection_ignores_malformed_provenance():
    structure = {
        "pages": {"1": {}, "invalid": {}},
        "texts": [
            "not-an-entry",
            {"label": "text", "prov": "not-a-list"},
            {"label": "text", "charspan": [-1, 2], "prov": [None]},
            {
                "label": "text",
                "prov": [
                    {"page_no": 0, "bbox": {}},
                    {
                        "page_no": 1,
                        "bbox": {"l": "bad", "b": 0, "r": 10, "t": 20},
                    },
                ],
            },
        ],
    }

    regions = storage._regions_from_docling_structure(structure, native_regions={1: ()})

    assert storage._page_numbers(structure) == [1]
    assert len(regions[1]) == 1
    assert regions[1][0].extraction_method == "ocr"
    assert regions[1][0].bbox is None
    assert regions[1][0].text_start is None
    assert regions[1][0].text_end is None


@pytest.mark.unit
@pytest.mark.no_network
def test_mixed_pdf_page_classifies_regions_from_native_cell_bounds():
    structure = {
        "texts": [
            {"label": "heading", "charspan": [0, 6], "prov": [{"page_no": 1, "bbox": {"l": 10, "b": 90, "r": 80, "t": 110}}]},
            {"label": "text", "charspan": [7, 15], "prov": [{"page_no": 1, "bbox": {"l": 10, "b": 10, "r": 80, "t": 30}}]},
        ]
    }

    regions = storage._regions_from_docling_structure(
        structure,
        native_regions={1: ((9, 89, 81, 111),)},
    )

    assert [region.extraction_method for region in regions[1]] == [
        "native_text",
        "ocr",
    ]

    page = DocumentSnapshotPage(
        page_number=1,
        text="Heading\nScanned body",
        regions=regions[1],
    )
    assert storage._page_locator(page)["extraction_method"] == "mixed"


@pytest.mark.unit
@pytest.mark.no_network
def test_document_index_uses_native_lines_without_flattening_real_tables(monkeypatch):
    structure = {
        "pages": {"1": {}, "2": {}},
        "texts": [],
        "tables": [
            {
                "label": "document_index",
                "charspan": [0, 20],
                "prov": [{"page_no": 1, "bbox": {"l": 10, "b": 10, "r": 190, "t": 100}}],
            },
            {
                "label": "table",
                "charspan": [21, 40],
                "prov": [{"page_no": 2, "bbox": {"l": 10, "b": 10, "r": 190, "t": 100}}],
            }
        ],
    }

    class ParsedDocument:
        def export_to_dict(self, **_kwargs):
            return structure

        def export_to_markdown(self, page_no=None):
            if page_no == 1:
                return "| malformed contents |"
            if page_no == 2:
                return "| name | value |\n|---|---|\n| alpha | 1 |"
            return "| malformed contents |\n\n| name | value |"

    cells = (
        NativePdfTextCell("Contents", (10, 90, 60, 100)),
        NativePdfTextCell("Chapter One", (10, 70, 80, 80)),
        NativePdfTextCell("12", (170, 70, 190, 80)),
        NativePdfTextCell("Chapter Two", (10, 50, 80, 60)),
        NativePdfTextCell("24", (170, 50, 190, 60)),
    )
    monkeypatch.setattr(storage, "_parse_with_docling", lambda *_: ParsedDocument())
    monkeypatch.setattr(storage, "_native_pdf_cells", lambda _content: {1: cells})
    monkeypatch.setattr(storage, "_docling_version", lambda: "test")

    snapshot = storage._extract_docling_snapshot(b"pdf", ".pdf")

    assert snapshot.pages[0].text == "Contents\nChapter One 12\nChapter Two 24"
    assert snapshot.pages[1].text == "| name | value |\n|---|---|\n| alpha | 1 |"
    assert snapshot.text == (
        "Contents\nChapter One 12\nChapter Two 24\n\n"
        "| name | value |\n|---|---|\n| alpha | 1 |"
    )
    assert snapshot.pages[1].regions[0].element_type == "table"
    assert snapshot.pages[0].regions[0].extraction_method == "native_text"
    assert all(
        region.text_start is None and region.text_end is None
        for page in snapshot.pages
        for region in page.regions
    )


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
