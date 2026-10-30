import asyncio
import io
import json
from types import SimpleNamespace

import pytest
from pypdf import PdfReader, PdfWriter

from common.exceptions import DependencyError
from core.knowledge.documents import extraction as extraction_module
from core.knowledge.documents.extraction import (
    DocumentChunk,
    DocumentParseSnapshot,
    DocumentSnapshotPage,
    LayoutRegion,
    NativePdfTextCell,
    PdfPageGroup,
)
from core.knowledge.documents.indexer import DocumentIndexer
from tests.fixtures.documents import (
    build_docx_bytes,
    build_notebook_bytes,
    build_pdf_bytes,
    build_png_bytes,
)


@pytest.mark.unit
@pytest.mark.no_network
def test_document_index_ignores_malformed_parser_regions_and_keeps_new_ocr_cells():
    valid_box = {"l": 1, "b": 2, "r": 3, "t": 4}
    structure = {
        "texts": [
            {"label": "document_index", "prov": "invalid"},
            {"label": "document_index", "prov": [None, {"page_no": 0, "bbox": valid_box}]},
            {"label": "document_index", "prov": [{"page_no": 1, "bbox": valid_box}]},
            {"label": "document_index", "prov": [{"page_no": 1, "bbox": {**valid_box, "r": float("inf")}}]},
        ],
        "tables": [
            {"label": "document_index", "prov": "invalid"},
            {"label": "document_index", "prov": [{"page_no": 2}], "data": {}},
            {
                "label": "document_index",
                "prov": [{"page_no": 1}],
                "data": {"table_cells": [None, {"text": " "}, {"text": "Known"}, {"text": "New OCR cell"}]},
            },
        ],
    }

    assert extraction_module._document_index_boxes(structure) == {
        1: ((1.0, 2.0, 3.0, 4.0),)
    }
    assert extraction_module._missing_index_cell_text(structure, 1, "Known") == "New OCR cell"
    assert extraction_module._missing_index_cell_text({"tables": None}, 1, "") == ""


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("serialized", "message"),
    [
        ("not a notebook cell", "no recognizable cells"),
        (
            "[[KNOGGIN_NOTEBOOK_CELL index=1 type=code length=100]]\nprint(1)",
            "content is incomplete",
        ),
        (
            "[[KNOGGIN_NOTEBOOK_CELL index=1 type=code length=8]]\nprint(1)next",
            "separator is invalid",
        ),
    ],
)
def test_notebook_split_rejects_incomplete_serialized_cells(serialized, message):
    with pytest.raises(ValueError, match=message):
        extraction_module._split_notebook(serialized)


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
    monkeypatch.setattr(extraction_module, "_extract_docling_snapshot", lambda *_: snapshot)

    extraction = extraction_module.extract_and_split_document(
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
    ("detail", "expected", "error_type", "classification"),
    [
        (
            "LocalEntryNotFoundError: missing cache",
            "models are not installed",
            DependencyError,
            ("transient_dependency", True),
        ),
        (
            "malformed package",
            "could not parse",
            ValueError,
            ("invalid_content", False),
        ),
    ],
)
def test_docling_parse_failures_distinguish_missing_models_from_bad_content(
    monkeypatch, tmp_path, detail, expected, error_type, classification
):
    monkeypatch.setenv("DOCLING_ARTIFACTS_PATH", str(tmp_path))  # models present

    class FailingConverter:
        def convert(self, _stream):
            raise RuntimeError(detail)

    monkeypatch.setattr(
        extraction_module,
        "_docling_converter",
        lambda _mode="standard": FailingConverter(),
    )

    with pytest.raises(error_type, match=expected) as error:
        extraction_module._parse_with_docling(b"document", ".pdf")
    assert DocumentIndexer._classify_index_failure(error.value) == classification


@pytest.mark.unit
@pytest.mark.no_network
def test_a_failed_pdf_parse_without_the_models_folder_says_to_install_them(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("DOCLING_ARTIFACTS_PATH", str(tmp_path / "missing"))

    class FailingConverter:
        def convert(self, _stream):
            raise FileNotFoundError("model.safetensors")

    monkeypatch.setattr(
        extraction_module, "_docling_converter", lambda _mode="standard": FailingConverter()
    )
    with pytest.raises(DependencyError, match="prefetch_models.py"):
        extraction_module._parse_with_docling(b"document", ".pdf")


@pytest.mark.unit
@pytest.mark.no_network
def test_docling_extraction_modes_change_ocr_without_disabling_tables():
    standard = extraction_module._docling_parser_configuration("standard")
    ocr = extraction_module._docling_parser_configuration("ocr")

    assert standard["do_ocr"] is False
    assert ocr["do_ocr"] is True
    assert standard["do_table_structure"] is True
    assert ocr["do_table_structure"] is True
    assert extraction_module._DOCLING_CPU_THREADS == 4
    fingerprints = extraction_module.locator_repair_fingerprints()
    assert fingerprints[".pdf"] != fingerprints[".pdf:ocr"]


@pytest.mark.unit
@pytest.mark.no_network
def test_pdf_ocr_preflight_recommends_only_image_backed_low_text_pages(monkeypatch):
    class FakePage:
        def __init__(self, text, *, scanned):
            self._text = text
            image = {
                "/Subtype": "/Image",
                "/Width": 1000,
                "/Height": 1400,
            }
            self._resources = {"/XObject": {"/scan": image}} if scanned else {}

        def extract_text(self):
            return self._text

        def get(self, key):
            return self._resources if key == "/Resources" else None

    pages = [FakePage("", scanned=True) for _ in range(3)]
    monkeypatch.setattr(
        extraction_module,
        "PdfReader",
        lambda _stream: SimpleNamespace(pages=pages),
    )

    result = extraction_module.inspect_pdf_ocr_need(b"pdf")

    assert result.page_count == 3
    assert result.low_text_page_count == 3
    assert result.scanned_page_count == 3
    assert result.ocr_recommended is True


@pytest.mark.unit
@pytest.mark.no_network
def test_pdf_ocr_preflight_does_not_recommend_for_native_text(monkeypatch):
    page = SimpleNamespace(
        extract_text=lambda: "native text " * 20,
        get=lambda _key: {},
    )
    monkeypatch.setattr(
        extraction_module,
        "PdfReader",
        lambda _stream: SimpleNamespace(pages=[page]),
    )

    result = extraction_module.inspect_pdf_ocr_need(b"pdf")

    assert result.low_text_page_count == 0
    assert result.scanned_page_count == 0
    assert result.ocr_recommended is False


@pytest.mark.unit
@pytest.mark.no_network
def test_pdf_page_groups_keep_chapters_whole_and_cover_every_page():
    writer = PdfWriter()
    for _ in range(12):
        writer.add_blank_page(width=612, height=792)
    part = writer.add_outline_item("Part I", 0)
    chapter_one = writer.add_outline_item("Chapter 1. First", 1, parent=part)
    writer.add_outline_item("Section 1.1", 2, parent=chapter_one)
    writer.add_outline_item("Chapter 2. Oversized", 5, parent=part)
    writer.add_outline_item("Back Matter", 10)
    payload = io.BytesIO()
    writer.write(payload)

    groups = extraction_module._pdf_page_groups(
        PdfReader(io.BytesIO(payload.getvalue())),
        target_pages=4,
    )

    assert groups == (
        PdfPageGroup(1, 1),
        PdfPageGroup(2, 5),
        PdfPageGroup(6, 10),
        PdfPageGroup(11, 12),
    )
    assert [page for group in groups for page in range(group.start_page, group.end_page + 1)] == list(
        range(1, 13)
    )


@pytest.mark.unit
@pytest.mark.no_network
def test_pdf_page_groups_fall_back_to_fixed_ranges_without_chapter_outline():
    writer = PdfWriter()
    for _ in range(9):
        writer.add_blank_page(width=612, height=792)
    payload = io.BytesIO()
    writer.write(payload)

    assert extraction_module._pdf_page_groups(
        PdfReader(io.BytesIO(payload.getvalue())),
        target_pages=4,
    ) == (
        PdfPageGroup(1, 4),
        PdfPageGroup(5, 8),
        PdfPageGroup(9, 9),
    )


@pytest.mark.unit
@pytest.mark.no_network
def test_pdf_page_groups_require_more_than_one_chapter_bookmark():
    writer = PdfWriter()
    for _ in range(6):
        writer.add_blank_page(width=612, height=792)
    writer.add_outline_item("Chapter 1. Only bookmark", 2)
    payload = io.BytesIO()
    writer.write(payload)

    assert extraction_module._pdf_page_groups(
        PdfReader(io.BytesIO(payload.getvalue())),
        target_pages=4,
    ) == (PdfPageGroup(1, 4), PdfPageGroup(5, 6))


def _blank_pdf(page_count: int) -> bytes:
    writer = PdfWriter()
    for _ in range(page_count):
        writer.add_blank_page(width=612, height=792)
    payload = io.BytesIO()
    writer.write(payload)
    return payload.getvalue()


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize("strategy", ["auto", "full"])
async def test_small_auto_and_full_pdf_use_original_document(
    monkeypatch,
    strategy,
):
    content = _blank_pdf(3)
    seen = []

    async def extract_once(payload, extension, mode):
        seen.append((payload, extension, mode))
        return object()

    monkeypatch.setattr(
        extraction_module,
        "_extract_and_split_docling_document_unlocked",
        extract_once,
    )

    result = await extraction_module.extract_and_split_docling_document(
        content,
        ".pdf",
        extraction_strategy=strategy,
    )

    assert result is not None
    assert seen == [(content, ".pdf", "standard")]


@pytest.mark.unit
@pytest.mark.no_network
async def test_bounded_pdf_uses_page_groups_even_when_small(monkeypatch):
    content = _blank_pdf(3)
    seen = []
    grouped_content = b"bounded-pdf"

    async def extract_group(payload, extension, mode):
        seen.append((payload, extension, mode))
        return object()

    monkeypatch.setattr(
        extraction_module,
        "_extract_and_split_docling_document_unlocked",
        extract_group,
    )
    monkeypatch.setattr(
        extraction_module,
        "_pdf_group_bytes",
        lambda _reader, _group: grouped_content,
    )
    monkeypatch.setattr(
        extraction_module,
        "_rebase_pdf_group_extraction",
        lambda result, _group: result,
    )
    monkeypatch.setattr(
        extraction_module,
        "_combine_pdf_group_extractions",
        lambda groups, _mode, _grouping_method: groups,
    )

    result = await extraction_module.extract_and_split_docling_document(
        content,
        ".pdf",
        extraction_strategy="bounded",
    )

    assert len(seen) == 1
    assert seen[0][0] == grouped_content
    assert result[0][0] == PdfPageGroup(1, 3)


@pytest.mark.unit
@pytest.mark.no_network
async def test_large_auto_pdf_uses_fixed_page_fallback_without_outline(monkeypatch):
    content = _blank_pdf(5)
    grouped_content = b"group"
    grouping_methods = []

    async def extract_group(_payload, _extension, _mode):
        return object()

    monkeypatch.setattr(extraction_module, "_DOCLING_PAGE_GROUP_TARGET", 2)
    monkeypatch.setattr(
        extraction_module,
        "_extract_and_split_docling_document_unlocked",
        extract_group,
    )
    monkeypatch.setattr(
        extraction_module,
        "_pdf_group_bytes",
        lambda _reader, _group: grouped_content,
    )
    monkeypatch.setattr(
        extraction_module,
        "_rebase_pdf_group_extraction",
        lambda result, _group: result,
    )

    def combine(groups, _mode, grouping_method):
        grouping_methods.append(grouping_method)
        return groups

    monkeypatch.setattr(
        extraction_module,
        "_combine_pdf_group_extractions",
        combine,
    )

    result = await extraction_module.extract_and_split_docling_document(
        content,
        ".pdf",
        extraction_strategy="auto",
    )

    assert [group for group, _result in result] == [
        PdfPageGroup(1, 2),
        PdfPageGroup(3, 4),
        PdfPageGroup(5, 5),
    ]
    assert grouping_methods == ["fixed_pages"]


@pytest.mark.unit
@pytest.mark.no_network
def test_docling_parse_selects_requested_extraction_mode(monkeypatch):
    selected = []

    class Converter:
        @staticmethod
        def convert(_stream):
            return type("Result", (), {"document": "parsed"})()

    def converter(mode):
        selected.append(mode)
        return Converter()

    monkeypatch.setattr(extraction_module, "_docling_converter", converter)

    assert extraction_module._parse_with_docling(b"pdf", ".pdf", "ocr") == "parsed"
    assert selected == ["ocr"]


@pytest.mark.unit
@pytest.mark.no_network
async def test_docling_extraction_process_returns_stable_document_projection():
    extraction = await extraction_module.extract_and_split_docling_document(
        build_docx_bytes(
            [
                ("Isolated extraction", 1),
                ("The child process returns ordinary Knoggin chunks.", None),
            ]
        ),
        ".docx",
    )

    assert extraction.snapshot.parser_name == "docling"
    assert extraction.text.startswith("## Isolated extraction")
    assert "child process returns ordinary Knoggin chunks" in extraction.chunks[0].content


@pytest.mark.unit
@pytest.mark.no_network
async def test_docling_extraction_process_preserves_invalid_content_failure():
    with pytest.raises(ValueError, match="Docling could not parse"):
        await extraction_module.extract_and_split_docling_document(
            b"not a docx archive",
            ".docx",
        )


@pytest.mark.unit
@pytest.mark.no_network
async def test_docling_extraction_process_timeout_stops_worker(monkeypatch):
    monkeypatch.setattr(extraction_module, "_DOCLING_PROCESS_TIMEOUT_SECONDS", 0.01)

    with pytest.raises(TimeoutError, match="exceeded 0.01 seconds"):
        await extraction_module.extract_and_split_docling_document(
            build_docx_bytes([("Timeout", 1)]),
            ".docx",
        )


@pytest.mark.unit
@pytest.mark.no_network
async def test_docling_extraction_process_cancellation_reaps_worker():
    extraction = asyncio.create_task(
        extraction_module.extract_and_split_docling_document(
            build_docx_bytes([("Cancelled", 1)]),
            ".docx",
        )
    )
    await asyncio.sleep(0)
    extraction.cancel()

    with pytest.raises(asyncio.CancelledError):
        await extraction


@pytest.mark.unit
@pytest.mark.no_network
def test_standard_mode_never_labels_docling_text_as_ocr():
    regions = extraction_module._regions_from_docling_structure(
        {
            "texts": [
                {
                    "label": "text",
                    "prov": [
                        {
                            "page_no": 1,
                            "bbox": {"l": 0, "b": 0, "r": 10, "t": 10},
                        }
                    ],
                }
            ]
        },
        native_regions={},
        ocr_enabled=False,
    )

    assert regions[1][0].extraction_method == "native_text"


@pytest.mark.unit
@pytest.mark.no_network
def test_docling_region_projection_ignores_malformed_provenance():
    structure = {
        "pages": {"1": {}, "invalid": {}},
        "texts": [
            "not-an-entry",
            {"label": "text", "prov": "not-a-list"},
            {"label": "text", "prov": [None]},
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

    regions = extraction_module._regions_from_docling_structure(structure, native_regions={1: ()})

    assert extraction_module._page_numbers(structure) == [1]
    assert len(regions[1]) == 1
    assert regions[1][0].extraction_method == "ocr"
    assert regions[1][0].bbox is None
    assert regions[1][0].text_start is None
    assert regions[1][0].text_end is None


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize("invalid_coordinate", [float("nan"), float("inf"), -float("inf")])
def test_docling_snapshot_sanitizes_nonfinite_structure_and_drops_invalid_bounds(
    monkeypatch, invalid_coordinate
):
    structure = {
        "pages": {"1": {}},
        "texts": [
            {
                "label": "text",
                "prov": [
                    {
                        "page_no": 1,
                        "bbox": {"l": invalid_coordinate, "b": 0, "r": 10, "t": 20},
                    }
                ],
            }
        ],
        "nested": {"scores": [float("inf"), -float("inf"), float("nan")]},
    }

    class ParsedDocument:
        def export_to_dict(self, **_kwargs):
            return structure

        def export_to_markdown(self, page_no=None):
            return "Readable text"

    monkeypatch.setattr(
        extraction_module, "_parse_with_docling", lambda *_: ParsedDocument()
    )
    monkeypatch.setattr(
        extraction_module,
        "_native_pdf_cells",
        lambda _content: {1: (NativePdfTextCell("Readable text", (0, 0, 10, 20)),)},
    )
    monkeypatch.setattr(extraction_module, "_docling_version", lambda: "test")

    snapshot = extraction_module._extract_docling_snapshot(b"pdf", ".pdf")
    payload = json.loads(json.dumps(snapshot.to_storage_payload(), allow_nan=False))

    assert payload["structure"]["texts"][0]["prov"][0]["bbox"]["l"] is None
    assert payload["structure"]["nested"]["scores"] == [None, None, None]
    assert payload["pages"][0]["regions"][0]["extraction_method"] == "unknown"
    assert "bbox" not in payload["pages"][0]["regions"][0]


@pytest.mark.unit
@pytest.mark.no_network
def test_native_pdf_cells_keep_text_without_nonfinite_coordinates(monkeypatch):
    from docling_parse import pdf_parser

    def cell(text, left):
        return SimpleNamespace(
            text=text,
            rect=SimpleNamespace(
                r_x0=left, r_x1=left + 1, r_x2=left + 1, r_x3=left,
                r_y0=0, r_y1=0, r_y2=2, r_y3=2,
            ),
        )

    class ParsedPdf:
        unloaded = False

        def iterate_pages(self):
            yield 1, SimpleNamespace(textline_cells=[cell("bad", float("nan")), cell("good", 2)])

        def unload(self):
            self.unloaded = True

    parsed = ParsedPdf()
    monkeypatch.setattr(pdf_parser, "DoclingPdfParser", lambda: SimpleNamespace(load=lambda _: parsed))

    cells = extraction_module._native_pdf_cells(b"pdf")

    assert cells == {
        1: (
            NativePdfTextCell("bad", None),
            NativePdfTextCell("good", (2, 0, 3, 2)),
        )
    }
    assert parsed.unloaded


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("page_markdown", "unpositioned_text", "expected_text"),
    [
        ("Index\nMissing bounds", "Missing bounds", "Index\nMissing bounds"),
        ("| malformed index |", "Missing bounds", "Index\nMissing bounds"),
        ("", "Missing bounds", "Index\nMissing bounds"),
        ("Index", "Index", "Index\nIndex"),
    ],
)
def test_document_index_preserves_text_from_native_cell_with_invalid_bounds(
    monkeypatch, page_markdown, unpositioned_text, expected_text
):
    from docling_parse import pdf_parser

    class ParsedDocument:
        def export_to_dict(self, **_kwargs):
            return {
                "pages": {"1": {}},
                "tables": [
                    {
                        "label": "document_index",
                        "prov": [
                            {
                                "page_no": 1,
                                "bbox": {"l": 0, "b": 0, "r": 100, "t": 100},
                            }
                        ],
                    }
                ],
            }

        def export_to_markdown(self, page_no=None, labels=None):
            if labels is not None:
                return ""
            return page_markdown

    def native_cell(text, left):
        return SimpleNamespace(
            text=text,
            rect=SimpleNamespace(
                r_x0=left, r_x1=left + 10, r_x2=left + 10, r_x3=left,
                r_y0=0, r_y1=0, r_y2=10, r_y3=10,
            ),
        )

    class ParsedPdf:
        def iterate_pages(self):
            yield 1, SimpleNamespace(
                textline_cells=[
                    native_cell("Index", 0),
                    native_cell(unpositioned_text, float("nan")),
                ]
            )

        def unload(self):
            pass

    monkeypatch.setattr(
        extraction_module, "_parse_with_docling", lambda *_: ParsedDocument()
    )
    monkeypatch.setattr(
        pdf_parser,
        "DoclingPdfParser",
        lambda: SimpleNamespace(load=lambda _: ParsedPdf()),
    )
    monkeypatch.setattr(extraction_module, "_docling_version", lambda: "test")

    extraction = extraction_module.extract_and_split_document(b"pdf", ".pdf")

    assert extraction.snapshot.pages[0].text == expected_text
    assert extraction.snapshot.text == expected_text
    assert unpositioned_text in " ".join(
        chunk.content for chunk in extraction.chunks
    )


@pytest.mark.unit
@pytest.mark.no_network
def test_pdf_uses_page_text_when_document_markdown_is_blank(monkeypatch):
    class ParsedDocument:
        def export_to_dict(self, **_kwargs):
            return {
                "pages": {"1": {}},
                "texts": [
                    {
                        "label": "text",
                        "charspan": [0, 8],
                        "prov": [{"page_no": 1}],
                    }
                ],
            }

        def export_to_markdown(self, page_no=None):
            return "" if page_no is None else "Ordinary PDF page text"

    monkeypatch.setattr(
        extraction_module, "_parse_with_docling", lambda *_: ParsedDocument()
    )
    monkeypatch.setattr(extraction_module, "_native_pdf_cells", lambda _content: {})
    monkeypatch.setattr(extraction_module, "_docling_version", lambda: "test")

    extraction = extraction_module.extract_and_split_document(b"pdf", ".pdf")

    assert extraction.text == "Ordinary PDF page text"
    assert extraction.snapshot.text == extraction.snapshot.pages[0].text
    assert extraction.snapshot.pages[0].regions[0].text_start is None
    assert extraction.snapshot.pages[0].regions[0].text_end is None
    assert "Ordinary PDF page text" in " ".join(
        chunk.content for chunk in extraction.chunks
    )


@pytest.mark.unit
@pytest.mark.no_network
def test_mixed_pdf_page_classifies_regions_from_native_cell_bounds():
    structure = {
        "texts": [
            {"label": "heading", "prov": [{"page_no": 1, "charspan": [0, 6], "bbox": {"l": 10, "b": 90, "r": 80, "t": 110}}]},
            {"label": "text", "prov": [{"page_no": 1, "charspan": [0, 8], "bbox": {"l": 10, "b": 10, "r": 80, "t": 30}}]},
        ]
    }

    regions = extraction_module._regions_from_docling_structure(
        structure,
        native_regions={1: ((9, 89, 81, 111),)},
    )

    assert [region.extraction_method for region in regions[1]] == [
        "native_text",
        "ocr",
    ]
    assert all(
        region.text_start is None and region.text_end is None
        for region in regions[1]
    )

    page = DocumentSnapshotPage(
        page_number=1,
        text="Heading\nScanned body",
        regions=regions[1],
    )
    assert extraction_module._page_locator(page)["extraction_method"] == "mixed"


@pytest.mark.unit
@pytest.mark.no_network
def test_document_index_uses_native_lines_without_flattening_real_tables(monkeypatch):
    structure = {
        "pages": {"1": {}, "2": {}},
        "texts": [],
        "tables": [
            {
                "label": "document_index",
                "prov": [{"page_no": 1, "charspan": [0, 0], "bbox": {"l": 10, "b": 10, "r": 190, "t": 100}}],
            },
            {
                "label": "table",
                "prov": [{"page_no": 2, "charspan": [0, 0], "bbox": {"l": 10, "b": 10, "r": 190, "t": 100}}],
            }
        ],
    }

    class ParsedDocument:
        def export_to_dict(self, **_kwargs):
            return structure

        def export_to_markdown(self, page_no=None, labels=None):
            if page_no == 1 and labels is not None:
                return ""
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
    monkeypatch.setattr(extraction_module, "_parse_with_docling", lambda *_: ParsedDocument())
    monkeypatch.setattr(extraction_module, "_native_pdf_cells", lambda _content: {1: cells})
    monkeypatch.setattr(extraction_module, "_docling_version", lambda: "test")

    snapshot = extraction_module._extract_docling_snapshot(b"pdf", ".pdf")

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
def test_mixed_index_page_keeps_ocr_text_without_duplicating_native_text(monkeypatch):
    from docling_core.types.doc import (
        BoundingBox,
        CoordOrigin,
        DocItemLabel,
        DoclingDocument,
        ProvenanceItem,
        TableCell,
        TableData,
    )

    def provenance(left, bottom, right, top, text=""):
        return ProvenanceItem(
            page_no=1,
            bbox=BoundingBox(
                l=left,
                b=bottom,
                r=right,
                t=top,
                coord_origin=CoordOrigin.BOTTOMLEFT,
            ),
            charspan=(0, len(text)),
        )

    document = DoclingDocument(name="mixed-index")
    document.add_table(
        TableData(
            num_rows=2,
            num_cols=2,
            table_cells=[
                TableCell(
                    start_row_offset_idx=0,
                    end_row_offset_idx=1,
                    start_col_offset_idx=0,
                    end_col_offset_idx=1,
                    text="Chapter One",
                ),
                TableCell(
                    start_row_offset_idx=0,
                    end_row_offset_idx=1,
                    start_col_offset_idx=1,
                    end_col_offset_idx=2,
                    text="12",
                ),
                TableCell(
                    start_row_offset_idx=1,
                    end_row_offset_idx=2,
                    start_col_offset_idx=0,
                    end_col_offset_idx=1,
                    text="Scanned appendix entry",
                ),
                TableCell(
                    start_row_offset_idx=1,
                    end_row_offset_idx=2,
                    start_col_offset_idx=1,
                    end_col_offset_idx=2,
                    text="33",
                ),
            ],
        ),
        label=DocItemLabel.DOCUMENT_INDEX,
        prov=provenance(10, 10, 190, 100),
    )
    document.add_text(
        label=DocItemLabel.TEXT,
        text="Native page heading",
        prov=provenance(10, 150, 190, 170, "Native page heading"),
    )
    document.add_text(
        label=DocItemLabel.TEXT,
        text="OCR-only appendix note",
        prov=provenance(10, 110, 190, 130, "OCR-only appendix note"),
    )
    monkeypatch.setattr(extraction_module, "_parse_with_docling", lambda *_: document)
    monkeypatch.setattr(
        extraction_module,
        "_native_pdf_cells",
        lambda _content: {
            1: (
                NativePdfTextCell("Chapter One", (10, 70, 100, 80)),
                NativePdfTextCell("12", (170, 70, 190, 80)),
                NativePdfTextCell("Native page heading", (10, 150, 190, 170)),
            )
        },
    )
    monkeypatch.setattr(extraction_module, "_docling_version", lambda: "test")

    extraction = extraction_module.extract_and_split_document(b"pdf", ".pdf")

    assert extraction.snapshot.pages[0].text == (
        "Chapter One 12\nScanned appendix entry\n33\n\n"
        "Native page heading\n\nOCR-only appendix note"
    )
    assert extraction.snapshot.text == extraction.snapshot.pages[0].text
    assert extraction.text.count("Chapter One") == 1
    assert extraction.text.count("Scanned appendix entry") == 1
    assert extraction.text.count("Native page heading") == 1
    assert "|" not in extraction.text
    assert extraction.snapshot.structure["texts"][0]["prov"][0]["charspan"] == [
        0,
        len("Native page heading"),
    ]
    assert all(
        region.text_start is None and region.text_end is None
        for region in extraction.snapshot.pages[0].regions
    )
    assert "OCR-only appendix note" in " ".join(
        chunk.content for chunk in extraction.chunks
    )
    assert extraction.chunks[0].layout_region["extraction_method"] == "mixed"

    # Native text elsewhere on the page must not replace an OCR-only index.
    monkeypatch.setattr(
        extraction_module,
        "_native_pdf_cells",
        lambda _content: {
            1: (NativePdfTextCell("Native page heading", (10, 150, 190, 170)),)
        },
    )
    fallback = extraction_module.extract_and_split_document(b"pdf", ".pdf")
    assert fallback.snapshot.pages[0].text == document.export_to_markdown(page_no=1)
    assert "Scanned appendix entry" in fallback.text
    assert "OCR-only appendix note" in fallback.text

    # An OCR-only table cell makes the page mixed even without another OCR region.
    index_only = DoclingDocument(name="index-only")
    index_only.add_table(
        TableData(
            num_rows=2,
            num_cols=1,
            table_cells=[
                TableCell(
                    start_row_offset_idx=0,
                    end_row_offset_idx=1,
                    start_col_offset_idx=0,
                    end_col_offset_idx=1,
                    text="Chapter One",
                ),
                TableCell(
                    start_row_offset_idx=1,
                    end_row_offset_idx=2,
                    start_col_offset_idx=0,
                    end_col_offset_idx=1,
                    text="Scanned appendix entry",
                ),
            ],
        ),
        label=DocItemLabel.DOCUMENT_INDEX,
        prov=provenance(10, 10, 190, 100),
    )
    monkeypatch.setattr(extraction_module, "_parse_with_docling", lambda *_: index_only)
    monkeypatch.setattr(
        extraction_module,
        "_native_pdf_cells",
        lambda _content: {1: (NativePdfTextCell("Chapter One", (10, 70, 100, 80)),)},
    )
    table_only = extraction_module.extract_and_split_document(b"pdf", ".pdf")
    assert "Scanned appendix entry" in table_only.text
    assert table_only.chunks[0].layout_region["extraction_method"] == "mixed"


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
    monkeypatch.setattr(extraction_module, "_extract_docling_snapshot", lambda *_: snapshot)

    extraction = extraction_module.extract_and_split_document(
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
    extraction = extraction_module.extract_and_split_document(content, extension)

    assert extraction.chunks == expected_chunks


@pytest.mark.unit
@pytest.mark.no_network
def test_python_class_methods_receive_qualified_symbol_names():
    extraction = extraction_module.extract_and_split_document(
        b"""class Worker:
    category = "indexing"

    @classmethod
    async def recover(cls):
        return cls.category

    def wake(self):
        return "ready"

def outside():
    return Worker()
""",
        ".py",
    )

    assert [chunk.symbol_name for chunk in extraction.chunks] == [
        "Worker",
        "Worker.recover",
        "Worker.wake",
        "outside",
    ]
    assert extraction.chunks[1].content.startswith("@classmethod\n    async def recover")
    assert (extraction.chunks[1].start_line, extraction.chunks[1].end_line) == (4, 7)
    assert (extraction.chunks[2].start_line, extraction.chunks[2].end_line) == (8, 10)


@pytest.mark.unit
@pytest.mark.no_network
def test_split_python_method_retains_qualified_symbol_name():
    long_assignment = "        value = '" + ("x" * 1_100) + "'\n"
    content = (
        "class Worker:\n"
        "    def large(self):\n"
        + long_assignment
        + long_assignment
        + long_assignment
        + "        return value\n"
    ).encode()

    extraction = extraction_module.extract_and_split_document(content, ".py")

    method_chunks = [
        chunk
        for chunk in extraction.chunks
        if chunk.symbol_name == "Worker.large"
    ]
    assert len(method_chunks) == 2
    assert all(chunk.chunk_kind == "code" for chunk in method_chunks)


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("content", "extension", "error"),
    [
        (b"value = '" + b"x" * 2_500 + b"LATE_MARKER'\n", ".py", "Code line 1"),
        (b"name,value\nalpha," + b"x" * 2_100 + b"LATE_MARKER\n", ".csv", "CSV row 1"),
        (b"column" + b"x" * 2_100 + b"\nvalue\n", ".csv", "CSV header"),
    ],
)
def test_oversized_code_line_or_csv_row_fails_instead_of_silent_truncation(
    content, extension, error
):
    with pytest.raises(ValueError, match=error) as failure:
        extraction_module.extract_and_split_document(content, extension)
    assert DocumentIndexer._classify_index_failure(failure.value) == (
        "invalid_content",
        False,
    )


@pytest.mark.unit
@pytest.mark.no_network
def test_notebook_extraction_preserves_cell_type_order_and_line_locations():
    extraction = extraction_module.extract_and_split_document(
        build_notebook_bytes(),
        ".ipynb",
    )

    assert extraction.chunks == [
        DocumentChunk(
            content="# Launch notes\nDeterministic notebook text.",
            language="notebook",
            chunk_kind="notebook_markdown",
            symbol_name="cell 1",
            start_line=2,
            end_line=3,
        ),
        DocumentChunk(
            content="def launch():\n    return 'ready'",
            language="notebook",
            chunk_kind="notebook_code",
            symbol_name="cell 2",
            start_line=6,
            end_line=7,
        ),
    ]


@pytest.mark.unit
@pytest.mark.no_network
def test_large_notebook_cells_split_with_exact_within_cell_line_ranges():
    notebook = {
        "cells": [
            {
                "cell_type": "code",
                "source": "first = '" + "a" * 1_500 + "'\nsecond = '" + "b" * 1_500 + "'",
            },
            {
                "cell_type": "markdown",
                "source": "Intro " + "c" * 1_200 + "\nNext " + "d" * 1_200,
            },
        ]
    }

    extraction = extraction_module.extract_and_split_document(
        json.dumps(notebook).encode("utf-8"), ".ipynb"
    )

    assert [
        (chunk.chunk_kind, chunk.symbol_name, chunk.start_line, chunk.end_line)
        for chunk in extraction.chunks
    ] == [
        ("notebook_code", "cell 1", 2, 2),
        ("notebook_code", "cell 1", 3, 3),
        ("notebook_markdown", "cell 2", 6, 6),
        ("notebook_markdown", "cell 2", 7, 7),
    ]
    assert all(len(chunk.content) <= 2_400 for chunk in extraction.chunks)
    assert "second = '" + "b" * 1_500 in extraction.chunks[1].content
    assert "Next " + "d" * 1_200 in extraction.chunks[3].content
    snapshot_lines = extraction.snapshot.text.splitlines()
    for chunk in extraction.chunks:
        assert "\n".join(
            snapshot_lines[chunk.start_line - 1 : chunk.end_line]
        ).strip() == chunk.content


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("prefix", "line_number"), [("", 1), ("\n\n", 3)]
)
def test_notebook_code_cell_with_one_oversized_line_fails_clearly(
    prefix, line_number
):
    notebook = {
        "cells": [
            {
                "cell_type": "code",
                "source": prefix + "value = '" + "x" * 2_500 + "LATE_MARKER'",
            }
        ]
    }

    with pytest.raises(ValueError, match=f"Notebook cell 1: Code line {line_number}"):
        extraction_module.extract_and_split_document(
            json.dumps(notebook).encode("utf-8"), ".ipynb"
        )


@pytest.mark.unit
@pytest.mark.no_network
def test_notebook_cell_markers_inside_source_do_not_forge_cells_or_line_locations():
    marker = "[[KNOGGIN_NOTEBOOK_CELL index=99 type=code length=1]]"
    notebook = {
        "cells": [
            {
                "cell_type": "code",
                "source": f"\n\nprint(1)\n{marker}\nprint(2)\n\n",
            }
        ]
    }

    extraction = extraction_module.extract_and_split_document(
        json.dumps(notebook).encode("utf-8"), ".ipynb"
    )

    assert extraction.chunks == [
        DocumentChunk(
            content=f"print(1)\n{marker}\nprint(2)",
            language="notebook",
            chunk_kind="notebook_code",
            symbol_name="cell 1",
            start_line=4,
            end_line=6,
        )
    ]
    assert extraction.snapshot.text.splitlines()[3:6] == [
        "print(1)",
        marker,
        "print(2)",
    ]


@pytest.mark.unit
@pytest.mark.no_network
def test_notebook_global_lines_handle_carriage_return_at_cell_boundary():
    notebook = {
        "cells": [
            {"cell_type": "markdown", "source": "first\r"},
            {"cell_type": "markdown", "source": "second"},
        ]
    }

    extraction = extraction_module.extract_and_split_document(
        json.dumps(notebook).encode("utf-8"), ".ipynb"
    )

    second = extraction.chunks[1]
    assert (second.start_line, second.end_line) == (5, 5)
    assert extraction.snapshot.text.splitlines()[second.start_line - 1] == "second"


@pytest.mark.unit
@pytest.mark.no_network
def test_image_ocr_is_indexed_as_line_located_text(monkeypatch):
    monkeypatch.setattr(
        extraction_module.pytesseract,
        "image_to_string",
        lambda _: "Launch ready.\nProceed now.\n",
    )

    extraction = extraction_module.extract_and_split_document(build_png_bytes(), ".png")

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
        extraction_module.extract_and_split_document(content, extension)


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize("extension", [".pdf", ".docx"])
def test_empty_structured_documents_fail_before_chunk_publication(monkeypatch, extension):
    monkeypatch.setattr(
        extraction_module,
        "_extract_docling_snapshot",
        lambda *_: (_ for _ in ()).throw(ValueError("Document contains no extractable text")),
    )

    with pytest.raises(ValueError, match="no extractable text"):
        extraction_module.extract_and_split_document(b"empty", extension)


@pytest.mark.unit
@pytest.mark.no_network
def test_empty_image_ocr_is_rejected(monkeypatch):
    monkeypatch.setattr(extraction_module.pytesseract, "image_to_string", lambda _: " \n")

    with pytest.raises(ValueError, match="no readable text"):
        extraction_module.extract_and_split_document(build_png_bytes(), ".png")
