"""Format-aware document extraction, snapshots, and chunk construction."""

import ast
import asyncio
import csv
import hashlib
import io
import json
import math
import os
import pickle
import re
import shutil
import sys
import tempfile
import threading
import warnings
from dataclasses import dataclass, replace
from functools import lru_cache
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytesseract
import tree_sitter_bash
import tree_sitter_c
import tree_sitter_c_sharp
import tree_sitter_cpp

try:
    import tree_sitter_dockerfile
except ImportError:  # pragma: no cover - dependency is declared by the server
    tree_sitter_dockerfile = None
import tree_sitter_go
import tree_sitter_java
import tree_sitter_javascript
import tree_sitter_python
import tree_sitter_rust
import tree_sitter_sql
import tree_sitter_typescript
import tree_sitter_yaml
import tree_sitter_zig
from llama_index.core.node_parser import SentenceSplitter
from loguru import logger
from PIL import Image as PILImage
from pypdf import PdfReader, PdfWriter
from tree_sitter import Language, Parser

from common.exceptions import DependencyError
from common.schema.document import DocumentExtractionMode, DocumentExtractionStrategy
from core.knowledge.documents.constants import (
    ACCEPTED_EXTENSIONS,
    CHUNK_OVERLAP_TOKENS,
    CHUNK_SIZE_TOKENS,
    IMAGE_EXTENSIONS,
)

# Module-level splitter — constructing SentenceSplitter loads a tokenizer,
# so we create it once rather than on every split_text() call.
_SPLITTER = SentenceSplitter(
    chunk_size=CHUNK_SIZE_TOKENS,
    chunk_overlap=CHUNK_OVERLAP_TOKENS,
)


@dataclass(frozen=True)
class DocumentChunk:
    """One retrieval chunk with a displayable location in its source document."""

    content: str
    language: Optional[str] = None
    chunk_kind: str = "text"
    symbol_name: Optional[str] = None
    page_number: Optional[int] = None
    start_line: Optional[int] = None
    end_line: Optional[int] = None
    start_row: Optional[int] = None
    end_row: Optional[int] = None
    section_path: Optional[tuple[str, ...]] = None
    start_paragraph: Optional[int] = None
    end_paragraph: Optional[int] = None
    layout_region: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class LayoutRegion:
    """One source-grade region from a layout-aware parse snapshot.

    Coordinates stay in PDF points with Docling's bottom-left origin.  Keeping
    that convention avoids a lossy conversion between parsing, retrieval, and
    provenance display.
    """

    page_number: int
    element_type: str
    extraction_method: str
    bbox: tuple[float, float, float, float] | None = None
    text_start: int | None = None
    text_end: int | None = None

    def as_locator(self) -> Dict[str, Any]:
        locator: Dict[str, Any] = {
            "kind": "layout_region",
            "page": self.page_number,
            "element_type": self.element_type,
            "extraction_method": self.extraction_method,
            "coordinate_unit": "pdf_points",
            "coordinate_origin": "bottom_left",
        }
        if self.bbox is not None:
            left, bottom, right, top = self.bbox
            locator["bbox"] = {
                "left": left,
                "bottom": bottom,
                "right": right,
                "top": top,
            }
        if self.text_start is not None:
            locator["text_start"] = self.text_start
        if self.text_end is not None:
            locator["text_end"] = self.text_end
        return locator


@dataclass(frozen=True)
class NativePdfTextCell:
    """One native PDF text cell; invalid bounds do not discard its text."""

    text: str
    bbox: tuple[float, float, float, float] | None


@dataclass(frozen=True)
class DocumentSnapshotPage:
    """The canonical text and regions for one one-based parsed page."""

    page_number: int
    text: str
    regions: tuple[LayoutRegion, ...] = ()


@dataclass(frozen=True)
class DocumentParseSnapshot:
    """Immutable, versioned parse data published with a document index.

    This is the representation used by indexing, reading, selections, and
    source evidence.  Source bytes are only used to create this snapshot; they
    are not reparsed during ordinary reads.
    """

    text: str
    structure: Dict[str, Any]
    parser_name: str
    parser_version: str
    parser_fingerprint: str
    pages: tuple[DocumentSnapshotPage, ...] = ()
    schema_version: int = 1

    def to_storage_payload(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "text": self.text,
            "structure": self.structure,
            "parser": {
                "name": self.parser_name,
                "version": self.parser_version,
                "fingerprint": self.parser_fingerprint,
            },
            "pages": [
                {
                    "page_number": page.page_number,
                    "text": page.text,
                    "regions": [region.as_locator() for region in page.regions],
                }
                for page in self.pages
            ],
        }


@dataclass(frozen=True)
class DocumentExtraction:
    """The extracted text and location-preserving chunks for one document."""

    text: str
    chunks: List[DocumentChunk]
    snapshot: DocumentParseSnapshot


@dataclass(frozen=True)
class PdfPageGroup:
    """One inclusive original-page range sent through Docling as a unit."""

    start_page: int
    end_page: int

    @property
    def page_count(self) -> int:
        return self.end_page - self.start_page + 1


@dataclass(frozen=True)
class PdfOcrPreflight:
    """Cheap evidence used to recommend, but never silently enable, OCR."""

    page_count: int
    low_text_page_count: int
    scanned_page_count: int
    ocr_recommended: bool
    reason: str


_CODE_LANGUAGES = {
    ".bash": "bash", ".c": "c", ".cpp": "cpp", ".cs": "c_sharp",
    ".dockerfile": "dockerfile",
    ".go": "go", ".h": "c", ".hpp": "cpp", ".java": "java",
    ".js": "javascript", ".jsx": "javascript", ".kt": "kotlin",
    ".php": "php", ".py": "python", ".rb": "ruby", ".rs": "rust",
    ".sh": "bash", ".sql": "sql", ".swift": "swift", ".ts": "typescript",
    ".tsx": "typescript", ".vue": "vue", ".yaml": "yaml", ".yml": "yaml",
    ".zig": "zig", ".zsh": "bash",
}
_SYMBOL_PATTERN = re.compile(
    r"^(?:(?:export\s+)?(?:async\s+)?)?"
    r"(?:def|class|function|interface|type|func|struct|enum)\s+"
    r"([A-Za-z_$][\w$]*)"
)
_CODE_CHUNK_TARGET_CHARS = 2_400
_NOTEBOOK_CELL_HEADER = re.compile(
    r"^\[\[KNOGGIN_NOTEBOOK_CELL index=(\d+) type=(code|markdown) length=(\d+)\]\]\n",
    re.MULTILINE,
)
_DOCKERFILE_INSTRUCTION_PATTERN = re.compile(r"^([A-Za-z]+)(?:\s|$)")
_MARKDOWN_HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*#*\s*$")
_TEXT_CHUNK_TARGET_CHARS = CHUNK_SIZE_TOKENS * 4
_DOCLING_CONVERSION_LOCK = threading.RLock()
_DOCLING_PROCESS_LOCK = asyncio.Lock()
_DOCLING_CPU_THREADS = 4
_DOCLING_PROCESS_POLL_SECONDS = 0.1
_DOCLING_PROCESS_STOP_SECONDS = 5.0
_DOCLING_PROCESS_TIMEOUT_SECONDS = float(
    os.getenv("DOCLING_TIMEOUT_SECONDS", str(3 * 60 * 60))
)
_DOCLING_PAGE_GROUP_TARGET = int(
    os.getenv("DOCLING_PAGE_GROUP_SIZE", "100")
)
_OCR_PREFLIGHT_MAX_NATIVE_CHARACTERS = 50
_OCR_PREFLIGHT_MIN_SCANNED_RATIO = 0.60
_OCR_PREFLIGHT_MIN_IMAGE_PIXELS = 250_000


def _resources_have_large_raster_image(resources: Any, seen: set[int]) -> bool:
    try:
        resources = resources.get_object()
    except AttributeError:
        pass
    xobjects = resources.get("/XObject") or {}
    try:
        xobjects = xobjects.get_object()
    except AttributeError:
        pass
    for reference in xobjects.values():
        try:
            item = reference.get_object() if hasattr(reference, "get_object") else reference
            object_key = id(item)
            if object_key in seen:
                continue
            seen.add(object_key)
            if (
                item.get("/Subtype") == "/Image"
                and int(item.get("/Width", 0)) * int(item.get("/Height", 0))
                >= _OCR_PREFLIGHT_MIN_IMAGE_PIXELS
            ):
                return True
            if item.get("/Subtype") == "/Form" and _resources_have_large_raster_image(
                item.get("/Resources") or {}, seen
            ):
                return True
        except (AttributeError, TypeError, ValueError):
            continue
    return False


def _page_has_large_raster_image(page: Any) -> bool:
    return _resources_have_large_raster_image(page.get("/Resources") or {}, set())


def inspect_pdf_ocr_need(content: bytes) -> PdfOcrPreflight:
    """Inspect native text and raster evidence without running Docling or OCR."""
    reader = PdfReader(io.BytesIO(content))
    page_count = len(reader.pages)
    if page_count == 0:
        raise ValueError("PDF contains no pages")
    low_text_pages = 0
    scanned_pages = 0
    for page in reader.pages:
        try:
            native_text = page.extract_text() or ""
        except Exception:
            native_text = ""
        low_text = len("".join(native_text.split())) < _OCR_PREFLIGHT_MAX_NATIVE_CHARACTERS
        if low_text:
            low_text_pages += 1
            if _page_has_large_raster_image(page):
                scanned_pages += 1
    required_pages = max(1, math.ceil(page_count * _OCR_PREFLIGHT_MIN_SCANNED_RATIO))
    recommended = scanned_pages >= required_pages
    reason = (
        f"{scanned_pages} of {page_count} pages have little native text and a large raster image"
        if recommended
        else f"only {scanned_pages} of {page_count} pages look scanned"
    )
    return PdfOcrPreflight(
        page_count=page_count,
        low_text_page_count=low_text_pages,
        scanned_page_count=scanned_pages,
        ocr_recommended=recommended,
        reason=reason,
    )


def _docling_parser_configuration(
    extraction_mode: DocumentExtractionMode,
) -> Dict[str, Any]:
    if extraction_mode not in {"standard", "ocr"}:
        raise ValueError("Unsupported document extraction mode")
    return {
        "knoggin_projection_revision": 4,
        "extraction_mode": extraction_mode,
        "accelerator": "cpu",
        "do_ocr": extraction_mode == "ocr",
        "do_table_structure": True,
        "do_code_enrichment": False,
        "do_formula_enrichment": False,
        "do_picture_description": False,
        "do_picture_classification": False,
        "remote_services": False,
        "external_plugins": False,
        "chapter_page_group_target": _DOCLING_PAGE_GROUP_TARGET,
    }


def _fingerprint(value: Dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def locator_repair_fingerprints() -> dict[str, str]:
    """Parser identities that include the notebook and PDF locator repairs."""
    return {
        ".pdf": _fingerprint(_docling_parser_configuration("standard")),
        ".pdf:ocr": _fingerprint(_docling_parser_configuration("ocr")),
        ".ipynb": _fingerprint(
            {"format": ".ipynb", "mode": "exact_text", "knoggin_projection_revision": 2}
        ),
    }

def _dockerfile_language() -> Optional[Language]:
    """Load the Dockerfile grammar while isolating its legacy handle warning."""
    language_factory = getattr(tree_sitter_dockerfile, "language", None)
    if language_factory is None:
        return None
    # tree-sitter-dockerfile 0.2.0 still exports an integer language handle.
    # Remove this narrow filter once it adopts Tree-sitter's capsule-based API.
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="int argument support is deprecated",
            category=DeprecationWarning,
        )
        return Language(language_factory())


# Languages are immutable and safe to share. Parsers are created per call
# because document indexing can split files concurrently on worker threads.
_TREE_SITTER_LANGUAGES = {
    ".bash": Language(tree_sitter_bash.language()),
    ".c": Language(tree_sitter_c.language()),
    ".cpp": Language(tree_sitter_cpp.language()),
    ".cs": Language(tree_sitter_c_sharp.language()),
    ".go": Language(tree_sitter_go.language()),
    ".h": Language(tree_sitter_c.language()),
    ".hpp": Language(tree_sitter_cpp.language()),
    ".java": Language(tree_sitter_java.language()),
    ".js": Language(tree_sitter_javascript.language()),
    ".jsx": Language(tree_sitter_javascript.language()),
    ".py": Language(tree_sitter_python.language()),
    ".rs": Language(tree_sitter_rust.language()),
    ".sh": Language(tree_sitter_bash.language()),
    ".sql": Language(tree_sitter_sql.language()),
    ".ts": Language(tree_sitter_typescript.language_typescript()),
    ".tsx": Language(tree_sitter_typescript.language_tsx()),
    ".yaml": Language(tree_sitter_yaml.language()),
    ".yml": Language(tree_sitter_yaml.language()),
    ".zig": Language(tree_sitter_zig.language()),
    ".zsh": Language(tree_sitter_bash.language()),
}
_DOCKERFILE_LANGUAGE = _dockerfile_language()
if _DOCKERFILE_LANGUAGE is not None:
    _TREE_SITTER_LANGUAGES[".dockerfile"] = _DOCKERFILE_LANGUAGE
_TREE_SITTER_SYMBOL_TYPES = {
    "bash": {"function_definition"},
    "c": {
        "enum_specifier",
        "function_definition",
        "struct_specifier",
        "type_definition",
    },
    "c_sharp": {
        "class_declaration",
        "enum_declaration",
        "interface_declaration",
        "struct_declaration",
    },
    "cpp": {
        "class_specifier",
        "enum_specifier",
        "function_definition",
        "namespace_definition",
        "struct_specifier",
    },
    "dockerfile": {"from_instruction", "run_instruction"},
    "go": {"function_declaration", "method_declaration", "type_declaration"},
    "java": {"class_declaration", "enum_declaration", "interface_declaration"},
    "javascript": {
        "class_declaration",
        "function_declaration",
        "interface_declaration",
        "lexical_declaration",
    },
    "python": {"class_definition", "function_definition"},
    "rust": {
        "const_item",
        "enum_item",
        "function_item",
        "impl_item",
        "mod_item",
        "static_item",
        "struct_item",
        "trait_item",
        "type_item",
    },
    "sql": {"statement"},
    "typescript": {
        "class_declaration",
        "enum_declaration",
        "function_declaration",
        "interface_declaration",
        "type_alias_declaration",
    },
    "yaml": set(),
    "zig": {"function_declaration", "test_declaration", "variable_declaration"},
}
_TREE_SITTER_WRAPPER_TYPES = {
    "ambient_declaration",
    "decorated_definition",
    "export_statement",
}


def looks_binary(content: bytes) -> bool:
    if not content:
        return False
    if b"\x00" in content:
        return True
    control_bytes = sum(
        byte < 32 and byte not in (8, 9, 10, 12, 13) for byte in content
    )
    return control_bytes / len(content) > 0.30


def is_accepted_extension(extension: str) -> bool:
    """Return True if the extension is supported for upload and indexing."""
    return extension.lower() in ACCEPTED_EXTENSIONS


def is_code_extension(extension: str) -> bool:
    """Return whether the extension is rendered as source code."""
    return extension.lower() in _CODE_LANGUAGES


def extract_text(content: bytes, extension: str) -> str:
    ext = extension.lower()

    if ext not in ACCEPTED_EXTENSIONS:
        raise ValueError(
            f"Unsupported file type '{ext}'. "
            f"Accepted types include PDF, DOCX, plain text, source code, and images."
        )

    if ext in {".pdf", ".docx"}:
        return _extract_docling_snapshot(content, ext).text
    elif ext == ".ipynb":
        text = _extract_notebook_text(content)
    elif ext in IMAGE_EXTENSIONS:
        image = PILImage.open(io.BytesIO(content))
        text = pytesseract.image_to_string(image)
        if not text.strip():
            raise ValueError(
                "Image contains no readable text — it may be a photo or illustration."
            )
        return text
    else:
        if looks_binary(content):
            raise ValueError("Document appears to contain binary content")
        try:
            text = content.decode("utf-8-sig", errors="strict")
        except UnicodeDecodeError as exc:
            raise ValueError("Document is not valid UTF-8 text") from exc

    if not text.strip():
        raise ValueError("Document contains no extractable text")
    return text


def _docling_version() -> str:
    try:
        return version("docling")
    except PackageNotFoundError:  # pragma: no cover - dependency is declared.
        return "unavailable"


@lru_cache(maxsize=1)
def _docling_converter(extraction_mode: DocumentExtractionMode = "standard"):
    """Build the one local-only converter shared by blocking index workers."""

    from docling.datamodel.accelerator_options import (
        AcceleratorDevice,
        AcceleratorOptions,
    )
    from docling.datamodel.base_models import InputFormat
    from docling.datamodel.pipeline_options import PdfPipelineOptions
    from docling.document_converter import DocumentConverter, PdfFormatOption

    pdf_options = PdfPipelineOptions(
        accelerator_options=AcceleratorOptions(
            device=AcceleratorDevice.CPU,
            num_threads=_DOCLING_CPU_THREADS,
        ),
        do_ocr=extraction_mode == "ocr",
        do_table_structure=True,
        do_code_enrichment=False,
        do_formula_enrichment=False,
        do_picture_description=False,
        do_picture_classification=False,
        enable_remote_services=False,
        allow_external_plugins=False,
    )
    return DocumentConverter(
        allowed_formats=[InputFormat.PDF, InputFormat.DOCX],
        format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=pdf_options)},
    )


def _parse_with_docling(
    content: bytes,
    extension: str,
    extraction_mode: DocumentExtractionMode = "standard",
):
    """Convert in a single serialized local pipeline invocation.

    Docling keeps parser/model state inside its converter.  The lock prevents
    concurrent index workers from racing that state while preserving the async
    runtime because this function is always called through the blocking runner.
    """

    from docling_core.types.io import DocumentStream

    try:
        with _DOCLING_CONVERSION_LOCK:
            return _docling_converter(extraction_mode).convert(
                DocumentStream(
                    name=f"captured-document{extension}",
                    stream=io.BytesIO(content),
                )
            ).document
    except Exception as exc:
        detail = str(exc).strip() or type(exc).__name__
        if "LocalEntryNotFoundError" in detail or "huggingface.co" in detail:
            raise DependencyError(
                "Docling PDF models are not installed; run "
                "uv run --package server python server/scripts/prefetch_models.py before indexing PDFs"
            ) from exc
        raise ValueError(f"Docling could not parse the {extension} document: {detail}") from exc


def _native_pdf_cells(
    content: bytes,
) -> Dict[int, tuple[NativePdfTextCell, ...]]:
    """Read native text and bounds before Docling adds OCR-derived text."""

    from docling_parse.pdf_parser import DoclingPdfParser

    parser = DoclingPdfParser()
    document = parser.load(io.BytesIO(content))
    try:
        regions: Dict[int, tuple[NativePdfTextCell, ...]] = {}
        for page_number, page in document.iterate_pages():
            boxes = []
            for cell in page.textline_cells:
                if not cell.text.strip():
                    continue
                rect = cell.rect
                coordinates = (
                    rect.r_x0, rect.r_x1, rect.r_x2, rect.r_x3,
                    rect.r_y0, rect.r_y1, rect.r_y2, rect.r_y3,
                )
                if not all(math.isfinite(value) for value in coordinates):
                    boxes.append(NativePdfTextCell(text=cell.text.strip(), bbox=None))
                    continue
                boxes.append(
                    NativePdfTextCell(
                        text=cell.text.strip(),
                        bbox=(
                            min(rect.r_x0, rect.r_x1, rect.r_x2, rect.r_x3),
                            min(rect.r_y0, rect.r_y1, rect.r_y2, rect.r_y3),
                            max(rect.r_x0, rect.r_x1, rect.r_x2, rect.r_x3),
                            max(rect.r_y0, rect.r_y1, rect.r_y2, rect.r_y3),
                        ),
                    )
                )
            if boxes:
                regions[page_number] = tuple(boxes)
        return regions
    finally:
        document.unload()


def _page_numbers(structure: Dict[str, Any]) -> List[int]:
    pages = structure.get("pages")
    if not isinstance(pages, dict):
        return []
    return sorted(
        int(page_number)
        for page_number in pages
        if str(page_number).isdigit() and int(page_number) >= 1
    )


def _region_bbox(value: object) -> tuple[float, float, float, float] | None:
    if not isinstance(value, dict):
        return None
    values = (value.get("l"), value.get("b"), value.get("r"), value.get("t"))
    if not all(isinstance(item, (int, float)) for item in values):
        return None
    left, bottom, right, top = (float(item) for item in values)
    if not all(math.isfinite(item) for item in (left, bottom, right, top)):
        return None
    if right <= left or top <= bottom:
        return None
    return left, bottom, right, top


def _sanitize_parser_structure(value: Any) -> Any:
    """Keep parser data JSON-compatible without inventing coordinates."""

    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {key: _sanitize_parser_structure(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize_parser_structure(item) for item in value]
    return value


def _regions_from_docling_structure(
    structure: Dict[str, Any],
    *,
    native_regions: Dict[int, tuple[tuple[float, float, float, float], ...]],
    ocr_enabled: bool = True,
) -> Dict[int, tuple[LayoutRegion, ...]]:
    """Retain Docling element provenance in a small stable region projection."""

    regions: Dict[int, list[LayoutRegion]] = {}
    for collection in ("texts", "tables", "pictures", "key_value_items"):
        entries = structure.get(collection)
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            element_type = str(entry.get("label") or collection.rstrip("s"))
            provenance = entry.get("prov")
            if not isinstance(provenance, list):
                continue
            for item in provenance:
                if not isinstance(item, dict):
                    continue
                page_number = item.get("page_no")
                if not isinstance(page_number, int) or page_number < 1:
                    continue
                bbox = _region_bbox(item.get("bbox"))
                native_boxes = native_regions.get(page_number, ())
                if collection != "texts":
                    extraction_method = "model_interpretation"
                elif not ocr_enabled:
                    extraction_method = "unknown" if bbox is None else "native_text"
                elif not native_boxes:
                    extraction_method = "ocr"
                elif bbox is None:
                    extraction_method = "unknown"
                elif any(_boxes_overlap(bbox, native) for native in native_boxes):
                    extraction_method = "native_text"
                else:
                    extraction_method = "ocr"
                # Docling's provenance charspan indexes this item, not our
                # canonical page text, so no page text offsets can be asserted.
                regions.setdefault(page_number, []).append(
                    LayoutRegion(
                        page_number=page_number,
                        element_type=element_type,
                        extraction_method=extraction_method,
                        bbox=bbox,
                    )
                )
    return {page: tuple(items) for page, items in regions.items()}


def _document_index_page_numbers(structure: Dict[str, Any]) -> set[int]:
    """Return pages whose text was classified as a document index."""

    pages: set[int] = set()
    for collection in ("texts", "tables", "pictures", "key_value_items"):
        entries = structure.get(collection)
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict) or entry.get("label") != "document_index":
                continue
            provenance = entry.get("prov")
            if not isinstance(provenance, list):
                continue
            for item in provenance:
                if isinstance(item, dict) and isinstance(item.get("page_no"), int):
                    pages.add(item["page_no"])
    return pages


def _document_index_boxes(
    structure: Dict[str, Any],
) -> Dict[int, tuple[tuple[float, float, float, float], ...]]:
    """Locate index regions so adjacent native text is not indexed twice."""

    boxes: Dict[int, list[tuple[float, float, float, float]]] = {}
    for collection in ("texts", "tables", "pictures", "key_value_items"):
        entries = structure.get(collection)
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict) or entry.get("label") != "document_index":
                continue
            provenance_items = entry.get("prov")
            if not isinstance(provenance_items, list):
                continue
            for provenance in provenance_items:
                if not isinstance(provenance, dict):
                    continue
                page_number = provenance.get("page_no")
                bbox = _region_bbox(provenance.get("bbox"))
                if isinstance(page_number, int) and page_number >= 1 and bbox is not None:
                    boxes.setdefault(page_number, []).append(bbox)
    return {page: tuple(page_boxes) for page, page_boxes in boxes.items()}


def _missing_index_cell_text(
    structure: Dict[str, Any], page_number: int, native_text: str
) -> str:
    """Keep OCR-only index cells without copying Docling's whole table rendering."""

    entries = structure.get("tables")
    if not isinstance(entries, list):
        return ""
    known_text = " ".join(native_text.split()).casefold()
    missing: list[str] = []
    for entry in entries:
        if not isinstance(entry, dict) or entry.get("label") != "document_index":
            continue
        provenance = entry.get("prov")
        if not isinstance(provenance, list) or not any(
            isinstance(item, dict) and item.get("page_no") == page_number
            for item in provenance
        ):
            continue
        data = entry.get("data")
        cells = data.get("table_cells") if isinstance(data, dict) else None
        if not isinstance(cells, list):
            continue
        for cell in cells:
            value = cell.get("text") if isinstance(cell, dict) else None
            if not isinstance(value, str) or not value.strip():
                continue
            value = value.strip()
            normalized = " ".join(value.split()).casefold()
            if f" {normalized} " not in f" {known_text} ":
                missing.append(value)
                known_text = f"{known_text} {normalized}"
    return "\n".join(missing)


def _native_page_text(cells: tuple[NativePdfTextCell, ...]) -> str:
    """Reconstruct readable lines from native cells in PDF coordinates."""

    lines: list[dict[str, Any]] = []
    positioned = [cell for cell in cells if cell.bbox is not None]
    unpositioned = [cell.text for cell in cells if cell.bbox is None]
    for cell in sorted(positioned, key=lambda item: (-item.bbox[3], item.bbox[0])):
        bottom, top = cell.bbox[1], cell.bbox[3]
        center = (bottom + top) / 2
        height = top - bottom
        line = next(
            (
                candidate
                for candidate in lines
                if abs(center - candidate["center"])
                <= max(2.0, min(height, candidate["height"]) * 0.5)
            ),
            None,
        )
        if line is None:
            lines.append({"center": center, "height": height, "cells": [cell]})
        else:
            line["cells"].append(cell)
    rendered = []
    for line in sorted(lines, key=lambda value: -value["center"]):
        ordered = sorted(line["cells"], key=lambda item: item.bbox[0])
        rendered.append(" ".join(cell.text for cell in ordered))
    # Invalid bounds prevent placement, but the cell's text still belongs in the index.
    return "\n".join(value for value in (*rendered, *unpositioned) if value.strip()).strip()


def _boxes_overlap(
    first: tuple[float, float, float, float],
    second: tuple[float, float, float, float],
) -> bool:
    """Return whether two PDF-point rectangles overlap with positive area."""

    return (
        min(first[2], second[2]) > max(first[0], second[0])
        and min(first[3], second[3]) > max(first[1], second[1])
    )


def _extract_docling_snapshot(
    content: bytes,
    extension: str,
    extraction_mode: DocumentExtractionMode = "standard",
) -> DocumentParseSnapshot:
    document = _parse_with_docling(content, extension, extraction_mode)
    structure = _sanitize_parser_structure(document.export_to_dict(coord_precision=3))
    text = document.export_to_markdown().strip()
    if not text and extension != ".pdf":
        raise ValueError("Document contains no extractable text")

    pages: tuple[DocumentSnapshotPage, ...] = ()
    if extension == ".pdf":
        native_cells = _native_pdf_cells(content)
        regions = _regions_from_docling_structure(
            structure,
            native_regions={
                page: tuple(cell.bbox for cell in cells if cell.bbox is not None)
                for page, cells in native_cells.items()
            },
            ocr_enabled=extraction_mode == "ocr",
        )
        document_index_pages = _document_index_page_numbers(structure)
        index_boxes = _document_index_boxes(structure)
        page_numbers = _page_numbers(structure) or sorted(regions)
        native_index_pages: set[int] = set()
        mixed_index_pages: set[int] = set()
        page_results = []
        for page_number in page_numbers:
            page_text = document.export_to_markdown(page_no=page_number).strip()
            if page_number in document_index_pages and native_cells.get(page_number):
                boxes = index_boxes.get(page_number, ())
                index_cells = tuple(
                    cell
                    for cell in native_cells[page_number]
                    if cell.bbox is None
                    or not boxes
                    or any(_boxes_overlap(cell.bbox, box) for box in boxes)
                )
                native_text = _native_page_text(index_cells)
                if native_text:
                    from docling_core.types.doc import DocItemLabel

                    recovered_cells = _missing_index_cell_text(
                        structure, page_number, native_text
                    )
                    if recovered_cells:
                        mixed_index_pages.add(page_number)
                    other_text = document.export_to_markdown(
                        page_no=page_number,
                        labels=set(DocItemLabel) - {DocItemLabel.DOCUMENT_INDEX},
                    ).strip()
                    page_text = "\n\n".join(
                        part
                        for part in (
                            "\n".join(part for part in (native_text, recovered_cells) if part),
                            other_text,
                        )
                        if part
                    )
                    native_index_pages.add(page_number)
            page_results.append(
                DocumentSnapshotPage(
                    page_number=page_number,
                    text=page_text,
                    regions=regions.get(page_number, ()),
                )
            )
        pages = tuple(page_results)
        if not any(page.text for page in pages):
            raise ValueError("Document contains no extractable text")
        if native_index_pages or not text:
            text = "\n\n".join(page.text for page in pages if page.text)
            pages = tuple(
                replace(
                    page,
                    regions=tuple(
                        replace(
                            region,
                            extraction_method=(
                                (
                                    "mixed"
                                    if page.page_number in mixed_index_pages
                                    else "native_text"
                                )
                                if page.page_number in native_index_pages
                                and region.element_type == "document_index"
                                else region.extraction_method
                            ),
                        )
                        for region in page.regions
                    ),
                )
                for page in pages
            )

    return DocumentParseSnapshot(
        text=text,
        structure=structure,
        parser_name="docling",
        parser_version=_docling_version(),
        parser_fingerprint=_fingerprint(
            _docling_parser_configuration(extraction_mode)
        ),
        pages=pages,
    )


def _exact_text_snapshot(text: str, extension: str) -> DocumentParseSnapshot:
    configuration = {"format": extension.lower(), "mode": "exact_text"}
    if extension.lower() == ".ipynb":
        configuration["knoggin_projection_revision"] = 2
    return DocumentParseSnapshot(
        text=text,
        structure=configuration,
        parser_name="knoggin-exact-text",
        parser_version="1",
        parser_fingerprint=_fingerprint(configuration),
    )


def _page_locator(page: DocumentSnapshotPage) -> Dict[str, Any]:
    methods = {region.extraction_method for region in page.regions}
    method = next(iter(methods)) if len(methods) == 1 else "mixed" if methods else "unknown"
    return LayoutRegion(
        page_number=page.page_number,
        element_type="page",
        extraction_method=method,
    ).as_locator()


def extract_and_split_document(
    content: bytes,
    extension: str,
    extraction_mode: DocumentExtractionMode = "standard",
) -> DocumentExtraction:
    """Capture one canonical parse, then derive all retrieval chunks from it."""

    normalized_extension = extension.lower()
    if normalized_extension == ".pdf":
        snapshot = _extract_docling_snapshot(
            content,
            normalized_extension,
            extraction_mode,
        )
        chunks = [
            DocumentChunk(
                content=chunk,
                page_number=page.page_number,
                layout_region=_page_locator(page),
            )
            for page in snapshot.pages
            if page.text
            for chunk in split_text(page.text)
        ]
    elif normalized_extension == ".docx":
        snapshot = _extract_docling_snapshot(content, normalized_extension)
        chunks = split_document(snapshot.text, extension=".md")
    else:
        text = extract_text(content, normalized_extension)
        snapshot = _exact_text_snapshot(text, normalized_extension)
        chunks = split_document(text, extension=normalized_extension)
    if not chunks:
        raise ValueError("Document produced no non-empty chunks")
    return DocumentExtraction(text=snapshot.text, chunks=chunks, snapshot=snapshot)


def _flatten_pdf_outline(
    reader: PdfReader,
) -> list[tuple[int, str, int]]:
    """Return valid one-based outline destinations in reading order."""

    flattened: list[tuple[int, str, int]] = []

    def visit(items: list[Any], depth: int) -> None:
        for item in items:
            if isinstance(item, list):
                visit(item, depth + 1)
                continue
            try:
                page_number = reader.get_destination_page_number(item) + 1
            except Exception:
                continue
            title = str(getattr(item, "title", "")).strip()
            if title and 1 <= page_number <= len(reader.pages):
                flattened.append((page_number, title, depth))

    try:
        visit(reader.outline, 0)
    except Exception:
        return []
    return flattened


def _chapter_outline_entries(
    outline: list[tuple[int, str, int]],
) -> list[tuple[int, str, int]]:
    explicit = [
        entry
        for entry in outline
        if re.match(r"^chapter\s+\d+(?:\D|$)", entry[1], re.IGNORECASE)
    ]
    candidates = explicit or [
        entry for entry in outline if re.match(r"^\d+\s+\S", entry[1])
    ]
    if not candidates:
        return []
    if not explicit:
        shallowest_depth = min(entry[2] for entry in candidates)
        candidates = [entry for entry in candidates if entry[2] == shallowest_depth]
    by_page: dict[int, tuple[int, str, int]] = {}
    for entry in candidates:
        by_page.setdefault(entry[0], entry)
    chapters = [by_page[page] for page in sorted(by_page)]
    # One bookmark is not enough evidence of a chapter structure. Treat it as
    # an ordinary bookmark so auto mode can use the safer fixed-page fallback.
    return chapters if len(chapters) >= 2 else []


def _pdf_page_groups(
    reader: PdfReader,
    *,
    target_pages: int,
) -> tuple[PdfPageGroup, ...]:
    """Pack whole outline chapters into bounded, gap-free page groups."""

    if target_pages <= 0:
        raise ValueError("Docling page group target must be positive")
    page_count = len(reader.pages)
    if page_count == 0:
        raise ValueError("PDF contains no pages")

    outline = _flatten_pdf_outline(reader)
    chapters = _chapter_outline_entries(outline)
    if not chapters:
        return tuple(
            PdfPageGroup(start, min(start + target_pages - 1, page_count))
            for start in range(1, page_count + 1, target_pages)
        )

    boundaries = {1, page_count + 1}
    for start_page, _title, depth in chapters:
        boundaries.add(start_page)
        next_peer_page = next(
            (
                page
                for page, _next_title, next_depth in outline
                if page > start_page and next_depth <= depth
            ),
            page_count + 1,
        )
        boundaries.add(next_peer_page)
    ordered_boundaries = sorted(boundaries)
    spans = [
        PdfPageGroup(start, end - 1)
        for start, end in zip(ordered_boundaries, ordered_boundaries[1:])
        if start < end
    ]

    groups: list[PdfPageGroup] = []
    current: PdfPageGroup | None = None
    for span in spans:
        if current is None:
            current = span
            continue
        if current.page_count + span.page_count <= target_pages:
            current = PdfPageGroup(current.start_page, span.end_page)
            continue
        groups.append(current)
        current = span
    if current is not None:
        groups.append(current)
    return tuple(groups)


def _pdf_group_bytes(reader: PdfReader, group: PdfPageGroup) -> bytes:
    writer = PdfWriter()
    for page_index in range(group.start_page - 1, group.end_page):
        writer.add_page(reader.pages[page_index])
    output = io.BytesIO()
    writer.write(output)
    return output.getvalue()


def _rebase_pdf_group_extraction(
    extraction: DocumentExtraction,
    group: PdfPageGroup,
) -> DocumentExtraction:
    page_offset = group.start_page - 1
    pages = tuple(
        replace(
            page,
            page_number=page.page_number + page_offset,
            regions=tuple(
                replace(region, page_number=region.page_number + page_offset)
                for region in page.regions
            ),
        )
        for page in extraction.snapshot.pages
    )
    chunks = [
        replace(
            chunk,
            page_number=(
                chunk.page_number + page_offset
                if chunk.page_number is not None
                else None
            ),
            layout_region=(
                {**chunk.layout_region, "page": chunk.layout_region["page"] + page_offset}
                if chunk.layout_region is not None
                else None
            ),
        )
        for chunk in extraction.chunks
    ]
    return DocumentExtraction(
        text="\n\n".join(page.text for page in pages if page.text),
        chunks=chunks,
        snapshot=replace(extraction.snapshot, text="", pages=pages),
    )


def _combine_pdf_group_extractions(
    groups: list[tuple[PdfPageGroup, DocumentExtraction]],
    extraction_mode: DocumentExtractionMode,
    grouping_method: str,
) -> DocumentExtraction:
    if not groups:
        raise ValueError("PDF extraction produced no page groups")
    pages = tuple(page for _group, result in groups for page in result.snapshot.pages)
    text = "\n\n".join(page.text for page in pages if page.text)
    chunks = [chunk for _group, result in groups for chunk in result.chunks]
    first_snapshot = groups[0][1].snapshot
    structure = {
        "schema_name": "KnogginGroupedDoclingDocument",
        "version": 1,
        "page_group_target": _DOCLING_PAGE_GROUP_TARGET,
        "grouping_method": grouping_method,
        "groups": [
            {
                "start_page": group.start_page,
                "end_page": group.end_page,
                "structure": result.snapshot.structure,
            }
            for group, result in groups
        ],
    }
    snapshot = DocumentParseSnapshot(
        text=text,
        structure=structure,
        parser_name=first_snapshot.parser_name,
        parser_version=first_snapshot.parser_version,
        parser_fingerprint=_fingerprint(_docling_parser_configuration(extraction_mode)),
        pages=pages,
    )
    return DocumentExtraction(text=text, chunks=chunks, snapshot=snapshot)


async def _stop_docling_process(process: asyncio.subprocess.Process) -> None:
    """Stop and reap a dedicated extraction subprocess."""

    if process.returncode is not None:
        return
    process.terminate()
    try:
        async with asyncio.timeout(_DOCLING_PROCESS_STOP_SECONDS):
            await process.wait()
    except TimeoutError:
        process.kill()
        await process.wait()


async def extract_and_split_docling_document(
    content: bytes,
    extension: str,
    extraction_mode: DocumentExtractionMode = "standard",
    extraction_strategy: DocumentExtractionStrategy = "auto",
) -> DocumentExtraction:
    """Run PDF/DOCX extraction in a disposable spawned process.

    Process ownership stays here so success, failure, timeout, and caller
    cancellation all reap the worker.  Only the stable Knoggin extraction
    projection crosses back to the server process; Docling state dies with
    the worker.
    """

    normalized_extension = extension.lower()
    if normalized_extension not in {".pdf", ".docx"}:
        raise ValueError("Isolated Docling extraction only supports PDF and DOCX")
    if extraction_strategy not in {"auto", "full", "bounded"}:
        raise ValueError("Unsupported document extraction strategy")
    if _DOCLING_PROCESS_TIMEOUT_SECONDS <= 0:
        raise ValueError("DOCLING_TIMEOUT_SECONDS must be positive")
    if _DOCLING_PAGE_GROUP_TARGET <= 0:
        raise ValueError("DOCLING_PAGE_GROUP_SIZE must be positive")

    async with _DOCLING_PROCESS_LOCK:
        if normalized_extension == ".pdf":
            reader = PdfReader(io.BytesIO(content))
            page_count = len(reader.pages)
            if page_count == 0:
                raise ValueError("PDF contains no pages")
            use_bounded_groups = extraction_strategy == "bounded" or (
                extraction_strategy == "auto" and page_count > _DOCLING_PAGE_GROUP_TARGET
            )
            if not use_bounded_groups:
                return await _extract_and_split_docling_document_unlocked(
                    content,
                    normalized_extension,
                    extraction_mode,
                )
            has_chapter_outline = bool(
                _chapter_outline_entries(_flatten_pdf_outline(reader))
            )
            page_groups = _pdf_page_groups(
                reader,
                target_pages=_DOCLING_PAGE_GROUP_TARGET,
            )
            extracted_groups: list[tuple[PdfPageGroup, DocumentExtraction]] = []
            for group_index, page_group in enumerate(page_groups, start=1):
                logger.info(
                    "Docling extraction group "
                    f"{group_index}/{len(page_groups)} | "
                    f"pages={page_group.start_page}-{page_group.end_page}"
                )
                group_result = await _extract_and_split_docling_document_unlocked(
                    _pdf_group_bytes(reader, page_group),
                    normalized_extension,
                    extraction_mode,
                )
                extracted_groups.append(
                    (
                        page_group,
                        _rebase_pdf_group_extraction(group_result, page_group),
                    )
                )
                logger.info(
                    "Completed Docling extraction group "
                    f"{group_index}/{len(page_groups)} | "
                    f"pages={page_group.start_page}-{page_group.end_page}"
                )
            return _combine_pdf_group_extractions(
                extracted_groups,
                extraction_mode,
                "chapters" if has_chapter_outline else "fixed_pages",
            )
        return await _extract_and_split_docling_document_unlocked(
            content,
            normalized_extension,
            extraction_mode,
        )


async def _extract_and_split_docling_document_unlocked(
    content: bytes,
    normalized_extension: str,
    extraction_mode: DocumentExtractionMode,
) -> DocumentExtraction:
    """Own one child process while the global Docling admission lock is held."""

    work_directory = Path(tempfile.mkdtemp(prefix="knoggin-docling-"))
    input_path = work_directory / "source.bin"
    result_path = work_directory / "result.pickle"
    stderr_path = work_directory / "stderr.log"
    input_path.write_bytes(content)
    try:
        with stderr_path.open("wb") as stderr_file:
            start_process = asyncio.create_task(
                asyncio.create_subprocess_exec(
                    sys.executable,
                    "-m",
                    "core.knowledge.documents.extraction_worker",
                    str(input_path),
                    str(result_path),
                    normalized_extension,
                    extraction_mode,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=stderr_file,
                )
            )
            try:
                process = await asyncio.shield(start_process)
            except asyncio.CancelledError:
                process = await start_process
                await asyncio.shield(_stop_docling_process(process))
                raise
        try:
            async with asyncio.timeout(_DOCLING_PROCESS_TIMEOUT_SECONDS):
                while not result_path.is_file() and process.returncode is None:
                    await asyncio.sleep(_DOCLING_PROCESS_POLL_SECONDS)
        except TimeoutError as exc:
            raise TimeoutError(
                "Docling extraction exceeded "
                f"{_DOCLING_PROCESS_TIMEOUT_SECONDS:g} seconds"
            ) from exc
        finally:
            await asyncio.shield(_stop_docling_process(process))

        if not result_path.is_file():
            detail = stderr_path.read_text(encoding="utf-8", errors="replace").strip()
            raise RuntimeError(
                "Docling extraction process exited without returning a result "
                f"(exit code {process.returncode}): {detail[-1000:]}"
            )
        with result_path.open("rb") as result_file:
            status, payload = pickle.load(result_file)
    finally:
        shutil.rmtree(work_directory, ignore_errors=True)

    if status == "ok":
        return payload
    if status == "dependency_error":
        raise DependencyError(payload)
    if status == "value_error":
        raise ValueError(payload)
    raise RuntimeError(f"Docling extraction process failed: {payload}")


def split_text(text: str) -> List[str]:
    chunks = [chunk.strip() for chunk in _SPLITTER.split_text(text)]
    chunks = [chunk for chunk in chunks if chunk]
    if not chunks:
        raise ValueError("Document produced no non-empty chunks")
    return chunks


def split_document(
    text: str,
    *,
    extension: str,
) -> List[DocumentChunk]:
    """Split prose generically and source code on line-preserving sections."""
    language = _CODE_LANGUAGES.get(extension.lower())
    normalized_extension = extension.lower()
    if normalized_extension == ".ipynb":
        return _split_notebook(text)
    if normalized_extension == ".csv":
        return _split_csv(text)
    if normalized_extension == ".md":
        return _split_markdown(text)
    if language is None:
        return _split_text_with_lines(text)
    return _split_code(text, language, extension.lower())


def _split_text_with_lines(
    text: str,
    *,
    start_line: int = 1,
    section_path: Optional[tuple[str, ...]] = None,
) -> List[DocumentChunk]:
    """Split prose on whole lines, preserving exact one-based line spans."""

    lines = text.splitlines()
    if not lines:
        raise ValueError("Document produced no non-empty chunks")

    chunks: List[DocumentChunk] = []
    current: List[str] = []
    current_start = start_line
    current_size = 0
    for line_offset, line in enumerate(lines):
        line_number = start_line + line_offset
        line_size = len(line) + 1
        if current and current_size + line_size > _TEXT_CHUNK_TARGET_CHARS:
            chunk = _line_chunk(current, current_start, section_path)
            if chunk is not None:
                chunks.append(chunk)
            current = []
            current_start = line_number
            current_size = 0
        if not current and line_size > _TEXT_CHUNK_TARGET_CHARS:
            for excerpt in split_text(line):
                chunks.append(
                    DocumentChunk(
                        content=excerpt,
                        start_line=line_number,
                        end_line=line_number,
                        section_path=section_path,
                    )
                )
            current_start = line_number + 1
            continue
        if not current:
            current_start = line_number
        current.append(line)
        current_size += line_size

    chunk = _line_chunk(current, current_start, section_path)
    if chunk is not None:
        chunks.append(chunk)
    if not chunks:
        raise ValueError("Document produced no non-empty chunks")
    return chunks


def _line_chunk(
    lines: List[str],
    start_line: int,
    section_path: Optional[tuple[str, ...]],
) -> Optional[DocumentChunk]:
    non_empty = [index for index, line in enumerate(lines) if line.strip()]
    if not non_empty:
        return None
    first, last = non_empty[0], non_empty[-1]
    return DocumentChunk(
        content="\n".join(lines[first : last + 1]),
        start_line=start_line + first,
        end_line=start_line + last,
        section_path=section_path,
    )


def _split_markdown(text: str) -> List[DocumentChunk]:
    """Keep Markdown chunks inside their active heading path."""

    lines = text.splitlines()
    if not lines:
        raise ValueError("Document produced no non-empty chunks")
    chunks: List[DocumentChunk] = []
    section_start = 0
    section_path: Optional[tuple[str, ...]] = None
    active_path: List[str] = []

    for line_index, line in enumerate(lines):
        heading = _MARKDOWN_HEADING.match(line)
        if heading is None:
            continue
        if line_index > section_start and any(
            part.strip() for part in lines[section_start:line_index]
        ):
            chunks.extend(
                _split_text_with_lines(
                    "\n".join(lines[section_start:line_index]),
                    start_line=section_start + 1,
                    section_path=section_path,
                )
            )
        level = len(heading.group(1))
        title = heading.group(2).strip()
        active_path = active_path[: level - 1]
        active_path.append(title)
        section_path = tuple(active_path)
        section_start = line_index

    if section_start < len(lines) and any(part.strip() for part in lines[section_start:]):
        chunks.extend(
            _split_text_with_lines(
                "\n".join(lines[section_start:]),
                start_line=section_start + 1,
                section_path=section_path,
            )
        )
    if not chunks:
        raise ValueError("Document produced no non-empty chunks")
    return chunks


def _split_csv(text: str) -> List[DocumentChunk]:
    """Split CSV data into bounded chunks with one-based data-row ranges."""

    rows = _parse_csv_rows(text)
    if len(rows) < 2:
        raise ValueError("CSV document contains no data rows")

    header = _render_csv_row(rows[0])
    if len(header) + 1 >= _TEXT_CHUNK_TARGET_CHARS:
        raise ValueError("CSV header exceeds the document chunk size limit")
    chunks: List[DocumentChunk] = []
    current: List[str] = []
    start_row = 1
    current_size = len(header) + 1
    for row_number, row in enumerate(rows[1:], start=1):
        rendered = _render_csv_row(row)
        if len(header) + 1 + len(rendered) > _TEXT_CHUNK_TARGET_CHARS:
            raise ValueError(
                f"CSV row {row_number} exceeds the document chunk size limit"
            )
        if current and current_size + len(rendered) + 1 > _TEXT_CHUNK_TARGET_CHARS:
            chunks.append(
                DocumentChunk(
                    content="\n".join([header, *current]),
                    chunk_kind="csv",
                    start_row=start_row,
                    end_row=row_number - 1,
                )
            )
            current = []
            start_row = row_number
            current_size = len(header) + 1
        current.append(rendered)
        current_size += len(rendered) + 1
    if current:
        chunks.append(
            DocumentChunk(
                content="\n".join([header, *current]),
                chunk_kind="csv",
                start_row=start_row,
                end_row=len(rows) - 1,
            )
        )
    return chunks


def _render_csv_row(row: List[str]) -> str:
    output = io.StringIO(newline="")
    csv.writer(output, lineterminator="").writerow(row)
    return output.getvalue()


def csv_data_rows(text: str) -> List[str]:
    """Return exact rendered data rows for a bounded CSV document read."""

    rows = _parse_csv_rows(text)
    if len(rows) < 2:
        raise ValueError("CSV document contains no data rows")
    return [_render_csv_row(row) for row in rows[1:]]


def _parse_csv_rows(text: str) -> List[List[str]]:
    try:
        return list(csv.reader(io.StringIO(text, newline="")))
    except csv.Error as exc:
        raise ValueError("CSV document is invalid") from exc


def _extract_notebook_text(content: bytes) -> str:
    try:
        notebook = json.loads(content.decode("utf-8-sig", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Notebook is not valid JSON") from exc
    cells = notebook.get("cells") if isinstance(notebook, dict) else None
    if not isinstance(cells, list):
        raise ValueError("Notebook contains no cell list")

    serialized_cells = []
    for index, cell in enumerate(cells, start=1):
        if not isinstance(cell, dict):
            continue
        cell_type = cell.get("cell_type")
        source = cell.get("source", "")
        if cell_type not in {"code", "markdown"}:
            continue
        if isinstance(source, list):
            source = "".join(str(line) for line in source)
        if not isinstance(source, str) or not source.strip():
            continue
        serialized_cells.append(
            f"[[KNOGGIN_NOTEBOOK_CELL index={index} type={cell_type} "
            f"length={len(source)}]]\n{source}"
        )
    if not serialized_cells:
        raise ValueError("Notebook contains no extractable code or Markdown cells")
    return "\n\n".join(serialized_cells)


def _split_notebook(text: str) -> List[DocumentChunk]:
    chunks = []
    position = 0
    lines_before_position = 0
    while position < len(text):
        match = _NOTEBOOK_CELL_HEADER.match(text, position)
        if match is None:
            raise ValueError("Notebook produced no recognizable cells")
        cell_number, cell_type, source_length = match.groups()
        # The serialized header is one line; chunk ranges refer to this same
        # snapshot text, which is what read_document later slices.
        lines_before_source = lines_before_position + 1
        end = match.end() + int(source_length)
        if end > len(text):
            raise ValueError("Notebook cell content is incomplete")
        content = text[match.end() : end]
        position = end
        lines_before_position = lines_before_source + sum(
            part.endswith(("\n", "\r", "\v", "\f", "\x1c", "\x1d", "\x1e", "\x85", "\u2028", "\u2029"))
            for part in content.splitlines(keepends=True)
        )
        if position < len(text):
            if not text.startswith("\n\n", position):
                raise ValueError("Notebook cell separator is invalid")
            position += 2
            # A trailing CR joins the separator's first LF into one CRLF.
            lines_before_position += 1 if content.endswith("\r") else 2
        if not content.strip():
            continue
        try:
            if cell_type == "code":
                lines = content.splitlines()
                non_empty = [index for index, line in enumerate(lines) if line.strip()]
                first, last = non_empty[0], non_empty[-1]
                cell_chunks = _split_code_sections(
                    "\n".join(lines[first : last + 1]),
                    "notebook",
                    [],
                    line_offset=first,
                )
            else:
                cell_chunks = _split_text_with_lines(content)
        except ValueError as exc:
            raise ValueError(f"Notebook cell {cell_number}: {exc}") from exc
        chunks.extend(
            replace(
                chunk,
                language="notebook",
                chunk_kind=f"notebook_{cell_type}",
                symbol_name=f"cell {cell_number}",
                start_line=chunk.start_line + lines_before_source,
                end_line=chunk.end_line + lines_before_source,
            )
            for chunk in cell_chunks
        )
    if not chunks:
        raise ValueError("Notebook produced no non-empty cells")
    return chunks


def embedding_text(chunk: DocumentChunk, relative_path: str) -> str:
    """Add stable source context to code embeddings without altering stored text."""
    if chunk.language is None:
        return chunk.content
    header = [f"File: {relative_path}", f"Language: {chunk.language}"]
    if chunk.symbol_name:
        header.append(f"Symbol: {chunk.symbol_name}")
    return "\n".join(header) + "\n\n" + chunk.content


def _split_code(
    text: str,
    language: str,
    extension: str,
) -> List[DocumentChunk]:
    tree_sitter_chunks = _split_code_with_tree_sitter(
        text,
        language,
        extension,
    )
    if tree_sitter_chunks is not None:
        return tree_sitter_chunks
    return _split_code_with_regex(text, language)


def _split_code_with_tree_sitter(
    text: str,
    language: str,
    extension: str,
) -> Optional[List[DocumentChunk]]:
    tree_sitter_language = _TREE_SITTER_LANGUAGES.get(extension)
    symbol_types = _TREE_SITTER_SYMBOL_TYPES.get(language)
    if tree_sitter_language is None or symbol_types is None:
        return None

    source = text.encode("utf-8")
    tree = Parser(tree_sitter_language).parse(source)
    if language == "zig":
        boundaries = _zig_boundaries(tree.root_node, source)
    elif language == "yaml":
        boundaries = _yaml_boundaries(tree.root_node, source)
    else:
        boundaries = []
        for node in tree.root_node.named_children:
            symbol_node = _unwrap_tree_sitter_node(node, symbol_types)
            if symbol_node is None:
                continue
            symbol_name = _tree_sitter_symbol_name(
                symbol_node,
                source,
                language,
            )
            if symbol_name is None:
                continue
            boundaries.append((node.start_point.row, symbol_name))
        if language == "python":
            boundaries.extend(_python_method_boundaries(text))
            boundaries.sort(key=lambda boundary: boundary[0])
    if not boundaries:
        return None
    return _split_code_sections(text, language, boundaries)


def _python_method_boundaries(
    text: str,
) -> List[tuple[int, str]]:
    """Return direct Python class methods with qualified names."""
    try:
        module = ast.parse(text)
    except SyntaxError:
        return []
    boundaries = []
    for declaration in module.body:
        if not isinstance(declaration, ast.ClassDef):
            continue
        for member in declaration.body:
            if not isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            start_line = min(
                (decorator.lineno for decorator in member.decorator_list),
                default=member.lineno,
            )
            boundaries.append(
                (start_line - 1, f"{declaration.name}.{member.name}")
            )
    return boundaries


def _zig_boundaries(root_node, source: bytes) -> List[tuple[int, str]]:
    """Find file declarations and methods inside named Zig containers."""
    boundaries: List[tuple[int, str]] = []
    container_types = {"struct_declaration", "enum_declaration", "union_declaration"}

    def visit(nodes, scope: str = "") -> None:
        for node in nodes:
            if node.type == "function_declaration":
                name_node = node.child_by_field_name("name")
            elif node.type == "variable_declaration":
                name_node = next(
                    (child for child in node.named_children if child.type == "identifier"),
                    None,
                )
            elif node.type == "test_declaration":
                name_node = next(
                    (child for child in node.named_children if child.type == "string"),
                    None,
                )
            else:
                continue
            if name_node is None:
                continue
            name = _tree_sitter_node_text(name_node, source)
            if node.type == "test_declaration":
                name = f"test {name}"
            symbol_name = f"{scope}.{name}" if scope else name
            boundaries.append((node.start_point.row, symbol_name))
            if node.type == "variable_declaration":
                container = next(
                    (child for child in node.named_children if child.type in container_types),
                    None,
                )
                if container is not None:
                    visit(container.named_children, symbol_name)

    visit(root_node.named_children)
    return boundaries


def _unwrap_tree_sitter_node(node, symbol_types):
    """Return a supported declaration inside one top-level wrapper node."""
    if node.type.endswith("_instruction"):
        return node
    if node.type in symbol_types:
        return node
    if node.type not in _TREE_SITTER_WRAPPER_TYPES:
        return None
    for child in node.named_children:
        if child.type in symbol_types:
            return child
    return None


def _tree_sitter_symbol_name(
    node,
    source: bytes,
    language: str,
) -> Optional[str]:
    name_node = node.child_by_field_name("name")
    if name_node is not None:
        return _tree_sitter_node_text(name_node, source)
    if language in {"c", "cpp", "go", "sql"}:
        return _first_identifier(node, source)
    if language == "dockerfile":
        return node.type.removesuffix("_instruction").upper()
    return None


def _yaml_boundaries(root_node, source: bytes) -> List[tuple[int, str]]:
    """Return top-level mapping keys as YAML retrieval sections."""
    document = next(
        (node for node in root_node.named_children if node.type == "document"),
        None,
    )
    if document is None:
        return []
    mapping = next(
        (
            node
            for node in document.children[0].named_children
            if node.type == "block_mapping"
        ),
        None,
    )
    if mapping is None:
        return []
    boundaries = []
    for pair in mapping.named_children:
        if pair.type != "block_mapping_pair":
            continue
        key = pair.child_by_field_name("key")
        if key is not None:
            boundaries.append(
                (pair.start_point.row, _tree_sitter_node_text(key, source))
            )
    return boundaries


def _first_identifier(node, source: bytes) -> Optional[str]:
    if node.type in {"identifier", "type_identifier", "word"}:
        return _tree_sitter_node_text(node, source)
    for child in node.named_children:
        identifier = _first_identifier(child, source)
        if identifier is not None:
            return identifier
    return None


def _tree_sitter_node_text(node, source: bytes) -> str:
    return source[node.start_byte : node.end_byte].decode("utf-8")


def _split_code_with_regex(text: str, language: str) -> List[DocumentChunk]:
    lines = text.splitlines()
    if not lines:
        raise ValueError("Document produced no non-empty chunks")
    boundaries = []
    for index, line in enumerate(lines):
        if language == "dockerfile":
            match = _DOCKERFILE_INSTRUCTION_PATTERN.match(line)
        else:
            match = _SYMBOL_PATTERN.match(line) if line == line.lstrip() else None
        if match:
            boundaries.append((index, match.group(1).upper() if language == "dockerfile" else match.group(1)))
    return _split_code_sections(text, language, boundaries)


def _split_code_sections(
    text: str,
    language: str,
    boundaries: List[tuple[int, str]],
    *,
    line_offset: int = 0,
) -> List[DocumentChunk]:
    lines = text.splitlines()
    if not lines:
        raise ValueError("Document produced no non-empty chunks")
    section_starts = [(0, None)]
    for start, symbol_name in boundaries:
        if start > 0 and start != section_starts[-1][0]:
            section_starts.append((start, symbol_name))
        elif start == 0:
            section_starts[0] = (0, symbol_name)

    chunks = []
    for index, (section_start, symbol_name) in enumerate(section_starts):
        section_end = (
            section_starts[index + 1][0]
            if index + 1 < len(section_starts)
            else len(lines)
        )
        section_lines = lines[section_start:section_end]
        # Split oversized sections by line length while retaining exact ranges.
        current = []
        current_chars = 0
        start = section_start
        for line_index, line in enumerate(section_lines, start=section_start):
            if len(line) > _CODE_CHUNK_TARGET_CHARS:
                raise ValueError(
                    f"Code line {line_index + line_offset + 1} "
                    "exceeds the document chunk size limit"
                )
            if current and current_chars + len(line) + 1 > _CODE_CHUNK_TARGET_CHARS:
                chunks.append(
                    _code_chunk(current, language, start + line_offset, symbol_name)
                )
                current = []
                current_chars = 0
                start = line_index
            current.append(line)
            current_chars += len(line) + 1
        if current:
            chunks.append(
                _code_chunk(current, language, start + line_offset, symbol_name)
            )
    return [chunk for chunk in chunks if chunk.content]


def _code_chunk(
    lines: List[str],
    language: str,
    start_index: int,
    symbol_name: Optional[str],
) -> DocumentChunk:
    content = "\n".join(lines).strip()
    return DocumentChunk(
        content=content,
        language=language,
        chunk_kind="code",
        symbol_name=symbol_name,
        start_line=start_index + 1,
        end_line=start_index + len(lines),
    )
