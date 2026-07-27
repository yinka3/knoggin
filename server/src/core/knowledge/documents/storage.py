import csv
import io
import json
import re
import warnings
from dataclasses import dataclass
from typing import List, Optional

import pytesseract
import tree_sitter_bash
import tree_sitter_c
import tree_sitter_c_sharp
import tree_sitter_cpp
import tree_sitter_dockerfile
import tree_sitter_go
import tree_sitter_java
import tree_sitter_javascript
import tree_sitter_python
import tree_sitter_rust
import tree_sitter_sql
import tree_sitter_typescript
import tree_sitter_yaml
from llama_index.core.node_parser import SentenceSplitter
from PIL import Image as PILImage
from pypdf import PdfReader
from docx import Document as DocxDocument
from tree_sitter import Language, Parser

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


@dataclass(frozen=True)
class DocumentExtraction:
    """The extracted text and location-preserving chunks for one document."""

    text: str
    chunks: List[DocumentChunk]


@dataclass(frozen=True)
class PdfPage:
    """Extracted text for one one-based PDF page."""

    page_number: int
    text: str


@dataclass(frozen=True)
class DocxParagraph:
    """One body paragraph with its stable one-based Word position."""

    paragraph_number: int
    text: str
    heading_level: Optional[int] = None


_CODE_LANGUAGES = {
    ".bash": "bash", ".c": "c", ".cpp": "cpp", ".cs": "c_sharp",
    ".dockerfile": "dockerfile",
    ".go": "go", ".h": "c", ".hpp": "cpp", ".java": "java",
    ".js": "javascript", ".jsx": "javascript", ".kt": "kotlin",
    ".php": "php", ".py": "python", ".rb": "ruby", ".rs": "rust",
    ".sh": "bash", ".sql": "sql", ".swift": "swift", ".ts": "typescript",
    ".tsx": "typescript", ".vue": "vue", ".yaml": "yaml", ".yml": "yaml",
    ".zsh": "bash",
}
_SYMBOL_PATTERN = re.compile(
    r"^(?:(?:export\s+)?(?:async\s+)?)?"
    r"(?:def|class|function|interface|type|func|struct|enum)\s+"
    r"([A-Za-z_$][\w$]*)"
)
_CODE_CHUNK_TARGET_CHARS = 2_400
_NOTEBOOK_CELL_HEADER = re.compile(
    r"^\[\[KNOGGIN_NOTEBOOK_CELL index=(\d+) type=(code|markdown)\]\]$",
    re.MULTILINE,
)
_MARKDOWN_HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*#*\s*$")
_DOCX_HEADING_STYLE = re.compile(r"^Heading ([1-9])$", re.IGNORECASE)
_TEXT_CHUNK_TARGET_CHARS = CHUNK_SIZE_TOKENS * 4

def _dockerfile_language() -> Language:
    """Load the Dockerfile grammar while isolating its legacy handle warning."""
    # tree-sitter-dockerfile 0.2.0 still exports an integer language handle.
    # Remove this narrow filter once it adopts Tree-sitter's capsule-based API.
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="int argument support is deprecated",
            category=DeprecationWarning,
        )
        return Language(tree_sitter_dockerfile.language())


# Languages are immutable and safe to share. Parsers are created per call
# because document indexing can split files concurrently on worker threads.
_TREE_SITTER_LANGUAGES = {
    ".bash": Language(tree_sitter_bash.language()),
    ".c": Language(tree_sitter_c.language()),
    ".cpp": Language(tree_sitter_cpp.language()),
    ".cs": Language(tree_sitter_c_sharp.language()),
    ".dockerfile": _dockerfile_language(),
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
    ".zsh": Language(tree_sitter_bash.language()),
}
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


def extract_text(content: bytes, extension: str) -> str:
    ext = extension.lower()

    if ext not in ACCEPTED_EXTENSIONS:
        raise ValueError(
            f"Unsupported file type '{ext}'. "
            f"Accepted types include PDF, DOCX, plain text, source code, and images."
        )

    if ext == ".pdf":
        text = "\n\n".join(page.text for page in extract_pdf_pages(content))
    elif ext == ".docx":
        text = "\n".join(
            paragraph.text for paragraph in extract_docx_paragraphs(content)
            if paragraph.text.strip()
        )
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


def extract_pdf_pages(content: bytes) -> List[PdfPage]:
    """Extract non-empty PDF pages without flattening their locations."""

    reader = PdfReader(io.BytesIO(content))
    pages = [
        PdfPage(page_number=index, text=page.extract_text() or "")
        for index, page in enumerate(reader.pages, start=1)
    ]
    if not any(page.text.strip() for page in pages):
        raise ValueError("Document contains no extractable text")
    return pages


def extract_docx_paragraphs(content: bytes) -> List[DocxParagraph]:
    """Extract DOCX body paragraphs without losing their Word positions."""

    document = DocxDocument(io.BytesIO(content))
    paragraphs = [
        DocxParagraph(
            paragraph_number=index,
            text=paragraph.text,
            heading_level=_docx_heading_level(paragraph.style.name),
        )
        for index, paragraph in enumerate(document.paragraphs, start=1)
    ]
    if not any(paragraph.text.strip() for paragraph in paragraphs):
        raise ValueError("Document contains no extractable text")
    return paragraphs


def _docx_heading_level(style_name: str) -> Optional[int]:
    match = _DOCX_HEADING_STYLE.fullmatch(style_name or "")
    return int(match.group(1)) if match else None


def docx_heading_path(
    paragraphs: List[DocxParagraph], paragraph_number: int
) -> Optional[tuple[str, ...]]:
    """Return the active Word heading path at a one-based paragraph position."""

    active_path: List[str] = []
    for paragraph in paragraphs:
        if paragraph.paragraph_number > paragraph_number:
            break
        if paragraph.heading_level is not None:
            active_path = active_path[: paragraph.heading_level - 1]
            active_path.append(paragraph.text.strip())
    return tuple(active_path) or None


def extract_and_split_document(content: bytes, extension: str) -> DocumentExtraction:
    """Produce text and chunks together so PDF page boundaries remain intact."""

    if extension.lower() == ".pdf":
        pages = extract_pdf_pages(content)
        chunks = [
            DocumentChunk(content=chunk, page_number=page.page_number)
            for page in pages
            if page.text.strip()
            for chunk in split_text(page.text)
        ]
        if not chunks:
            raise ValueError("Document produced no non-empty chunks")
        return DocumentExtraction(
            text="\n\n".join(page.text for page in pages),
            chunks=chunks,
        )

    if extension.lower() == ".docx":
        paragraphs = extract_docx_paragraphs(content)
        return DocumentExtraction(
            text="\n".join(
                paragraph.text for paragraph in paragraphs if paragraph.text.strip()
            ),
            chunks=_split_docx(paragraphs),
        )

    text = extract_text(content, extension)
    return DocumentExtraction(text=text, chunks=split_document(text, extension=extension))


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


def _split_docx(paragraphs: List[DocxParagraph]) -> List[DocumentChunk]:
    """Keep DOCX chunks within a Word heading path and paragraph range."""

    chunks: List[DocumentChunk] = []
    active_path: List[str] = []
    section_path: Optional[tuple[str, ...]] = None
    section: List[DocxParagraph] = []

    for paragraph in paragraphs:
        if paragraph.heading_level is not None:
            chunks.extend(_split_docx_section(section, section_path))
            section = []
            active_path = active_path[: paragraph.heading_level - 1]
            active_path.append(paragraph.text.strip())
            section_path = tuple(active_path)
        section.append(paragraph)
    chunks.extend(_split_docx_section(section, section_path))
    if not chunks:
        raise ValueError("Document produced no non-empty chunks")
    return chunks


def _split_docx_section(
    paragraphs: List[DocxParagraph],
    heading_path: Optional[tuple[str, ...]],
) -> List[DocumentChunk]:
    chunks: List[DocumentChunk] = []
    current: List[DocxParagraph] = []
    current_size = 0
    for paragraph in paragraphs:
        paragraph_size = len(paragraph.text) + 1
        if current and current_size + paragraph_size > _TEXT_CHUNK_TARGET_CHARS:
            chunk = _docx_chunk(current, heading_path)
            if chunk is not None:
                chunks.append(chunk)
            current = []
            current_size = 0
        current.append(paragraph)
        current_size += paragraph_size
    chunk = _docx_chunk(current, heading_path)
    if chunk is not None:
        chunks.append(chunk)
    return chunks


def _docx_chunk(
    paragraphs: List[DocxParagraph],
    heading_path: Optional[tuple[str, ...]],
) -> Optional[DocumentChunk]:
    non_empty = [paragraph for paragraph in paragraphs if paragraph.text.strip()]
    if not non_empty:
        return None
    return DocumentChunk(
        content="\n".join(paragraph.text for paragraph in non_empty),
        section_path=heading_path,
        start_paragraph=non_empty[0].paragraph_number,
        end_paragraph=non_empty[-1].paragraph_number,
    )


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
    chunks: List[DocumentChunk] = []
    current: List[str] = []
    start_row = 1
    current_size = len(header) + 1
    for row_number, row in enumerate(rows[1:], start=1):
        rendered = _render_csv_row(row)
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
            f"[[KNOGGIN_NOTEBOOK_CELL index={index} type={cell_type}]]\n"
            f"{source.strip()}"
        )
    if not serialized_cells:
        raise ValueError("Notebook contains no extractable code or Markdown cells")
    return "\n\n".join(serialized_cells)


def _split_notebook(text: str) -> List[DocumentChunk]:
    matches = list(_NOTEBOOK_CELL_HEADER.finditer(text))
    if not matches:
        raise ValueError("Notebook produced no recognizable cells")
    chunks = []
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        content = text[match.end() : end].strip()
        if not content:
            continue
        cell_number, cell_type = match.groups()
        chunks.append(
            DocumentChunk(
                content=content,
                language="notebook",
                chunk_kind=f"notebook_{cell_type}",
                symbol_name=f"cell {cell_number}",
                start_line=1,
                end_line=len(content.splitlines()),
            )
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
    if language == "yaml":
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
    if not boundaries:
        return None
    return _split_code_sections(text, language, boundaries)


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
        match = _SYMBOL_PATTERN.match(line) if line == line.lstrip() else None
        if match:
            boundaries.append((index, match.group(1)))
    return _split_code_sections(text, language, boundaries)


def _split_code_sections(
    text: str,
    language: str,
    boundaries: List[tuple[int, str]],
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
            if current and current_chars + len(line) + 1 > _CODE_CHUNK_TARGET_CHARS:
                chunks.append(
                    _code_chunk(current, language, start, symbol_name)
                )
                current = []
                current_chars = 0
                start = line_index
            current.append(line)
            current_chars += len(line) + 1
        if current:
            chunks.append(_code_chunk(current, language, start, symbol_name))
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
