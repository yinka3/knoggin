import io
import json
import re
import warnings
from dataclasses import dataclass
from typing import List, Optional

import docx2txt
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
    """One retrieval chunk with optional source-code navigation metadata."""

    content: str
    language: Optional[str] = None
    chunk_kind: str = "text"
    symbol_name: Optional[str] = None
    start_line: Optional[int] = None
    end_line: Optional[int] = None


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
        reader = PdfReader(io.BytesIO(content))
        text = "\n\n".join(page.extract_text() or "" for page in reader.pages)
    elif ext == ".docx":
        text = docx2txt.process(io.BytesIO(content)) or ""
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
    if extension.lower() == ".ipynb":
        return _split_notebook(text)
    if language is None:
        return [DocumentChunk(content=chunk) for chunk in split_text(text)]
    return _split_code(text, language, extension.lower())


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
