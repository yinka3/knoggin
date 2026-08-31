import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from common.schema.document import (
    DocumentSelection,
    FolderScanSettings,
    FolderUploadEntry,
)
from core.knowledge.documents import (
    DocumentIndexPolicy,
    DocumentService,
    ProjectFilesystemFactory,
)
from core.knowledge.documents import (
    storage as storage_module,
)
from core.knowledge.documents.constants import document_extension
from infrastructure.background_work import BackgroundWorkRejected


class MemoryPostgres:
    def __init__(self):
        self.rows = []
        self.scan_settings = {}
        self.chunks = []
        self.contents = {}  # document_id -> bytes
        self.extracted_text = {}  # document_id -> (content_hash, text)
        self.calls = []
        self.write_error = None
        self.transaction_error_at_chunk = None
        self.transaction_commit_error = None
        self.transaction_count = 0
        self.delete_error = None
        self.search_results = []

    @asynccontextmanager
    async def transaction(self):
        self.transaction_count += 1
        async with MemoryTransaction(self):
            async with MemoryCursor(self) as cursor:
                yield cursor

    @staticmethod
    def _visible(row, project_id):
        return row["project_id"] == project_id

    @staticmethod
    def _read_visible(row, readable_project_ids):
        return row["project_id"] in readable_project_ids

    async def execute(self, query, params=None):
        self.calls.append(("execute", query, params))
        if self.write_error:
            raise self.write_error
        if "project_document_scan_settings" in query:
            if "DELETE FROM" in query:
                self.scan_settings.pop(params[0], None)
                return 1
            project_id, settings, created_at, updated_at = params
            existing = self.scan_settings.get(project_id)
            self.scan_settings[project_id] = {
                "project_id": project_id,
                "settings": json.loads(settings),
                "created_at": (existing["created_at"] if existing else created_at),
                "updated_at": updated_at,
            }
            return 1
        return 1

    async def fetch_all(self, query, params=None):
        self.calls.append(("fetch_all", query, params))
        if (
            "FROM public.project_documents AS pd" in query
            and "ORDER BY pd.relative_path ASC" in query
        ):
            project_id, limit = params
            return [
                deepcopy(row)
                for row in sorted(
                    self.rows,
                    key=lambda row: (row["relative_path"], row["document_id"]),
                )
                if row["project_id"] == project_id
                and row["status"] != "deleted"
            ][:limit]
        if query.lstrip().startswith("DELETE FROM public.project_documents"):
            if self.delete_error is not None:
                raise self.delete_error
            document_id, project_id = params
            row = next(
                (
                    row
                    for row in self.rows
                    if row["document_id"] == document_id
                    and self._visible(row, project_id)
                ),
                None,
            )
            if row is None:
                return []
            self.rows.remove(row)
            self.chunks = [
                chunk for chunk in self.chunks if chunk["document_id"] != document_id
            ]
            self.contents.pop(document_id, None)
            return [dict(row)]
        if (
            query.lstrip().startswith("UPDATE public.project_documents")
            and "document_id = ANY(%s)" in query
            and "status = 'indexing'" in query
        ):
            updated_at, project_id, document_ids = params
            updated = []
            for row in self.rows:
                if (
                    row["project_id"] == project_id
                    and row["document_id"] in document_ids
                    and row["status"] == "indexing"
                ):
                    row.update(
                        {
                            "status": "queued",
                            "indexed_at": None,
                            "error_message": None,
                            "updated_at": updated_at,
                        }
                    )
                    updated.append({"document_id": row["document_id"]})
            return updated
        if (
            query.lstrip().startswith("UPDATE public.project_documents")
            and "status = 'indexing'" in query
        ):
            updated_at, project_id = params
            updated = []
            for row in self.rows:
                if row["project_id"] == project_id and row["status"] == "indexing":
                    row["status"] = "queued"
                    row["updated_at"] = updated_at
                    updated.append({"document_id": row["document_id"]})
            return updated

        if "COUNT(*)::INTEGER AS count" in query and "status = 'queued'" in query:
            project_id = params[0]
            return [
                {
                    "count": sum(
                        row["project_id"] == project_id and row["status"] == "queued"
                        for row in self.rows
                    )
                }
            ]

        if (
            "FROM public.project_documents" in query
            and "status = 'queued'" in query
            and "LIMIT %s" in query
            and "pd." not in query
        ):
            project_id, limit = params
            return [
                deepcopy(row)
                for row in self.rows
                if row["project_id"] == project_id
                and row["status"] == "queued"
            ][:limit]
        if "FROM public.project_document_scan_settings" in query:
            row = self.scan_settings.get(params[0])
            return [deepcopy(row)] if row else []
        if "dc.extracted_text" in query:
            document_id, content_hash, readable_project_ids = params
            document = next(
                (
                    row
                    for row in self.rows
                    if row["document_id"] == document_id
                    and self._read_visible(
                        row,
                        readable_project_ids,
                    )
                ),
                None,
            )
            if document is None:
                return []
            cached = self.extracted_text.get(document_id)
            if cached is None or cached[0] != content_hash:
                return []
            return [{"extracted_text": cached[1]}]
        if "FROM public.document_content" in query:
            document_id, readable_project_ids = params
            document = next(
                (
                    row
                    for row in self.rows
                    if row["document_id"] == document_id
                    and self._read_visible(
                        row,
                        readable_project_ids,
                    )
                ),
                None,
            )
            if document is None:
                return []
            raw = self.contents.get(document_id)
            if raw is None:
                return []
            return [{"content": raw}]
        if (
            "FROM public.document_chunks AS dc" in query
            and "JOIN public.project_documents AS pd" in query
        ):
            return deepcopy(self.search_results)
        if "LIMIT 2" in query and (
            "pd.document_id = %s" in query or "pd.relative_path = %s" in query
        ):
            selector_value, readable_project_ids = params
            selector_key = (
                "document_id" if "pd.document_id = %s" in query else "relative_path"
            )
            results = []
            for row in reversed(self.rows):
                if row[selector_key] == selector_value and self._read_visible(
                    row,
                    readable_project_ids,
                ):
                    result = dict(row)
                    result["chunk_count"] = sum(
                        chunk["document_id"] == row["document_id"]
                        for chunk in self.chunks
                    )
                    results.append(result)
            return results[:2]

        readable_project_ids, *filters = params
        path_prefix = None
        if "pd.relative_path LIKE %s" in query:
            path_prefix = filters.pop(0)
            filters.pop(0)
        rows = [
            row
            for row in self.rows
            if self._read_visible(
                row,
                readable_project_ids,
            )
            and (
                path_prefix is None
                or row["relative_path"] == path_prefix
                or row["relative_path"].startswith(f"{path_prefix}/")
            )
        ]
        results = []
        for row in reversed(rows):
            result = dict(row)
            result["chunk_count"] = sum(
                chunk["document_id"] == row["document_id"] for chunk in self.chunks
            )
            results.append(result)
        return results


class MemoryCopy:
    def __init__(self, postgres):
        self.postgres = postgres

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def write_row(self, row):
        if (
            self.postgres.transaction_error_at_chunk is not None
            and len(self.postgres.chunks) == self.postgres.transaction_error_at_chunk
        ):
            raise RuntimeError("chunk insert failed")
        (
            chunk_id,
            document_id,
            chunk_index,
            content,
            relative_path,
            embedding,
            language,
            chunk_kind,
            symbol_name,
            page_number,
            start_line,
            end_line,
            start_row,
            end_row,
            section_path,
            start_paragraph,
            end_paragraph,
        ) = row
        self.postgres.chunks.append(
            {
                "chunk_id": chunk_id,
                "document_id": document_id,
                "chunk_index": chunk_index,
                "content": content,
                "relative_path": relative_path,
                "embedding": embedding,
                "language": language,
                "chunk_kind": chunk_kind,
                "symbol_name": symbol_name,
                "page_number": page_number,
                "start_line": start_line,
                "end_line": end_line,
                "start_row": start_row,
                "end_row": end_row,
                "section_path": section_path,
                "start_paragraph": start_paragraph,
                "end_paragraph": end_paragraph,
            }
        )


class MemoryCursor:
    def __init__(self, postgres):
        self.postgres = postgres
        self.result = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def copy(self, query):
        self.postgres.calls.append(("cursor.copy", query, None))
        return MemoryCopy(self.postgres)

    async def execute(self, query, params=None):
        self.postgres.calls.append(("cursor.execute", query, params))
        if self.postgres.write_error is not None:
            raise self.postgres.write_error
        normalized = " ".join(query.split())


        if (
            normalized.startswith("SELECT document_id, content_hash")
            and "document_id = ANY(%s)" in normalized
            and "FOR UPDATE" in normalized
        ):
            project_id, document_ids = params
            self.result = [
                {
                    "document_id": row["document_id"],
                    "content_hash": row["content_hash"],
                }
                for row in self.postgres.rows
                if row["project_id"] == project_id
                and row["document_id"] in document_ids
            ]
            return


        if normalized.startswith("SELECT") and "FOR UPDATE" in normalized:
            document_id, project_id = params
            row = next(
                (
                    row
                    for row in self.postgres.rows
                    if row["document_id"] == document_id
                    and self.postgres._visible(row, project_id)
                ),
                None,
            )
            if row is None:
                self.result = None
            elif "SELECT status FROM" in normalized:
                self.result = {"status": row["status"]}
            else:
                self.result = dict(row)
                self.result["chunk_count"] = sum(
                    chunk["document_id"] == document_id
                    for chunk in self.postgres.chunks
                )
            return

        if (
            normalized.startswith("UPDATE public.project_documents")
            and "SET status = %s" in normalized
        ):
            status, updated_at, document_id, project_id, allowed = params
            row = next(
                (
                    row
                    for row in self.postgres.rows
                    if row["document_id"] == document_id
                    and self.postgres._visible(row, project_id)
                    and row["status"] in allowed
                ),
                None,
            )
            if row is None:
                self.result = None
            else:
                row.update(
                    {
                        "status": status,
                        "indexed_at": None,
                        "error_message": None,
                        "updated_at": updated_at,
                    }
                )
                self.result = dict(row)
            return

        if (
            normalized.startswith("UPDATE public.project_documents")
            and "SET status = 'deleted'" in normalized
        ):
            if self.postgres.delete_error is not None:
                raise self.postgres.delete_error
            document_id, project_id = params
            row = next(
                (
                    row
                    for row in self.postgres.rows
                    if row["document_id"] == document_id
                    and row["project_id"] == project_id
                    and row["status"] != "deleted"
                    and self.postgres._visible(row, project_id)
                ),
                None,
            )
            if row is None:
                self.result = None
                return
            row.update(
                {
                    "status": "deleted",
                    "deleted_at": row.get("deleted_at") or "deleted-now",
                    "indexed_at": None,
                    "error_message": None,
                    "updated_at": "deleted-now",
                }
            )
            self.result = dict(row)
            return

        if normalized.startswith("DELETE FROM public.document_chunks"):
            document_ids = params[0]
            if not isinstance(document_ids, list):
                document_ids = [document_ids]
            self.postgres.chunks = [
                chunk
                for chunk in self.postgres.chunks
                if chunk["document_id"] not in document_ids
            ]
            self.result = None
            return

        if normalized.startswith("DELETE FROM public.document_content"):
            document_id = params[0]
            self.postgres.contents.pop(document_id, None)
            self.postgres.extracted_text.pop(document_id, None)
            self.result = None
            return


        if normalized.startswith("DELETE FROM public.project_documents"):
            if self.postgres.delete_error is not None:
                raise self.postgres.delete_error
            document_id, project_id = params
            row = next(
                (
                    row
                    for row in self.postgres.rows
                    if row["document_id"] == document_id
                    and row["project_id"] == project_id
                    and self.postgres._visible(row, project_id)
                ),
                None,
            )
            if row is None:
                self.result = None
                return
            self.postgres.rows.remove(row)
            self.postgres.chunks = [
                chunk
                for chunk in self.postgres.chunks
                if chunk["document_id"] != document_id
            ]
            self.postgres.contents.pop(document_id, None)
            self.postgres.extracted_text.pop(document_id, None)
            self.result = dict(row)
            return

        if normalized.startswith("INSERT INTO public.document_content"):
            document_id, content, *derived = params
            self.postgres.contents[document_id] = bytes(content)
            if derived:
                self.postgres.extracted_text[document_id] = (
                    derived[1],
                    derived[0],
                )
            else:
                self.postgres.extracted_text.pop(document_id, None)
            self.result = None
            return

        if normalized.startswith("UPDATE public.document_content"):
            extracted_text, content_hash, document_id = params
            self.postgres.extracted_text[document_id] = (
                content_hash,
                extracted_text,
            )
            self.result = None
            return


        if normalized.startswith("INSERT INTO public.project_documents"):
            if "'queued'" in normalized:
                (
                    document_id,
                    project_id,
                    original_name,
                    relative_path,
                    extension,
                    size_bytes,
                    content_hash,
                    created_at,
                    updated_at,
                ) = params
                self.postgres.rows.append(
                    {
                        "document_id": document_id,
                        "project_id": project_id,
                        "original_name": original_name,
                        "relative_path": relative_path,
                        "extension": extension,
                        "size_bytes": size_bytes,
                        "content_hash": content_hash,
                        "status": "queued",
                        "deleted_at": None,
                        "indexed_at": None,
                        "error_message": None,
                        "created_at": created_at,
                        "updated_at": updated_at,
                        "chunk_count": 0,
                    }
                )
                self.result = None
                return

        if "SET status = 'indexed'" in normalized and "ANY(%s)" in normalized:
            indexed_at, updated_at, document_ids = params
            updated = []
            for row in self.postgres.rows:
                if row["document_id"] in document_ids:
                    row.update(
                        {
                            "status": "indexed",
                            "indexed_at": indexed_at,
                            "error_message": None,
                            "updated_at": updated_at,
                        }
                    )
                    updated.append({"document_id": row["document_id"]})
            self.result = updated
            return

        if "SET status = 'indexed'" in normalized:
            indexed_at, updated_at, document_id = params
            row = next(
                row for row in self.postgres.rows if row["document_id"] == document_id
            )
            row.update(
                {
                    "status": "indexed",
                    "indexed_at": indexed_at,
                    "error_message": None,
                    "updated_at": updated_at,
                }
            )
            self.result = dict(row)
            return

        if "SET status = 'failed'" in normalized:
            error_message, updated_at, document_id = params
            row = next(
                row for row in self.postgres.rows if row["document_id"] == document_id
            )
            if row["status"] != "indexed":
                row.update(
                    {
                        "status": "failed",
                        "indexed_at": None,
                        "error_message": error_message,
                        "updated_at": updated_at,
                    }
                )
            self.result = None
            return

        raise AssertionError(f"Unexpected transaction query: {normalized}")

    async def fetchone(self):
        return self.result

    async def fetchall(self):
        return self.result or []


class MemoryTransaction:
    def __init__(self, postgres):
        self.postgres = postgres

    async def __aenter__(self):
        self.rows = deepcopy(self.postgres.rows)
        self.chunks = deepcopy(self.postgres.chunks)
        self.contents = deepcopy(self.postgres.contents)
        self.extracted_text = deepcopy(self.postgres.extracted_text)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is not None or self.postgres.transaction_commit_error:
            self.postgres.rows = self.rows
            self.postgres.chunks = self.chunks
            self.postgres.contents = self.contents
            self.postgres.extracted_text = self.extracted_text
        if exc_type is None and self.postgres.transaction_commit_error:
            raise self.postgres.transaction_commit_error
        return False


class FakeEmbeddingService:
    def __init__(self):
        self.calls = []
        self.single_calls = []
        self.embeddings = None
        self.single_embedding = [0.1] * 1024
        self.rerank_calls = []

    async def encode(self, values):
        self.calls.append(list(values))
        if self.embeddings is not None:
            return self.embeddings
        return [[0.1] * 1024 for _ in values]

    async def encode_single(self, value):
        self.single_calls.append(value)
        return self.single_embedding

    async def rerank(self, query, candidates):
        self.rerank_calls.append((query, list(candidates)))
        return list(range(len(candidates)))


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("extension", "source", "expected_symbols", "expected_ranges"),
    [
        (
            ".py",
            "@tracked\ndef build_index():\n    return 1\n",
            ["build_index"],
            [(1, 3)],
        ),
        (
            ".ts",
            "export interface SearchResult {}\n\nexport function search() {}\n",
            ["SearchResult", "search"],
            [(1, 2), (3, 3)],
        ),
        (
            ".rs",
            "pub struct SearchIndex;\n\npub fn rebuild() {}\n",
            ["SearchIndex", "rebuild"],
            [(1, 2), (3, 3)],
        ),
        (
            ".go",
            "type Index struct {}\n\nfunc Build() {}\n",
            ["Index", "Build"],
            [(1, 2), (3, 3)],
        ),
        (
            ".java",
            "public class Index {}\n\ninterface Searchable {}\n",
            ["Index", "Searchable"],
            [(1, 2), (3, 3)],
        ),
        (
            ".c",
            "struct Index {};\n\nvoid build(void) {}\n",
            ["Index", "build"],
            [(1, 2), (3, 3)],
        ),
        (
            ".cpp",
            "class Index {};\n\nvoid build() {}\n",
            ["Index", "build"],
            [(1, 2), (3, 3)],
        ),
        (
            ".cs",
            "public class Index {}\n\npublic interface Searchable {}\n",
            ["Index", "Searchable"],
            [(1, 2), (3, 3)],
        ),
        (
            ".sh",
            "build() { echo ok; }\n",
            ["build"],
            [(1, 1)],
        ),
        (
            ".sql",
            "CREATE TABLE indexes (id INT);\n\nSELECT * FROM indexes;\n",
            ["indexes", "indexes"],
            [(1, 2), (3, 3)],
        ),
        (
            ".yaml",
            "services:\n  api:\n    image: x\nversion: '3'\n",
            ["services", "version"],
            [(1, 3), (4, 4)],
        ),
        (
            ".dockerfile",
            "FROM python:3.12\nRUN echo ok\n",
            ["FROM", "RUN"],
            [(1, 1), (2, 2)],
        ),
    ],
)
def test_tree_sitter_preserves_top_level_code_symbols(
    extension,
    source,
    expected_symbols,
    expected_ranges,
):
    chunks = storage_module.split_document(source, extension=extension)

    assert [chunk.symbol_name for chunk in chunks] == expected_symbols
    assert [(chunk.start_line, chunk.end_line) for chunk in chunks] == expected_ranges


@pytest.mark.unit
@pytest.mark.no_network
def test_tree_sitter_falls_back_to_regex_for_incomplete_python():
    chunks = storage_module.split_document("def unfinished(", extension=".py")

    assert len(chunks) == 1
    assert chunks[0].symbol_name == "unfinished"


@pytest.mark.unit
@pytest.mark.no_network
def test_pdf_extraction_splits_each_page_without_cross_page_chunks(monkeypatch):
    from types import SimpleNamespace

    pages = [
        SimpleNamespace(extract_text=lambda: "Page one only."),
        SimpleNamespace(extract_text=lambda: "Page two only."),
    ]
    monkeypatch.setattr(
        storage_module,
        "PdfReader",
        lambda _: SimpleNamespace(pages=pages),
    )

    extraction = storage_module.extract_and_split_document(b"pdf", ".pdf")

    assert extraction.text == "Page one only.\n\nPage two only."
    assert [(chunk.page_number, chunk.content) for chunk in extraction.chunks] == [
        (1, "Page one only."),
        (2, "Page two only."),
    ]


@pytest.mark.unit
@pytest.mark.no_network
def test_text_markdown_and_csv_chunks_have_reliable_locators():
    text_chunks = storage_module.split_document(
        "\nFirst line\nSecond line\n",
        extension=".txt",
    )
    markdown_chunks = storage_module.split_document(
        "# Overview\nIntroduction\n\n## Risks\nMitigation\n",
        extension=".md",
    )
    csv_chunks = storage_module.split_document(
        "name,value\nalpha,1\nbeta,2\n",
        extension=".csv",
    )

    assert [(chunk.start_line, chunk.end_line) for chunk in text_chunks] == [(2, 3)]
    assert [chunk.section_path for chunk in markdown_chunks] == [
        ("Overview",),
        ("Overview", "Risks"),
    ]
    assert [(chunk.start_line, chunk.end_line) for chunk in markdown_chunks] == [
        (1, 2),
        (4, 5),
    ]
    assert [(chunk.start_row, chunk.end_row) for chunk in csv_chunks] == [(1, 2)]
    assert csv_chunks[0].content == "name,value\nalpha,1\nbeta,2"


@pytest.mark.unit
@pytest.mark.no_network
def test_docx_chunks_preserve_paragraph_ranges_and_heading_paths(monkeypatch):
    from types import SimpleNamespace

    paragraphs = [
        SimpleNamespace(text="Overview", style=SimpleNamespace(name="Heading 1")),
        SimpleNamespace(text="The introduction.", style=SimpleNamespace(name="Normal")),
        SimpleNamespace(text="Risks", style=SimpleNamespace(name="Heading 2")),
        SimpleNamespace(
            text="Mitigate dependency risk.", style=SimpleNamespace(name="Normal")
        ),
    ]
    monkeypatch.setattr(
        storage_module,
        "DocxDocument",
        lambda _: SimpleNamespace(paragraphs=paragraphs),
    )

    extraction = storage_module.extract_and_split_document(b"docx", ".docx")

    assert [
        (chunk.start_paragraph, chunk.end_paragraph, chunk.section_path)
        for chunk in extraction.chunks
    ] == [
        (1, 2, ("Overview",)),
        (3, 4, ("Overview", "Risks")),
    ]


@pytest.mark.unit
def test_document_extension_recognizes_extensionless_container_files():
    assert document_extension("Dockerfile") == ".dockerfile"
    assert document_extension("containers/Containerfile") == ".dockerfile"
    assert document_extension("src/index.py") == ".py"


@pytest.mark.unit
@pytest.mark.no_network
def test_notebook_cells_become_retrievable_chunks():
    notebook = {
        "cells": [
            {"cell_type": "markdown", "source": ["# Overview\n", "Notes"]},
            {"cell_type": "code", "source": "print('hello')\n"},
            {"cell_type": "raw", "source": "ignored"},
        ]
    }

    text = storage_module.extract_text(
        json.dumps(notebook).encode(),
        ".ipynb",
    )
    chunks = storage_module.split_document(text, extension=".ipynb")

    assert [
        (chunk.chunk_kind, chunk.symbol_name, chunk.content) for chunk in chunks
    ] == [
        ("notebook_markdown", "cell 1", "# Overview\nNotes"),
        ("notebook_code", "cell 2", "print('hello')"),
    ]


@pytest.fixture
def document_harness(tmp_path):
    postgres = MemoryPostgres()
    embedding = FakeEmbeddingService()

    async def run_inline(function, *args, **kwargs):
        return function(*args, **kwargs)

    service = DocumentService(
        project_id="project-1",
        postgres_client=postgres,
        embedding_service=embedding,
        blocking_runner=run_inline,
        document_rerank_enabled=False,
        filesystem_factory=ProjectFilesystemFactory(tmp_path / "projects"),
    )
    return service, postgres


@pytest.mark.storage
@pytest.mark.no_network
async def test_add_document_stores_bytes_and_persists_metadata(document_harness):
    service, postgres = document_harness
    content = b"alpha beta gamma"

    metadata = await service.add_document(
        content=content,
        original_name="Notes.MD",
        relative_path=r"docs\Notes.MD",
    )

    assert metadata["project_id"] == "project-1"
    assert metadata["relative_path"] == "docs/Notes.MD"
    assert metadata["extension"] == ".md"
    assert metadata["size_bytes"] == len(content)
    assert metadata["content_hash"] == hashlib.sha256(content).hexdigest()
    assert metadata["status"] == "queued"
    assert metadata["chunk_count"] == 0
    assert "storage_key" not in metadata

    assert postgres.contents[metadata["document_id"]] == content
    assert postgres.rows[0]["original_name"] == "Notes.MD"
    assert postgres.transaction_count == 1


@pytest.mark.storage
@pytest.mark.no_network
async def test_database_failure_leaves_no_content_or_metadata(document_harness):
    service, postgres = document_harness
    postgres.write_error = RuntimeError("insert failed")

    with pytest.raises(RuntimeError, match="insert failed"):
        await service.add_document(content=b"alpha", original_name="notes.md")

    assert postgres.rows == []
    assert postgres.contents == {}
    assert list(service._filesystem.iter_files()) == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_manual_project_documents_read_and_index_from_the_local_file(
    document_harness,
):
    service, postgres = document_harness

    document = await service.add_document(
        content=b"current local text",
        original_name="notes.md",
    )
    postgres.contents[document["document_id"]] = b"stale database text"

    read = await service.read_document(document_id=document["document_id"])
    indexed = await service.index_document(document_id=document["document_id"])

    assert read["content"] == "1: current local text"
    assert indexed["status"] == "indexed"
    assert postgres.chunks[0]["content"] == "current local text"


@pytest.mark.storage
@pytest.mark.no_network
async def test_reconciliation_catalogs_local_changes_and_tombstones_missing_files(
    document_harness,
):
    service, postgres = document_harness
    filesystem = service._filesystem
    assert filesystem is not None
    filesystem.write_bytes("external.md", b"first external version")

    created = await service.reconcile_project_files()
    current = next(row for row in postgres.rows if row["relative_path"] == "external.md")

    filesystem.write_bytes("external.md", b"second external version", overwrite=True)
    changed = await service.reconcile_project_files()
    filesystem.delete_file("external.md")
    deleted = await service.reconcile_project_files()

    assert created["created"] == 1
    assert changed["changed"] == 1
    assert deleted["deleted"] == 1
    assert current["status"] == "deleted"


@pytest.mark.storage
@pytest.mark.no_network
async def test_native_project_file_operations_reconcile_the_document_catalog(
    document_harness,
):
    service, postgres = document_harness

    created = await service.create_project_file("notes/draft.md", "one\n")
    listed = await service.list_project_files(path_prefix="notes")
    updated = await service.update_project_file(
        "notes/draft.md",
        "two\n",
        expected_content_hash=created["content_hash"],
    )
    appended = await service.append_project_file(
        "notes/draft.md",
        "three\n",
        expected_content_hash=updated["content_hash"],
    )
    moved = await service.move_project_file(
        "notes/draft.md",
        "notes/archive.md",
        expected_content_hash=appended["content_hash"],
    )

    assert [entry["relative_path"] for entry in listed] == ["notes/draft.md"]
    assert moved["relative_path"] == "notes/archive.md"
    assert moved["content"] == "two\nthree\n"
    assert await service.create_project_folder("notes/empty") == {
        "relative_path": "notes/empty"
    }

    deleted = await service.delete_project_file(
        "notes/archive.md",
        expected_content_hash=moved["content_hash"],
    )
    assert deleted["deleted"] is True
    assert any(
        row["relative_path"] == "notes/draft.md" and row["status"] == "deleted"
        for row in postgres.rows
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_indexer_reconciles_project_files_when_the_runtime_starts(
    document_harness,
):
    service, postgres = document_harness
    filesystem = service._filesystem
    assert filesystem is not None
    filesystem.write_bytes("startup.md", b"discover this at startup")

    await service.indexer.start()
    try:
        assert any(row["relative_path"] == "startup.md" for row in postgres.rows)
        assert service.indexer._reconciliation_task is not None
    finally:
        await service.indexer.shutdown()


@pytest.mark.storage
@pytest.mark.no_network
async def test_document_listing_is_scoped_to_the_active_project():
    postgres = MemoryPostgres()
    embedding = FakeEmbeddingService()
    project_one = DocumentService("project-1", postgres, embedding)
    project_two = DocumentService("project-2", postgres, embedding)

    project_file = await project_one.add_document(
        content=b"project",
        original_name="project.md",
    )
    second_project_one_file = await project_one.add_document(
        content=b"one",
        original_name="one.md",
    )
    await project_one.add_document(
        content=b"two",
        original_name="two.md",
    )
    await project_two.add_document(
        content=b"other project",
        original_name="other.md",
    )

    visible = await project_one.list_documents()

    assert {row["document_id"] for row in visible} == {
        project_file["document_id"],
        second_project_one_file["document_id"],
        next(row["document_id"] for row in postgres.rows if row["original_name"] == "two.md"),
    }
    assert all(row["project_id"] == "project-1" for row in visible)
    assert all("storage_key" not in row for row in visible)


@pytest.mark.storage
@pytest.mark.no_network
async def test_repeated_uploads_reject_a_live_path_collision(document_harness):
    service, postgres = document_harness

    first = await service.add_document(
        content=b"same",
        original_name="notes.md",
        relative_path="docs/notes.md",
    )
    with pytest.raises(FileExistsError, match="already exists"):
        await service.add_document(
            content=b"same",
            original_name="notes.md",
            relative_path="docs/notes.md",
        )

    assert first["content_hash"] == hashlib.sha256(b"same").hexdigest()
    assert len(postgres.rows) == 1


@pytest.mark.storage
@pytest.mark.no_network
@pytest.mark.parametrize(
    "relative_path",
    [
        "../secret.txt",
        "docs/../../secret.txt",
        "/absolute/path.txt",
        r"C:\absolute\path.txt",
        "\x00bad.txt",
    ],
)
async def test_add_document_rejects_unsafe_relative_paths(
    document_harness, relative_path
):
    service, postgres = document_harness

    with pytest.raises(ValueError):
        await service.add_document(
            content=b"alpha",
            original_name="notes.md",
            relative_path=relative_path,
        )

    assert postgres.rows == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_add_document_rejects_invalid_content_scope_and_size(
    monkeypatch, document_harness
):
    service, postgres = document_harness

    with pytest.raises(ValueError, match="must not be empty"):
        await service.add_document(content=b"", original_name="notes.md")
    with pytest.raises(ValueError, match="Unsupported file type"):
        await service.add_document(content=b"data", original_name="video.mp4")
    with pytest.raises(ValueError, match="Unsupported file type"):
        await service.add_document(content=b"data", original_name="archive.zip")

    monkeypatch.setattr(
        "core.knowledge.documents.service.MAX_DOCUMENT_SIZE",
        3,
    )
    with pytest.raises(ValueError, match="50 MB"):
        await service.add_document(content=b"four", original_name="notes.md")

    assert postgres.rows == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_list_documents_normalizes_database_timestamps(document_harness):
    service, postgres = document_harness
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)
    postgres.rows.append(
        {
            "document_id": "a785ecfe-b738-4a43-9e6d-bbdc3f831b20",
            "project_id": "project-1",
            "original_name": "notes.md",
            "relative_path": "notes.md",
            "extension": ".md",
            "size_bytes": 5,
            "content_hash": "hash",
            "status": "queued",
            "created_at": timestamp,
            "updated_at": timestamp,
            "chunk_count": 0,
        }
    )

    files = await service.list_documents()

    assert files[0]["created_at"] == timestamp.isoformat()
    assert files[0]["updated_at"] == timestamp.isoformat()
    assert "storage_key" not in files[0]


@pytest.mark.storage
@pytest.mark.no_network
async def test_get_document_info_resolves_visible_document_without_storage_key(
    document_harness,
):
    service, _ = document_harness
    uploaded = await service.add_document(
        content=b"alpha\nbeta",
        original_name="notes.txt",
        relative_path="docs/notes.txt",
    )

    info = await service.get_document_info(document_id=uploaded["document_id"])

    assert info["document_id"] == uploaded["document_id"]
    assert info["relative_path"] == "docs/notes.txt"
    assert info["status"] == "queued"
    assert "storage_key" not in info


@pytest.mark.storage
@pytest.mark.no_network
async def test_get_document_info_enforces_reference_rules():
    postgres = MemoryPostgres()
    embedding = FakeEmbeddingService()
    service = DocumentService("project-1", postgres, embedding)
    uploaded = await service.add_document(
        content=b"private",
        original_name="private.txt",
    )

    with pytest.raises(ValueError, match="exactly one"):
        await service.get_document_info()
    with pytest.raises(ValueError, match="exactly one"):
        await service.get_document_info(
            document_id=uploaded["document_id"],
            relative_path="private.txt",
        )
    info = await service.get_document_info(
        document_id=uploaded["document_id"],
    )
    assert info["document_id"] == uploaded["document_id"]


@pytest.mark.storage
@pytest.mark.no_network
async def test_relative_path_lookup_is_unambiguous_within_the_project_tree(
    document_harness,
):
    service, _ = document_harness
    first = await service.add_document(
        content=b"first",
        original_name="notes.txt",
        relative_path="docs/notes.txt",
    )
    second = await service.add_document(
        content=b"second",
        original_name="notes.txt",
        relative_path="archive/notes.txt",
    )

    assert (await service.get_document_info(relative_path="docs/notes.txt"))["document_id"] == first["document_id"]
    assert (await service.get_document_info(relative_path="archive/notes.txt"))["document_id"] == second["document_id"]


@pytest.mark.storage
@pytest.mark.no_network
async def test_read_document_returns_bounded_numbered_lines(document_harness):
    service, _ = document_harness
    uploaded = await service.add_document(
        content=b"first\nsecond\nthird\nfourth",
        original_name="notes.txt",
    )

    result = await service.read_document(
        document_id=uploaded["document_id"],
        start_line=2,
        end_line=3,
    )

    assert result["content"] == "2: second\n3: third"
    assert result["document_name"] == "notes.txt"
    assert result["chunk_index"] == "lines:2-3"
    assert result["start_line"] == 2
    assert result["end_line"] == 3
    assert result["total_lines"] == 4
    assert result["truncated"] is True
    assert "storage_key" not in result


@pytest.mark.storage
@pytest.mark.no_network
async def test_document_selection_resolves_current_server_canonical_code_passage(
    document_harness,
):
    service, _ = document_harness
    uploaded = await service.add_document(
        content=b"one\ndef useful():\n    return 42\n",
        original_name="notes.py",
    )

    selection = await service.resolve_document_selection(
        document_id=uploaded["document_id"],
        selection=DocumentSelection(
            content_hash=uploaded["content_hash"],
            locator={
                "kind": "code_lines",
                "start_line": 2,
                "end_line": 3,
                "symbol_name": "client-must-not-control-this",
            },
        ),
    )

    assert selection["content"] == "2: def useful():\n3:     return 42"
    assert selection["locator"] == {
        "kind": "code_lines",
        "start_line": 2,
        "end_line": 3,
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_document_selection_rejects_stale_or_incompatible_coordinates(
    document_harness,
):
    service, _ = document_harness
    uploaded = await service.add_document(
        content=b"first\nsecond\nthird",
        original_name="notes.txt",
    )

    with pytest.raises(ValueError, match="stale"):
        await service.resolve_document_selection(
            document_id=uploaded["document_id"],
            selection=DocumentSelection(
                content_hash="a" * 64,
                locator={"kind": "text_lines", "start_line": 1, "end_line": 1},
            ),
        )
    with pytest.raises(ValueError, match="Code line selections"):
        await service.resolve_document_selection(
            document_id=uploaded["document_id"],
            selection=DocumentSelection(
                content_hash=uploaded["content_hash"],
                locator={"kind": "code_lines", "start_line": 1, "end_line": 1},
            ),
        )
    with pytest.raises(ValueError, match="outside the current document range"):
        await service.resolve_document_selection(
            document_id=uploaded["document_id"],
            selection=DocumentSelection(
                content_hash=uploaded["content_hash"],
                locator={"kind": "text_lines", "start_line": 2, "end_line": 4},
            ),
        )


@pytest.mark.storage
@pytest.mark.no_network
async def test_read_document_uses_persisted_extracted_text_after_index(
    monkeypatch,
    document_harness,
):
    service, postgres = document_harness

    uploaded = await service.add_document(
        content=b"first\nsecond\nthird",
        original_name="notes.txt",
    )
    await service.index_document(document_id=uploaded["document_id"])

    assert postgres.extracted_text[uploaded["document_id"]][1] == (
        "first\nsecond\nthird"
    )

    def fail_if_reparsed(*args, **kwargs):
        raise AssertionError("read_document reparsed the original document")

    monkeypatch.setattr(
        "core.knowledge.documents.service.extract_text",
        fail_if_reparsed,
    )
    result = await service.read_document(
        document_id=uploaded["document_id"],
        start_line=2,
        end_line=2,
    )

    assert result["content"] == "2: second"


@pytest.mark.storage
@pytest.mark.no_network
async def test_read_document_validates_ranges_and_character_limit(
    monkeypatch, document_harness
):
    service, _ = document_harness
    uploaded = await service.add_document(
        content=b"0123456789\nsecond",
        original_name="notes.txt",
    )

    with pytest.raises(ValueError, match="positive integer"):
        await service.read_document(document_id=uploaded["document_id"], start_line=0)
    with pytest.raises(ValueError, match="at least start_line"):
        await service.read_document(
            document_id=uploaded["document_id"],
            start_line=2,
            end_line=1,
        )
    with pytest.raises(ValueError, match="limited to 200 lines"):
        await service.read_document(
            document_id=uploaded["document_id"],
            start_line=1,
            end_line=201,
        )
    with pytest.raises(ValueError, match="exceeds document length"):
        await service.read_document(document_id=uploaded["document_id"], start_line=3)

    monkeypatch.setattr(
        "core.knowledge.documents.service.MAX_READ_CHARACTERS",
        8,
    )
    result = await service.read_document(
        document_id=uploaded["document_id"], end_line=1
    )
    assert result["content"] == "1: 01234"
    assert result["truncated"] is True


@pytest.mark.storage
@pytest.mark.no_network
async def test_delete_document_tombstones_metadata_and_removes_chunks_and_bytes(
    monkeypatch, document_harness
):
    service, postgres = document_harness

    class OneChunkSplitter:
        def __init__(self, **kwargs):
            pass

        def split_text(self, text):
            return [text]

    monkeypatch.setattr(storage_module, "SentenceSplitter", OneChunkSplitter)
    first = await service.add_document(
        content=b"same content",
        original_name="notes.txt",
    )
    second = await service.add_document(
        content=b"same content",
        original_name="copy.txt",
        relative_path="archive/copy.txt",
    )
    await service.index_document(document_id=first["document_id"])

    deleted = await service.delete_document(document_id=first["document_id"])

    assert deleted["document_id"] == first["document_id"]
    assert deleted["deleted"] is True
    assert "storage_key" not in deleted
    assert first["document_id"] not in postgres.contents
    assert postgres.contents.get(second["document_id"]) == b"same content"
    assert not service._filesystem.root.joinpath("notes.txt").exists()
    tombstone = next(
        row for row in postgres.rows if row["document_id"] == first["document_id"]
    )
    assert tombstone["status"] == "deleted"
    assert tombstone["deleted_at"]
    assert tombstone["content_hash"] == first["content_hash"]
    assert second["document_id"] in [row["document_id"] for row in postgres.rows]
    assert all(
        chunk["document_id"] != first["document_id"] for chunk in postgres.chunks
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_delete_document_enforces_project_ownership():
    postgres = MemoryPostgres()
    embedding = FakeEmbeddingService()
    project_one = DocumentService("project-1", postgres, embedding)
    project_two = DocumentService("project-2", postgres, embedding)
    uploaded = await project_one.add_document(
        content=b"private",
        original_name="private.txt",
    )

    with pytest.raises(FileNotFoundError, match="Document not found"):
        await project_two.delete_document(
            document_id=uploaded["document_id"],
        )

    assert len(postgres.rows) == 1
    deleted = await project_one.delete_document(
        document_id=uploaded["document_id"],
    )
    assert deleted["deleted"] is True
    assert postgres.rows[0]["status"] == "deleted"


@pytest.mark.storage
@pytest.mark.no_network
async def test_reupload_after_delete_creates_a_new_independent_document(
    document_harness,
):
    service, postgres = document_harness
    original = await service.add_document(
        content=b"first version",
        original_name="notes.txt",
    )
    await service.delete_document(document_id=original["document_id"])

    replacement = await service.add_document(
        content=b"second version",
        original_name="notes.txt",
    )
    assert replacement["document_id"] != original["document_id"]
    assert replacement["status"] == "queued"
    assert len(postgres.rows) == 2


@pytest.mark.storage
@pytest.mark.no_network
async def test_indexer_recovery_requeues_interrupted_document_indexing(document_harness):
    service, postgres = document_harness
    await service.add_document(
        content=b"alpha beta gamma",
        original_name="notes.md",
    )
    postgres.rows[0]["status"] = "indexing"

    recovered = await service.indexer.recover_pending_indexes()

    assert recovered == 1
    assert postgres.rows[0]["status"] == "indexed"
    assert service.indexer.indexing_snapshot()["last_recovery_requeued"] == 1


@pytest.mark.storage
@pytest.mark.no_network
async def test_indexer_start_drains_more_than_one_recovery_batch(document_harness):
    service, postgres = document_harness
    for index in range(17):
        await service.add_document(
            content=f"document {index}".encode(),
            original_name=f"notes-{index}.md",
        )

    await service.indexer.start()
    for _ in range(100):
        if {row["status"] for row in postgres.rows} == {"indexed"}:
            break
        await asyncio.sleep(0.01)

    assert {row["status"] for row in postgres.rows} == {"indexed"}
    await service.shutdown()


@pytest.mark.storage
@pytest.mark.no_network
async def test_indexer_retries_document_admission_after_temporary_rejection(
    document_harness,
):
    class RejectOnceBackgroundWork:
        def __init__(self):
            self.calls = 0
            self.admitted = asyncio.Event()

        async def submit(self, project_id, operation, *, name, coalesce_key):
            self.calls += 1
            if self.calls == 1:
                raise BackgroundWorkRejected(
                    project_id=project_id,
                    name=name,
                    reason="global_queue_full",
                    limit=1,
                    queued=1,
                )
            self.admitted.set()
            return await operation()

    service, postgres = document_harness
    background_work = RejectOnceBackgroundWork()
    service.indexer._background_work = background_work
    service.indexer.update_policy(DocumentIndexPolicy.capture(inline_index_max_bytes=1))
    await service.indexer.start()
    document = await service.add_document(
        content=b"this document must be admitted in the background",
        original_name="notes.md",
    )

    await service.schedule_document_index(document_id=document["document_id"])
    await asyncio.wait_for(background_work.admitted.wait(), timeout=1)
    for _ in range(100):
        if postgres.rows[0]["status"] == "indexed":
            break
        await asyncio.sleep(0.01)

    assert background_work.calls >= 2
    assert postgres.rows[0]["status"] == "indexed"
    await service.shutdown()


@pytest.mark.storage
@pytest.mark.no_network
async def test_delete_document_rejects_empty_or_inaccessible_ids(document_harness):
    service, postgres = document_harness

    with pytest.raises(ValueError, match="must not be empty"):
        await service.delete_document(document_id=" ")
    with pytest.raises(FileNotFoundError, match="Document not found"):
        await service.delete_document(
            document_id="a785ecfe-b738-4a43-9e6d-bbdc3f831b20"
        )

    assert postgres.rows == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_document_service_search_remains_empty(document_harness):
    service, _ = document_harness

    assert await service.search("alpha") == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_document_service_search_embeds_query_with_project_scope(
    document_harness,
):
    service, postgres = document_harness
    document_id = UUID("a785ecfe-b738-4a43-9e6d-bbdc3f831b20")
    postgres.search_results = [
        {
            "document_id": document_id,
            "original_name": "notes.md",
            "relative_path": "docs/notes.md",
            "chunk_index": 2,
            "content": "Relevant content",
            "score": Decimal("0.875"),
        }
    ]

    results = await service.search(
        "  alpha question  ",
        n_results=3,
    )

    assert service._embedding.single_calls == ["alpha question"]
    assert results == [
        {
            "document_id": str(document_id),
            "document_name": "notes.md",
            "original_name": "notes.md",
            "relative_path": "docs/notes.md",
            "chunk_index": 2,
            "content": "Relevant content",
            "score": 0.875,
        }
    ]
    _, sql, params = postgres.calls[-1]
    assert "pd.project_id = ANY(%s)" in sql
    assert "pd.status = 'indexed'" in sql
    assert "websearch_to_tsquery('simple', %s)" in sql
    assert "ts_rank_cd(vc.search_vector, sq.terms)" in sql
    assert "1.0 / (60 + sc.semantic_rank)" in sql
    assert params[0:3] == (
        "alpha question",
        json.dumps([0.1] * 1024),
        ["project-1"],
    )
    assert params[-1] == 3


@pytest.mark.storage
@pytest.mark.no_network
async def test_document_service_optionally_reranks_hybrid_candidates():
    postgres = MemoryPostgres()
    embedding = FakeEmbeddingService()

    async def run_inline(function, *args, **kwargs):
        return function(*args, **kwargs)

    service = DocumentService(
        project_id="project-1",
        postgres_client=postgres,
        embedding_service=embedding,
        blocking_runner=run_inline,
        document_rerank_enabled=True,
        document_rerank_candidates=3,
    )
    postgres.search_results = [
        {
            "document_id": UUID(f"a785ecfe-b738-4a43-9e6d-bbdc3f831b2{index}"),
            "original_name": f"file-{index}.py",
            "relative_path": f"src/file-{index}.py",
            "chunk_index": 0,
            "content": f"candidate {index}",
            "symbol_name": "target" if index == 1 else None,
            "score": Decimal("0.5"),
        }
        for index in range(3)
    ]

    results = await service.search("target", n_results=2)

    assert [result["content"] for result in results] == [
        "candidate 2",
        "candidate 1",
    ]
    assert embedding.rerank_calls == [
        (
            "target",
            [
                "File: src/file-0.py\ncandidate 0",
                "File: src/file-1.py\nSymbol: target\ncandidate 1",
                "File: src/file-2.py\ncandidate 2",
            ],
        )
    ]
    assert postgres.calls[-1][2][-1] == 3


@pytest.mark.storage
@pytest.mark.no_network
async def test_document_service_search_applies_document_filter(document_harness):
    service, postgres = document_harness

    await service.search(
        "alpha",
        document_filter="a785ecfe-b738-4a43-9e6d-bbdc3f831b20",
    )

    _, sql, params = postgres.calls[-1]
    assert "AND pd.document_id = %s" in sql
    assert params[3] == "a785ecfe-b738-4a43-9e6d-bbdc3f831b20"


@pytest.mark.storage
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("query", "n_results", "error"),
    [
        ("", 5, "query must not be empty"),
        ("   ", 5, "query must not be empty"),
        ("alpha", 0, "between 1 and 50"),
        ("alpha", 51, "between 1 and 50"),
        ("alpha", True, "between 1 and 50"),
    ],
)
async def test_document_service_search_validates_inputs(
    document_harness, query, n_results, error
):
    service, postgres = document_harness

    with pytest.raises(ValueError, match=error):
        await service.search(query, n_results=n_results)

    assert service._embedding.single_calls == []
    assert not any(
        method == "fetch_all"
        and "FROM public.document_chunks AS dc" in sql
        and "JOIN public.project_documents AS pd" in sql
        for method, sql, _ in postgres.calls
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_document_service_search_validates_embedding_dimension(document_harness):
    service, postgres = document_harness
    service._embedding.single_embedding = [0.1] * 3

    with pytest.raises(ValueError, match="exactly 1024 dimensions"):
        await service.search("alpha")

    assert not any(
        method == "fetch_all"
        and "FROM public.document_chunks AS dc" in sql
        and "JOIN public.project_documents AS pd" in sql
        for method, sql, _ in postgres.calls
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_index_document_extracts_utf8_bom_and_persists_chunks(document_harness):
    service, postgres = document_harness
    uploaded = await service.add_document(
        content=b"\xef\xbb\xbfAlpha beta gamma.",
        original_name="notes.md",
    )

    indexed = await service.index_document(document_id=uploaded["document_id"])

    assert indexed["status"] == "indexed"
    assert indexed["indexed_at"] is not None
    assert indexed["error_message"] is None
    assert indexed["chunk_count"] == 1
    assert postgres.chunks[0]["chunk_index"] == 0
    assert postgres.chunks[0]["content"] == "Alpha beta gamma."
    assert service._embedding.calls == [["Alpha beta gamma."]]


@pytest.mark.storage
@pytest.mark.no_network
async def test_index_document_preserves_code_location_metadata(document_harness):
    service, postgres = document_harness
    uploaded = await service.add_document(
        content=(
            b"import os\n\n"
            b"def build_index(path):\n"
            b"    return path\n\n"
            b"class Searcher:\n"
            b"    pass\n"
        ),
        original_name="indexer.py",
        relative_path="src/indexer.py",
    )

    indexed = await service.index_document(document_id=uploaded["document_id"])

    assert indexed["chunk_count"] == 3
    assert [
        (
            chunk["relative_path"],
            chunk["language"],
            chunk["chunk_kind"],
            chunk["symbol_name"],
            chunk["start_line"],
            chunk["end_line"],
        )
        for chunk in postgres.chunks
    ] == [
        ("src/indexer.py", "python", "code", None, 1, 2),
        ("src/indexer.py", "python", "code", "build_index", 3, 5),
        ("src/indexer.py", "python", "code", "Searcher", 6, 7),
    ]
    assert service._embedding.calls == [
        [
            "File: src/indexer.py\nLanguage: python\n\nimport os",
            "File: src/indexer.py\nLanguage: python\nSymbol: build_index\n\ndef build_index(path):\n    return path",
            "File: src/indexer.py\nLanguage: python\nSymbol: Searcher\n\nclass Searcher:\n    pass",
        ]
    ]


@pytest.mark.storage
@pytest.mark.no_network
async def test_index_document_uses_its_captured_embedding_batch_policy(
    document_harness,
):
    service, _ = document_harness
    uploaded = await service.add_document(
        content=(
            b"import os\n\n"
            b"def build_index(path):\n"
            b"    return path\n\n"
            b"class Searcher:\n"
            b"    pass\n"
        ),
        original_name="indexer.py",
    )
    policy = DocumentIndexPolicy.capture(embedding_chunk_batch_size=1)

    await service.index_document(
        document_id=uploaded["document_id"],
        policy=policy,
    )

    assert len(service._embedding.calls) == 3
    assert all(len(call) == 1 for call in service._embedding.calls)


@pytest.mark.storage
@pytest.mark.no_network
async def test_cancelled_document_index_releases_its_durable_claim(document_harness):
    service, postgres = document_harness
    uploaded = await service.add_document(content=b"alpha", original_name="notes.md")
    started = asyncio.Event()

    async def wait_for_cancellation(*_args, **_kwargs):
        started.set()
        await asyncio.Event().wait()

    service.indexer._run_blocking = wait_for_cancellation
    task = asyncio.create_task(
        service.index_document(document_id=uploaded["document_id"])
    )
    await started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert postgres.rows[0]["status"] == "queued"
    assert postgres.rows[0]["error_message"] is None


@pytest.mark.storage
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("original_name", "content", "error"),
    [
        ("empty.txt", b" \n\t", "no extractable text"),
        ("binary.txt", b"alpha\x00beta", "binary content"),
        ("invalid.txt", b"\xff\xfe\xfa", "valid UTF-8"),
    ],
)
async def test_index_document_records_text_extraction_failures(
    document_harness, original_name, content, error
):
    service, postgres = document_harness
    uploaded = await service.add_document(content=content, original_name=original_name)

    with pytest.raises(RuntimeError, match=error):
        await service.index_document(document_id=uploaded["document_id"])

    row = postgres.rows[0]
    assert row["status"] == "failed"
    assert error in row["error_message"]
    assert row["indexed_at"] is None
    assert postgres.chunks == []


@pytest.mark.storage
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("extension", "expected_chunks"),
    [
        (".pdf", ["First page", "Second page"]),
        (".docx", ["Document text"]),
    ],
)
async def test_index_document_extracts_supported_documents(
    monkeypatch, document_harness, extension, expected_chunks
):
    from types import SimpleNamespace

    service, postgres = document_harness
    if extension == ".pdf":
        pages = [
            SimpleNamespace(extract_text=lambda: "First page"),
            SimpleNamespace(extract_text=lambda: "Second page"),
        ]
        monkeypatch.setattr(
            storage_module,
            "PdfReader",
            lambda buf: SimpleNamespace(pages=pages),
        )
    else:
        paragraphs = [
            SimpleNamespace(text="Document text", style=SimpleNamespace(name="Normal"))
        ]
        monkeypatch.setattr(
            storage_module,
            "DocxDocument",
            lambda buf: SimpleNamespace(paragraphs=paragraphs),
        )

    uploaded = await service.add_document(
        content=b"managed document bytes",
        original_name=f"notes{extension}",
    )
    await service.index_document(document_id=uploaded["document_id"])

    assert [chunk["content"] for chunk in postgres.chunks] == expected_chunks
    if extension == ".pdf":
        assert [chunk["page_number"] for chunk in postgres.chunks] == [1, 2]
    else:
        assert postgres.chunks[0]["start_paragraph"] == 1
        assert postgres.chunks[0]["end_paragraph"] == 1


@pytest.mark.storage
@pytest.mark.no_network
async def test_read_document_keeps_pdf_line_ranges_page_local(
    monkeypatch, document_harness
):
    from types import SimpleNamespace

    pages = [
        SimpleNamespace(extract_text=lambda: "First page line"),
        SimpleNamespace(extract_text=lambda: "Second page first\nSecond page last"),
    ]
    monkeypatch.setattr(
        storage_module,
        "PdfReader",
        lambda _: SimpleNamespace(pages=pages),
    )
    service, _ = document_harness
    uploaded = await service.add_document(content=b"pdf", original_name="report.pdf")
    await service.index_document(document_id=uploaded["document_id"])

    result = await service.read_document(
        document_id=uploaded["document_id"],
        page_number=2,
        start_line=2,
    )

    assert result["page_number"] == 2
    assert result["locator"] == {"kind": "pdf_page", "page": 2}
    assert result["start_line"] == result["end_line"] == 2
    assert result["content"] == "2: Second page last"
    selection = await service.resolve_document_selection(
        document_id=uploaded["document_id"],
        selection=DocumentSelection(
            content_hash=uploaded["content_hash"],
            locator={"kind": "pdf_page", "page": 2},
        ),
    )
    assert selection["locator"] == {"kind": "pdf_page", "page": 2}
    assert selection["content"] == "1: Second page first\n2: Second page last"


@pytest.mark.storage
@pytest.mark.no_network
async def test_document_selection_derives_docx_heading_path(
    monkeypatch,
    document_harness,
):
    from types import SimpleNamespace

    paragraphs = [
        SimpleNamespace(text="Overview", style=SimpleNamespace(name="Heading 1")),
        SimpleNamespace(text="Current selection", style=SimpleNamespace(name="Normal")),
    ]
    monkeypatch.setattr(
        storage_module,
        "DocxDocument",
        lambda _: SimpleNamespace(paragraphs=paragraphs),
    )
    service, _ = document_harness
    uploaded = await service.add_document(content=b"docx", original_name="notes.docx")

    selection = await service.resolve_document_selection(
        document_id=uploaded["document_id"],
        selection=DocumentSelection(
            content_hash=uploaded["content_hash"],
            locator={
                "kind": "docx_paragraphs",
                "start_paragraph": 2,
                "end_paragraph": 2,
                "heading_path": ["client-must-not-control-this"],
            },
        ),
    )

    assert selection["content"] == "2: Current selection"
    assert selection["locator"] == {
        "kind": "docx_paragraphs",
        "start_paragraph": 2,
        "end_paragraph": 2,
        "heading_path": ["Overview"],
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_read_document_reports_csv_data_rows_not_physical_file_lines(
    document_harness,
):
    service, _ = document_harness
    uploaded = await service.add_document(
        content=b"name,value\nalpha,1\nbeta,2\n",
        original_name="metrics.csv",
    )
    await service.index_document(document_id=uploaded["document_id"])

    result = await service.read_document(
        document_id=uploaded["document_id"],
        start_line=2,
    )

    assert result["locator"] == {
        "kind": "csv_rows",
        "start_row": 2,
        "end_row": 2,
    }
    assert result["chunk_index"] == "rows:2-2"
    assert result["content"] == "2: beta,2"
    selection = await service.resolve_document_selection(
        document_id=uploaded["document_id"],
        selection=DocumentSelection(
            content_hash=uploaded["content_hash"],
            locator={"kind": "csv_rows", "start_row": 1, "end_row": 2},
        ),
    )
    assert selection["locator"] == {
        "kind": "csv_rows",
        "start_row": 1,
        "end_row": 2,
    }
    assert selection["content"] == "1: alpha,1\n2: beta,2"


@pytest.mark.storage
@pytest.mark.no_network
async def test_index_document_records_document_parser_errors(
    monkeypatch, document_harness
):
    service, postgres = document_harness

    def fail_pdf_parse(buf):
        raise ValueError("damaged PDF")

    monkeypatch.setattr(storage_module, "PdfReader", fail_pdf_parse)
    uploaded = await service.add_document(
        content=b"not a valid PDF",
        original_name="notes.pdf",
    )

    with pytest.raises(RuntimeError, match="damaged PDF"):
        await service.index_document(document_id=uploaded["document_id"])

    assert postgres.rows[0]["status"] == "failed"
    assert postgres.rows[0]["error_message"] == "damaged PDF"


@pytest.mark.storage
@pytest.mark.no_network
async def test_index_document_enforces_project_ownership():
    postgres = MemoryPostgres()
    embedding = FakeEmbeddingService()
    project_one = DocumentService("project-1", postgres, embedding)
    project_two = DocumentService("project-2", postgres, embedding)
    uploaded = await project_one.add_document(
        content=b"session content",
        original_name="private.txt",
    )

    with pytest.raises(FileNotFoundError, match="Document not found"):
        await project_two.index_document(
            document_id=uploaded["document_id"],
        )

    indexed = await project_one.index_document(
        document_id=uploaded["document_id"],
    )
    assert indexed["status"] == "indexed"


@pytest.mark.storage
@pytest.mark.no_network
async def test_index_document_is_idempotent_after_success(document_harness):
    service, postgres = document_harness
    uploaded = await service.add_document(content=b"alpha", original_name="notes.txt")
    first = await service.index_document(document_id=uploaded["document_id"])
    calls_after_first = list(service._embedding.calls)
    chunks_after_first = deepcopy(postgres.chunks)

    second = await service.index_document(document_id=uploaded["document_id"])

    assert second["status"] == "indexed"
    assert second["chunk_count"] == first["chunk_count"]
    assert service._embedding.calls == calls_after_first
    assert postgres.chunks == chunks_after_first


@pytest.mark.storage
@pytest.mark.no_network
async def test_index_document_does_not_publish_after_content_changes(document_harness):
    service, postgres = document_harness
    uploaded = await service.add_document(content=b"alpha", original_name="notes.txt")
    original_encode = service._embedding.encode

    async def change_content_hash(values):
        embeddings = await original_encode(values)
        postgres.rows[0]["content_hash"] = "b" * 64
        postgres.rows[0]["status"] = "queued"
        return embeddings

    service._embedding.encode = change_content_hash

    result = await service.index_document(document_id=uploaded["document_id"])

    assert result["status"] == "queued"
    assert result["content_hash"] == "b" * 64
    assert postgres.chunks == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_index_document_validates_embedding_count_and_dimension(document_harness):
    service, postgres = document_harness
    uploaded = await service.add_document(content=b"alpha", original_name="notes.txt")
    service._embedding.embeddings = []

    with pytest.raises(RuntimeError, match="Embedding count"):
        await service.index_document(document_id=uploaded["document_id"])

    assert postgres.rows[0]["status"] == "failed"
    service._embedding.embeddings = [[0.1] * 3]

    with pytest.raises(RuntimeError, match="exactly 1024 dimensions"):
        await service.index_document(document_id=uploaded["document_id"])

    assert postgres.rows[0]["status"] == "failed"


@pytest.mark.storage
@pytest.mark.no_network
async def test_index_transaction_locks_parent_and_rechecks_indexed_state(
    document_harness,
):
    service, postgres = document_harness
    uploaded = await service.add_document(content=b"alpha", original_name="notes.txt")

    await service.index_document(document_id=uploaded["document_id"])

    locking_queries = [
        query
        for method, query, _ in postgres.calls
        if method == "cursor.execute" and "FOR UPDATE" in query
    ]
    assert locking_queries


@pytest.mark.storage
@pytest.mark.no_network
async def test_accept_folder_indexes_selected_subset_atomically(
    monkeypatch,
    document_harness,
):
    service, postgres = document_harness

    class OneChunkSplitter:
        def __init__(self, **kwargs):
            pass

        def split_text(self, text):
            return [text]

    monkeypatch.setattr(
        storage_module,
        "SentenceSplitter",
        OneChunkSplitter,
    )
    result = await service.accept_folder(
        folder_name="repo",
        entries=[
            FolderUploadEntry(
                relative_path="src/main.py",
                content=b"print('main')",
            ),
            FolderUploadEntry(
                relative_path="README.md",
                content=b"readme",
            ),
            FolderUploadEntry(
                relative_path="ignored.log",
                content=b"ignored",
            ),
        ],
        selected_paths=["src/main.py", "README.md"],
    )

    assert result["document_count"] == 2
    assert set(result["relative_paths"]) == {
        "README.md",
        "src/main.py",
    }
    assert result["path_prefix"] is None
    assert len(postgres.rows) == 2
    assert len(postgres.chunks) == 0


@pytest.mark.storage
@pytest.mark.no_network
async def test_project_scan_settings_round_trip_defaults_reset_and_isolation():
    postgres = MemoryPostgres()
    embedding = FakeEmbeddingService()
    project_one = DocumentService("project-1", postgres, embedding)
    project_two = DocumentService("project-2", postgres, embedding)

    defaults = await project_one.get_scan_settings()
    saved = await project_one.save_scan_settings(
        FolderScanSettings(
            include_hidden=True,
            blocked_extensions={".foo"},
        )
    )

    assert defaults == FolderScanSettings()
    assert saved.include_hidden is True
    assert (await project_one.get_scan_settings()).blocked_extensions == {".foo"}
    assert await project_two.get_scan_settings() == FolderScanSettings()
    assert postgres.scan_settings["project-1"]["created_at"]
    assert postgres.scan_settings["project-1"]["updated_at"]
    with pytest.raises(ValueError):
        await project_one.save_scan_settings({"max_file_count": 0})

    reset = await project_one.reset_scan_settings()

    assert reset == FolderScanSettings()
    assert await project_one.get_scan_settings() == FolderScanSettings()


@pytest.mark.storage
@pytest.mark.no_network
async def test_preview_uses_saved_settings_and_explicit_settings_do_not_persist(
    document_harness,
):
    service, _ = document_harness
    await service.save_scan_settings(FolderScanSettings(blocked_extensions={".foo"}))
    entries = [
        FolderUploadEntry(relative_path="notes.txt", content=b"notes"),
        FolderUploadEntry(relative_path="debug.foo", content=b"debug"),
    ]

    saved_preview = await service.preview_folder(
        folder_name="repo",
        entries=entries,
    )
    explicit_preview = await service.preview_folder(
        folder_name="repo",
        entries=entries,
        settings=FolderScanSettings(blocked_extensions=set()),
    )

    assert [entry.relative_path for entry in saved_preview.included] == ["notes.txt"]
    assert {entry.relative_path for entry in explicit_preview.included} == {
        "debug.foo",
        "notes.txt",
    }
    assert (await service.get_scan_settings()).blocked_extensions == {".foo"}


@pytest.mark.storage
@pytest.mark.no_network
async def test_accept_folder_without_selection_accepts_all_eligible_documents(
    monkeypatch,
    document_harness,
):
    service, postgres = document_harness

    class OneChunkSplitter:
        def __init__(self, **kwargs):
            pass

        def split_text(self, text):
            return [text]

    monkeypatch.setattr(
        storage_module,
        "SentenceSplitter",
        OneChunkSplitter,
    )
    await service.save_scan_settings(FolderScanSettings(blocked_extensions={".foo"}))

    result = await service.accept_folder(
        folder_name="repo",
        entries=[
            FolderUploadEntry(relative_path="a.txt", content=b"alpha"),
            FolderUploadEntry(relative_path="b.md", content=b"beta"),
            FolderUploadEntry(relative_path="debug.foo", content=b"debug"),
        ],
        selected_paths=None,
    )

    assert set(result["relative_paths"]) == {
        "a.txt",
        "b.md",
    }
    assert result["scan_settings"]["blocked_extensions"] == [".foo"]
    assert len(postgres.rows) == 2


@pytest.mark.storage
@pytest.mark.no_network
async def test_repeated_folder_acceptance_rejects_an_existing_project_path(
    monkeypatch,
    document_harness,
):
    service, postgres = document_harness

    class OneChunkSplitter:
        def __init__(self, **kwargs):
            pass

        def split_text(self, text):
            return [text]

    monkeypatch.setattr(
        storage_module,
        "SentenceSplitter",
        OneChunkSplitter,
    )
    entries = [FolderUploadEntry(relative_path="notes.txt", content=b"notes")]

    first = await service.accept_folder(
        folder_name="repo",
        entries=entries,
        selected_paths=["notes.txt"],
    )
    with pytest.raises(FileExistsError):
        await service.accept_folder(
            folder_name="repo",
            entries=entries,
            selected_paths=["notes.txt"],
        )
    assert first["relative_paths"] == ["notes.txt"]
    assert len(postgres.rows) == 1


@pytest.mark.storage
@pytest.mark.no_network
async def test_accept_folder_rejects_unknown_and_excluded_selections(
    document_harness,
):
    service, postgres = document_harness
    entries = [
        FolderUploadEntry(relative_path="notes.txt", content=b"notes"),
        FolderUploadEntry(relative_path=".env", content=b"secret"),
    ]

    with pytest.raises(ValueError, match="unknown"):
        await service.accept_folder(
            folder_name="repo",
            entries=entries,
            selected_paths=["missing.txt"],
        )
    with pytest.raises(ValueError, match="excluded"):
        await service.accept_folder(
            folder_name="repo",
            entries=entries,
            selected_paths=[".env"],
            force_include_paths=[".env"],
        )
    with pytest.raises(ValueError, match="duplicates"):
        await service.accept_folder(
            folder_name="repo",
            entries=entries,
            selected_paths=["notes.txt", "notes.txt"],
        )
    with pytest.raises(ValueError, match="at least one"):
        await service.accept_folder(
            folder_name="repo",
            entries=entries,
            selected_paths=[],
        )

    assert postgres.rows == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_accept_folder_admits_bytes_without_creating_a_folder_batch(
    document_harness,
):
    service, postgres = document_harness

    result = await service.accept_folder(
        folder_name="repo",
        entries=[FolderUploadEntry(relative_path="broken.txt", content=b"\xff")],
        selected_paths=["broken.txt"],
    )

    assert result["relative_paths"] == ["broken.txt"]
    assert len(postgres.rows) == 1
    assert postgres.rows[0]["status"] == "queued"
    assert postgres.chunks == []
    assert postgres.contents[postgres.rows[0]["document_id"]] == b"\xff"


@pytest.mark.storage
@pytest.mark.no_network
async def test_accept_folder_admits_selected_paths_before_background_indexing(
    monkeypatch,
    document_harness,
):
    service, postgres = document_harness

    class OneChunkSplitter:
        def __init__(self, **kwargs):
            pass

        def split_text(self, text):
            return [text]

    monkeypatch.setattr(
        storage_module,
        "SentenceSplitter",
        OneChunkSplitter,
    )
    result = await service.accept_folder(
        folder_name="repo",
        entries=[
            FolderUploadEntry(relative_path="a.txt", content=b"alpha"),
            FolderUploadEntry(relative_path="b.txt", content=b"beta"),
        ],
        selected_paths=["a.txt", "b.txt"],
    )

    assert result["relative_paths"] == ["a.txt", "b.txt"]
    assert len(postgres.rows) == 2
    assert len(postgres.chunks) == 0
    assert {row["status"] for row in postgres.rows} == {"queued"}
    assert set(postgres.contents.values()) == {b"alpha", b"beta"}


@pytest.mark.storage
@pytest.mark.no_network
async def test_accept_folder_commit_failure_removes_rows_and_bytes(
    monkeypatch,
    document_harness,
):
    service, postgres = document_harness

    class OneChunkSplitter:
        def __init__(self, **kwargs):
            pass

        def split_text(self, text):
            return [text]

    monkeypatch.setattr(
        storage_module,
        "SentenceSplitter",
        OneChunkSplitter,
    )
    postgres.transaction_commit_error = RuntimeError("commit failed")

    with pytest.raises(RuntimeError, match="commit failed"):
        await service.accept_folder(
            folder_name="repo",
            entries=[
                FolderUploadEntry(
                    relative_path="notes.txt",
                    content=b"notes",
                )
            ],
            selected_paths=["notes.txt"],
        )

    assert postgres.rows == []
    assert postgres.chunks == []
    assert postgres.contents == {}
