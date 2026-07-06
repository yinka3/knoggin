import hashlib
import json
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID

import pytest

from common.schema.document import FolderScanSettings, FolderUploadEntry
from core.knowledge.services import (
    document_service as document_service_module,
)
from core.knowledge.services.document_service import DocumentService


class MemoryPostgres:
    def __init__(self):
        self.rows = []
        self.folders = []
        self.scan_settings = {}
        self.chunks = []
        self.calls = []
        self.write_error = None
        self.transaction_error_at_chunk = None
        self.transaction_commit_error = None
        self.delete_error = None
        self.search_results = []
        self.async_pool = MemoryPool(self)

    @staticmethod
    def _visible(row, project_id, session_id):
        return row["project_id"] == project_id and (
            row["visibility_scope"] == "project"
            or (
                row["visibility_scope"] == "session"
                and row["session_id"] == session_id
            )
        )

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
                "created_at": (
                    existing["created_at"] if existing else created_at
                ),
                "updated_at": updated_at,
            }
            return 1
        (
            document_id,
            project_id,
            session_id,
            visibility_scope,
            original_name,
            relative_path,
            extension,
            size_bytes,
            content_hash,
            storage_key,
            created_at,
            updated_at,
        ) = params
        self.rows.append(
            {
                "document_id": document_id,
                "project_id": project_id,
                "session_id": session_id,
                "visibility_scope": visibility_scope,
                "folder_root_id": None,
                "source_kind": "manual_upload",
                "original_name": original_name,
                "relative_path": relative_path,
                "extension": extension,
                "size_bytes": size_bytes,
                "content_hash": content_hash,
                "storage_key": storage_key,
                "status": "uploaded",
                "indexed_at": None,
                "error_message": None,
                "created_at": created_at,
                "updated_at": updated_at,
                "chunk_count": 0,
            }
        )
        return 1

    async def fetch_all(self, query, params=None):
        self.calls.append(("fetch_all", query, params))
        if "FROM public.project_document_scan_settings" in query:
            row = self.scan_settings.get(params[0])
            return [deepcopy(row)] if row else []
        if "FROM public.document_folder_uploads" in query:
            if "folder_root_id = %s" in query:
                folder_root_id, project_id, session_id = params
                return [
                    deepcopy(folder)
                    for folder in self.folders
                    if folder["folder_root_id"] == folder_root_id
                    and self._visible(folder, project_id, session_id)
                ]
            project_id, session_id, *filters = params
            scope = None
            if "visibility_scope = %s" in query:
                scope = filters.pop(0)
            limit = filters[-1]
            folders = [
                deepcopy(folder)
                for folder in reversed(self.folders)
                if self._visible(folder, project_id, session_id)
                and (
                    scope is None
                    or folder["visibility_scope"] == scope
                )
            ]
            return folders[:limit]
        if (
            "FROM public.project_documents AS pd" in query
            and "pd.folder_root_id = %s" in query
            and "pd.original_name" in query
            and "pd.content_hash" not in query
        ):
            project_id, folder_root_id, session_id, *path_filters = params
            rows = [
                row
                for row in self.rows
                if row["project_id"] == project_id
                and row.get("folder_root_id") == folder_root_id
                and self._visible(row, project_id, session_id)
            ]
            if path_filters:
                prefix = path_filters[0]
                rows = [
                    row
                    for row in rows
                    if row["relative_path"] == prefix
                    or row["relative_path"].startswith(f"{prefix}/")
                ]
            return [
                {
                    "document_id": row["document_id"],
                    "folder_root_id": row["folder_root_id"],
                    "original_name": row["original_name"],
                    "relative_path": row["relative_path"],
                    "extension": row["extension"],
                    "size_bytes": row["size_bytes"],
                    "status": row["status"],
                    "chunk_count": sum(
                        chunk["document_id"] == row["document_id"]
                        for chunk in self.chunks
                    ),
                }
                for row in sorted(rows, key=lambda item: item["relative_path"])
            ]
        if (
            "FROM public.document_chunks AS dc" in query
            and "JOIN public.project_documents AS pd" in query
        ):
            return deepcopy(self.search_results)
        if "LIMIT 2" in query and (
            "pd.document_id = %s" in query
            or "pd.relative_path = %s" in query
        ):
            selector_value, project_id, session_id = params
            selector_key = (
                "document_id"
                if "pd.document_id = %s" in query
                else "relative_path"
            )
            results = []
            for row in reversed(self.rows):
                if row[selector_key] == selector_value and self._visible(
                    row, project_id, session_id
                ):
                    result = dict(row)
                    result["chunk_count"] = sum(
                        chunk["document_id"] == row["document_id"]
                        for chunk in self.chunks
                    )
                    results.append(result)
            return results[:2]

        project_id, session_id, *filters = params
        scope = None
        if "pd.visibility_scope = %s" in query:
            scope = filters.pop(0)
        folder_root_id = None
        if "pd.folder_root_id = %s" in query:
            folder_root_id = filters.pop(0)
        path_prefix = None
        if "pd.relative_path LIKE %s" in query:
            path_prefix = filters.pop(0)
            filters.pop(0)
        rows = [
            row
            for row in self.rows
            if self._visible(row, project_id, session_id)
            and (scope is None or row["visibility_scope"] == scope)
            and (
                folder_root_id is None
                or row.get("folder_root_id") == folder_root_id
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
                chunk["document_id"] == row["document_id"]
                for chunk in self.chunks
            )
            results.append(result)
        return results


class MemoryCursor:
    def __init__(self, postgres):
        self.postgres = postgres
        self.result = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def execute(self, query, params=None):
        self.postgres.calls.append(("cursor.execute", query, params))
        normalized = " ".join(query.split())

        if normalized.startswith("SELECT") and "FOR UPDATE" in normalized:
            document_id, project_id, session_id = params
            row = next(
                (
                    row
                    for row in self.postgres.rows
                    if row["document_id"] == document_id
                    and self.postgres._visible(row, project_id, session_id)
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

        if normalized.startswith("DELETE FROM public.document_chunks"):
            document_id = params[0]
            self.postgres.chunks = [
                chunk
                for chunk in self.postgres.chunks
                if chunk["document_id"] != document_id
            ]
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
            self.result = {"document_id": document_id}
            return

        if normalized.startswith("INSERT INTO public.document_chunks"):
            if (
                self.postgres.transaction_error_at_chunk is not None
                and len(self.postgres.chunks)
                == self.postgres.transaction_error_at_chunk
            ):
                raise RuntimeError("chunk insert failed")
            chunk_id, document_id, chunk_index, content, embedding = params
            self.postgres.chunks.append(
                {
                    "chunk_id": chunk_id,
                    "document_id": document_id,
                    "chunk_index": chunk_index,
                    "content": content,
                    "embedding": embedding,
                }
            )
            self.result = None
            return

        if normalized.startswith(
            "INSERT INTO public.document_folder_uploads"
        ):
            (
                folder_root_id,
                project_id,
                session_id,
                visibility_scope,
                folder_name,
                candidate_count,
                candidate_bytes,
                document_count,
                total_size_bytes,
                excluded_count,
                excluded_bytes,
                excluded_directory_count,
                excluded_reason_counts,
                scan_settings,
                created_at,
                indexed_at,
            ) = params
            self.postgres.folders.append(
                {
                    "folder_root_id": folder_root_id,
                    "project_id": project_id,
                    "session_id": session_id,
                    "visibility_scope": visibility_scope,
                    "folder_name": folder_name,
                    "candidate_count": candidate_count,
                    "candidate_bytes": candidate_bytes,
                    "document_count": document_count,
                    "total_size_bytes": total_size_bytes,
                    "excluded_count": excluded_count,
                    "excluded_bytes": excluded_bytes,
                    "excluded_directory_count": excluded_directory_count,
                    "excluded_reason_counts": json.loads(
                        excluded_reason_counts
                    ),
                    "scan_settings": json.loads(scan_settings),
                    "created_at": created_at,
                    "indexed_at": indexed_at,
                }
            )
            self.result = None
            return

        if normalized.startswith(
            "INSERT INTO public.project_documents"
        ):
            (
                document_id,
                project_id,
                session_id,
                visibility_scope,
                folder_root_id,
                original_name,
                relative_path,
                extension,
                size_bytes,
                content_hash,
                storage_key,
                indexed_at,
                created_at,
                updated_at,
            ) = params
            self.postgres.rows.append(
                {
                    "document_id": document_id,
                    "project_id": project_id,
                    "session_id": session_id,
                    "visibility_scope": visibility_scope,
                    "folder_root_id": folder_root_id,
                    "source_kind": "folder_upload",
                    "original_name": original_name,
                    "relative_path": relative_path,
                    "extension": extension,
                    "size_bytes": size_bytes,
                    "content_hash": content_hash,
                    "storage_key": storage_key,
                    "status": "indexed",
                    "indexed_at": indexed_at,
                    "error_message": None,
                    "created_at": created_at,
                    "updated_at": updated_at,
                    "chunk_count": 0,
                }
            )
            self.result = None
            return

        if "SET status = 'indexed'" in normalized:
            indexed_at, updated_at, document_id = params
            row = next(
                row
                for row in self.postgres.rows
                if row["document_id"] == document_id
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
                row
                for row in self.postgres.rows
                if row["document_id"] == document_id
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


class MemoryTransaction:
    def __init__(self, postgres):
        self.postgres = postgres

    async def __aenter__(self):
        self.rows = deepcopy(self.postgres.rows)
        self.chunks = deepcopy(self.postgres.chunks)
        self.folders = deepcopy(self.postgres.folders)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is not None or self.postgres.transaction_commit_error:
            self.postgres.rows = self.rows
            self.postgres.chunks = self.chunks
            self.postgres.folders = self.folders
        if exc_type is None and self.postgres.transaction_commit_error:
            raise self.postgres.transaction_commit_error
        return False


class MemoryConnection:
    def __init__(self, postgres):
        self.postgres = postgres

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def transaction(self):
        return MemoryTransaction(self.postgres)

    def cursor(self):
        return MemoryCursor(self.postgres)


class MemoryPool:
    def __init__(self, postgres):
        self.postgres = postgres

    def connection(self):
        return MemoryConnection(self.postgres)


class FakeEmbeddingService:
    def __init__(self):
        self.calls = []
        self.single_calls = []
        self.embeddings = None
        self.single_embedding = [0.1] * 1024

    async def encode(self, values):
        self.calls.append(list(values))
        if self.embeddings is not None:
            return self.embeddings
        return [[0.1] * 1024 for _ in values]

    async def encode_single(self, value):
        self.single_calls.append(value)
        return self.single_embedding


@pytest.fixture
def document_harness(tmp_path):
    postgres = MemoryPostgres()
    embedding = FakeEmbeddingService()
    service = DocumentService(
        project_id="project-1",
        postgres_client=postgres,
        storage_root=tmp_path,
        embedding_service=embedding,
    )
    return service, postgres, tmp_path


@pytest.mark.storage
@pytest.mark.no_network
async def test_add_document_writes_managed_copy_and_persists_metadata(document_harness):
    service, postgres, storage_root = document_harness
    content = b"alpha beta gamma"

    metadata = await service.add_document(
        content=content,
        original_name="Notes.MD",
        relative_path=r"docs\Notes.MD",
    )

    assert metadata["project_id"] == "project-1"
    assert metadata["visibility_scope"] == "project"
    assert metadata["session_id"] is None
    assert metadata["relative_path"] == "docs/Notes.MD"
    assert metadata["extension"] == ".md"
    assert metadata["size_bytes"] == len(content)
    assert metadata["content_hash"] == hashlib.sha256(content).hexdigest()
    assert metadata["status"] == "uploaded"
    assert metadata["folder_root_id"] is None
    assert metadata["source_kind"] == "manual_upload"
    assert metadata["chunk_count"] == 0
    assert "storage_key" not in metadata

    stored_row = postgres.rows[0]
    stored_path = storage_root / stored_row["storage_key"]
    assert stored_path.read_bytes() == content
    assert stored_path.name == "content"
    assert stored_row["original_name"] == "Notes.MD"


@pytest.mark.storage
@pytest.mark.no_network
async def test_database_failure_removes_newly_written_document(document_harness):
    service, postgres, storage_root = document_harness
    postgres.write_error = RuntimeError("insert failed")

    with pytest.raises(RuntimeError, match="insert failed"):
        await service.add_document(content=b"alpha", original_name="notes.md")

    assert list(storage_root.rglob("content")) == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_visibility_rules_are_applied_when_listing_files(tmp_path):
    postgres = MemoryPostgres()
    embedding = FakeEmbeddingService()
    project_one = DocumentService("project-1", postgres, tmp_path, embedding)
    project_two = DocumentService("project-2", postgres, tmp_path, embedding)

    project_file = await project_one.add_document(
        content=b"project",
        original_name="project.md",
    )
    session_one = await project_one.add_document(
        content=b"one",
        original_name="one.md",
        session_id="session-1",
        visibility_scope="session",
    )
    await project_one.add_document(
        content=b"two",
        original_name="two.md",
        session_id="session-2",
        visibility_scope="session",
    )
    await project_two.add_document(
        content=b"other project",
        original_name="other.md",
    )

    visible_to_one = await project_one.list_documents(session_id="session-1")
    visible_to_two = await project_one.list_documents(session_id="session-2")
    project_only = await project_one.list_documents(
        session_id="session-1",
        visibility_scope="project",
    )

    assert {row["document_id"] for row in visible_to_one} == {
        project_file["document_id"],
        session_one["document_id"],
    }
    assert {row["original_name"] for row in visible_to_two} == {
        "project.md",
        "two.md",
    }
    assert [row["original_name"] for row in project_only] == ["project.md"]
    assert all("storage_key" not in row for row in visible_to_one)


@pytest.mark.storage
@pytest.mark.no_network
async def test_repeated_uploads_create_separate_records(document_harness):
    service, postgres, _ = document_harness

    first = await service.add_document(
        content=b"same",
        original_name="notes.md",
        relative_path="docs/notes.md",
    )
    second = await service.add_document(
        content=b"same",
        original_name="notes.md",
        relative_path="docs/notes.md",
    )

    assert first["document_id"] != second["document_id"]
    assert first["content_hash"] == second["content_hash"]
    assert len(postgres.rows) == 2


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
async def test_add_document_rejects_unsafe_relative_paths(document_harness, relative_path):
    service, postgres, _ = document_harness

    with pytest.raises(ValueError):
        await service.add_document(
            content=b"alpha",
            original_name="notes.md",
            relative_path=relative_path,
        )

    assert postgres.rows == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_generated_storage_path_cannot_escape_storage_root(tmp_path):
    service = DocumentService(
        project_id="../outside",
        postgres_client=MemoryPostgres(),
        storage_root=tmp_path,
        embedding_service=FakeEmbeddingService(),
    )

    with pytest.raises(ValueError, match="escaped"):
        await service.add_document(content=b"alpha", original_name="notes.md")

    assert list(tmp_path.rglob("content")) == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_add_document_rejects_invalid_content_scope_and_size(
    monkeypatch, document_harness
):
    service, postgres, _ = document_harness

    with pytest.raises(ValueError, match="must not be empty"):
        await service.add_document(content=b"", original_name="notes.md")
    with pytest.raises(ValueError, match="either 'project' or 'session'"):
        await service.add_document(
            content=b"alpha",
            original_name="notes.md",
            visibility_scope="private",
        )
    with pytest.raises(ValueError, match="require session_id"):
        await service.add_document(
            content=b"alpha",
            original_name="notes.md",
            visibility_scope="session",
        )

    monkeypatch.setattr(document_service_module, "MAX_DOCUMENT_SIZE", 3)
    with pytest.raises(ValueError, match="50 MB"):
        await service.add_document(content=b"four", original_name="notes.md")

    assert postgres.rows == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_list_documents_normalizes_database_timestamps(document_harness):
    service, postgres, _ = document_harness
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)
    postgres.rows.append(
        {
            "document_id": "a785ecfe-b738-4a43-9e6d-bbdc3f831b20",
            "project_id": "project-1",
            "session_id": None,
            "visibility_scope": "project",
            "original_name": "notes.md",
            "relative_path": "notes.md",
            "extension": ".md",
            "size_bytes": 5,
            "content_hash": "hash",
            "storage_key": "hidden",
            "status": "uploaded",
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
async def test_get_document_info_resolves_visible_document_without_storage_key(document_harness):
    service, _, _ = document_harness
    uploaded = await service.add_document(
        content=b"alpha\nbeta",
        original_name="notes.txt",
        relative_path="docs/notes.txt",
    )

    info = await service.get_document_info(document_id=uploaded["document_id"])

    assert info["document_id"] == uploaded["document_id"]
    assert info["relative_path"] == "docs/notes.txt"
    assert info["status"] == "uploaded"
    assert "storage_key" not in info


@pytest.mark.storage
@pytest.mark.no_network
async def test_get_document_info_enforces_visibility_and_reference_rules(tmp_path):
    postgres = MemoryPostgres()
    embedding = FakeEmbeddingService()
    service = DocumentService("project-1", postgres, tmp_path, embedding)
    uploaded = await service.add_document(
        content=b"private",
        original_name="private.txt",
        session_id="session-1",
        visibility_scope="session",
    )

    with pytest.raises(ValueError, match="exactly one"):
        await service.get_document_info()
    with pytest.raises(ValueError, match="exactly one"):
        await service.get_document_info(
            document_id=uploaded["document_id"],
            relative_path="private.txt",
        )
    with pytest.raises(FileNotFoundError, match="Document not found"):
        await service.get_document_info(
            document_id=uploaded["document_id"],
            session_id="session-2",
        )

    info = await service.get_document_info(
        document_id=uploaded["document_id"],
        session_id="session-1",
    )
    assert info["document_id"] == uploaded["document_id"]


@pytest.mark.storage
@pytest.mark.no_network
async def test_relative_path_lookup_requires_document_id_when_uploads_repeat(document_harness):
    service, _, _ = document_harness
    first = await service.add_document(
        content=b"first",
        original_name="notes.txt",
        relative_path="docs/notes.txt",
    )
    await service.add_document(
        content=b"second",
        original_name="notes.txt",
        relative_path="docs/notes.txt",
    )

    with pytest.raises(ValueError, match="Multiple visible uploads"):
        await service.get_document_info(relative_path="docs/notes.txt")

    assert (
        await service.get_document_info(document_id=first["document_id"])
    )["document_id"] == first["document_id"]


@pytest.mark.storage
@pytest.mark.no_network
async def test_read_document_returns_bounded_numbered_lines(document_harness):
    service, _, _ = document_harness
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
async def test_read_document_validates_ranges_and_character_limit(
    monkeypatch, document_harness
):
    service, _, _ = document_harness
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

    monkeypatch.setattr(document_service_module, "MAX_READ_CHARACTERS", 8)
    result = await service.read_document(document_id=uploaded["document_id"], end_line=1)
    assert result["content"] == "1: 01234"
    assert result["truncated"] is True


@pytest.mark.storage
@pytest.mark.no_network
async def test_delete_document_permanently_removes_metadata_chunks_and_bytes(
    monkeypatch, document_harness
):
    service, postgres, storage_root = document_harness

    class OneChunkSplitter:
        def __init__(self, **kwargs):
            pass

        def split_text(self, text):
            return [text]

    monkeypatch.setattr(
        document_service_module, "SentenceSplitter", OneChunkSplitter
    )
    first = await service.add_document(
        content=b"same content",
        original_name="notes.txt",
    )
    second = await service.add_document(
        content=b"same content",
        original_name="notes.txt",
    )
    await service.index_document(document_id=first["document_id"])
    first_storage = storage_root / next(
        row["storage_key"]
        for row in postgres.rows
        if row["document_id"] == first["document_id"]
    )
    second_storage = storage_root / next(
        row["storage_key"]
        for row in postgres.rows
        if row["document_id"] == second["document_id"]
    )

    deleted = await service.delete_document(document_id=first["document_id"])

    assert deleted["document_id"] == first["document_id"]
    assert deleted["deleted"] is True
    assert "storage_key" not in deleted
    assert not first_storage.exists()
    assert not first_storage.parent.exists()
    assert second_storage.read_bytes() == b"same content"
    assert [row["document_id"] for row in postgres.rows] == [second["document_id"]]
    assert all(
        chunk["document_id"] != first["document_id"] for chunk in postgres.chunks
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_delete_document_enforces_project_and_session_visibility(tmp_path):
    postgres = MemoryPostgres()
    embedding = FakeEmbeddingService()
    project_one = DocumentService("project-1", postgres, tmp_path, embedding)
    project_two = DocumentService("project-2", postgres, tmp_path, embedding)
    uploaded = await project_one.add_document(
        content=b"private",
        original_name="private.txt",
        session_id="session-1",
        visibility_scope="session",
    )

    with pytest.raises(FileNotFoundError, match="Document not found"):
        await project_one.delete_document(
            document_id=uploaded["document_id"],
            session_id="session-2",
        )
    with pytest.raises(FileNotFoundError, match="Document not found"):
        await project_two.delete_document(
            document_id=uploaded["document_id"],
            session_id="session-1",
        )

    assert len(postgres.rows) == 1
    deleted = await project_one.delete_document(
        document_id=uploaded["document_id"],
        session_id="session-1",
    )
    assert deleted["deleted"] is True
    assert postgres.rows == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_delete_document_restores_bytes_when_database_delete_fails(document_harness):
    service, postgres, storage_root = document_harness
    uploaded = await service.add_document(
        content=b"keep me",
        original_name="notes.txt",
    )
    stored_path = storage_root / postgres.rows[0]["storage_key"]
    postgres.delete_error = RuntimeError("delete failed")

    with pytest.raises(RuntimeError, match="delete failed"):
        await service.delete_document(document_id=uploaded["document_id"])

    assert stored_path.read_bytes() == b"keep me"
    assert postgres.rows[0]["document_id"] == uploaded["document_id"]
    assert not list(stored_path.parent.parent.glob(".*.deleting-*"))


@pytest.mark.storage
@pytest.mark.no_network
async def test_delete_document_removes_metadata_when_managed_bytes_are_missing(
    document_harness,
):
    service, postgres, storage_root = document_harness
    uploaded = await service.add_document(
        content=b"gone",
        original_name="notes.txt",
    )
    stored_path = storage_root / postgres.rows[0]["storage_key"]
    service._remove_stored_file(stored_path)

    deleted = await service.delete_document(document_id=uploaded["document_id"])

    assert deleted["deleted"] is True
    assert postgres.rows == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_delete_document_rejects_empty_or_inaccessible_ids(document_harness):
    service, postgres, _ = document_harness

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
    service, _, _ = document_harness

    assert await service.search("alpha") == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_document_service_search_embeds_query_and_enforces_visibility(
    document_harness,
):
    service, postgres, _ = document_harness
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
        session_id="session-1",
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
    assert "pd.project_id = %s" in sql
    assert "pd.status = 'indexed'" in sql
    assert "pd.visibility_scope = 'project'" in sql
    assert "pd.session_id = %s" in sql
    assert "dc.embedding <=> %s::vector" in sql
    assert params[1:3] == ("project-1", "session-1")
    assert params[-1] == 3


@pytest.mark.storage
@pytest.mark.no_network
async def test_document_service_search_applies_document_filter(document_harness):
    service, postgres, _ = document_harness

    await service.search(
        "alpha",
        session_id="session-1",
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
    service, postgres, _ = document_harness

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
    service, postgres, _ = document_harness
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
    service, postgres, _ = document_harness
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
async def test_index_document_uses_configured_sentence_splitter(monkeypatch, document_harness):
    service, postgres, _ = document_harness
    settings = {}

    class RecordingSplitter:
        def __init__(self, *, chunk_size, chunk_overlap):
            settings.update(
                {"chunk_size": chunk_size, "chunk_overlap": chunk_overlap}
            )

        def split_text(self, text):
            assert text == "alpha beta gamma"
            return [" alpha ", "", " beta gamma "]

    monkeypatch.setattr(
        document_service_module, "SentenceSplitter", RecordingSplitter
    )
    uploaded = await service.add_document(
        content=b"alpha beta gamma",
        original_name="notes.txt",
    )

    indexed = await service.index_document(document_id=uploaded["document_id"])

    assert settings == {"chunk_size": 512, "chunk_overlap": 50}
    assert indexed["chunk_count"] == 2
    assert [
        (chunk["chunk_index"], chunk["content"]) for chunk in postgres.chunks
    ] == [(0, "alpha"), (1, "beta gamma")]


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
    service, postgres, _ = document_harness
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
async def test_index_document_records_missing_managed_content(document_harness):
    service, postgres, storage_root = document_harness
    uploaded = await service.add_document(content=b"alpha", original_name="notes.txt")
    stored_path = storage_root / postgres.rows[0]["storage_key"]
    stored_path.unlink()

    with pytest.raises(
        RuntimeError, match="Managed document content is missing"
    ):
        await service.index_document(document_id=uploaded["document_id"])

    assert postgres.rows[0]["status"] == "failed"


@pytest.mark.storage
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("extension", "expected"),
    [
        (".pdf", "First page\n\nSecond page"),
        (".docx", "Document text"),
    ],
)
async def test_index_document_extracts_supported_documents(
    monkeypatch, document_harness, extension, expected
):
    service, postgres, _ = document_harness
    if extension == ".pdf":
        pages = [
            SimpleNamespace(extract_text=lambda: "First page"),
            SimpleNamespace(extract_text=lambda: "Second page"),
        ]
        monkeypatch.setattr(
            document_service_module,
            "PdfReader",
            lambda path: SimpleNamespace(pages=pages),
        )
    else:
        monkeypatch.setattr(
            document_service_module.docx2txt,
            "process",
            lambda path: "Document text",
        )

    uploaded = await service.add_document(
        content=b"managed document bytes",
        original_name=f"notes{extension}",
    )
    await service.index_document(document_id=uploaded["document_id"])

    assert postgres.chunks[0]["content"] == expected


@pytest.mark.storage
@pytest.mark.no_network
async def test_index_document_records_document_parser_errors(monkeypatch, document_harness):
    service, postgres, _ = document_harness

    def fail_pdf_parse(path):
        raise ValueError("damaged PDF")

    monkeypatch.setattr(document_service_module, "PdfReader", fail_pdf_parse)
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
async def test_index_document_enforces_project_and_session_visibility(tmp_path):
    postgres = MemoryPostgres()
    embedding = FakeEmbeddingService()
    project_one = DocumentService("project-1", postgres, tmp_path, embedding)
    project_two = DocumentService("project-2", postgres, tmp_path, embedding)
    uploaded = await project_one.add_document(
        content=b"session content",
        original_name="private.txt",
        session_id="session-1",
        visibility_scope="session",
    )

    with pytest.raises(FileNotFoundError, match="Document not found"):
        await project_one.index_document(
            document_id=uploaded["document_id"],
            session_id="session-2",
        )
    with pytest.raises(FileNotFoundError, match="Document not found"):
        await project_two.index_document(
            document_id=uploaded["document_id"],
            session_id="session-1",
        )

    indexed = await project_one.index_document(
        document_id=uploaded["document_id"],
        session_id="session-1",
    )
    assert indexed["status"] == "indexed"


@pytest.mark.storage
@pytest.mark.no_network
async def test_index_document_is_idempotent_after_success(document_harness):
    service, postgres, _ = document_harness
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
async def test_index_document_validates_embedding_count_and_dimension(document_harness):
    service, postgres, _ = document_harness
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
async def test_index_transaction_rolls_back_partial_chunks_and_can_retry(
    monkeypatch, document_harness
):
    service, postgres, _ = document_harness

    class TwoChunkSplitter:
        def __init__(self, **kwargs):
            pass

        def split_text(self, text):
            return ["alpha", "beta"]

    monkeypatch.setattr(
        document_service_module, "SentenceSplitter", TwoChunkSplitter
    )
    uploaded = await service.add_document(
        content=b"alpha beta",
        original_name="notes.txt",
    )
    postgres.transaction_error_at_chunk = 1

    with pytest.raises(RuntimeError, match="chunk insert failed"):
        await service.index_document(document_id=uploaded["document_id"])

    assert postgres.chunks == []
    assert postgres.rows[0]["status"] == "failed"

    postgres.transaction_error_at_chunk = None
    indexed = await service.index_document(document_id=uploaded["document_id"])

    assert indexed["status"] == "indexed"
    assert indexed["chunk_count"] == 2
    assert [chunk["chunk_index"] for chunk in postgres.chunks] == [0, 1]


@pytest.mark.storage
@pytest.mark.no_network
async def test_index_transaction_locks_parent_and_rechecks_indexed_state(document_harness):
    service, postgres, _ = document_harness
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
    service, postgres, storage_root = document_harness

    class OneChunkSplitter:
        def __init__(self, **kwargs):
            pass

        def split_text(self, text):
            return [text]

    monkeypatch.setattr(
        document_service_module,
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
    assert {item["relative_path"] for item in result["documents"]} == {
        "README.md",
        "src/main.py",
    }
    assert all(item["status"] == "indexed" for item in result["documents"])
    assert len(service._embedding.calls) == 2
    assert len(postgres.folders) == 1
    assert len(postgres.rows) == 2
    assert len(postgres.chunks) == 2
    assert all(row["source_kind"] == "folder_upload" for row in postgres.rows)
    assert all(
        row["folder_root_id"] == result["folder_root_id"]
        for row in postgres.rows
    )
    assert all(
        (storage_root / row["storage_key"]).is_file()
        for row in postgres.rows
    )
    assert not (storage_root / ".staging").exists()


@pytest.mark.storage
@pytest.mark.no_network
async def test_project_scan_settings_round_trip_defaults_reset_and_isolation(
    tmp_path,
):
    postgres = MemoryPostgres()
    embedding = FakeEmbeddingService()
    project_one = DocumentService("project-1", postgres, tmp_path, embedding)
    project_two = DocumentService("project-2", postgres, tmp_path, embedding)

    defaults = await project_one.get_scan_settings()
    saved = await project_one.save_scan_settings(
        FolderScanSettings(
            include_hidden=True,
            blocked_extensions={".foo"},
        )
    )

    assert defaults == FolderScanSettings()
    assert saved.include_hidden is True
    assert (await project_one.get_scan_settings()).blocked_extensions == {
        ".foo"
    }
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
    service, _, _ = document_harness
    await service.save_scan_settings(
        FolderScanSettings(blocked_extensions={".foo"})
    )
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

    assert [entry.relative_path for entry in saved_preview.included] == [
        "notes.txt"
    ]
    assert {
        entry.relative_path for entry in explicit_preview.included
    } == {"debug.foo", "notes.txt"}
    assert (await service.get_scan_settings()).blocked_extensions == {".foo"}


@pytest.mark.storage
@pytest.mark.no_network
async def test_accept_folder_without_selection_accepts_all_eligible_documents(
    monkeypatch,
    document_harness,
):
    service, postgres, _ = document_harness

    class OneChunkSplitter:
        def __init__(self, **kwargs):
            pass

        def split_text(self, text):
            return [text]

    monkeypatch.setattr(
        document_service_module,
        "SentenceSplitter",
        OneChunkSplitter,
    )
    await service.save_scan_settings(
        FolderScanSettings(blocked_extensions={".foo"})
    )

    result = await service.accept_folder(
        folder_name="repo",
        entries=[
            FolderUploadEntry(relative_path="a.txt", content=b"alpha"),
            FolderUploadEntry(relative_path="b.md", content=b"beta"),
            FolderUploadEntry(relative_path="debug.foo", content=b"debug"),
        ],
        selected_paths=None,
    )

    assert {item["relative_path"] for item in result["documents"]} == {
        "a.txt",
        "b.md",
    }
    assert result["scan_settings"]["blocked_extensions"] == [".foo"]
    assert len(postgres.rows) == 2


@pytest.mark.storage
@pytest.mark.no_network
async def test_repeated_folder_acceptance_creates_independent_batches(
    monkeypatch,
    document_harness,
):
    service, postgres, _ = document_harness

    class OneChunkSplitter:
        def __init__(self, **kwargs):
            pass

        def split_text(self, text):
            return [text]

    monkeypatch.setattr(
        document_service_module,
        "SentenceSplitter",
        OneChunkSplitter,
    )
    entries = [
        FolderUploadEntry(relative_path="notes.txt", content=b"notes")
    ]

    first = await service.accept_folder(
        folder_name="repo",
        entries=entries,
        selected_paths=["notes.txt"],
    )
    second = await service.accept_folder(
        folder_name="repo",
        entries=entries,
        selected_paths=["notes.txt"],
    )

    assert first["folder_root_id"] != second["folder_root_id"]
    assert (
        first["documents"][0]["document_id"]
        != second["documents"][0]["document_id"]
    )
    assert len(postgres.folders) == 2
    assert len(postgres.rows) == 2


@pytest.mark.storage
@pytest.mark.no_network
async def test_accept_folder_rejects_unknown_and_excluded_selections(
    document_harness,
):
    service, postgres, _ = document_harness
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
    with pytest.raises(ValueError, match="require session_id"):
        await service.accept_folder(
            folder_name="repo",
            entries=entries,
            selected_paths=["notes.txt"],
            visibility_scope="session",
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

    assert postgres.folders == []
    assert postgres.rows == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_accept_folder_preparation_failure_leaves_no_state(
    document_harness,
):
    service, postgres, storage_root = document_harness

    with pytest.raises(ValueError, match="valid UTF-8"):
        await service.accept_folder(
            folder_name="repo",
            entries=[
                FolderUploadEntry(
                    relative_path="broken.txt",
                    content=b"\xff",
                )
            ],
            selected_paths=["broken.txt"],
        )

    assert postgres.folders == []
    assert postgres.rows == []
    assert postgres.chunks == []
    assert not (storage_root / "project-1").exists()
    assert not (storage_root / ".staging").exists()


@pytest.mark.storage
@pytest.mark.no_network
async def test_accept_folder_rolls_back_rows_chunks_and_bytes(
    monkeypatch,
    document_harness,
):
    service, postgres, storage_root = document_harness

    class OneChunkSplitter:
        def __init__(self, **kwargs):
            pass

        def split_text(self, text):
            return [text]

    monkeypatch.setattr(
        document_service_module,
        "SentenceSplitter",
        OneChunkSplitter,
    )
    postgres.transaction_error_at_chunk = 1

    with pytest.raises(RuntimeError, match="chunk insert failed"):
        await service.accept_folder(
            folder_name="repo",
            entries=[
                FolderUploadEntry(relative_path="a.txt", content=b"alpha"),
                FolderUploadEntry(relative_path="b.txt", content=b"beta"),
            ],
            selected_paths=["a.txt", "b.txt"],
        )

    assert postgres.folders == []
    assert postgres.rows == []
    assert postgres.chunks == []
    assert not (storage_root / "project-1").exists()
    assert not (storage_root / ".staging").exists()


@pytest.mark.storage
@pytest.mark.no_network
async def test_accept_folder_commit_failure_removes_rows_and_moved_bytes(
    monkeypatch,
    document_harness,
):
    service, postgres, storage_root = document_harness

    class OneChunkSplitter:
        def __init__(self, **kwargs):
            pass

        def split_text(self, text):
            return [text]

    monkeypatch.setattr(
        document_service_module,
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

    assert postgres.folders == []
    assert postgres.rows == []
    assert postgres.chunks == []
    assert not (storage_root / "project-1").exists()
    assert not (storage_root / ".staging").exists()


@pytest.mark.storage
@pytest.mark.no_network
async def test_folder_reads_enforce_visibility_and_build_tree(
    monkeypatch,
    tmp_path,
):
    postgres = MemoryPostgres()
    embedding = FakeEmbeddingService()
    project_one = DocumentService("project-1", postgres, tmp_path, embedding)
    project_two = DocumentService("project-2", postgres, tmp_path, embedding)

    class OneChunkSplitter:
        def __init__(self, **kwargs):
            pass

        def split_text(self, text):
            return [text]

    monkeypatch.setattr(
        document_service_module,
        "SentenceSplitter",
        OneChunkSplitter,
    )
    accepted = await project_one.accept_folder(
        folder_name="repo",
        entries=[
            FolderUploadEntry(
                relative_path="src/app.py",
                content=b"app",
            ),
            FolderUploadEntry(
                relative_path="src/lib/util.py",
                content=b"util",
            ),
            FolderUploadEntry(
                relative_path="README.md",
                content=b"readme",
            ),
        ],
        selected_paths=["src/app.py", "src/lib/util.py", "README.md"],
        visibility_scope="session",
        session_id="session-1",
    )
    folder_root_id = accepted["folder_root_id"]

    assert await project_one.list_folder_uploads(session_id="session-2") == []
    with pytest.raises(FileNotFoundError):
        await project_one.get_folder_upload_summary(
            folder_root_id=folder_root_id,
            session_id="session-2",
        )
    with pytest.raises(FileNotFoundError):
        await project_two.list_folder_tree(
            folder_root_id=folder_root_id,
            session_id="session-1",
        )

    summary = await project_one.get_folder_upload_summary(
        folder_root_id=folder_root_id,
        session_id="session-1",
    )
    assert summary["document_count"] == 3
    assert summary["total_size_bytes"] == len(b"apputilreadme")
    assert summary["scan_settings"]["respect_gitignore"] is True
    tree = summary["tree"]
    assert [node["name"] for node in tree] == ["src", "README.md"]
    assert tree[0]["type"] == "directory"
    assert tree[1]["type"] == "document"

    truncated_tree = await project_one.list_folder_tree(
        folder_root_id=folder_root_id,
        session_id="session-1",
        max_depth=1,
    )
    assert truncated_tree[0]["name"] == "src"
    assert truncated_tree[0]["truncated"] is True
    assert truncated_tree[0]["children"] == []

    subtree_focus = await project_one.resolve_focus_target(
        folder_root_id=folder_root_id,
        path_prefix="src",
        session_id="session-1",
    )
    batch_focus = await project_one.resolve_focus_target(
        folder_root_id=folder_root_id,
        session_id="session-1",
    )
    assert subtree_focus == {
        "target_type": "subtree",
        "document_id": None,
        "relative_path": None,
        "folder_root_id": folder_root_id,
        "path_prefix": "src",
    }
    assert batch_focus["target_type"] == "folder_upload"
    with pytest.raises(FileNotFoundError):
        await project_one.resolve_focus_target(
            folder_root_id=folder_root_id,
            path_prefix="src",
            session_id="session-2",
        )

    documents = await project_one.list_documents(
        folder_root_id=folder_root_id,
        path_prefix="src",
        session_id="session-1",
    )
    assert {item["relative_path"] for item in documents} == {
        "src/app.py",
        "src/lib/util.py",
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_folder_search_adds_folder_and_path_filters(document_harness):
    service, postgres, _ = document_harness
    folder_root_id = "a785ecfe-b738-4a43-9e6d-bbdc3f831b20"
    postgres.folders.append(
        {
            "folder_root_id": folder_root_id,
            "project_id": "project-1",
            "session_id": None,
            "visibility_scope": "project",
        }
    )

    await service.search(
        "alpha",
        folder_root_id=folder_root_id,
        path_prefix="src",
    )

    _, sql, params = postgres.calls[-1]
    assert "pd.folder_root_id = %s" in sql
    assert "pd.relative_path LIKE %s" in sql
    assert folder_root_id in params
    assert "src/%" in params
