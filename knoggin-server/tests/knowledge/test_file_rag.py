import hashlib
from datetime import datetime, timezone

import pytest

from knoggin_server.knowledge.services import file_rag as file_rag_module
from knoggin_server.knowledge.services.file_rag import FileRAGService


class MemoryPostgres:
    def __init__(self):
        self.rows = []
        self.calls = []
        self.write_error = None

    async def execute_write(self, query, params=None):
        self.calls.append(("execute_write", query, params))
        if self.write_error:
            raise self.write_error
        (
            file_id,
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
                "file_id": file_id,
                "project_id": project_id,
                "session_id": session_id,
                "visibility_scope": visibility_scope,
                "original_name": original_name,
                "relative_path": relative_path,
                "extension": extension,
                "size_bytes": size_bytes,
                "content_hash": content_hash,
                "storage_key": storage_key,
                "status": "uploaded",
                "created_at": created_at,
                "updated_at": updated_at,
                "chunk_count": 0,
            }
        )
        return 1

    async def execute_read(self, query, params=None):
        self.calls.append(("execute_read", query, params))
        project_id, session_id, *scope = params
        rows = [
            row
            for row in self.rows
            if row["project_id"] == project_id
            and (
                row["visibility_scope"] == "project"
                or (
                    row["visibility_scope"] == "session"
                    and row["session_id"] == session_id
                )
            )
            and (not scope or row["visibility_scope"] == scope[0])
        ]
        return list(reversed(rows))


@pytest.fixture
def filerag(tmp_path):
    postgres = MemoryPostgres()
    service = FileRAGService(
        project_id="project-1",
        postgres_client=postgres,
        storage_root=tmp_path,
    )
    return service, postgres, tmp_path


@pytest.mark.storage
@pytest.mark.no_network
async def test_add_file_writes_managed_copy_and_persists_metadata(filerag):
    service, postgres, storage_root = filerag
    content = b"alpha beta gamma"

    metadata = await service.add_file(
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
    assert metadata["chunk_count"] == 0
    assert "storage_key" not in metadata

    stored_row = postgres.rows[0]
    stored_path = storage_root / stored_row["storage_key"]
    assert stored_path.read_bytes() == content
    assert stored_path.name == "content"
    assert stored_row["original_name"] == "Notes.MD"


@pytest.mark.storage
@pytest.mark.no_network
async def test_database_failure_removes_newly_written_file(filerag):
    service, postgres, storage_root = filerag
    postgres.write_error = RuntimeError("insert failed")

    with pytest.raises(RuntimeError, match="insert failed"):
        await service.add_file(content=b"alpha", original_name="notes.md")

    assert list(storage_root.rglob("content")) == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_visibility_rules_are_applied_when_listing_files(tmp_path):
    postgres = MemoryPostgres()
    project_one = FileRAGService("project-1", postgres, tmp_path)
    project_two = FileRAGService("project-2", postgres, tmp_path)

    project_file = await project_one.add_file(
        content=b"project",
        original_name="project.md",
    )
    session_one = await project_one.add_file(
        content=b"one",
        original_name="one.md",
        session_id="session-1",
        visibility_scope="session",
    )
    await project_one.add_file(
        content=b"two",
        original_name="two.md",
        session_id="session-2",
        visibility_scope="session",
    )
    await project_two.add_file(
        content=b"other project",
        original_name="other.md",
    )

    visible_to_one = await project_one.list_files(session_id="session-1")
    visible_to_two = await project_one.list_files(session_id="session-2")
    project_only = await project_one.list_files(
        session_id="session-1",
        visibility_scope="project",
    )

    assert {row["file_id"] for row in visible_to_one} == {
        project_file["file_id"],
        session_one["file_id"],
    }
    assert {row["original_name"] for row in visible_to_two} == {
        "project.md",
        "two.md",
    }
    assert [row["original_name"] for row in project_only] == ["project.md"]
    assert all("storage_key" not in row for row in visible_to_one)


@pytest.mark.storage
@pytest.mark.no_network
async def test_repeated_uploads_create_separate_records(filerag):
    service, postgres, _ = filerag

    first = await service.add_file(
        content=b"same",
        original_name="notes.md",
        relative_path="docs/notes.md",
    )
    second = await service.add_file(
        content=b"same",
        original_name="notes.md",
        relative_path="docs/notes.md",
    )

    assert first["file_id"] != second["file_id"]
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
async def test_add_file_rejects_unsafe_relative_paths(filerag, relative_path):
    service, postgres, _ = filerag

    with pytest.raises(ValueError):
        await service.add_file(
            content=b"alpha",
            original_name="notes.md",
            relative_path=relative_path,
        )

    assert postgres.rows == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_generated_storage_path_cannot_escape_storage_root(tmp_path):
    service = FileRAGService(
        project_id="../outside",
        postgres_client=MemoryPostgres(),
        storage_root=tmp_path,
    )

    with pytest.raises(ValueError, match="escaped"):
        await service.add_file(content=b"alpha", original_name="notes.md")

    assert list(tmp_path.rglob("content")) == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_add_file_rejects_invalid_content_scope_and_size(
    monkeypatch, filerag
):
    service, postgres, _ = filerag

    with pytest.raises(ValueError, match="must not be empty"):
        await service.add_file(content=b"", original_name="notes.md")
    with pytest.raises(ValueError, match="either 'project' or 'session'"):
        await service.add_file(
            content=b"alpha",
            original_name="notes.md",
            visibility_scope="private",
        )
    with pytest.raises(ValueError, match="require session_id"):
        await service.add_file(
            content=b"alpha",
            original_name="notes.md",
            visibility_scope="session",
        )

    monkeypatch.setattr(file_rag_module, "MAX_FILE_SIZE", 3)
    with pytest.raises(ValueError, match="50 MB"):
        await service.add_file(content=b"four", original_name="notes.md")

    assert postgres.rows == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_list_files_normalizes_database_timestamps(filerag):
    service, postgres, _ = filerag
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)
    postgres.rows.append(
        {
            "file_id": "a785ecfe-b738-4a43-9e6d-bbdc3f831b20",
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

    files = await service.list_files()

    assert files[0]["created_at"] == timestamp.isoformat()
    assert files[0]["updated_at"] == timestamp.isoformat()
    assert "storage_key" not in files[0]


@pytest.mark.storage
@pytest.mark.no_network
async def test_file_rag_search_remains_empty(filerag):
    service, _, _ = filerag

    assert await service.search("alpha") == []
