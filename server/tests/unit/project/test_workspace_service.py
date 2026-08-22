
import pytest

from common.exceptions import WorkspaceConflictError
from core.project.workspace_service import ProjectWorkspaceService


class FakeWorkspaceReader:
    def __init__(self):
        self.source = {
            "source_id": "source-1",
            "project_id": "project-1",
            "visibility_scope": "project",
            "ownership_mode": "managed_project_workspace",
            "display_name": "Project Workspace",
        }
        self.files = {}

    async def fetch_managed_workspace_source(self):
        return self.source

    async def list_managed_workspace_documents(self, *, path_prefix, limit):
        rows = list(self.files.values())
        if path_prefix is not None:
            rows = [
                row
                for row in rows
                if row["relative_path"] == path_prefix
                or row["relative_path"].startswith(path_prefix + "/")
            ]
        return rows[:limit]

    async def fetch_managed_workspace_file(self, *, relative_path):
        row = self.files.get(relative_path)
        if row is None:
            return None
        return {**row, "content": row["_content"]}


class FakeWorkspaceWriter:
    def __init__(self, reader):
        self.reader = reader
        self.queued = []

    async def insert_managed_workspace_file(self, **kwargs):
        path = kwargs["relative_path"]
        if path in self.reader.files:
            raise FileExistsError("duplicate")
        content = kwargs["content"]
        row = {
            "document_id": f"document-{len(self.reader.files) + 1}",
            "project_id": "project-1",
            "visibility_scope": "project",
            "source_kind": "workspace",
            "original_name": kwargs["original_name"],
            "relative_path": path,
            "extension": kwargs["extension"],
            "size_bytes": len(content),
            "content_hash": kwargs["content_hash"],
            "status": "queued",
            "created_at": kwargs["updated_at"],
            "updated_at": kwargs["updated_at"],
            "_content": content,
        }
        self.reader.files[path] = row
        return {key: value for key, value in row.items() if not key.startswith("_")}

    async def insert_managed_workspace_source(self, **kwargs):
        self.reader.source = {
            "source_id": kwargs["source_id"],
            "project_id": "project-1",
            "visibility_scope": "project",
            "ownership_mode": "managed_project_workspace",
            "display_name": kwargs["display_name"],
        }

    async def update_managed_workspace_file(self, **kwargs):
        row = self.reader.files.get(kwargs["relative_path"])
        if row is None:
            raise FileNotFoundError
        if row["content_hash"] != kwargs["expected_content_hash"]:
            raise WorkspaceConflictError()
        content = kwargs["content"]
        row.update(
            {
                "size_bytes": len(content),
                "content_hash": kwargs["content_hash"],
                "updated_at": kwargs["updated_at"],
                "status": "queued",
                "_content": content,
            }
        )
        return {key: value for key, value in row.items() if not key.startswith("_")}

    async def append_managed_workspace_file(self, **kwargs):
        row = self.reader.files.get(kwargs["relative_path"])
        if row is None:
            raise FileNotFoundError
        if row["content_hash"] != kwargs["expected_content_hash"]:
            raise WorkspaceConflictError()
        content = row["_content"] + kwargs["append_content"]
        row.update(
            {
                "size_bytes": len(content),
                "content_hash": __import__("hashlib").sha256(content).hexdigest(),
                "updated_at": kwargs["updated_at"],
                "status": "queued",
                "_content": content,
            }
        )
        return {key: value for key, value in row.items() if not key.startswith("_")}


class FakeWorkspaceIndexer:
    def __init__(self):
        self.queue_calls = []

    def queue_workspace_source_indexing(self, **kwargs):
        self.queue_calls.append(kwargs)


@pytest.fixture
def workspace():
    reader = FakeWorkspaceReader()
    writer = FakeWorkspaceWriter(reader)
    indexer = FakeWorkspaceIndexer()
    return ProjectWorkspaceService(
        project_id="project-1",
        reader=reader,
        writer=writer,
        indexer=indexer,
    )


@pytest.mark.asyncio
async def test_managed_workspace_normalizes_paths_and_rejects_duplicates(workspace):
    with pytest.raises(ValueError, match="must not escape"):
        await workspace.create_file("../README.md", "bad")

    first = await workspace.create_file("docs\\README.md", "one")
    assert first["relative_path"] == "docs/README.md"
    with pytest.raises(FileExistsError):
        await workspace.create_file("docs/README.md", "two")
    assert len(workspace._indexer.queue_calls) == 1


@pytest.mark.asyncio
async def test_workspace_creates_its_managed_source_through_the_writer():
    reader = FakeWorkspaceReader()
    reader.source = None
    writer = FakeWorkspaceWriter(reader)
    workspace = ProjectWorkspaceService(
        project_id="project-1",
        reader=reader,
        writer=writer,
        indexer=FakeWorkspaceIndexer(),
    )

    source = await workspace.ensure_source(" Team Workspace ")

    assert source["project_id"] == "project-1"
    assert source["display_name"] == "Team Workspace"
    assert reader.source["source_id"] == source["source_id"]


@pytest.mark.asyncio
async def test_stale_update_does_not_queue_and_current_append_does(workspace):
    created = await workspace.create_file("notes.md", "one")
    with pytest.raises(WorkspaceConflictError):
        await workspace.update_file(
            "notes.md", "stale", expected_content_hash="wrong-hash"
        )
    assert len(workspace._indexer.queue_calls) == 1

    updated = await workspace.append_file(
        "notes.md",
        " two",
        expected_content_hash=created["content_hash"],
    )
    assert updated["size_bytes"] == len(b"one two")
    assert len(workspace._indexer.queue_calls) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["report.pdf", "image.png", "notes.docx"])
async def test_managed_workspace_rejects_rich_document_and_image_writes(
    workspace, path
):
    with pytest.raises(ValueError, match="text, code, or configuration"):
        await workspace.create_file(path, "not writable workspace text")


@pytest.mark.asyncio
async def test_read_file_is_bounded(workspace):
    await workspace.create_file("notes.md", "one\ntwo\nthree\n")
    result = await workspace.read_file("notes.md", max_lines=2)
    assert result["content"] == "one\ntwo\n"
    assert result["start_line"] == 1
    assert result["end_line"] == 2
    assert result["truncated"] is True


@pytest.mark.asyncio
async def test_read_project_context_is_direct_and_legacy_missing_is_allowed(workspace):
    assert await workspace.read_project_context() is None
    await workspace.create_file("PROJECT.md", "# Research\nUse direct context.\n")
    assert await workspace.read_project_context() == (
        "# Research\nUse direct context.\n"
    )
