import asyncio
import os
import threading
from contextlib import asynccontextmanager

import pytest

from core.knowledge.documents.filesystem import (
    ProjectFilesystem,
    ProjectFilesystemFactory,
)
from core.project.project_files import PROJECT_FILE_PATH, build_project_markdown
from core.project.project_manager import ProjectManager
from tests.fixtures.factories import make_domain_config


class RecordingPostgres:
    def __init__(self, *, fail_on_project_insert=False):
        self.calls = []
        self.transactions = []
        self.fail_on_project_insert = fail_on_project_insert

    @asynccontextmanager
    async def transaction(self):
        transaction = []
        self.transactions.append(transaction)
        try:
            yield RecordingCursor(self, transaction)
        except Exception:
            transaction.append(("rollback",))
            raise
        else:
            transaction.append(("commit",))

    async def fetch_all(self, query, params=None):
        self.calls.append((query, params))
        project_id = (params or {}).get("project_id", "project-1")
        return [
            {
                "project_id": project_id,
                "user_name": "ada",
                "name": "Research",
                "description": "A project description",
                "status": "active",
                "domain_config": {},
                "allowed_projects": [],
                "session_count": 0,
                "created_at": None,
                "updated_at": None,
                "archived_at": None,
                "deleted_at": None,
                "last_activity_at": None,
            }
        ]


class RecordingCursor:
    def __init__(self, postgres, transaction):
        self.postgres = postgres
        self.transaction = transaction

    async def execute(self, query, params=None):
        self.transaction.append((query, params))
        self.postgres.calls.append((query, params))
        if self.postgres.fail_on_project_insert and "INSERT INTO public.projects" in query:
            raise RuntimeError("project insert failed")


class Resources:
    def __init__(self, postgres):
        self.postgres = postgres

    async def get_vp01(self, _language):
        return type("VP01", (), {"extract_entities": lambda *_args, **_kwargs: []})()


def _manager_without_filesystem() -> ProjectManager:
    return ProjectManager(
        resources=Resources(RecordingPostgres()),
        user_name="ada",
    )


@pytest.mark.asyncio
async def test_project_creation_requires_configured_filesystem():
    with pytest.raises(RuntimeError, match="filesystem is not configured"):
        await _manager_without_filesystem().create_project(
            "Research",
            domain_config=make_domain_config(version=0),
        )


@pytest.mark.asyncio
async def test_project_file_cleanup_requires_configured_filesystem():
    with pytest.raises(RuntimeError, match="filesystem is not configured"):
        await _manager_without_filesystem()._finish_project_file_cleanup("project-1")


@pytest.mark.asyncio
async def test_project_creation_seeds_native_project_file(tmp_path):
    postgres = RecordingPostgres()
    filesystem_factory = ProjectFilesystemFactory(tmp_path / "projects")
    manager = ProjectManager(
        resources=Resources(postgres),
        user_name="ada",
        filesystem_factory=filesystem_factory,
    )

    await manager.create_project(
        "Research",
        domain_config=make_domain_config(version=0),
        description="A project description",
    )

    transaction = postgres.transactions[0]
    assert transaction[-1] == ("commit",)
    queries = [entry[0] for entry in transaction if entry[0] != "rollback"]
    assert any("INSERT INTO public.projects" in query for query in queries)
    assert not any("INSERT INTO public.project_documents" in query for query in queries)
    created_project_id = transaction[0][1]["project_id"]
    assert (
        filesystem_factory.for_project(created_project_id).read_bytes(PROJECT_FILE_PATH)
        == build_project_markdown("Research", "A project description").encode("utf-8")
    )


@pytest.mark.asyncio
async def test_project_creation_removes_native_project_file_when_database_insert_fails(
    tmp_path,
):
    postgres = RecordingPostgres(fail_on_project_insert=True)
    filesystem_factory = ProjectFilesystemFactory(tmp_path / "projects")
    manager = ProjectManager(
        resources=Resources(postgres),
        user_name="ada",
        filesystem_factory=filesystem_factory,
    )

    with pytest.raises(RuntimeError, match="project insert failed"):
        await manager.create_project(
            "Research",
            domain_config=make_domain_config(version=0),
        )

    assert postgres.transactions[0][-1] == ("rollback",)
    project_id = postgres.transactions[0][0][1]["project_id"]
    assert not filesystem_factory.for_project(project_id).root.joinpath(
        PROJECT_FILE_PATH
    ).exists()
    assert not filesystem_factory.for_project(project_id).root.exists()


@pytest.mark.asyncio
async def test_cancelled_project_creation_removes_file_after_worker_settles(
    tmp_path, monkeypatch
):
    postgres = RecordingPostgres()
    filesystem_factory = ProjectFilesystemFactory(tmp_path / "projects")
    manager = ProjectManager(
        resources=Resources(postgres),
        user_name="ada",
        filesystem_factory=filesystem_factory,
    )
    written = threading.Event()
    release = threading.Event()
    original_write = ProjectFilesystem.write_bytes

    def delayed_write(self, *args, **kwargs):
        result = original_write(self, *args, **kwargs)
        written.set()
        assert release.wait(10)
        return result

    monkeypatch.setattr(ProjectFilesystem, "write_bytes", delayed_write)
    task = asyncio.create_task(
        manager.create_project("Research", domain_config=make_domain_config(version=0))
    )
    try:
        assert await asyncio.to_thread(written.wait, 10)
        task.cancel()
        await asyncio.sleep(0)
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    project_directories = [
        path for path in filesystem_factory.library_root.iterdir()
        if path.is_dir() and path.name != ".knoggin-file-locks"
    ]
    assert project_directories == []
    assert postgres.transactions == []


@pytest.mark.asyncio
async def test_cancelled_project_cleanup_waits_for_file_worker(tmp_path, monkeypatch):
    factory = ProjectFilesystemFactory(tmp_path / "projects")
    filesystem = factory.for_project("project-1")
    filesystem.write_bytes("notes.md", b"owned")
    manager = ProjectManager(
        resources=Resources(RecordingPostgres()),
        user_name="ada",
        filesystem_factory=factory,
    )
    entered = threading.Event()
    release = threading.Event()
    original_remove = factory.remove_project_directory

    def delayed_remove(project_id, **kwargs):
        entered.set()
        assert release.wait(10)
        return original_remove(project_id, **kwargs)

    monkeypatch.setattr(factory, "remove_project_directory", delayed_remove)
    async with manager._project_file_lease("project-1") as lease:
        cleanup = asyncio.create_task(
            manager._finish_project_file_cleanup("project-1", lease=lease)
        )
        try:
            assert await asyncio.to_thread(entered.wait, 10)
            cleanup.cancel()
            await asyncio.sleep(0)
            assert not cleanup.done()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await cleanup
    assert not filesystem.root.exists()


@pytest.mark.asyncio
async def test_project_cleanup_keeps_retry_pending_after_filesystem_failure(
    tmp_path, monkeypatch
):
    factory = ProjectFilesystemFactory(tmp_path / "projects")
    filesystem = factory.for_project("project-1")
    filesystem.write_bytes("notes.md", b"owned")
    manager = ProjectManager(
        resources=Resources(RecordingPostgres()),
        user_name="ada",
        filesystem_factory=factory,
    )

    def unavailable_removal(_project_id, **_kwargs):
        raise OSError("storage unavailable")

    monkeypatch.setattr(factory, "remove_project_directory", unavailable_removal)
    assert await manager._finish_project_file_cleanup("project-1") == "pending"
    assert filesystem.root.joinpath("notes.md").exists()


@pytest.mark.asyncio
async def test_project_cleanup_keeps_retry_pending_after_completion_failure(
    tmp_path, monkeypatch
):
    factory = ProjectFilesystemFactory(tmp_path / "projects")
    filesystem = factory.for_project("project-1")
    filesystem.write_bytes("notes.md", b"owned")
    manager = ProjectManager(
        resources=Resources(RecordingPostgres()),
        user_name="ada",
        filesystem_factory=factory,
    )

    async def unavailable_completion(**_kwargs):
        raise RuntimeError("catalog unavailable")

    monkeypatch.setattr(
        manager._project_deletion_writer,
        "complete_file_cleanup",
        unavailable_completion,
    )
    assert await manager._finish_project_file_cleanup("project-1") == "pending"
    assert not filesystem.root.exists()


@pytest.mark.asyncio
async def test_project_cleanup_retry_processes_pending_ids(tmp_path, monkeypatch):
    manager = ProjectManager(
        resources=Resources(RecordingPostgres()),
        user_name="ada",
        filesystem_factory=ProjectFilesystemFactory(tmp_path / "projects"),
    )
    attempts = []

    async def pending_ids(**_kwargs):
        return ["project-1", "project-2"]

    async def finish(project_id):
        attempts.append(project_id)
        return "complete"

    monkeypatch.setattr(
        manager._project_deletion_writer,
        "list_pending_file_cleanup_project_ids",
        pending_ids,
    )
    monkeypatch.setattr(manager, "_finish_project_file_cleanup", finish)
    await manager.retry_pending_project_file_cleanup()
    assert attempts == ["project-1", "project-2"]


@pytest.mark.asyncio
@pytest.mark.skipif(os.name == "nt", reason="POSIX symlink root")
async def test_project_file_lease_skips_a_linked_project_root(tmp_path):
    library = tmp_path / "projects"
    library.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep.md").write_bytes(b"private")
    (library / "project-1").symlink_to(outside, target_is_directory=True)
    manager = ProjectManager(
        resources=Resources(RecordingPostgres()),
        user_name="ada",
        filesystem_factory=ProjectFilesystemFactory(library),
    )

    async with manager._project_file_lease("project-1") as lease:
        assert lease is None
    assert (outside / "keep.md").read_bytes() == b"private"


@pytest.mark.asyncio
async def test_failed_creation_cleanup_does_not_hide_original_error(
    tmp_path, monkeypatch
):
    factory = ProjectFilesystemFactory(tmp_path / "projects")
    manager = ProjectManager(
        resources=Resources(RecordingPostgres()),
        user_name="ada",
        filesystem_factory=factory,
    )

    def unavailable_removal(_project_id):
        raise OSError("storage unavailable")

    monkeypatch.setattr(factory, "remove_project_directory", unavailable_removal)
    await manager._remove_failed_project_directory("project-1")


def test_project_markdown_seed_contains_trusted_metadata_and_is_utf8():
    content = build_project_markdown("Research", "A project description")
    assert content.startswith("# Research\n")
    assert "A project description" in content
    assert content.endswith("\n")
    content.encode("utf-8")
