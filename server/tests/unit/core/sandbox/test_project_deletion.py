from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from core.knowledge.documents.filesystem import ProjectFilesystemFactory
from core.project.project_manager import ProjectManager


class Postgres:
    async def fetch_all(self, _query, _params=None):
        return [{
            "project_id": "project-1",
            "user_name": "ada",
            "status": "active",
            "allowed_projects": None,
            "workspaces": {},
        }]

    @asynccontextmanager
    async def transaction(self):
        yield self

    async def execute(self, _query, _params=None):
        return 1


class DeletionWriter:
    async def delete_project(self, *, user_name, project_id):
        return {"projects": 1}

    async def complete_file_cleanup(self, *, user_name, project_id):
        pass


@pytest.mark.parametrize("sandbox_fails", [False, True])
async def test_delete_removes_sandbox_after_project_files(tmp_path, sandbox_fails):
    factory = ProjectFilesystemFactory(tmp_path / "projects")
    filesystem = factory.for_project("project-1")
    filesystem.write_bytes("documents/file.txt", b"data")
    manager = ProjectManager(
        resources=SimpleNamespace(postgres=Postgres()),
        user_name="ada",
        filesystem_factory=factory,
    )
    manager._project_deletion_writer = DeletionWriter()
    calls = []

    class Sandbox:
        async def remove_project(self, project_id):
            calls.append((project_id, filesystem.root.exists()))
            if sandbox_fails:
                raise RuntimeError("runtime unavailable")

    manager.attach_sandbox_manager(Sandbox())
    deleted = await manager.delete_project("project-1")
    assert deleted["status"] == "deleted"
    assert calls == [("project-1", False)]
