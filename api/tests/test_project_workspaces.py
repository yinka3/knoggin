import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import httpx

from app import create_app
from common.exceptions import WorkspaceConflictError


def test_workspaces_are_added_removed_and_shown_on_documents(monkeypatch, tmp_path):
    workspaces = []
    folders = []

    class Documents:
        async def create_project_folder(self, path):
            if path == "notes.md":
                raise NotADirectoryError(path)
            folders.append(path)
            return {"relative_path": path}

        async def list_documents(self, **options):
            stamp = datetime(2026, 10, 27, tzinfo=timezone.utc)
            return [
                {"document_id": "d1", "relative_path": "backend/main.py", "created_at": stamp},
                {"document_id": "d2", "relative_path": "PROJECT.md", "created_at": stamp},
                {"document_id": "d3", "relative_path": "other/x.md", "created_at": stamp},
            ]

    class Projects:
        async def get_project(self, project_id):
            if project_id != "p1":
                return None
            return {"id": "p1", "name": "Repos", "status": "active", "workspaces": workspaces}

        async def add_workspace(self, project_id, name):
            if name in workspaces or len(workspaces) == 2:
                raise ValueError("unavailable")
            workspaces.append(name)
            return list(workspaces)

        async def remove_workspace(self, project_id, name):
            if name == "backend":
                raise WorkspaceConflictError("has documents")
            if name not in workspaces:
                return None
            workspaces.remove(name)
            return list(workspaces)

        async def acquire_project_for_session(self, project_id, lease_id):
            return SimpleNamespace(document_service=Documents())

        async def release_project_for_session(self, project_id, lease_id):
            pass

    async def start(**_kwargs):
        async def shutdown():
            pass
        return SimpleNamespace(projects=Projects(), shutdown=shutdown)

    monkeypatch.setattr("app.ApplicationRuntime.start", start)
    app = create_app(user_name="ada", config_dir=tmp_path)

    async def exercise():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                base = "/v1/projects/p1/workspaces"
                added = await client.post(base, json={"name": "backend"})
                assert added.status_code == 201
                assert added.json() == {"workspaces": ["backend"]}
                assert folders == ["backend"]
                assert (await client.post(base, json={"name": "a/b"})).status_code == 422
                assert (await client.post(base, json={"name": ".git"})).status_code == 422
                clash = await client.post(base, json={"name": "notes.md"})
                assert clash.status_code == 409 and workspaces == ["backend"]
                await client.post(base, json={"name": "frontend"})
                full = await client.post(base, json={"name": "infra"})
                assert full.json()["error"]["code"] == "workspace_unavailable"

                project = (await client.get("/v1/projects/p1")).json()
                assert project["workspaces"] == ["backend", "frontend"]
                listed = (await client.get("/v1/projects/p1/documents")).json()["items"]
                assert [item["workspace"] for item in listed] == ["backend", None, None]

                busy = await client.delete(base + "/backend")
                assert busy.json()["error"]["code"] == "workspace_not_empty"
                assert (await client.delete(base + "/frontend")).json() == {
                    "workspaces": ["backend"]
                }
                assert (await client.delete(base + "/frontend")).status_code == 404

    asyncio.run(exercise())
