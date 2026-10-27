import asyncio
from types import SimpleNamespace

import httpx

from app import create_app
from common.exceptions import WorkspaceConflictError


def test_workspace_writes_use_file_owner_and_release_lease(monkeypatch, tmp_path):
    leases = set()
    calls = []
    content_hash = "a" * 64

    class Documents:
        async def list_project_files(self, **options):
            return [{"relative_path": "PROJECT.md", "content_hash": content_hash}]

        async def read_project_file(self, path, **options):
            calls.append(("read", path, options))
            if path.casefold() == "context.md":
                raise ValueError("controlled Context file")
            return {"relative_path": path, "content": "hello", "content_hash": content_hash}

        async def update_project_file(self, path, content, **options):
            assert leases
            calls.append(("update", path, content, options))
            if options["expected_content_hash"] != content_hash:
                raise WorkspaceConflictError("stale")
            return {"relative_path": path, "content_hash": "b" * 64}

    class Projects:
        async def get_project(self, project_id):
            return {"status": "active", "workspaces": []} if project_id == "p1" else None

        async def acquire_project_for_session(self, project_id, lease_id):
            leases.add(lease_id)
            return SimpleNamespace(document_service=Documents())

        async def release_project_for_session(self, project_id, lease_id):
            leases.remove(lease_id)

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
                base = "/v1/projects/p1/workspace"
                assert (await client.get(base + "/files")).status_code == 200
                assert (await client.get(base + "/file", params={
                    "path": "CONTEXT.md"
                })).status_code == 422
                updated = await client.put(base + "/file", json={
                    "path": "PROJECT.md", "content": "updated",
                    "expected_content_hash": content_hash,
                })
                assert updated.status_code == 200
                stale = await client.put(base + "/file", json={
                    "path": "PROJECT.md", "content": "updated",
                    "expected_content_hash": "c" * 64,
                })
                assert stale.status_code == 409
                assert stale.json()["error"]["code"] == "workspace_conflict"
                assert (await client.put(base + "/file", json={
                    "path": "PROJECT.md", "content": "updated"
                })).status_code == 422
                assert (await client.get("/v1/projects/missing/workspace/files")).status_code == 404
                assert not leases

    asyncio.run(exercise())
    assert ("update", "PROJECT.md", "updated", {
        "expected_content_hash": content_hash,
    }) in calls


def test_archived_workspace_reads_without_starting_project(monkeypatch, tmp_path):
    class Reader:
        async def list_project_files(self, **_options):
            return [{"relative_path": "PROJECT.md"}]

        async def read_project_file(self, path, **_options):
            return {"relative_path": path, "content": "Archived brief"}

    class Projects:
        async def get_project(self, project_id):
            return {"status": "archived", "workspaces": []} if project_id == "p1" else None

        async def archived_document_reader(self, project_id):
            assert project_id == "p1"
            return Reader()

        async def acquire_project_for_session(self, *_args):
            raise AssertionError("Archived project runtime must not start")

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
                base = "/v1/projects/p1/workspace"
                assert (await client.get(base + "/files")).json()["items"] == [
                    {"relative_path": "PROJECT.md", "workspace": None}
                ]
                assert (await client.get(base + "/file", params={
                    "path": "PROJECT.md"
                })).json()["content"] == "Archived brief"
                assert (await client.put(base + "/file", json={
                    "path": "PROJECT.md", "content": "No",
                    "expected_content_hash": "a" * 64,
                })).status_code == 409

    asyncio.run(exercise())
