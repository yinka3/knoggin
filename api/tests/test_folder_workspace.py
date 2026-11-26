import asyncio
import base64
from types import SimpleNamespace

import httpx

from app import create_app
from common.schema.document import FolderScanSettings
from common.schema.settings import DocumentSettings
from core.knowledge.documents.scanning import build_folder_preview


def test_a_folder_import_can_land_in_a_workspace(monkeypatch, tmp_path):
    saved = []

    class Documents:
        async def preview_folder(self, *, folder_name, entries, force_include_paths):
            return build_folder_preview(folder_name, entries, FolderScanSettings(), force_include_paths)

        async def submit_document(self, **kwargs):
            saved.append(kwargs["relative_path"])
            return {"document_id": f"doc-{len(saved)}", "project_id": "project-1",
                    "original_name": kwargs["original_name"],
                    "relative_path": kwargs["relative_path"], "status": "queued"}

    class Projects:
        async def get_project(self, project_id):
            return {"status": "active", "workspaces": {"api": {"read_only": False}}}

        async def acquire_project_for_session(self, project_id, lease_id):
            return SimpleNamespace(document_service=Documents())

        async def release_project_for_session(self, project_id, lease_id):
            pass

    async def start(**_kwargs):
        async def shutdown():
            pass
        return SimpleNamespace(
            projects=Projects(), shutdown=shutdown, sessions=SimpleNamespace(),
            config_manager=SimpleNamespace(config=SimpleNamespace(
                developer_settings=SimpleNamespace(documents=DocumentSettings())
            )),
        )

    monkeypatch.setattr("app.ApplicationRuntime.start", start)
    app = create_app(user_name="ada", config_dir=tmp_path)
    base = "/v1/projects/project-1/folders"
    body = {"folder_name": "repo", "workspace": "api", "files": [
        {"relative_path": r"repo\src\notes.md",
         "content_base64": base64.b64encode(b"notes").decode()},
    ]}

    async def exercise():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                intake = await client.post(base + "/intake", json=body)
                missing = await client.post(base + "/intake", json={**body, "workspace": "web"})
                staged = await client.post(base + "/intakes", json={
                    "folder_name": "repo", "workspace": "api",
                    "files": [{"relative_path": "repo/a.md", "size_bytes": 3}],
                })
                return intake, missing, staged

    intake, missing, staged = asyncio.run(exercise())
    assert intake.status_code == 200 and saved == ["api/repo/src/notes.md"]
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "workspace_not_found"
    assert [item["relative_path"] for item in staged.json()["files"]] == ["api/repo/a.md"]
