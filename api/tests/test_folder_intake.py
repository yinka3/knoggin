import asyncio
import base64
from types import SimpleNamespace

import httpx

from app import create_app
from common.schema.document import FolderScanSettings
from core.knowledge.documents.scanning import build_folder_preview


def test_folder_preview_and_intake_apply_scan_rules_before_admission(monkeypatch, tmp_path):
    leases = set()
    saved = []
    status = "active"

    class Documents:
        async def preview_folder(self, *, folder_name, entries, force_include_paths):
            assert leases
            return build_folder_preview(
                folder_name, entries, FolderScanSettings(), force_include_paths,
            )

        async def submit_document(self, **kwargs):
            assert leases
            saved.append(kwargs)
            return {"document_id": f"doc-{len(saved)}", "project_id": "project-1",
                    "original_name": kwargs["original_name"],
                    "relative_path": kwargs["relative_path"], "status": "queued"}

    class Projects:
        async def get_project(self, project_id):
            return {"status": status} if project_id == "project-1" else None

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
    base = "/v1/projects/project-1/folders"
    files = [
        (r"repo\src\notes.md", b"source notes"),
        ("repo/tests/notes.md", b"test notes"),
        ("repo/.secret.md", b"hidden notes"),
    ]
    body = {"folder_name": "repo", "files": [
        {"relative_path": path, "content_base64": base64.b64encode(content).decode()}
        for path, content in files
    ]}

    async def exercise():
        nonlocal status
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                preview = await client.post(base + "/preview", json=body)
                assert preview.status_code == 200
                assert [row["relative_path"] for row in preview.json()["included"]] == [
                    "repo/src/notes.md", "repo/tests/notes.md",
                ]
                assert preview.json()["excluded"][0]["relative_path"] == "repo/.secret.md"
                assert not saved

                intake = await client.post(base + "/intake", json=body)
                assert intake.status_code == 200
                assert [row["relative_path"] for row in intake.json()["accepted"]] == [
                    "repo/src/notes.md", "repo/tests/notes.md",
                ]
                assert [row["content"] for row in saved] == [b"source notes", b"test notes"]
                assert not leases

                duplicate = {**body, "files": [body["files"][0], {
                    **body["files"][0], "relative_path": "repo/src/notes.md",
                }]}
                assert (await client.post(base + "/intake", json=duplicate)).status_code == 422
                assert len(saved) == 2
                status = "archived"
                assert (await client.post(base + "/preview", json=body)).status_code == 409
                assert (await client.post(base + "/intake", json=body)).status_code == 409

    asyncio.run(exercise())


def test_staged_folder_uses_one_preview_across_separate_uploads(monkeypatch, tmp_path):
    saved = []
    leases = set()
    failed_once = False

    class Documents:
        async def get_scan_settings(self):
            return FolderScanSettings()

        async def preview_folder(self, *, folder_name, entries, settings,
                                 force_include_paths, content_loader):
            assert leases
            return build_folder_preview(
                folder_name, entries, settings, force_include_paths,
                content_loader=content_loader,
            )

        async def submit_document(self, **kwargs):
            nonlocal failed_once
            assert leases
            if kwargs["relative_path"] == "repo/sub/second.md" and not failed_once:
                failed_once = True
                raise ValueError("Synthetic admission failure")
            saved.append(kwargs)
            return {"document_id": f"doc-{len(saved)}", "project_id": "project-1",
                    "original_name": kwargs["original_name"],
                    "relative_path": kwargs["relative_path"], "status": "queued"}

    class Projects:
        async def get_project(self, project_id):
            return {"status": "active"} if project_id == "project-1" else None

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
    files = [
        ("repo/sub/first.md", b"First source"),
        ("repo/ignored.txt", b"Ignore me"),
        ("repo/.gitignore", b"ignored.txt\n"),
        ("repo/sub/second.md", b"Second source"),
    ]
    base = "/v1/projects/project-1/folders/intakes"

    async def exercise():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                created = await client.post(base, json={
                    "folder_name": "repo",
                    "files": [{"relative_path": path, "size_bytes": len(content)}
                              for path, content in files],
                })
                assert created.status_code == 201, created.text
                intake = created.json()
                url = base + "/" + intake["intake_id"]
                assert (await client.post(url + "/preview")).status_code == 409
                for item, (_path, content) in zip(intake["files"], files, strict=True):
                    upload = url + "/files/" + item["file_id"]
                    assert (await client.put(upload, content=content)).status_code == 200
                    assert (await client.put(upload, content=content)).status_code == 200
                assert (await client.put(url + "/files/0", content=b"Other source")).status_code == 409
                preview = await client.post(url + "/preview")
                assert preview.status_code == 200, preview.text
                assert (await client.post(url + "/preview")).json() == preview.json()
                assert [item["relative_path"] for item in preview.json()["included"]] == [
                    "repo/sub/first.md", "repo/sub/second.md",
                ]
                assert not saved
                assert (await client.put(url + "/files/0", content=b"First source")).status_code == 409
                first = await client.post(url + "/commit", json={})
                assert first.status_code == 200, first.text
                assert len(saved) == 1
                second = await client.post(url + "/commit", json={})
                assert second.status_code == 200, second.text
                assert second.json()["status"] == "complete"
                assert second.json()["files"][-1]["status"] == "failed"
                retried = await client.post(url + "/commit", json={"retry_failed": True})
                assert retried.status_code == 200, retried.text
                assert retried.json()["files"][-1]["status"] == "accepted"
                assert [item["relative_path"] for item in saved] == [
                    "repo/sub/first.md", "repo/sub/second.md",
                ]
                assert (await client.get(url)).json()["status"] == "complete"
                assert (await client.delete(url)).status_code == 200
                assert (await client.get(url)).status_code == 404
                assert not leases

    asyncio.run(exercise())
