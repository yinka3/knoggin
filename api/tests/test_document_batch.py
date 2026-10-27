import asyncio
import base64
from types import SimpleNamespace

import httpx

from app import create_app
from common.schema.settings import DocumentSettings


def test_batch_reports_each_result_and_releases_lease(monkeypatch, tmp_path):
    active_leases = set()
    submitted = []

    class Documents:
        async def submit_document(self, **kwargs):
            assert active_leases
            submitted.append(kwargs)
            return {"document_id": "doc-1", "project_id": "project-1",
                    "original_name": "a.txt", "status": "queued"}

        async def save_web_link(self, **kwargs):
            assert active_leases
            raise ValueError("Private backend detail")

    class Projects:
        async def get_project(self, project_id):
            return {"status": "active"} if project_id == "project-1" else None

        async def acquire_project_for_session(self, project_id, lease_id):
            active_leases.add(lease_id)
            return SimpleNamespace(document_service=Documents())

        async def release_project_for_session(self, project_id, lease_id):
            active_leases.remove(lease_id)

    async def start(**_kwargs):
        async def shutdown():
            pass

        return SimpleNamespace(
            projects=Projects(), shutdown=shutdown,
            config_manager=SimpleNamespace(config=SimpleNamespace(
                developer_settings=SimpleNamespace(documents=DocumentSettings())
            )),
        )

    monkeypatch.setattr("app.ApplicationRuntime.start", start)
    app = create_app(user_name="ada", config_dir=tmp_path)

    async def exercise():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                result = await client.post("/v1/projects/project-1/sources/batch", json={
                    "items": [
                        {"source_type": "document", "original_name": "a.txt",
                         "content_base64": base64.b64encode(b"text").decode()},
                        {"source_type": "web_link", "url": "https://example.com"},
                    ]
                })
                assert result.status_code == 200
                rows = result.json()["results"]
                assert [row["status"] for row in rows] == ["accepted_document", "failed"]
                assert "Private backend detail" not in str(rows)
                assert submitted[0]["content"] == b"text"
                assert not active_leases
                app.state.runtime.config_manager.config.developer_settings.documents = (
                    DocumentSettings(max_source_batch_items=1)
                )
                oversized = await client.post("/v1/projects/project-1/sources/batch", json={
                    "items": [
                        {"source_type": "web_link", "url": "https://example.com/1"},
                        {"source_type": "web_link", "url": "https://example.com/2"},
                    ]
                })
                assert oversized.status_code == 413
                assert oversized.json()["error"]["code"] == "batch_too_large"
                assert (await client.post("/v1/documents/ocr-preflight", json={
                    "original_name": "a.txt", "content_base64": "dGV4dA==",
                })).status_code == 422

    asyncio.run(exercise())


def test_source_promotion_scopes_lookup_and_releases_lease(monkeypatch, tmp_path):
    leases = set()
    lookups = []

    class Documents:
        async def promote_source(self, source, **kwargs):
            assert leases
            return {"link_id": "link-1", "project_id": "project-1",
                    "url": "https://example.com", "title": kwargs["title"]}

    class Projects:
        status = "active"

        async def get_project(self, project_id):
            return {"status": self.status} if project_id == "project-1" else None

        async def acquire_project_for_session(self, project_id, lease_id):
            leases.add(lease_id)
            return SimpleNamespace(document_service=Documents())

        async def release_project_for_session(self, project_id, lease_id):
            leases.remove(lease_id)

    class Store:
        async def get_source_reference(self, source_id, **scope):
            assert leases
            lookups.append((source_id, scope))
            return SimpleNamespace()

    projects = Projects()

    async def start(**_kwargs):
        async def shutdown():
            pass

        return SimpleNamespace(
            projects=projects, resources=SimpleNamespace(knowledge_store=Store()),
            sessions=SimpleNamespace(user_name="ada"), shutdown=shutdown,
        )

    monkeypatch.setattr("app.ApplicationRuntime.start", start)
    app = create_app(user_name="ada", config_dir=tmp_path)

    async def exercise():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                url = "/v1/projects/project-1/sources/promote"
                body = {"session_id": "session-1", "source_ref_id": "source-1", "title": "Saved"}
                projects.status = "archived"
                assert (await client.post(url, json=body)).status_code == 409
                assert not lookups
                projects.status = "active"
                result = await client.post(url, json=body)
                assert result.status_code == 201
                assert result.json()["link_id"] == "link-1"
                assert lookups == [("source-1", {
                    "user_name": "ada", "project_id": "project-1", "session_id": "session-1",
                })]
                assert not leases

    asyncio.run(exercise())


def test_public_document_rows_hide_internal_index_failure_detail():
    from routes.documents import DOCUMENT_FIELDS, _public

    row = {
        "document_id": "doc-1", "status": "failed",
        "last_index_failure_kind": "invalid_content",
        "error_message": "private damaged PDF detail",
    }

    public = _public(row, DOCUMENT_FIELDS)

    assert public["last_index_failure_kind"] == "invalid_content"
    assert "error_message" not in public
