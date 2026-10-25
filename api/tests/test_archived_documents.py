import asyncio
from types import SimpleNamespace

import httpx

from app import create_app


def test_archived_documents_and_links_remain_readable_without_a_runtime(monkeypatch, tmp_path):
    status = {"project-1": "archived", "project-2": "active", "project-3": "deleted"}
    leases = set()
    archived_reads = []

    class Documents:
        async def list_documents(self, *, limit, path_prefix, before):
            return [{"document_id": "doc-1", "project_id": "project-1", "original_name": "a.txt"}]

        async def search(self, query, *, n_results, document_filter, relative_path, path_prefix):
            return [{"document_id": "doc-1", "project_id": "project-1",
                     "relative_path": "a.txt", "content": query}]

        async def get_document_info(self, *, document_id):
            return {"document_id": document_id, "project_id": "project-1"}

        async def read_document(self, *, document_id, page_number, start_line, end_line):
            if document_id == "pending":
                raise RuntimeError("Document has not been indexed")
            if page_number == 4:
                raise ValueError("page_number 4 exceeds document page count 3")
            return {"document_id": document_id, "content": "1: hello", "start_line": start_line,
                    "page_number": page_number, "total_pages": 3}

        async def list_saved_web_links(self, *, limit, before):
            return [{"link_id": "link-1", "project_id": "project-1", "url": "https://example.com"}]

    class Projects:
        async def get_project(self, project_id):
            return {"status": status[project_id]} if project_id in status else None

        async def archived_document_reader(self, project_id):
            assert status[project_id] == "archived"
            archived_reads.append(project_id)
            return Documents()

        async def acquire_project_for_session(self, project_id, lease_id):
            assert status[project_id] == "active"
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
                base = "/v1/projects/project-1"
                for path in ("/documents", "/documents/doc-1", "/documents/doc-1/content", "/links"):
                    assert (await client.get(base + path)).status_code == 200
                    assert (await client.get("/v1/projects/project-2" + path)).status_code == 200
                    assert (await client.get("/v1/projects/project-3" + path)).status_code == 404
                assert (await client.get(base + "/documents/search?query=hello")).json()["items"][0]["content"] == "hello"
                page = await client.get(base + "/documents/doc-1/content?page_number=2")
                assert page.json()["page_number"] == 2
                assert page.json()["total_pages"] == 3
                assert (await client.get(base + "/documents/doc-1/content?page_number=0")).status_code == 422
                assert (await client.get(base + "/documents/doc-1/content?page_number=4")).json()["error"]["code"] == "invalid_read_range"
                assert (await client.get(base + "/documents/pending/content")).json()["error"]["code"] == "document_not_ready"
                assert (await client.post(base + "/links", json={"url": "https://example.com"})).status_code == 409
                assert (await client.delete(base + "/documents/doc-1")).status_code == 409
                assert archived_reads == ["project-1"] * 8
                assert not leases

    asyncio.run(exercise())
