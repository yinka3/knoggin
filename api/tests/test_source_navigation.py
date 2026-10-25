import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID

import httpx

from app import create_app


def test_source_lists_continue_with_cursor_and_search_stays_scoped(monkeypatch, tmp_path):
    stamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    documents = [
        {"document_id": str(UUID(int=number)), "project_id": "project-1",
         "created_at": stamp, "relative_path": f"src/{number}.md"}
        for number in (3, 2, 1)
    ]
    links = [
        {"link_id": str(UUID(int=number)), "project_id": "project-1",
         "updated_at": stamp, "url": f"https://example.com/{number}"}
        for number in (3, 2, 1)
    ]
    searches = []
    leases = set()

    class Documents:
        async def list_documents(self, *, limit, path_prefix, before):
            rows = [row for row in documents if path_prefix is None or row["relative_path"].startswith(path_prefix + "/")]
            if before:
                rows = [row for row in rows if (row["created_at"], UUID(row["document_id"])) < before]
            return rows[:limit]

        async def list_saved_web_links(self, *, limit, before):
            rows = links
            if before:
                rows = [row for row in rows if (row["updated_at"], UUID(row["link_id"])) < before]
            return rows[:limit]

        async def search(self, query, **kwargs):
            searches.append((query, kwargs))
            return [{"document_id": documents[0]["document_id"], "project_id": "project-1",
                     "content": "found passage", "score": 0.8, "embedding": [1, 2]}]

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

    async def exercise():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                base = "/v1/projects/project-1"
                for collection, key in (("documents", "document_id"), ("links", "link_id")):
                    seen = []
                    cursor = None
                    while True:
                        params = {"limit": 1}
                        if cursor:
                            params["cursor"] = cursor
                        response = await client.get(f"{base}/{collection}", params=params)
                        assert response.status_code == 200
                        page = response.json()
                        seen.extend(row[key] for row in page["items"])
                        cursor = page["next_cursor"]
                        if cursor is None:
                            break
                    assert seen == [str(UUID(int=number)) for number in (3, 2, 1)]
                assert (await client.get(base + "/documents", params={"cursor": "bad"})).status_code == 422
                assert (await client.get(base + "/links", params={"cursor": "bad"})).status_code == 422
                filtered = await client.get(base + "/documents", params={"path_prefix": "src"})
                assert len(filtered.json()["items"]) == 3
                found = await client.get(base + "/documents/search", params={
                    "query": "passage", "limit": 2, "path_prefix": "src",
                })
                assert found.status_code == 200
                assert found.json()["items"][0]["content"] == "found passage"
                assert "embedding" not in found.json()["items"][0]
                assert searches == [("passage", {
                    "n_results": 2, "document_filter": None,
                    "relative_path": None, "path_prefix": "src",
                })]
                assert (await client.get(base + "/documents/search", params={"query": ""})).status_code == 422
                assert not leases

    asyncio.run(exercise())
