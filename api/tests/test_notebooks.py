import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import httpx

from app import create_app
from core.project.notebook_store import NotebookConflict


def test_notebook_routes_scope_and_revision_conflicts(monkeypatch, tmp_path):
    notebook_id = str(uuid4())
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    project = {"status": "active"}
    row = {
        "notebook_id": notebook_id, "project_id": "project-1", "user_name": "ada",
        "title": "Sources", "view": "source_desk", "entries": [], "revision": 1,
        "created_at": now, "updated_at": now,
    }

    class Notebooks:
        async def list(self, project_id):
            return [row] if project_id == "project-1" else []

        async def get(self, project_id, requested_id):
            return row if project_id == "project-1" and requested_id == notebook_id else None

        async def create(self, project_id, body):
            assert project_id == "project-1" and body.title == "Sources"
            return row

        async def replace(self, project_id, requested_id, body):
            assert project_id == "project-1" and requested_id == notebook_id
            if body.expected_revision != row["revision"]:
                raise NotebookConflict("stale")
            row["revision"] += 1
            row["title"] = body.title
            return row

        async def delete(self, project_id, requested_id, expected_revision):
            assert project_id == "project-1" and requested_id == notebook_id
            if expected_revision != row["revision"]:
                raise NotebookConflict("stale")
            return True

        async def save_finding(self, project_id, requested_id, *, expected_revision, finding):
            assert (project_id, requested_id, expected_revision) == (
                "project-1", notebook_id, row["revision"]
            )
            assert finding.target_id == "42"
            assert finding.answer_excerpt == "Selected answer"
            row["entries"].append({
                **finding.model_dump(mode="json"), "entry_id": str(uuid4()),
                "source_version": "1", "source_versions": {},
            })
            row["revision"] += 1
            return row

    class Projects:
        notebooks = Notebooks()

        async def get_project(self, project_id):
            return project if project_id == "project-1" else None

    async def start(**_kwargs):
        async def shutdown():
            pass
        return SimpleNamespace(projects=Projects(), shutdown=shutdown)

    monkeypatch.setattr("app.ApplicationRuntime.start", start)
    app = create_app(user_name="ada", config_dir=tmp_path)

    async def exercise():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
                base = "/v1/projects/project-1/notebooks"
                assert (await client.get("/v1/projects/other/notebooks")).status_code == 404
                assert (await client.post(base, json={"title": "Sources", "view": "source_desk"})).status_code == 201
                assert (await client.get(base)).json()["notebooks"][0]["revision"] == 1
                assert (await client.get(base + "/" + notebook_id)).status_code == 200
                update = {"title": "Sources updated", "view": "source_desk", "entries": [], "expected_revision": 1}
                assert (await client.put(base + "/" + notebook_id, json=update)).json()["revision"] == 2
                assert (await client.put(base + "/" + notebook_id, json=update)).status_code == 409
                assert (await client.request("DELETE", base + "/" + notebook_id, json={"expected_revision": 1})).status_code == 409
                finding = await client.post(base + "/" + notebook_id + "/findings", json={
                    "expected_revision": 2, "assistant_message_id": 42,
                    "answer_excerpt": "Selected answer", "note": "My note",
                })
                assert finding.status_code == 201
                assert finding.json()["entries"][0]["answer_excerpt"] == "Selected answer"
                project["status"] = "archived"
                assert (await client.get(base + "/" + notebook_id)).status_code == 200
                assert (await client.post(base, json={"title": "Blocked", "view": "timeline"})).status_code == 409

    asyncio.run(exercise())
