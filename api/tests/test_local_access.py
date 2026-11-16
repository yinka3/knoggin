import asyncio
from types import SimpleNamespace

import httpx

from app import create_app


def test_other_sites_and_rebound_hosts_cannot_drive_the_api(monkeypatch, tmp_path):
    monkeypatch.delenv("KNOGGIN_ALLOWED_HOSTS")  # the real default
    archived = []

    class Projects:
        async def get_project(self, project_id):
            return {"id": project_id, "name": "P", "status": "active", "workspaces": []}

        async def archive_project(self, project_id):
            archived.append(project_id)
            return {"id": project_id, "name": "P", "status": "archived", "workspaces": []}

    async def start(**_kwargs):
        async def shutdown():
            pass
        return SimpleNamespace(projects=Projects(), shutdown=shutdown)

    monkeypatch.setattr("app.ApplicationRuntime.start", start)
    app = create_app(user_name="ada", config_dir=tmp_path)

    async def exercise():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8000"
            ) as client:
                archive = "/v1/projects/p1/archive"
                # A page on another site: a body-less POST needs no preflight.
                hostile = await client.post(archive, headers={"Origin": "https://evil.example"})
                sandboxed = await client.post(archive, headers={"Origin": "null"})
                app_page = await client.post(archive, headers={"Origin": "http://localhost:3000"})
                script = await client.post(archive)
                rebound = await client.get(
                    "/v1/projects/p1", headers={"Host": "evil.example:8000"}
                )
                return hostile, sandboxed, app_page, script, rebound

    hostile, sandboxed, app_page, script, rebound = asyncio.run(exercise())
    assert hostile.status_code == 403
    assert hostile.json()["error"]["code"] == "origin_not_allowed"
    assert sandboxed.status_code == 403
    assert app_page.status_code == 200 and script.status_code == 200
    assert archived == ["p1", "p1"]  # only the app page and the script got through
    assert rebound.status_code == 400
