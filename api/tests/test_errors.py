import asyncio
from types import SimpleNamespace

import httpx

from app import create_app
from errors import handle_error
from common.exceptions import StorageWriteError


def test_unexpected_engine_failure_does_not_reach_http_body(monkeypatch, tmp_path):
    class Projects:
        async def list_projects(self):
            raise RuntimeError("private database connection detail")

    async def start(**_kwargs):
        async def shutdown():
            pass
        return SimpleNamespace(projects=Projects(), shutdown=shutdown)

    monkeypatch.setattr("app.ApplicationRuntime.start", start)
    app = create_app(user_name="local", config_dir=tmp_path)

    async def exercise():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                base_url="http://test",
            ) as client:
                response = await client.get("/v1/projects")
        assert response.status_code == 500
        assert response.json()["error"]["code"] == "internal_error"
        assert "private database" not in response.text

    asyncio.run(exercise())


def test_storage_failure_is_redacted_at_http_boundary():
    async def exercise():
        response = await handle_error(None, StorageWriteError("secret SQL detail"))
        assert response.status_code == 503
        assert b"secret SQL detail" not in response.body
        assert b"dependency_unavailable" in response.body

    asyncio.run(exercise())
