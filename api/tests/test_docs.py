import asyncio
from types import SimpleNamespace

import httpx

from app import create_app


def test_docs_lists_every_served_route(monkeypatch, tmp_path):
    async def start(**_kwargs):
        async def shutdown():
            pass
        return SimpleNamespace(shutdown=shutdown)

    monkeypatch.setattr("app.ApplicationRuntime.start", start)
    app = create_app(user_name="ada", config_dir=tmp_path)

    async def exercise():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                return (await client.get("/v1/docs")).json()["routes"]

    listed = {(item["method"], item["path"]) for item in asyncio.run(exercise())}
    served = {
        (method, route.path)
        for route in app.routes for method in route.methods - {"HEAD"}
    }
    assert listed == served
    assert ("POST", "/v1/sessions/{session_id}/runs") in listed
