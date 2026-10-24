import asyncio
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from api.app import create_app


def test_one_runtime_owns_http_lifespan(monkeypatch, tmp_path):
    events = []

    async def start(*, user_name, config_dir):
        events.append(("start", user_name, config_dir))

        async def shutdown():
            events.append(("shutdown",))

        return SimpleNamespace(shutdown=shutdown)

    monkeypatch.setattr("api.app.ApplicationRuntime.start", start)
    app = create_app(user_name=" ada ", config_dir=tmp_path)

    async def exercise():
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.get("/v1/health")
            assert response.status_code == 200
            assert response.json() == {"status": "ready"}
            assert events == [("start", "ada", tmp_path)]

    asyncio.run(exercise())

    assert events == [("start", "ada", tmp_path), ("shutdown",)]
    assert not hasattr(app.state, "runtime")


def test_user_scope_is_process_configuration(monkeypatch, tmp_path):
    monkeypatch.delenv("RUNTIME_USER_NAME", raising=False)
    with pytest.raises(ValueError, match="RUNTIME_USER_NAME"):
        create_app(config_dir=tmp_path)

    monkeypatch.setenv("RUNTIME_USER_NAME", "configured-user")
    monkeypatch.setenv("SETTINGS_DIR", str(tmp_path))
    app = create_app()
    assert app is not None


def test_start_failure_does_not_publish_runtime(monkeypatch, tmp_path):
    async def start(*, user_name, config_dir):
        assert (user_name, config_dir) == ("ada", Path(tmp_path))
        raise RuntimeError("startup failed")

    monkeypatch.setattr("api.app.ApplicationRuntime.start", start)
    app = create_app(user_name="ada", config_dir=tmp_path)
    async def exercise():
        async with app.router.lifespan_context(app):
            pass

    with pytest.raises(RuntimeError, match="startup failed"):
        asyncio.run(exercise())
    assert not hasattr(app.state, "runtime")
