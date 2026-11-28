import asyncio
from types import SimpleNamespace

import httpx
import pytest

from app import create_app
from common import credential_store


@pytest.fixture
def store(tmp_path, monkeypatch):
    path = tmp_path / "credentials.json"
    monkeypatch.setattr(credential_store, "_path", lambda: path)
    monkeypatch.setattr(credential_store, "_cache", None)
    for service in credential_store.SERVICES.values():
        for field in service.fields:
            monkeypatch.delenv(field, raising=False)
    return path


def _app(monkeypatch, tmp_path, *, project_status="active", sandbox=True):
    state = SimpleNamespace(llm_updates=0, resets=[])

    class Llm:
        def update_settings(self, _settings):
            state.llm_updates += 1

    class Projects:
        async def get_project(self, project_id):
            return {"status": project_status} if project_id == "p1" else None

    class Sandbox:
        async def reset(self, project_id):
            state.resets.append(project_id)

    async def start(**_kwargs):
        async def shutdown():
            pass

        return SimpleNamespace(
            projects=Projects(), shutdown=shutdown,
            sandbox=Sandbox() if sandbox else None,
            resources=SimpleNamespace(llm_service=Llm()),
            config_manager=SimpleNamespace(config=SimpleNamespace(llm=object())),
        )

    monkeypatch.setattr("app.ApplicationRuntime.start", start)
    return create_app(user_name="ada", config_dir=tmp_path), state


def _run(app, exercise):
    async def wrapped():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                await exercise(client)

    asyncio.run(wrapped())


def test_logins_go_in_but_never_come_back_out(monkeypatch, tmp_path, store):
    app, state = _app(monkeypatch, tmp_path)

    async def exercise(client):
        saved = await client.put(
            "/v1/connections/kaggle",
            json={"values": {"KAGGLE_USERNAME": "ada", "KAGGLE_KEY": "secret-key"}},
        )
        assert saved.status_code == 200
        assert "secret-key" not in saved.text
        kaggle = next(row for row in saved.json()["connections"] if row["service"] == "kaggle")
        assert kaggle["configured"] is True and kaggle["source"] == "saved"
        assert "secret-key" not in (await client.get("/v1/connections")).text

        partial = await client.put(
            "/v1/connections/kaggle", json={"values": {"KAGGLE_KEY": "only"}},
        )
        assert partial.status_code == 422
        assert (await client.put("/v1/connections/dropbox", json={"values": {}})).status_code == 404

        assert (await client.delete("/v1/connections/kaggle")).status_code == 200
        assert (await client.delete("/v1/connections/kaggle")).status_code == 404
        assert state.llm_updates == 0

        await client.put("/v1/connections/openrouter", json={"values": {"OPENROUTER_API_KEY": "sk"}})
        assert state.llm_updates == 1  # the model client picks up the new key now

    _run(app, exercise)


def test_the_sandbox_can_be_reset_for_an_active_project(monkeypatch, tmp_path, store):
    app, state = _app(monkeypatch, tmp_path)

    async def exercise(client):
        assert (await client.post("/v1/projects/p1/sandbox/reset")).json() == {"reset": True}
        assert (await client.post("/v1/projects/missing/sandbox/reset")).status_code == 404
        assert state.resets == ["p1"]

    _run(app, exercise)

    app, _state = _app(monkeypatch, tmp_path, project_status="archived")

    async def archived(client):
        response = await client.post("/v1/projects/p1/sandbox/reset")
        assert response.status_code == 409

    _run(app, archived)

    app, _state = _app(monkeypatch, tmp_path, sandbox=False)

    async def unavailable(client):
        assert (await client.post("/v1/projects/p1/sandbox/reset")).status_code == 503

    _run(app, unavailable)
