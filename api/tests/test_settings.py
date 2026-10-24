import asyncio
from types import SimpleNamespace

import httpx
import pytest
import yaml

from app import create_app
from common.conf.manager import ConfigManager


@pytest.fixture
def settings_app(monkeypatch, tmp_path):
    monkeypatch.setattr(ConfigManager, "_instance", None)
    manager = ConfigManager(tmp_path)
    startup = manager.config

    async def start(*, user_name, config_dir):
        assert user_name == "local"
        assert config_dir == tmp_path

        async def shutdown():
            pass

        return SimpleNamespace(
            config_manager=manager,
            startup_config=startup,
            shutdown=shutdown,
        )

    monkeypatch.setattr("app.ApplicationRuntime.start", start)
    return create_app(user_name="local", config_dir=tmp_path), manager


def test_settings_http_round_trip_redacts_secrets_and_reports_restart(settings_app):
    app, manager = settings_app

    async def exercise():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                initial = await client.get("/v1/settings")
                assert initial.status_code == 200
                assert initial.json()["settings"]["llm"]["base_url"] is None
                assert (
                    initial.json()["settings"]["jev"]["shutdown_timeout_seconds"] == 5
                )
                assert initial.json()["settings"]["jev"]["identity_mode"] == "disabled"

                updated = await client.patch(
                    "/v1/settings/developer_settings",
                    json={"documents": {"rerank_enabled": False}},
                )
                assert updated.status_code == 200
                assert updated.json()["status"]["restart_required_fields"] == [
                    "developer_settings.documents.rerank_enabled"
                ]
                assert updated.json()["status"]["fully_applied"] is False

                llm = await client.patch(
                    "/v1/settings/llm", json={"base_url": "https://example.test/v1"}
                )
                assert llm.status_code == 200
                jev = await client.patch(
                    "/v1/settings/jev", json={"shutdown_timeout_seconds": 8}
                )
                assert jev.status_code == 200

                secret = "test-private-api-key"
                set_key = await client.put(
                    "/v1/settings/llm/secrets/api_key", json={"value": secret}
                )
                assert set_key.status_code == 200
                visible = await client.get("/v1/settings")
                assert visible.json()["settings"]["llm"]["api_key_configured"] is True
                assert (
                    visible.json()["settings"]["llm"]["base_url"]
                    == "https://example.test/v1"
                )
                assert (
                    visible.json()["settings"]["jev"]["shutdown_timeout_seconds"] == 8
                )
                assert secret not in visible.text
                assert secret not in set_key.text

                before = manager.config.model_dump()
                saved_before = manager.config_file.read_text()
                rejected = await client.patch(
                    "/v1/settings/llm", json={"api_key": secret}
                )
                assert rejected.status_code == 422
                assert secret not in rejected.text
                invalid = await client.patch(
                    "/v1/settings/jev", json={"shutdown_timeout_seconds": -1}
                )
                assert invalid.status_code == 422
                unknown = await client.patch(
                    "/v1/settings/llm", json={"nonexistent_field": "x"}
                )
                assert unknown.status_code == 422
                assert manager.config.model_dump() == before
                assert manager.config_file.read_text() == saved_before

                cleared = await client.delete("/v1/settings/llm/secrets/api_key")
                assert cleared.status_code == 200
                assert (await client.get("/v1/settings")).json()["settings"]["llm"][
                    "api_key_configured"
                ] is False
                assert (await client.post("/v1/settings/reload")).status_code == 200
                assert (
                    await client.post("/v1/settings/retry-applies")
                ).status_code == 200

    asyncio.run(exercise())
    persisted = yaml.safe_load(manager.config_file.read_text())
    assert persisted["developer_settings"]["documents"]["rerank_enabled"] is False
    assert persisted["llm"]["base_url"] == "https://example.test/v1"
    assert persisted["llm"]["api_key"] == ""


def test_failed_settings_write_does_not_activate(settings_app, monkeypatch):
    app, manager = settings_app
    before = manager.config.model_dump()
    monkeypatch.setattr(manager, "_persist_config", lambda _candidate: False)

    async def exercise():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await client.patch(
                    "/v1/settings/llm", json={"agent_model": "new-model"}
                )
                assert response.status_code == 503
                assert response.json()["error"]["code"] == "settings_write_failed"
                manager.config_file.write_text("llm: [invalid]\n")
                reload_response = await client.post("/v1/settings/reload")
                assert reload_response.status_code == 422

    asyncio.run(exercise())
    assert manager.config.model_dump() == before
