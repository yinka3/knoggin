import threading
from types import SimpleNamespace

import httpx
import pytest

from api.app import create_app
from common.conf.manager import ConfigManager
from runtime.api_port import ApplicationRuntimePort

pytestmark = [pytest.mark.unit, pytest.mark.no_network]


@pytest.fixture
def settings_port(tmp_path):
    manager = ConfigManager(tmp_path)
    manager.load()
    port = ApplicationRuntimePort(SimpleNamespace(sessions=SimpleNamespace(user_name="ada"), config_manager=manager))
    return port, manager


def client(port, user="ada"):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(port), raise_app_exceptions=False),
                            base_url="http://test", headers={"X-User-Name": user})


async def test_settings_credentials_are_write_only_and_updates_publish_on_owner_thread(settings_port):
    port, manager = settings_port
    owner = threading.get_ident()
    threads = []
    manager.subscribe(lambda value: threads.append(threading.get_ident()), "llm.api_key")
    async with client(port) as http:
        updated = await http.patch("/v1/settings", json={"updates": {
            "llm": {"api_key": "SECRET-LLM", "base_url": "https://secret:password@example.com"},
            "search": {"brave_api_key": "SECRET-BRAVE", "tavily_api_key": "SECRET-TAVILY"},
        }})
        read = await http.get("/v1/settings")
        status = await http.get("/v1/settings/status")
    assert updated.status_code == read.status_code == status.status_code == 200
    assert updated.json()["accepted"] and updated.json()["status"]["fully_applied"]
    assert read.json()["llm"]["api_key_configured"]
    assert read.json()["search"]["brave_api_key_configured"]
    assert "api_key" not in read.json()["llm"]
    assert "base_url" not in read.json()["llm"]
    assert all("SECRET" not in response.text and "password" not in response.text for response in (updated, read, status))
    assert threads == [owner, owner]
    assert manager.config.llm.api_key == "SECRET-LLM"


async def test_settings_apply_failure_and_retry_preserve_persisted_candidate(settings_port):
    port, manager = settings_port
    failing = False

    def subscriber(value):
        if failing:
            raise RuntimeError("SECRET subscriber exception")

    manager.subscribe(subscriber, "user_aliases")
    failing = True
    async with client(port) as http:
        updated = await http.patch("/v1/settings", json={"updates": {"user_aliases": ["Ada"]}})
        assert updated.json()["accepted"]
        assert updated.json()["status"]["persisted"]
        assert not updated.json()["status"]["fully_applied"]
        assert updated.json()["status"]["failed_subscriptions"]
        source = manager.config_file.read_bytes()
        failing = False
        retried = await http.post("/v1/settings/retry-applies")
    assert retried.json()["fully_applied"]
    assert "SECRET" not in updated.text + retried.text
    assert manager.config_file.read_bytes() == source


@pytest.mark.parametrize("updates", [{"unknown": "SECRET"}, {"llm": {"unknown": "SECRET"}},
                                        {"developer_settings": {"community": {"interval_minutes": 0}}}])
async def test_invalid_settings_do_not_persist_publish_or_leak(settings_port, updates):
    port, manager = settings_port
    source = manager.config_file.read_bytes()
    status = manager.last_apply_status
    async with client(port) as http:
        response = await http.patch("/v1/settings", json={"updates": updates})
    assert response.status_code == 422
    assert "SECRET" not in response.text
    assert manager.config_file.read_bytes() == source
    assert manager.last_apply_status == status


async def test_settings_reload_retains_active_state_on_invalid_file(settings_port):
    port, manager = settings_port
    manager.config_file.write_text("user_aliases: [Changed]\n", encoding="utf-8")
    source = manager.config_file.read_bytes()
    async with client(port) as http:
        reloaded = await http.post("/v1/settings/reload")
        assert reloaded.json()["accepted"]
        assert reloaded.json()["status"]["persisted"]  # candidate already exists on disk
        assert manager.config_file.read_bytes() == source  # reload did not rewrite YAML
        assert manager.config.user_aliases == ["Changed"]
        manager.config_file.write_text("llm: SECRET-invalid\n", encoding="utf-8")
        rejected = await http.post("/v1/settings/reload")
    assert not rejected.json()["accepted"]
    assert manager.config.user_aliases == ["Changed"]
    assert "SECRET" not in rejected.text


async def test_persistence_failure_does_not_activate_candidate(settings_port, monkeypatch):
    port, manager = settings_port
    monkeypatch.setattr(manager, "_persist_config", lambda candidate: False)
    async with client(port) as http:
        response = await http.patch("/v1/settings", json={"updates": {"user_aliases": ["Changed"]}})
    assert response.status_code == 200
    assert not response.json()["accepted"]
    assert not response.json()["status"]["activated"]
    assert manager.config.user_aliases == []


@pytest.mark.parametrize("method,path,body", [
    ("GET", "/v1/settings", None), ("GET", "/v1/settings/status", None),
    ("PATCH", "/v1/settings", {"updates": {"user_aliases": ["Changed"]}}),
    ("POST", "/v1/settings/reload", None), ("POST", "/v1/settings/retry-applies", None),
])
async def test_settings_scope_denial_precedes_config_access(settings_port, method, path, body):
    port, manager = settings_port
    port.runtime.config_manager = None  # accessing it would fail instead of giving 403
    async with client(port, "other") as http:
        response = await http.request(method, path, json=body)
    assert response.status_code == 403
