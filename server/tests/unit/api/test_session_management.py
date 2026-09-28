from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from api.app import create_app
from common.schema.public import UpdateSessionRequest
from runtime.api_port import ApplicationRuntimePort

pytestmark = [pytest.mark.unit, pytest.mark.no_network]


def make_port():
    row = dict(session_id="s1", project_id="p1", status="open", model="old",
               enabled_tools=None, document_focus={"private": "secret"})
    manager = SimpleNamespace(
        user_name="ada", list_sessions=AsyncMock(return_value={"s1": row}),
        get_or_resume_session=AsyncMock(side_effect=AssertionError("must not resume")),
        get_session_history_readonly=AsyncMock(return_value=[dict(
            message_id=1, role="assistant", content="  Exact\n", timestamp=datetime.now(timezone.utc),
            internal_secret="secret",
        )]),
        update_session=AsyncMock(), delete_session=AsyncMock(),
    )

    async def update(session_id, changes):
        row.update(changes)

    manager.update_session.side_effect = update
    return ApplicationRuntimePort(SimpleNamespace(sessions=manager)), manager


async def test_session_management_http_contract_and_read_only_history():
    port, manager = make_port()
    transport = httpx.ASGITransport(app=create_app(port), raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test",
                                 headers={"X-User-Name": "ada"}) as client:
        listed = await client.get("/v1/sessions")
        history = await client.get("/v1/sessions/s1/history?limit=7")
        updated = await client.patch("/v1/sessions/s1", json={"enabled_tools": []})
        deleted = await client.delete("/v1/sessions/s1")

    assert listed.status_code == history.status_code == updated.status_code == deleted.status_code == 200
    assert "document_focus" not in listed.text
    assert history.json()[0]["content"] == "  Exact\n"
    assert "secret" not in history.text
    assert updated.json()["enabled_tools"] == []
    assert updated.json()["model"] == "old"
    assert deleted.json() == {"session_id": "s1", "deleted": True}
    manager.get_session_history_readonly.assert_awaited_once_with("s1", limit=7)
    manager.update_session.assert_awaited_once_with("s1", {"enabled_tools": []})
    manager.delete_session.assert_awaited_once_with("s1")
    manager.get_or_resume_session.assert_not_awaited()


@pytest.mark.parametrize("changes", [{}, {"enabled_tools": None}, {"enabled_tools": []}])
async def test_session_patch_preserves_omitted_null_and_empty(changes):
    port, manager = make_port()
    await port.update_session(user_name="ada", session_id="s1", request=UpdateSessionRequest(**changes))
    manager.update_session.assert_awaited_once_with("s1", changes)


@pytest.mark.parametrize("method,path,body", [
    ("GET", "/v1/sessions", None),
    ("GET", "/v1/sessions/s1/history", None),
    ("PATCH", "/v1/sessions/s1", {"model": "new"}),
    ("DELETE", "/v1/sessions/s1", None),
])
async def test_session_routes_reject_other_user_before_manager_calls(method, path, body):
    port, manager = make_port()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(port), raise_app_exceptions=False),
                                 base_url="http://test") as client:
        response = await client.request(method, path, json=body, headers={"X-User-Name": "other"})
    assert response.status_code == 403
    manager.list_sessions.assert_not_awaited()
    manager.update_session.assert_not_awaited()
    manager.delete_session.assert_not_awaited()


async def test_session_routes_missing_invalid_input_and_openapi():
    port, manager = make_port()
    app = create_app(port)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                                 base_url="http://test", headers={"X-User-Name": "ada"}) as client:
        for method in ("GET", "PATCH", "DELETE"):
            path = "/v1/sessions/missing" + ("/history" if method == "GET" else "")
            response = await client.request(method, path, json={} if method == "PATCH" else None)
            assert response.status_code == 404
        assert (await client.get("/v1/sessions/s1/history?limit=1001")).status_code == 422
        for body in ({"project_id": "other"}, {"model": "  "}, {"enabled_tools": [" "]}):
            assert (await client.patch("/v1/sessions/s1", json=body)).status_code == 422
    manager.update_session.assert_not_awaited()
    manager.delete_session.assert_not_awaited()
    schema = app.openapi()
    assert schema["paths"]["/v1/sessions/{session_id}"]["patch"]["responses"]["200"]
