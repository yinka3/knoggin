from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import httpx
import pytest

from api.app import create_app
from core.community.runtime import AACAdmission, AACAdmissionOutcome
from runtime.api_port import ApplicationRuntimePort

pytestmark = [pytest.mark.unit, pytest.mark.no_network]
IDENTITY = UUID("11111111-1111-1111-1111-111111111111")


def make_port():
    now = datetime.now(timezone.utc)
    aac = SimpleNamespace(
        trigger_discussion=AsyncMock(return_value=AACAdmission(AACAdmissionOutcome.STARTED, "admitted", str(IDENTITY))),
        request_stop=AsyncMock(return_value=True),
        set_participation=AsyncMock(return_value=SimpleNamespace(id="a1", aac_enabled=True, brain="secret")),
        list_discussions=AsyncMock(return_value=[dict(discussion_id=IDENTITY, topic="Topic", status="active",
                                                     token_budget=100, tokens_used=0, started_at=now, internal="secret")]),
        list_timeline=AsyncMock(return_value=[dict(timeline_id=IDENTITY, kind="agent_message", agent_id="a1",
                                                  content="  Exact\n", created_at=now, event_sequence=7, internal="secret")]),
        list_insights=AsyncMock(return_value=[dict(insight_id=IDENTITY, discussion_id=IDENTITY, author_agent_id="a1",
                                                  visibility="private", content="Own private insight", created_at=now, internal="secret")]),
        list_insight_votes=AsyncMock(return_value=[dict(voter_agent_id="a2", vote="up", reason="Good",
                                                       created_at=now, updated_at=now, internal="secret")]),
    )
    return ApplicationRuntimePort(SimpleNamespace(sessions=SimpleNamespace(user_name="ada"), aac_runtime=aac)), aac


CASES = [
    ("POST", "/v1/aac/trigger", "trigger_discussion", None),
    ("POST", "/v1/aac/stop", "request_stop", None),
    ("PUT", "/v1/aac/agents/a1/participation", "set_participation", {"enabled": True}),
    ("GET", "/v1/aac/discussions?limit=9", "list_discussions", None),
    ("GET", "/v1/aac/discussions/d1/timeline?limit=8&after_sequence=6", "list_timeline", None),
    ("GET", "/v1/aac/insights?query=words&limit=5", "list_insights", None),
    ("GET", "/v1/aac/insights/i1/votes", "list_insight_votes", None),
]


@pytest.mark.parametrize("method,path,target,body", CASES)
async def test_aac_routes_delegate_with_public_allowlists(method, path, target, body):
    port, aac = make_port()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(port), raise_app_exceptions=False),
                                 base_url="http://test", headers={"X-User-Name": "ada"}) as client:
        response = await client.request(method, path, json=body)
    assert response.status_code == 200
    assert "secret" not in response.text
    operation = getattr(aac, target)
    operation.assert_awaited_once()
    if target == "list_timeline":
        operation.assert_awaited_once_with("d1", limit=8, after_sequence=6)
        assert response.json()[0]["content"] == "  Exact\n"
        assert response.json()[0]["event_sequence"] == 7
    elif target == "list_insights":
        operation.assert_awaited_once_with(query="words", limit=5)
        assert response.json()[0]["visibility"] == "private"
    elif target == "set_participation":
        operation.assert_awaited_once_with("a1", True)
        assert response.json() == {"agent_id": "a1", "enabled": True}


@pytest.mark.parametrize("method,path,target,body", CASES)
async def test_aac_routes_reject_other_user_before_aac_access(method, path, target, body):
    port, aac = make_port()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(port), raise_app_exceptions=False),
                                 base_url="http://test", headers={"X-User-Name": "other"}) as client:
        response = await client.request(method, path, json=body)
    assert response.status_code == 403
    getattr(aac, target).assert_not_awaited()


async def test_aac_skipped_stop_noop_and_missing_agent_are_explicit():
    port, aac = make_port()
    aac.trigger_discussion.return_value = AACAdmission(AACAdmissionOutcome.SKIPPED, "disabled")
    aac.request_stop.return_value = False
    aac.set_participation.return_value = None
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(port), raise_app_exceptions=False),
                                 base_url="http://test", headers={"X-User-Name": "ada"}) as client:
        trigger = await client.post("/v1/aac/trigger")
        stop = await client.post("/v1/aac/stop")
        missing = await client.put("/v1/aac/agents/a1/participation", json={"enabled": False})
    assert trigger.json() == {"outcome": "skipped", "reason": "disabled", "discussion_id": None}
    assert stop.json() == {"stop_requested": False}
    assert missing.status_code == 404


@pytest.mark.parametrize("path", ["/v1/aac/discussions?limit=101", "/v1/aac/insights?limit=0",
                                     "/v1/aac/discussions/d1/timeline?after_sequence=-1"])
async def test_aac_browsing_limits_rejected_before_runtime_access(path):
    port, aac = make_port()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(port), raise_app_exceptions=False),
                                 base_url="http://test", headers={"X-User-Name": "ada"}) as client:
        assert (await client.get(path)).status_code == 422
    aac.list_discussions.assert_not_awaited()
    aac.list_timeline.assert_not_awaited()
    aac.list_insights.assert_not_awaited()


async def test_aac_empty_scoped_reads_and_malformed_server_output():
    port, aac = make_port()
    aac.list_timeline.return_value = []
    aac.list_insight_votes.return_value = []
    aac.list_discussions.return_value = [{"status": "secret-invalid-status"}]
    app = create_app(port)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                                 base_url="http://test", headers={"X-User-Name": "ada"}) as client:
        assert (await client.get("/v1/aac/discussions/missing/timeline")).json() == []
        assert (await client.get("/v1/aac/insights/missing/votes")).json() == []
        response = await client.get("/v1/aac/discussions")
    assert response.status_code == 500
    assert "secret" not in response.text
    assert "/v1/aac/trigger" in app.openapi()["paths"]
