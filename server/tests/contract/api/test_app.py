from datetime import datetime, timezone

import httpx
import pytest

from api.app import create_app
from common.exceptions import StorageUnavailableError
from common.schema.public import (
    CreateProjectRequest,
    CreateSessionRequest,
    MessageAcceptance,
    ProjectResponse,
    RunResult,
    SessionResponse,
    StartRunRequest,
    SubmitMessageRequest,
)


class FakeApplication:
    def __init__(self):
        self.calls = []
        self.fail_projects = False

    async def create_project(self, *, user_name, request: CreateProjectRequest):
        self.calls.append(("project", user_name, request))
        if self.fail_projects:
            raise StorageUnavailableError("postgres://secret should never leak")
        return ProjectResponse(
            id="project-1",
            name=request.name,
            description=request.description,
            access_mode=request.access_mode,
            status="active",
        )

    async def create_session(self, *, user_name, request: CreateSessionRequest):
        self.calls.append(("session", user_name, request))
        return SessionResponse(
            session_id="session-1",
            project_id=request.project_id,
            model=request.model,
            agent_id=request.agent_id,
            enabled_tools=tuple(request.enabled_tools)
            if request.enabled_tools is not None
            else None,
        )

    async def submit_message(
        self,
        *,
        user_name,
        session_id,
        request: SubmitMessageRequest,
    ):
        self.calls.append(("message", user_name, session_id, request))
        return MessageAcceptance(message_id=42, idempotent=False)

    async def run_stream(self, *, user_name, request: StartRunRequest):
        self.calls.append(("run", user_name, request))
        now = datetime.now(timezone.utc)
        yield {
            "type": "run.started",
            "run_id": "run-1",
            "sequence": 0,
            "timestamp": now,
        }
        yield {
            "type": "message.delta",
            "run_id": "run-1",
            "sequence": 1,
            "timestamp": now,
            "content": "Done",
        }
        yield {
            "type": "run.completed",
            "run_id": "run-1",
            "sequence": 2,
            "timestamp": now,
            "result": RunResult(run_id="run-1", content="Done").model_dump(
                mode="json"
            ),
        }


class BrokenStreamApplication(FakeApplication):
    async def run_stream(self, *, user_name, request: StartRunRequest):
        yield {"type": "private.tool.payload", "secret": "do not expose"}


async def _client(app):
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


@pytest.mark.unit
@pytest.mark.no_network
async def test_first_vertical_slice_delegates_public_routes_to_injected_port():
    port = FakeApplication()
    app = create_app(port)

    async with await _client(app) as client:
        project = await client.post(
            "/v1/projects",
            headers={"X-User-Name": "ada"},
            json={"name": " Research ", "description": "notes"},
        )
        session = await client.post(
            "/v1/sessions",
            headers={"X-User-Name": "ada"},
            json={
                "project_id": "project-1",
                "enabled_tools": [" Search_Messages "],
            },
        )
        message = await client.post(
            "/v1/sessions/session-1/messages",
            headers={"X-User-Name": "ada"},
            json={"content": "remember this", "idempotency_key": "m-1"},
        )
        result = await client.post(
            "/v1/runs",
            headers={"X-User-Name": "ada"},
            json={"session_id": "session-1", "query": "What happened?"},
        )

    assert project.status_code == 201
    assert project.json()["id"] == "project-1"
    assert session.status_code == 201
    assert session.json()["enabled_tools"] == ["search_messages"]
    assert message.json() == {
        "message_id": 42,
        "accepted": True,
        "idempotent": False,
    }
    assert result.status_code == 200
    assert result.json() == {
        "run_id": "run-1",
        "content": "Done",
        "sources": [],
        "usage": None,
    }
    assert [call[0] for call in port.calls] == [
        "project",
        "session",
        "message",
        "run",
    ]


@pytest.mark.unit
@pytest.mark.no_network
async def test_stream_route_emits_only_validated_sse_events():
    app = create_app(FakeApplication())
    async with await _client(app) as client:
        response = await client.post(
            "/v1/runs/stream",
            headers={"X-User-Name": "ada", "X-Request-ID": "request-1"},
            json={"session_id": "session-1", "query": "What happened?"},
        )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.headers["x-request-id"] == "request-1"
    assert response.text.count("event: ") == 3
    assert '"type":"run.completed"' in response.text


@pytest.mark.unit
@pytest.mark.no_network
async def test_invalid_stream_event_becomes_sanitized_terminal_failure():
    app = create_app(BrokenStreamApplication())
    async with await _client(app) as client:
        response = await client.post(
            "/v1/runs/stream",
            json={"session_id": "session-1", "query": "What happened?"},
        )

    assert response.status_code == 200
    assert response.text.count("event: run.failed") == 1
    assert '"code":"invalid_request"' in response.text
    assert "private.tool.payload" not in response.text
    assert "do not expose" not in response.text


@pytest.mark.unit
@pytest.mark.no_network
async def test_api_errors_have_stable_sanitized_public_shape():
    port = FakeApplication()
    port.fail_projects = True
    app = create_app(port)

    async with await _client(app) as client:
        storage = await client.post(
            "/v1/projects",
            headers={"X-User-Name": "ada"},
            json={"name": "Research"},
        )
        invalid = await client.post(
            "/v1/projects",
            json={"name": "", "unknown": True},
        )

    assert storage.status_code == 503
    assert storage.json()["error"] == {
        "code": "storage_unavailable",
        "message": "Storage is temporarily unavailable.",
        "retryable": True,
        "request_id": storage.headers["x-request-id"],
        "run_id": None,
        "details": None,
    }
    assert "secret" not in storage.text
    assert invalid.status_code == 422
    assert invalid.json()["error"]["code"] == "invalid_request"
