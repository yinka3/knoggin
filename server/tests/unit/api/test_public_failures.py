import json
from datetime import datetime, timezone

import httpx
import pytest

from api.app import create_app


def event(kind, sequence, **fields):
    return dict(type=kind, run_id="run", sequence=sequence,
                timestamp=datetime.now(timezone.utc), **fields)


class Port:
    def __init__(self, events=(), project=None, error=None):
        self.events, self.project, self.error = events, project, error
        self.closed = False

    async def create_project(self, **kwargs):
        if self.error:
            raise self.error
        return self.project

    async def open_run_stream(self, **kwargs):
        async def stream():
            try:
                for value in self.events:
                    yield value
            finally:
                self.closed = True
        return stream()


async def post(port, path, body):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(port), raise_app_exceptions=False),
                                base_url="http://test") as client:
        return await client.post(path, json=body)


@pytest.mark.parametrize("port,status,code", [
    (Port(project={}), 500, "internal_error"),
    (Port(error=PermissionError("private-token")), 403, "forbidden"),
    (Port(error=FileNotFoundError("private-token")), 404, "not_found"),
])
async def test_server_projection_and_scope_errors_are_not_client_validation(port, status, code):
    response = await post(port, "/v1/projects", {"name": "test"})
    assert response.status_code == status
    assert response.json()["error"]["code"] == code
    assert "private-token" not in response.text


@pytest.mark.parametrize("path", ["/v1/runs", "/v1/runs/stream"])
async def test_public_failure_details_and_raw_messages_are_removed(path):
    port = Port([event("run.failed", 0, error=dict(code="forbidden", message="private-token",
                                                  details={"sql": "private-token"}))])
    response = await post(port, path, {"session_id": "session", "query": "hello"})
    assert response.status_code == (403 if path == "/v1/runs" else 200)
    assert "forbidden" in response.text
    assert "private-token" not in response.text
    assert port.closed


@pytest.mark.parametrize("invalid", ["mixed", "duplicate", "after_terminal", "missing"])
async def test_nonstream_rejects_invalid_stream_contract_and_closes(invalid):
    events = [event("run.started", 0)]
    if invalid == "mixed":
        events.append({**event("run.cancelled", 1), "run_id": "other"})
    elif invalid == "duplicate":
        events.append(event("message.delta", 0, content="text"))
    elif invalid == "after_terminal":
        events.extend([event("run.completed", 1, result=dict(run_id="run", content="done")),
                       event("run.failed", 2, error=dict(code="internal_error", message="private-token"))])
    port = Port(events)
    response = await post(port, "/v1/runs", {"session_id": "session", "query": "hello"})
    assert response.status_code == 500
    assert response.json()["error"]["code"] == "internal_error"
    assert port.closed
    assert "private-token" not in response.text


async def test_sse_exact_text_and_exposed_terminal_are_preserved():
    parts = [" hello ", "\n", "    code\n", "🧠 "]
    events = [event("message.delta", i, content=part) for i, part in enumerate(parts)]
    events.append(event("run.completed", 4, result=dict(run_id="run", content="".join(parts))))
    events.append(event("message.delta", 5, content="late"))
    response = await post(Port(events), "/v1/runs/stream", {"session_id": "session", "query": "hello"})
    frames = [json.loads(line[6:]) for line in response.text.splitlines() if line.startswith("data: ")]
    assert [value["content"] for value in frames if value["type"] == "message.delta"] == parts
    assert sum(value["type"].startswith("run.") for value in frames) == 1
    assert frames[-1]["type"] == "run.completed"


async def test_nonstream_cancelled_outcome_has_explicit_status():
    response = await post(Port([event("run.cancelled", 0)]), "/v1/runs", {"session_id": "session", "query": "hello"})
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "run_cancelled"
