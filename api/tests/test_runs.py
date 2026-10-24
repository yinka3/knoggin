import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from routes.runs import OwnedStreamResponse, _events, _request_focus
from common.utils.streams import ClosingAsyncIterator


def test_disconnect_closes_admitted_run_even_before_iteration():
    class Run:
        def __init__(self):
            self.closed = False

        def __aiter__(self):
            return self

        async def __anext__(self):
            await asyncio.sleep(100)

        async def aclose(self):
            self.closed = True

    async def exercise():
        run = Run()
        response = OwnedStreamResponse(
            ClosingAsyncIterator(_events(run, "run-1"), run)
        )

        async def receive():
            await asyncio.sleep(100)

        async def send(message):
            if message["type"] == "http.response.start":
                raise asyncio.CancelledError()

        with pytest.raises(asyncio.CancelledError):
            await response(
                {"type": "http", "http_version": "1.1", "method": "POST",
                 "path": "/", "headers": [], "asgi": {"spec_version": "2.4"}},
                receive, send,
            )
        assert run.closed

    asyncio.run(exercise())


def test_run_focus_is_resolved_before_admission():
    calls = []

    class Documents:
        async def resolve_focus_target(self, **kwargs):
            calls.append(kwargs)
            return {"target_type": "document", "document_id": "doc-1", "relative_path": "doc.txt"}

    async def exercise():
        from contracts.sessions import StartRun

        body = StartRun.model_validate({"query": "hello", "document_focus": {
            "target_type": "document", "document_id": "doc-1",
        }})
        focus = await _request_focus(SimpleNamespace(document_service=Documents()), body)
        assert focus.mode == "request"
        assert focus.document_id == "doc-1"
        assert calls == [{"document_id": "doc-1", "path_prefix": None}]

    asyncio.run(exercise())


def test_run_stream_emits_scoped_sources_before_completion():
    source = {
        "source_kind": "user_pasted_text",
        "locator": {"kind": "character_span", "start_char": 0, "end_char": 4},
        "excerpt": "text", "source_message_id": 1,
        "source_status": "available", "contributing_message_id": 2,
    }

    class Store:
        async def get_message_source_refs(self, message_id, **scope):
            assert message_id == 2
            assert scope == {"user_name": "ada", "project_id": "project-1", "session_id": "session-1"}
            return [source]

        async def get_message_artifact(self, message_id, **scope):
            return SimpleNamespace(
                model_dump=lambda: {
                    "artifact_id": "artifact-1", "project_id": "project-1",
                    "session_id": "session-1", "originating_message_id": 2,
                    "kind": "document", "title": "Draft", "status": "active",
                    "current_revision": 1,
                    "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
                    "updated_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
                }
            )

    async def raw():
        yield {"event": "response", "data": {
            "content": "answer", "assistant_message_id": 2,
            "source_ref_ids": ["source-1"],
            "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3},
        }}

    async def exercise():
        runtime = SimpleNamespace(resources=SimpleNamespace(knowledge_store=Store()))
        session = SimpleNamespace(user_name="ada", project_id="project-1", session_id="session-1")
        events = [frame async for frame in _events(raw(), "run-1", runtime=runtime, session=session)]
        names = [frame.decode().split("\n", 1)[0] for frame in events]
        assert names == ["event: run_started", "event: source_added", "event: usage_updated", "event: run_completed"]
        complete = json.loads(events[-1].decode().split("data: ", 1)[1])
        assert complete["source_ref_ids"] == ["source-1"]
        assert complete["artifact"]["artifact_id"] == "artifact-1"

    asyncio.run(exercise())


def test_run_steer_and_stop_routes_target_only_the_active_run(monkeypatch, tmp_path):
    import httpx
    from app import create_app

    class Session:
        stopped = False

        async def get_run_status(self, run_id):
            return {"run_id": run_id, "state": "cancelled" if self.stopped else "running", "steer": None} if run_id == "run-1" else None

        async def submit_steer(self, run_id, instruction):
            return {"steer_id": "steer-1", "state": "pending"} if run_id == "run-1" else None

        async def cancel_active_agent_run(self, *, run_id):
            if run_id != "run-1":
                return False
            self.stopped = True
            return True

    class Sessions:
        session = Session()

        async def list_sessions(self):
            return {"session-1": {"project_id": "project-1"}}

        def get_runtime_session(self, session_id):
            return self.session if session_id == "session-1" else None

    async def start(**_kwargs):
        async def shutdown():
            pass
        return SimpleNamespace(sessions=Sessions(), shutdown=shutdown)

    monkeypatch.setattr("app.ApplicationRuntime.start", start)
    app = create_app(user_name="ada", config_dir=tmp_path)

    async def exercise():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
                base = "/v1/sessions/session-1/runs/run-1"
                assert (await client.get(base)).json()["state"] == "running"
                assert (await client.post(base + "/steers", json={"instruction": "  Focus  "})).status_code == 202
                assert (await client.post(base + "/steers", json={"instruction": " "})).status_code == 422
                assert (await client.delete(base)).json()["state"] == "cancelled"
                assert (await client.get("/v1/sessions/other/runs/run-1")).status_code == 404
                assert (await client.post("/v1/sessions/session-1/runs/old/steers", json={"instruction": "now"})).status_code == 409

    asyncio.run(exercise())
