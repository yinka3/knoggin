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
