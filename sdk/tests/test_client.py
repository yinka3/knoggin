import pytest

from knoggin import (
    DocumentFocusDocument,
    Knoggin,
    Turn,
    source_provenance_from_response,
)


class _FakeSession:
    def __init__(self):
        self.calls = []
        self.session_id = "session-1"

    async def run_agent_stream(self, message, **kwargs):
        self.calls.append((message, kwargs))
        yield {"event": "thinking", "data": {"content": "local reasoning summary"}}
        yield {"event": "token", "data": {"content": "Hello"}}
        yield {
            "event": "tool_start",
            "data": {
                "call_id": "call-1",
                "tool": "search",
                "args": {"query": "Knoggin", "content": "SDK input"},
            },
        }
        yield {
            "event": "response",
            "data": {
                "content": "Durable answer",
                "usage": {"total_tokens": 8, "approximate": False},
                "assistant_message_id": 42,
                "source_ref_ids": ["source-ref-1"],
                "sources_consulted": [
                    {
                        "source_kind": "text_document",
                        "excerpt": "Knoggin keeps durable context.",
                        "document_id": "document-1",
                        "locator": {"kind": "text_line", "start_line": 1},
                        "content_hash": "a" * 64,
                        "encounter_kind": "document_read",
                        "metadata": {"local": "SDK receives source context"},
                    }
                ],
            },
        }


class _FakeDocumentService:
    def __init__(self):
        self.calls = []

    async def resolve_focus_target(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "target_type": "document",
            "document_id": "document-1",
            "relative_path": "notes/project.md",
        }


class _FakeSessions:
    def __init__(self, session):
        self.session = session

    async def get_or_resume_session(self, session_id):
        return self.session if session_id == "session-1" else None


class _FakeRuntime:
    def __init__(self, session):
        self.sessions = _FakeSessions(session)
        self.shutdown_called = False

    async def shutdown(self):
        self.shutdown_called = True


@pytest.mark.unit
@pytest.mark.no_network
async def test_sdk_opens_the_direct_engine_stream_without_http_run_events():
    session = _FakeSession()
    runtime = _FakeRuntime(session)
    knoggin = Knoggin(runtime)

    stream = await knoggin.open_turn_stream(
        session_id="session-1",
        turn=Turn(content="What is Knoggin?"),
        idempotency_key="request-1",
    )
    events = [event async for event in stream]

    assert [event["event"] for event in events] == [
        "thinking",
        "token",
        "tool_start",
        "response",
    ]
    assert events[2]["data"]["args"] == {
        "query": "Knoggin",
        "content": "SDK input",
    }
    assert session.calls[0][0].metadata == {"idempotency_key": "request-1"}

    sources = source_provenance_from_response(events[-1]["data"])
    assert sources[0].source_ref_id == "source-ref-1"
    assert sources[0].document_id == "document-1"
    assert sources[0].metadata == {"local": "SDK receives source context"}

    await knoggin.close()
    assert runtime.shutdown_called is True


@pytest.mark.unit
@pytest.mark.no_network
async def test_sdk_resolves_document_focus_before_opening_the_engine_stream():
    session = _FakeSession()
    session.document_service = _FakeDocumentService()
    knoggin = Knoggin(_FakeRuntime(session))

    stream = await knoggin.open_turn_stream(
        session_id="session-1",
        turn=Turn(
            content="Summarize this document",
            document_focus=DocumentFocusDocument(document_id="document-1"),
        ),
    )
    _ = [event async for event in stream]

    assert session.document_service.calls == [
        {"session_id": "session-1", "document_id": "document-1"}
    ]
    document_focus = session.calls[0][1]["document_focus"]
    assert document_focus.target_type == "document"
    assert document_focus.mode == "request"
    assert document_focus.document_id == "document-1"
    assert document_focus.relative_path == "notes/project.md"

    await knoggin.close()
