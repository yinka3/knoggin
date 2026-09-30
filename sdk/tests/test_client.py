import pytest
from knoggin import (
    DocumentFocusDocument,
    DocumentFocusSubtree,
    Knoggin,
    Turn,
    source_provenance_from_response,
)
from knoggin import client as client_module

from common.schema.document import FolderScanSettings
from common.schema.health import HealthActivity, HealthSnapshot


class _FakeSession:
    def __init__(self):
        self.calls = []
        self.session_id = "session-1"

    async def open_agent_run_stream(self, message, **kwargs):
        self.calls.append((message, kwargs))
        return self._events()

    async def _events(self):
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

    async def resolve_focus_target(self, *, document_id=None, path_prefix=None):
        kwargs = (
            {"document_id": document_id}
            if document_id is not None
            else {"path_prefix": path_prefix}
        )
        self.calls.append(kwargs)
        if path_prefix is not None:
            return {"target_type": "subtree", "path_prefix": path_prefix}
        return {
            "target_type": "document",
            "document_id": "document-1",
            "relative_path": "notes/project.md",
        }

    async def list_documents(self, *, limit):
        self.calls.append(("list_documents", limit))
        return [{"document_id": "document-1"}]

    async def list_saved_web_links(self, *, limit):
        self.calls.append(("list_saved_web_links", limit))
        return [{"link_id": "link-1"}]

    async def get_scan_settings(self):
        self.calls.append(("get_scan_settings",))
        return FolderScanSettings(blocked_extensions={".log"})


@pytest.mark.unit
@pytest.mark.no_network
async def test_sdk_preserves_empty_tools_and_uses_library_subtree_focus():
    session = _FakeSession()
    session.document_service = _FakeDocumentService()
    client = Knoggin(_FakeRuntime(session))
    stream = await client.open_turn_stream(
        session_id="session-1",
        turn=Turn(
            content="Read",
            enabled_tools=(),
            document_focus=DocumentFocusSubtree(path_prefix="notes"),
        ),
    )
    assert session.calls[0][0].metadata == {}
    assert session.calls[0][1]["idempotency_key"] is None
    assert session.calls[0][1]["enabled_tools"] == []
    assert session.document_service.calls == [{"path_prefix": "notes"}]
    assert session.calls[0][1]["document_focus"].target_type == "subtree"
    await stream.aclose()


class _FakeProject:
    def __init__(self, document_service):
        self.document_service = document_service


class _FakeProjects:
    def __init__(self, document_service):
        self.document_service = document_service
        self.calls = []

    async def acquire_project_for_session(self, project_id, lease_id):
        self.calls.append(("acquire", project_id, lease_id))
        return _FakeProject(self.document_service)

    async def release_project_for_session(self, project_id, lease_id):
        self.calls.append(("release", project_id, lease_id))


class _FakeSessions:
    def __init__(self, session):
        self.session = session

    async def get_or_resume_session(self, session_id):
        return self.session if session_id == "session-1" else None


class _FakeRuntime:
    def __init__(self, session):
        self.sessions = _FakeSessions(session)
        self.sessions.user_name = "ada"
        self.health_service = self._HealthService()
        self.projects = _FakeProjects(_FakeDocumentService())
        self.shutdown_called = False

    class _HealthService:
        async def get_engine_health(self):
            return HealthSnapshot(summary="Engine healthy")

        async def get_resource_health(self, *, project_id):
            assert project_id == "project-1"
            return HealthSnapshot(
                activity=HealthActivity.BUSY, summary="Resources busy"
            )

        async def get_ingestion_health(self, *, user_name, project_id):
            assert (user_name, project_id) == ("ada", "project-1")
            return HealthSnapshot(summary="Ingestion healthy")

        async def get_background_health(self, *, project_id):
            assert project_id == "project-1"
            return HealthSnapshot(summary="Background healthy")

    async def shutdown(self):
        self.shutdown_called = True


@pytest.mark.unit
@pytest.mark.no_network
async def test_sdk_exposes_all_health_drilldowns():
    knoggin = Knoggin(_FakeRuntime(_FakeSession()))

    assert (await knoggin.get_engine_health())["summary"] == "Engine healthy"
    assert (await knoggin.get_resource_health(project_id="project-1"))[
        "activity"
    ] == "busy"
    assert (await knoggin.get_ingestion_health(project_id="project-1"))[
        "summary"
    ] == "Ingestion healthy"
    assert (await knoggin.get_background_health(project_id="project-1"))[
        "summary"
    ] == "Background healthy"


@pytest.mark.unit
@pytest.mark.no_network
async def test_sdk_exposes_project_document_management_under_a_runtime_lease():
    runtime = _FakeRuntime(_FakeSession())
    knoggin = Knoggin(runtime)

    assert await knoggin.list_documents(project_id="project-1", limit=3) == [
        {"document_id": "document-1"}
    ]
    assert await knoggin.list_saved_web_links(project_id="project-1") == [
        {"link_id": "link-1"}
    ]
    assert (await knoggin.get_document_scan_settings(project_id="project-1"))[
        "blocked_extensions"
    ] == [".log"]

    assert [call[0] for call in runtime.projects.calls] == [
        "acquire",
        "release",
        "acquire",
        "release",
        "acquire",
        "release",
    ]
    for acquire, release in zip(
        runtime.projects.calls[::2], runtime.projects.calls[1::2], strict=True
    ):
        assert acquire[1:] == release[1:]


@pytest.mark.unit
@pytest.mark.no_network
async def test_sdk_forwards_explicit_configuration_directory(monkeypatch, tmp_path):
    received = {}
    runtime = _FakeRuntime(_FakeSession())

    async def start_runtime(**kwargs):
        received.update(kwargs)
        return runtime

    monkeypatch.setattr(client_module.ApplicationRuntime, "start", start_runtime)

    knoggin = await Knoggin.start(user_name="ada", config_dir=tmp_path)

    assert received == {
        "user_name": "ada",
        "config_dir": tmp_path,
        "num_workers": None,
    }
    await knoggin.close()


@pytest.mark.unit
@pytest.mark.no_network
async def test_sdk_uses_stable_user_configuration_directory(monkeypatch, tmp_path):
    received = {}
    runtime = _FakeRuntime(_FakeSession())
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    monkeypatch.delenv("KNOGGIN_CONFIG_DIR", raising=False)

    async def start_runtime(**kwargs):
        received.update(kwargs)
        return runtime

    monkeypatch.setattr(client_module.ApplicationRuntime, "start", start_runtime)

    knoggin = await Knoggin.start(user_name="ada")

    assert received["config_dir"] == tmp_path / "knoggin"
    await knoggin.close()


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
    assert session.calls[0][0].metadata == {}
    assert session.calls[0][1]["idempotency_key"] == "request-1"

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

    assert session.document_service.calls == [{"document_id": "document-1"}]
    document_focus = session.calls[0][1]["document_focus"]
    assert document_focus.target_type == "document"
    assert document_focus.mode == "request"
    assert document_focus.document_id == "document-1"
    assert document_focus.relative_path == "notes/project.md"

    await knoggin.close()
