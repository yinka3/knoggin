from datetime import datetime, timezone

import httpx
import pytest

from api.app import create_app
from common.exceptions import SessionBusyError, StorageReadError
from common.schema.artifacts import (
    ArtifactDraft,
    MarkdownArtifactBlock,
    artifact_content_hash,
    render_artifact_markdown,
)
from common.schema.public import (
    CreateProjectRequest,
    CreateSessionRequest,
    ProjectResponse,
    RunResult,
    SessionResponse,
    StartRunRequest,
)


def _artifact_payloads():
    draft = ArtifactDraft(
        kind="research_brief",
        title="Research brief",
        blocks=(MarkdownArtifactBlock(content="Finding"),),
    )
    markdown = render_artifact_markdown(draft)
    reference = {
        "artifact_id": "11111111-1111-1111-1111-111111111111",
        "project_id": "project-1",
        "session_id": "session-1",
        "originating_message_id": 42,
        "kind": "research_brief",
        "title": "Research brief",
        "status": "complete",
        "current_revision": 1,
        "created_at": datetime(2026, 1, 2, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 1, 2, tzinfo=timezone.utc),
    }
    revision = {
        "artifact_id": reference["artifact_id"],
        "revision": 1,
        "schema_version": 1,
        "kind": "research_brief",
        "title": "Research brief",
        "blocks": draft.model_dump(mode="json")["blocks"],
        "status": "complete",
        "markdown": markdown,
        "content_hash": artifact_content_hash(draft, markdown),
        "created_at": datetime(2026, 1, 2, tzinfo=timezone.utc),
    }
    return reference, revision


class FakeApplication:
    def __init__(self):
        self.calls = []
        self.fail_projects = False
        self.artifact, self.artifact_revision = _artifact_payloads()
        self.document_focus = None

    async def create_project(self, *, user_name, request: CreateProjectRequest):
        self.calls.append(("project", user_name, request))
        if self.fail_projects:
            raise StorageReadError("postgres://secret should never leak")
        return ProjectResponse(
            id="project-1",
            name=request.name,
            description=request.description,
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

    async def get_document_focus(self, *, user_name, session_id):
        self.calls.append(("document_focus_get", user_name, session_id))
        return self.document_focus

    async def set_document_focus(self, *, user_name, session_id, request):
        self.calls.append(("document_focus_set", user_name, session_id, request))
        target = request.model_dump()
        self.document_focus = {
            "mode": "pinned",
            "created_at": datetime(2026, 1, 2, tzinfo=timezone.utc),
            "target_type": target["target_type"],
            "document_id": target.get("document_id"),
            "relative_path": "docs/notes.py"
            if target["target_type"] == "document"
            else None,
            "path_prefix": target.get("path_prefix"),
        }
        return self.document_focus

    async def clear_document_focus(self, *, user_name, session_id):
        self.calls.append(("document_focus_clear", user_name, session_id))
        self.document_focus = None

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

    async def list_artifacts(self, *, user_name, project_id, session_id=None, limit=50):
        self.calls.append(("artifacts", user_name, project_id, session_id, limit))
        return [self.artifact]

    async def get_artifact(
        self, *, user_name, project_id, artifact_id, session_id=None
    ):
        self.calls.append(("artifact", user_name, project_id, artifact_id, session_id))
        return self.artifact if artifact_id == self.artifact["artifact_id"] else None

    async def get_artifact_revision(
        self, *, user_name, project_id, artifact_id, revision, session_id=None
    ):
        self.calls.append(
            ("artifact_revision", user_name, project_id, artifact_id, revision, session_id)
        )
        return (
            self.artifact_revision
            if artifact_id == self.artifact["artifact_id"] and revision == 1
            else None
        )

    @staticmethod
    def _maintenance_review(scope="user-global", project_id=None):
        return {
            "review_id": "review-1",
            "scope": scope,
            "project_id": project_id,
            "kind": "entity_merge" if scope == "user-global" else "relationship_interpretation",
            "reasoning": "Reviewed durable evidence.",
            "proposed_plan": {
                "kind": "entity_merge" if scope == "user-global" else "relationship_interpretation",
            },
            "expected_state": {"state_hash": "hash-1"},
            "status": "open",
        }

    async def list_global_maintenance_reviews(self, *, user_name):
        self.calls.append(("global_reviews", user_name))
        return [self._maintenance_review()]

    async def decide_global_maintenance_review(self, *, user_name, review_id, request):
        self.calls.append(("global_review_decision", user_name, review_id, request))
        return {"review_id": review_id, "action": request.action}

    async def list_project_maintenance_reviews(self, *, user_name, project_id):
        self.calls.append(("project_reviews", user_name, project_id))
        return [self._maintenance_review("project", project_id)]

    async def decide_project_maintenance_review(
        self, *, user_name, project_id, review_id, request
    ):
        self.calls.append(
            ("project_review_decision", user_name, project_id, review_id, request)
        )
        return {"review_id": review_id, "action": request.action}

    async def preview_entity_merge_rollback(self, *, user_name, merge_id):
        self.calls.append(("rollback_preview", user_name, merge_id))
        return {"merge_id": merge_id, "safe_mutation_ids": [1]}

    async def rollback_entity_merge(self, *, user_name, merge_id, request):
        self.calls.append(("rollback", user_name, merge_id, request))
        return {
            "merge_id": merge_id,
            "applied_mutation_ids": list(request.approved_mutation_ids),
        }


class BrokenStreamApplication(FakeApplication):
    async def run_stream(self, *, user_name, request: StartRunRequest):
        yield {"type": "private.tool.payload", "secret": "do not expose"}


class BusyRunApplication(FakeApplication):
    async def open_run_stream(self, *, user_name, request: StartRunRequest):
        raise SessionBusyError()


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
        result = await client.post(
            "/v1/runs",
            headers={"X-User-Name": "ada"},
            json={
                "session_id": "session-1",
                "query": "What happened?",
                "research_mode": "research",
            },
        )

    assert project.status_code == 201
    assert project.json()["id"] == "project-1"
    assert session.status_code == 201
    assert session.json()["enabled_tools"] == ["search_messages"]
    assert result.status_code == 200
    assert result.json() == {
        "run_id": "run-1",
        "content": "Done",
        "sources": [],
        "usage": None,
        "research_mode": "normal",
        "assistant_message_id": None,
        "source_ref_ids": [],
        "artifact": None,
    }
    assert [call[0] for call in port.calls] == [
        "project",
        "session",
        "run",
    ]
    assert port.calls[-1][2].research_mode == "research"


@pytest.mark.unit
@pytest.mark.no_network
async def test_document_focus_routes_keep_selection_request_only():
    port = FakeApplication()
    app = create_app(port)

    async with await _client(app) as client:
        initial = await client.get(
            "/v1/sessions/session-1/document-focus",
            headers={"X-User-Name": "ada"},
        )
        set_focus = await client.put(
            "/v1/sessions/session-1/document-focus",
            headers={"X-User-Name": "ada"},
            json={"target_type": "document", "document_id": "document-1"},
        )
        rejected_selection = await client.put(
            "/v1/sessions/session-1/document-focus",
            headers={"X-User-Name": "ada"},
            json={
                "target_type": "document",
                "document_id": "document-1",
                "selection": {
                    "content_hash": "a" * 64,
                    "locator": {
                        "kind": "text_lines",
                        "start_line": 1,
                        "end_line": 1,
                    },
                },
            },
        )
        cleared = await client.delete(
            "/v1/sessions/session-1/document-focus",
            headers={"X-User-Name": "ada"},
        )

    assert initial.status_code == 200
    assert initial.json() is None
    assert set_focus.status_code == 200
    assert set_focus.json()["relative_path"] == "docs/notes.py"
    assert rejected_selection.status_code == 422
    assert cleared.status_code == 204
    assert [call[0] for call in port.calls] == [
        "document_focus_get",
        "document_focus_set",
        "document_focus_clear",
    ]


@pytest.mark.unit
@pytest.mark.no_network
async def test_maintenance_routes_expose_review_decisions_and_rollback():
    port = FakeApplication()
    app = create_app(port)
    headers = {"X-User-Name": "ada"}

    async with await _client(app) as client:
        global_reviews = await client.get("/v1/maintenance/reviews", headers=headers)
        global_decision = await client.post(
            "/v1/maintenance/reviews/review-1/decision",
            headers=headers,
            json={"action": "apply", "expected_state": {"state_hash": "hash-1"}},
        )
        project_reviews = await client.get(
            "/v1/projects/project-1/maintenance/reviews",
            headers=headers,
        )
        project_decision = await client.post(
            "/v1/projects/project-1/maintenance/reviews/review-2/decision",
            headers=headers,
            json={"action": "dismiss", "reason": "Not applicable"},
        )
        preview = await client.get(
            "/v1/maintenance/entity-merges/merge-1/rollback",
            headers=headers,
        )
        rollback = await client.post(
            "/v1/maintenance/entity-merges/merge-1/rollback",
            headers=headers,
            json={"approved_mutation_ids": [1, 2]},
        )

    assert global_reviews.status_code == 200
    assert global_reviews.json()["reviews"][0]["review_id"] == "review-1"
    assert global_decision.json()["result"]["action"] == "apply"
    assert project_reviews.json()["reviews"][0]["project_id"] == "project-1"
    assert project_decision.json()["result"]["action"] == "dismiss"
    assert preview.json()["result"]["safe_mutation_ids"] == [1]
    assert rollback.json()["result"]["applied_mutation_ids"] == [1, 2]
    assert [call[0] for call in port.calls] == [
        "global_reviews",
        "global_review_decision",
        "project_reviews",
        "project_review_decision",
        "rollback_preview",
        "rollback",
    ]


@pytest.mark.unit
@pytest.mark.no_network
async def test_run_admission_conflict_is_returned_before_http_or_sse_starts():
    app = create_app(BusyRunApplication())

    async with await _client(app) as client:
        run = await client.post(
            "/v1/runs",
            headers={"X-User-Name": "ada"},
            json={"session_id": "session-1", "query": "hello"},
        )
        stream = await client.post(
            "/v1/runs/stream",
            headers={"X-User-Name": "ada"},
            json={"session_id": "session-1", "query": "hello"},
        )

    for response in (run, stream):
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "session_busy"


@pytest.mark.unit
@pytest.mark.no_network
async def test_artifact_routes_expose_scoped_reference_and_revision_contracts():
    port = FakeApplication()
    app = create_app(port)
    artifact_id = port.artifact["artifact_id"]

    async with await _client(app) as client:
        listed = await client.get(
            "/v1/projects/project-1/artifacts?session_id=session-1&limit=10",
            headers={"X-User-Name": "ada"},
        )
        reference = await client.get(
            f"/v1/projects/project-1/artifacts/{artifact_id}",
            headers={"X-User-Name": "ada"},
        )
        revision = await client.get(
            f"/v1/projects/project-1/artifacts/{artifact_id}/revisions/1",
            headers={"X-User-Name": "ada"},
        )
        missing = await client.get(
            "/v1/projects/project-1/artifacts/22222222-2222-2222-2222-222222222222",
            headers={"X-User-Name": "ada"},
        )
        invalid_limit = await client.get(
            "/v1/projects/project-1/artifacts?limit=0",
            headers={"X-User-Name": "ada"},
        )

    assert listed.status_code == 200
    assert listed.json()["artifacts"][0]["artifact_id"] == artifact_id
    assert reference.status_code == 200
    assert reference.json()["current_revision"] == 1
    assert revision.status_code == 200
    assert revision.json()["blocks"] == [{"kind": "markdown", "content": "Finding"}]
    assert revision.json()["markdown"] == "# Research brief\n\nFinding\n"
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "not_found"
    assert invalid_limit.status_code == 422
    assert invalid_limit.json()["error"]["code"] == "invalid_request"


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
