from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID

import pytest

from common.exceptions import NotFoundError
from common.schema.artifacts import ArtifactReference
from common.schema.public import (
    CreateProjectRequest,
    CreateSessionRequest,
    EntityMergeRollbackRequest,
    MaintenanceReviewDecisionRequest,
    SetDocumentFocusDocument,
    StartRunRequest,
    validate_public_stream,
)
from core.knowledge.maintenance_impact import MaintenanceImpactPlanner
from core.knowledge.maintenance_reviews import (
    EntityMergePlan,
    MaintenanceReview,
    MaintenanceReviewDetail,
)
from runtime.api_port import ApplicationRuntimePort


class FakeDocumentService:
    def __init__(self):
        self.calls = []

    async def resolve_focus_target(self, **kwargs):
        self.calls.append(("focus", kwargs))
        return {
            "target_type": "document",
            "document_id": kwargs["document_id"],
            "relative_path": "docs/notes.py",
        }

    async def resolve_document_selection(self, **kwargs):
        self.calls.append(("selection", kwargs))
        return {
            "content_hash": kwargs["selection"].content_hash,
            "locator": {
                "kind": "code_lines",
                "start_line": 2,
                "end_line": 3,
            },
        }


class FakeSession:
    user_name = "ada"
    session_id = "session-1"
    project_id = "project-1"
    model = "test-model"
    agent_id = "agent-1"
    enabled_tools = ["web_search"]

    def __init__(self):
        self.run_calls: list[dict] = []
        self.document_service = FakeDocumentService()

    async def open_agent_run_stream(self, message, **kwargs):
        self.run_calls.append({"message": message, **kwargs})
        return self._events()

    async def _events(self):
        yield {"event": "tool_start", "data": {"tool": "web_search"}}
        yield {"event": "token", "data": {"content": "Answer"}}
        yield {
            "event": "response",
            "data": {
                "content": "Answer",
                "usage": {
                    "prompt_tokens": 3,
                    "completion_tokens": 2,
                    "total_tokens": 5,
                    "approximate": False,
                },
                "research_mode": "deep_research",
                "assistant_message_id": 43,
                "source_ref_ids": [],
            },
        }


class FakeKnowledgeStore:
    def __init__(self, artifact):
        self.artifact = artifact
        self.calls = []

    async def list_project_artifacts(self, **kwargs):
        self.calls.append(("list", kwargs))
        return [self.artifact]

    async def get_project_artifact(self, artifact_id, **kwargs):
        self.calls.append(("get", artifact_id, kwargs))
        return self.artifact

    async def get_project_artifact_revision(self, artifact_id, revision, **kwargs):
        self.calls.append(("revision", artifact_id, revision, kwargs))
        return None

    async def get_message_source_refs(self, message_id, **kwargs):
        self.calls.append(("sources", message_id, kwargs))
        return []

    async def get_message_artifact(self, message_id, **kwargs):
        self.calls.append(("message_artifact", message_id, kwargs))
        return self.artifact


class FakeProjects:
    def __init__(self):
        self.calls = []
        self.maintenance_service = self

    @staticmethod
    def _review(*, scope="user-global", project_id=None, status="open"):
        return MaintenanceReview(
            review_id="review-1",
            user_name="ada",
            scope=scope,
            project_id=project_id,
            kind="entity_merge",
            reasoning="The evidence identifies one entity.",
            proposed_plan=EntityMergePlan(
                survivor_entity_id=2,
                retired_entity_id=3,
                expected_state_hash="hash-1",
            ),
            expected_state={"state_hash": "hash-1"},
            status=status,
        )

    async def create_project(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "id": "project-2",
            "name": kwargs["name"],
            "description": kwargs["description"],
            "status": "active",
            "allowed_projects": [],
        }

    async def list_global_maintenance_reviews(self):
        self.calls.append({"operation": "list_global_reviews"})
        return [self._review()]

    async def apply_global_entity_merge_review(self, review_id, **kwargs):
        self.calls.append(
            {"operation": "apply_global_review", "review_id": review_id, **kwargs}
        )
        return {"merge_id": "merge-1", "review_id": review_id}

    async def dismiss_global_maintenance_review(self, review_id, **kwargs):
        self.calls.append(
            {"operation": "dismiss_global_review", "review_id": review_id, **kwargs}
        )
        return self._review(status="dismissed")

    async def list_maintenance_reviews(self, project_id):
        self.calls.append(
            {"operation": "list_project_reviews", "project_id": project_id}
        )
        return [self._review(scope="project", project_id=project_id)]

    async def get_maintenance_review_detail(self, project_id, review_id):
        self.calls.append(
            {
                "operation": "get_project_review",
                "project_id": project_id,
                "review_id": review_id,
            }
        )
        review = self._review(scope="project", project_id=project_id)
        return MaintenanceReviewDetail(
            review=review,
            stored_snapshot=review.evidence_snapshot,
            evidence_state="current",
        )

    async def preview_maintenance_review(self, project_id, review_id):
        detail = await self.get_maintenance_review_detail(project_id, review_id)
        self.calls[-1]["operation"] = "preview_project_review"
        return detail, MaintenanceImpactPlanner.preview(detail.review)

    async def transition_maintenance_review(
        self, project_id, review_id, **kwargs
    ):
        self.calls.append(
            {
                "operation": "decide_project_review",
                "project_id": project_id,
                "review_id": review_id,
                **kwargs,
            }
        )
        return self._review(
            scope="project",
            project_id=project_id,
            status=kwargs["status"],
        )

    async def preview_global_entity_merge_rollback(self, merge_id):
        self.calls.append({"operation": "preview_rollback", "merge_id": merge_id})
        return {"merge_id": merge_id, "safe_mutation_ids": [1]}

    async def rollback_global_entity_merge(self, merge_id, **kwargs):
        self.calls.append(
            {"operation": "rollback", "merge_id": merge_id, **kwargs}
        )
        return {"merge_id": merge_id, "rolled_back": True}


class FakeSessions:
    user_name = "ada"

    def __init__(self, session):
        self.session = session
        self.create_calls = []
        self.document_focus = None
        self.focus_calls = []

    async def create_session(self, **kwargs):
        self.create_calls.append(kwargs)
        return self.session

    async def get_or_resume_session(self, session_id):
        return self.session if session_id == self.session.session_id else None

    async def get_document_focus(self, session_id):
        self.focus_calls.append(("get", session_id))
        return self.document_focus

    async def set_document_focus(self, session_id, **kwargs):
        self.focus_calls.append(("set", session_id, kwargs))
        target_type = (
            "document"
            if kwargs["document_id"] is not None
            else "subtree"
            if kwargs["path_prefix"] is not None
            else "subtree"
        )
        self.document_focus = {
            "mode": "pinned",
            "created_at": datetime(2026, 1, 2, tzinfo=timezone.utc),
            "target_type": target_type,
            "document_id": kwargs["document_id"],
            "relative_path": "docs/notes.py" if kwargs["document_id"] else None,
            "path_prefix": kwargs["path_prefix"],
        }
        return self.document_focus

    async def clear_document_focus(self, session_id):
        self.focus_calls.append(("clear", session_id))
        self.document_focus = None
        return True


@pytest.fixture
def port():
    created_at = datetime(2026, 1, 2, tzinfo=timezone.utc)
    artifact = ArtifactReference(
        artifact_id=UUID("11111111-1111-1111-1111-111111111111"),
        project_id="project-1",
        session_id="session-1",
        originating_message_id=43,
        kind="research_report",
        title="Research report",
        status="complete",
        current_revision=1,
        created_at=created_at,
        updated_at=created_at,
    )
    session = FakeSession()
    runtime = SimpleNamespace(
        sessions=FakeSessions(session),
        projects=FakeProjects(),
        resources=SimpleNamespace(knowledge_store=FakeKnowledgeStore(artifact)),
    )
    return ApplicationRuntimePort(runtime), runtime, session


@pytest.mark.runtime
@pytest.mark.no_network
async def test_runtime_port_translates_project_session_and_research_stream(
    port,
):
    application, runtime, session = port

    project = await application.create_project(
        user_name="ada",
        request=CreateProjectRequest(name="Research"),
    )
    assert project.id == "project-2"
    assert runtime.projects.calls[0]["domain_config"].version == 0

    created = await application.create_session(
        user_name="ada",
        request=CreateSessionRequest(project_id="project-1"),
    )
    assert created["session_id"] == "session-1"

    events = [
        event
        async for event in application.run_stream(
            user_name="ada",
            request=StartRunRequest(
                session_id="session-1",
                query="Investigate this",
                research_mode="deep_research",
            ),
        )
    ]
    parsed = validate_public_stream(events, require_terminal=True)
    assert parsed[-1].type == "run.completed"
    assert parsed[-1].result.research_mode == "deep_research"
    assert parsed[-1].result.artifact is not None
    assert session.run_calls[0]["research_mode"] == "deep_research"
    assert (
        "sources",
        43,
        {
            "user_name": "ada",
            "project_id": "project-1",
            "session_id": "session-1",
        },
    ) in runtime.resources.knowledge_store.calls
    assert (
        "message_artifact",
        43,
        {
            "user_name": "ada",
            "project_id": "project-1",
            "session_id": "session-1",
        },
    ) in runtime.resources.knowledge_store.calls


@pytest.mark.runtime
@pytest.mark.no_network
async def test_runtime_port_rejects_other_user_and_missing_session(port):
    application, _, _ = port

    with pytest.raises(PermissionError):
        await application.list_artifacts(user_name="other", project_id="project-1")

    with pytest.raises(NotFoundError):
        await application.open_run_stream(
            user_name="ada",
            request=StartRunRequest(session_id="missing", query="hello"),
        )


@pytest.mark.runtime
@pytest.mark.no_network
async def test_runtime_port_routes_pinned_and_request_document_focus(port):
    application, runtime, session = port

    pinned = await application.set_document_focus(
        user_name="ada",
        session_id="session-1",
        request=SetDocumentFocusDocument(
            target_type="document",
            document_id="document-1",
        ),
    )
    assert pinned.target_type == "document"
    assert pinned.relative_path == "docs/notes.py"
    assert await application.get_document_focus(
        user_name="ada",
        session_id="session-1",
    ) == pinned

    events = [
        event
        async for event in application.run_stream(
            user_name="ada",
            request=StartRunRequest(
                session_id="session-1",
                query="Explain the selection",
                document_focus={
                    "target_type": "document",
                    "document_id": "document-1",
                    "selection": {
                        "content_hash": "a" * 64,
                        "locator": {
                            "kind": "code_lines",
                            "start_line": 2,
                            "end_line": 3,
                            "symbol_name": "client-must-not-control-this",
                        },
                    },
                },
            ),
        )
    ]

    assert validate_public_stream(events, require_terminal=True)[-1].type == "run.completed"
    request_focus = session.run_calls[0]["document_focus"]
    assert request_focus.mode == "request"
    assert request_focus.relative_path == "docs/notes.py"
    assert request_focus.selection.locator.symbol_name is None
    assert runtime.sessions.focus_calls[-1] == ("get", "session-1")

    await application.clear_document_focus(user_name="ada", session_id="session-1")
    assert runtime.sessions.focus_calls[-1] == ("clear", "session-1")


@pytest.mark.runtime
@pytest.mark.no_network
async def test_runtime_port_routes_maintenance_through_project_owner(port):
    application, runtime, _ = port

    reviews = await application.list_global_maintenance_reviews(user_name="ada")
    applied = await application.decide_global_maintenance_review(
        user_name="ada",
        review_id="review-1",
        request=MaintenanceReviewDecisionRequest(
            action="apply",
            expected_state={"state_hash": "hash-1"},
        ),
    )
    project_reviews = await application.list_project_maintenance_reviews(
        user_name="ada",
        project_id="project-1",
    )
    detail = await application.get_project_maintenance_review(
        user_name="ada", project_id="project-1", review_id="review-1"
    )
    impact = await application.preview_project_maintenance_review(
        user_name="ada", project_id="project-1", review_id="review-1"
    )
    dismissed = await application.decide_project_maintenance_review(
        user_name="ada",
        project_id="project-1",
        review_id="review-1",
        request=MaintenanceReviewDecisionRequest(action="dismiss"),
    )
    preview = await application.preview_entity_merge_rollback(
        user_name="ada",
        merge_id="merge-1",
    )
    rollback = await application.rollback_entity_merge(
        user_name="ada",
        merge_id="merge-1",
        request=EntityMergeRollbackRequest(approved_mutation_ids=(1,)),
    )

    assert reviews[0].scope == "user-global"
    assert applied == {"merge_id": "merge-1", "review_id": "review-1"}
    assert project_reviews[0].project_id == "project-1"
    assert detail.evidence_state == "current"
    assert impact.impact.review_id == "review-1"
    assert dismissed["review"]["status"] == "dismissed"
    assert preview["safe_mutation_ids"] == [1]
    assert rollback["rolled_back"] is True
    assert [
        call["operation"]
        for call in runtime.projects.calls
        if "operation" in call
    ] == [
        "list_global_reviews",
        "apply_global_review",
        "list_project_reviews",
        "get_project_review",
        "preview_project_review",
        "decide_project_review",
        "preview_rollback",
        "rollback",
    ]
