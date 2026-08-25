from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID

import pytest

from common.exceptions import NotFoundError
from common.schema.artifacts import ArtifactReference
from common.schema.primitives import Message
from common.schema.public import (
    CreateProjectRequest,
    CreateSessionRequest,
    StartRunRequest,
    SubmitMessageRequest,
    validate_public_stream,
)
from runtime.api_port import ApplicationRuntimePort


class FakeSession:
    user_name = "ada"
    session_id = "session-1"
    project_id = "project-1"
    model = "test-model"
    agent_id = "agent-1"
    enabled_tools = ["web_search"]

    def __init__(self):
        self.accepted: list[Message] = []
        self.run_calls: list[dict] = []

    async def accept_message(self, message):
        self.accepted.append(message)
        message.id = 42
        return message, len(self.accepted) == 1

    async def run_agent_stream(self, message, **kwargs):
        self.run_calls.append({"message": message, **kwargs})
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

    async def create_project(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "id": "project-2",
            "name": kwargs["name"],
            "description": kwargs["description"],
            "access_mode": kwargs["access_mode"],
            "status": "active",
            "allowed_projects": [],
        }


class FakeSessions:
    user_name = "ada"

    def __init__(self, session):
        self.session = session
        self.create_calls = []

    async def create_session(self, **kwargs):
        self.create_calls.append(kwargs)
        return self.session

    async def get_or_resume_session(self, session_id):
        return self.session if session_id == self.session.session_id else None


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
async def test_runtime_port_translates_project_session_acceptance_and_research_stream(
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

    accepted = await application.submit_message(
        user_name="ada",
        session_id="session-1",
        request=SubmitMessageRequest(content="hello", idempotency_key="request-1"),
    )
    assert accepted.message_id == 42
    assert accepted.idempotent is False
    assert session.accepted[0].metadata == {"idempotency_key": "request-1"}

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


@pytest.mark.runtime
@pytest.mark.no_network
async def test_runtime_port_rejects_other_user_and_missing_session(port):
    application, _, _ = port

    with pytest.raises(PermissionError):
        await application.list_artifacts(user_name="other", project_id="project-1")

    with pytest.raises(NotFoundError):
        await application.submit_message(
            user_name="ada",
            session_id="missing",
            request=SubmitMessageRequest(content="hello"),
        )
