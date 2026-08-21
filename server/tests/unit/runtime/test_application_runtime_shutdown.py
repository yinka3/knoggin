import asyncio
from types import SimpleNamespace

import pytest

from runtime import application as application_module
from runtime.application import (
    ApplicationRuntime,
    ApplicationShutdownCoordinator,
    ApplicationShutdownError,
)


class RecordingOwner:
    def __init__(self, name, calls, error=None):
        self.name = name
        self.calls = calls
        self.error = error
        self.shutdown_count = 0

    async def shutdown(self):
        self.shutdown_count += 1
        self.calls.append(self.name)
        if self.error is not None:
            raise self.error


class RecordingSessions(RecordingOwner):
    def __init__(self, name, calls, error=None):
        super().__init__(name, calls, error)
        self.health_service = None

    def attach_health_service(self, health_service):
        self.health_service = health_service

    def get_runtime_session(self, _session_id):
        return None

    def active_runtime_count(self):
        return 0


@pytest.mark.runtime
@pytest.mark.no_network
async def test_application_shutdown_is_ordered_and_idempotent():
    calls = []
    sessions = RecordingOwner("sessions", calls)
    projects = RecordingOwner("projects", calls)
    resources = RecordingOwner("resources", calls)
    coordinator = ApplicationShutdownCoordinator(
        sessions=sessions,
        projects=projects,
        resources=resources,
    )

    await asyncio.gather(coordinator.shutdown(), coordinator.shutdown())
    await coordinator.shutdown()

    assert calls == ["sessions", "projects", "resources"]
    assert sessions.shutdown_count == 1
    assert projects.shutdown_count == 1
    assert resources.shutdown_count == 1


@pytest.mark.runtime
@pytest.mark.no_network
async def test_application_shutdown_continues_after_a_phase_failure():
    calls = []
    coordinator = ApplicationShutdownCoordinator(
        sessions=RecordingOwner("sessions", calls, RuntimeError("session failure")),
        projects=RecordingOwner("projects", calls),
        resources=RecordingOwner("resources", calls),
    )

    with pytest.raises(ApplicationShutdownError) as error:
        await coordinator.shutdown()

    assert calls == ["sessions", "projects", "resources"]
    assert [failure.phase for failure in error.value.failures] == ["sessions"]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_application_runtime_delegates_shutdown_to_the_coordinator():
    calls = []
    runtime = ApplicationRuntime(
        resources=RecordingOwner("resources", calls),
        projects=RecordingOwner("projects", calls),
        sessions=RecordingOwner("sessions", calls),
        agent_manager=SimpleNamespace(),
        agent_orchestrator=SimpleNamespace(),
    )

    await runtime.shutdown()

    assert calls == ["sessions", "projects", "resources"]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_application_runtime_owns_and_explicitly_attaches_health_service():
    calls = []
    resources = RecordingOwner("resources", calls)
    sessions = RecordingSessions("sessions", calls)
    runtime = ApplicationRuntime(
        resources=resources,
        projects=RecordingOwner("projects", calls),
        sessions=sessions,
        agent_manager=SimpleNamespace(),
        agent_orchestrator=SimpleNamespace(),
    )

    assert sessions.health_service is runtime.health_service
    assert not hasattr(resources, "health_service")


@pytest.mark.runtime
@pytest.mark.no_network
async def test_application_start_cleans_resources_when_composition_fails(monkeypatch):
    resources = RecordingOwner("resources", [])

    class KnowledgeStore:
        async def ensure_identity_entity(self, _user_name, _aliases):
            return None

    resources.knowledge_store = KnowledgeStore()

    async def create_resources(cls, *, num_workers=None):
        return resources

    def fail_project_manager(**_kwargs):
        raise RuntimeError("project composition failed")

    monkeypatch.setattr(
        application_module.RuntimeResources,
        "create",
        classmethod(create_resources),
    )
    monkeypatch.setattr(application_module, "ProjectManager", fail_project_manager)

    with pytest.raises(RuntimeError, match="project composition failed"):
        await application_module.ApplicationRuntime.start(user_name="ada")

    assert resources.shutdown_count == 1


@pytest.mark.runtime
@pytest.mark.no_network
async def test_application_start_establishes_identity_before_managers(monkeypatch):
    calls = []

    class KnowledgeStore:
        async def ensure_identity_entity(self, user_name, aliases):
            calls.append(("identity", user_name, aliases))

    resources = RecordingOwner("resources", calls)
    resources.knowledge_store = KnowledgeStore()
    projects = RecordingOwner("projects", calls)
    sessions = RecordingSessions("sessions", calls)

    class RecordingAgentManager:
        def __init__(self, received_resources, received_user_name):
            assert received_resources is resources
            assert received_user_name == "ada"
            calls.append("agent_manager")

        async def ensure_default_agent(self):
            calls.append("ensure_default_agent")

    class RecordingAgentOrchestrator:
        def __init__(self, manager):
            assert isinstance(manager, RecordingAgentManager)
            calls.append("agent_orchestrator")

    def create_sessions(**kwargs):
        assert isinstance(kwargs["agent_orchestrator"], RecordingAgentOrchestrator)
        calls.append("sessions")
        return sessions

    async def create_resources(cls, *, num_workers=None):
        return resources

    monkeypatch.setattr(
        application_module.RuntimeResources,
        "create",
        classmethod(create_resources),
    )
    monkeypatch.setattr(application_module, "ProjectManager", lambda **_kwargs: projects)
    monkeypatch.setattr(application_module, "AgentManager", RecordingAgentManager)
    monkeypatch.setattr(application_module, "AgentOrchestrator", RecordingAgentOrchestrator)
    monkeypatch.setattr(application_module, "SessionManager", create_sessions)
    monkeypatch.setattr(
        application_module.ConfigManager,
        "get",
        staticmethod(lambda: SimpleNamespace(config=SimpleNamespace(user_aliases=["Ada"]))),
    )

    runtime = await application_module.ApplicationRuntime.start(user_name="ada")

    assert calls == [
        ("identity", "ada", ["Ada"]),
        "agent_manager",
        "ensure_default_agent",
        "agent_orchestrator",
        "sessions",
    ]
    assert isinstance(runtime.agent_manager, RecordingAgentManager)
    assert isinstance(runtime.agent_orchestrator, RecordingAgentOrchestrator)
    await runtime.shutdown()
