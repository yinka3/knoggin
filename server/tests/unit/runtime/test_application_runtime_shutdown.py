from types import SimpleNamespace

import pytest

from runtime import application as application_module
from runtime.application import ApplicationRuntime, ApplicationShutdownError


class RecordingOwner:
    def __init__(self, name, calls, error=None):
        self.name = name
        self.calls = calls
        self.error = error
        self.shutdown_count = 0

    async def start(self):
        self.calls.append(f"{self.name}_start")

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
    runtime = ApplicationRuntime(
        resources=RecordingOwner("resources", calls),
        projects=RecordingOwner("projects", calls),
        sessions=RecordingSessions("sessions", calls),
        agent_manager=SimpleNamespace(),
        agent_orchestrator=SimpleNamespace(),
        aac_runtime=RecordingOwner("aac", calls),
    )

    await runtime.shutdown()
    await runtime.shutdown()

    assert calls == ["aac", "sessions", "projects", "resources"]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_application_shutdown_continues_after_a_phase_failure_and_replays_error():
    calls = []
    runtime = ApplicationRuntime(
        resources=RecordingOwner("resources", calls),
        projects=RecordingOwner("projects", calls),
        sessions=RecordingSessions("sessions", calls),
        agent_manager=SimpleNamespace(),
        agent_orchestrator=SimpleNamespace(),
        aac_runtime=RecordingOwner("aac", calls, RuntimeError("AAC failure")),
    )

    with pytest.raises(ApplicationShutdownError) as error:
        await runtime.shutdown()

    assert calls == ["aac", "sessions", "projects", "resources"]
    assert [failure.phase for failure in error.value.failures] == ["aac"]

    with pytest.raises(ApplicationShutdownError) as repeated_error:
        await runtime.shutdown()

    assert repeated_error.value is error.value
    assert calls == ["aac", "sessions", "projects", "resources"]



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
        aac_runtime=RecordingOwner("aac", calls),
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
async def test_application_start_cleans_aac_and_resources_when_aac_start_fails(monkeypatch):
    calls = []
    resources = RecordingOwner("resources", calls)
    projects = RecordingOwner("projects", calls)
    projects.entity_maintenance_service = SimpleNamespace()

    class KnowledgeStore:
        async def ensure_identity_entity(self, _user_name, _aliases):
            return None

    class RecordingAACRuntime(RecordingOwner):
        @classmethod
        async def create(cls, **_kwargs):
            return cls("aac", calls)

        async def start(self):
            raise RuntimeError("AAC start failed")

    resources.knowledge_store = KnowledgeStore()

    async def create_resources(cls, *, num_workers=None):
        return resources

    class RecordingAgentManager:
        def __init__(self, *_args):
            pass

        async def ensure_default_agent(self):
            pass

    monkeypatch.setattr(
        application_module.RuntimeResources,
        "create",
        classmethod(create_resources),
    )
    monkeypatch.setattr(
        application_module,
        "ProjectManager",
        lambda **_kwargs: projects,
    )
    monkeypatch.setattr(application_module, "AgentManager", RecordingAgentManager)
    monkeypatch.setattr(
        application_module,
        "AgentOrchestrator",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        application_module,
        "SessionManager",
        lambda **_kwargs: object(),
    )
    monkeypatch.setattr(application_module, "AACRuntime", RecordingAACRuntime)
    monkeypatch.setattr(
        application_module.ConfigManager,
        "get",
        staticmethod(lambda: SimpleNamespace(config=SimpleNamespace(user_aliases=[]))),
    )

    with pytest.raises(RuntimeError, match="AAC start failed"):
        await application_module.ApplicationRuntime.start(user_name="ada")

    assert calls == ["projects_start", "aac", "projects", "resources"]


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
    projects.entity_maintenance_service = SimpleNamespace()
    sessions = RecordingSessions("sessions", calls)

    class RecordingAgentManager:
        def __init__(self, received_resources, received_user_name):
            assert received_resources is resources
            assert received_user_name == "ada"
            calls.append("agent_manager")

        async def ensure_default_agent(self):
            calls.append("ensure_default_agent")

    class RecordingAgentOrchestrator:
        def __init__(self, manager, **kwargs):
            assert isinstance(manager, RecordingAgentManager)
            assert (
                kwargs["entity_maintenance_service"]
                is projects.entity_maintenance_service
            )
            calls.append("agent_orchestrator")

    class RecordingAACRuntime:
        @classmethod
        async def create(cls, **kwargs):
            assert kwargs["resources"] is resources
            assert isinstance(kwargs["agent_manager"], RecordingAgentManager)
            calls.append("aac_create")
            return cls()

        async def start(self):
            calls.append("aac_start")

        async def shutdown(self):
            calls.append("aac_shutdown")

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
    monkeypatch.setattr(application_module, "AACRuntime", RecordingAACRuntime)
    monkeypatch.setattr(
        application_module.ConfigManager,
        "get",
        staticmethod(lambda: SimpleNamespace(config=SimpleNamespace(user_aliases=["Ada"]))),
    )

    runtime = await application_module.ApplicationRuntime.start(user_name="ada")

    assert calls == [
        ("identity", "ada", ["Ada"]),
        "projects_start",
        "agent_manager",
        "ensure_default_agent",
        "agent_orchestrator",
        "sessions",
        "aac_create",
        "aac_start",
    ]
    assert isinstance(runtime.agent_manager, RecordingAgentManager)
    assert isinstance(runtime.agent_orchestrator, RecordingAgentOrchestrator)
    await runtime.shutdown()
