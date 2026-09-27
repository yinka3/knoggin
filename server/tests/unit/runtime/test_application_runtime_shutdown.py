import asyncio
from types import SimpleNamespace

import pytest

from runtime import application as application_module
from runtime.application import ApplicationRuntime, ApplicationShutdownError


@pytest.fixture(autouse=True)
def reset_config_manager():
    application_module.ConfigManager._instance = None
    yield
    application_module.ConfigManager._instance = None


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
        config_manager=SimpleNamespace(),
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
async def test_application_shutdown_retries_failure_without_repeating_success():
    calls = []
    runtime = ApplicationRuntime(
        config_manager=SimpleNamespace(),
        resources=RecordingOwner("resources", calls),
        projects=RecordingOwner("projects", calls),
        sessions=RecordingSessions("sessions", calls),
        agent_manager=SimpleNamespace(),
        agent_orchestrator=SimpleNamespace(),
        aac_runtime=RecordingOwner("aac", calls, RuntimeError("AAC failure")),
    )

    with pytest.raises(ApplicationShutdownError) as error:
        await runtime.shutdown()

    assert calls == ["aac", "sessions", "projects"]
    assert [failure.phase for failure in error.value.failures] == ["aac"]

    runtime.aac_runtime.error = None
    await runtime.shutdown()
    assert calls == ["aac", "sessions", "projects", "aac", "resources"]
    assert runtime._shutdown_complete


@pytest.mark.parametrize("failed_phase", ["sessions", "projects", "resources"])
async def test_failed_consumer_keeps_dependencies_until_retry(failed_phase):
    calls = []
    runtime = ApplicationRuntime(
        config_manager=SimpleNamespace(),
        resources=RecordingOwner("resources", calls),
        projects=RecordingOwner("projects", calls),
        sessions=RecordingSessions("sessions", calls),
        agent_manager=SimpleNamespace(),
        agent_orchestrator=SimpleNamespace(),
        aac_runtime=RecordingOwner("aac", calls),
    )
    owner = getattr(runtime, failed_phase)
    owner.error = RuntimeError("failed cleanup")
    with pytest.raises(ApplicationShutdownError):
        await runtime.shutdown()
    order = ["aac", "sessions", "projects", "resources"]
    assert calls == order[:order.index(failed_phase) + 1]
    assert not runtime._shutdown_complete
    owner.error = None
    await runtime.shutdown()
    assert calls == order[:order.index(failed_phase)] + [failed_phase] + order[order.index(failed_phase):]


async def test_concurrent_shutdown_calls_join_serialized_cleanup():
    calls = []
    entered = asyncio.Event()
    release = asyncio.Event()

    class WaitingOwner(RecordingOwner):
        async def shutdown(self):
            await super().shutdown()
            entered.set()
            await release.wait()

    runtime = ApplicationRuntime(
        config_manager=SimpleNamespace(),
        resources=RecordingOwner("resources", calls),
        projects=RecordingOwner("projects", calls),
        sessions=RecordingSessions("sessions", calls),
        agent_manager=SimpleNamespace(), agent_orchestrator=SimpleNamespace(),
        aac_runtime=WaitingOwner("aac", calls),
    )
    first = asyncio.create_task(runtime.shutdown())
    await entered.wait()
    second = asyncio.create_task(runtime.shutdown())
    await asyncio.sleep(0)
    assert calls == ["aac"]
    release.set()
    await asyncio.gather(first, second)
    assert calls == ["aac", "sessions", "projects", "resources"]



@pytest.mark.runtime
@pytest.mark.no_network
async def test_application_runtime_owns_and_explicitly_attaches_health_service():
    calls = []
    resources = RecordingOwner("resources", calls)
    sessions = RecordingSessions("sessions", calls)
    runtime = ApplicationRuntime(
        config_manager=SimpleNamespace(),
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
async def test_application_start_cleans_resources_when_composition_fails(
    monkeypatch, tmp_path
):
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
        await application_module.ApplicationRuntime.start(
            user_name="ada", config_dir=tmp_path
        )

    assert resources.shutdown_count == 1


@pytest.mark.runtime
@pytest.mark.no_network
async def test_application_start_cleans_aac_and_resources_when_aac_start_fails(
    monkeypatch, tmp_path
):
    calls = []
    resources = RecordingOwner("resources", calls)
    projects = RecordingOwner("projects", calls)
    projects.entity_maintenance_service = SimpleNamespace()
    projects.maintenance_service = SimpleNamespace()

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
        await application_module.ApplicationRuntime.start(
            user_name="ada", config_dir=tmp_path
        )

    assert calls == ["projects_start", "aac", "projects", "resources"]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_application_start_establishes_identity_before_managers(
    monkeypatch, tmp_path
):
    calls = []

    class KnowledgeStore:
        async def ensure_identity_entity(self, user_name, aliases):
            calls.append(("identity", user_name, aliases))

    resources = RecordingOwner("resources", calls)
    resources.knowledge_store = KnowledgeStore()
    projects = RecordingOwner("projects", calls)
    projects.entity_maintenance_service = SimpleNamespace()
    projects.maintenance_service = SimpleNamespace()
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
            assert (
                kwargs["project_maintenance_service"]
                is projects.maintenance_service
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
    config_manager = SimpleNamespace(
        config=SimpleNamespace(
            user_aliases=["Ada"],
            developer_settings=SimpleNamespace(
                documents=SimpleNamespace(project_library_root="data/projects")
            ),
        ),
        resolve_path=lambda path: tmp_path / path,
    )
    monkeypatch.setattr(
        application_module.ConfigManager,
        "initialize",
        staticmethod(lambda _config_dir: config_manager),
    )

    runtime = await application_module.ApplicationRuntime.start(
        user_name="ada", config_dir=tmp_path
    )

    assert runtime.config_manager is config_manager
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
