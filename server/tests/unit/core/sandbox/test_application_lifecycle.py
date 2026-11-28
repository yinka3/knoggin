from types import SimpleNamespace

from common.schema.settings import RootConfig
from runtime import application as application_module
from runtime.application import ApplicationRuntime, _StartupCleanup


class Owner:
    def __init__(self, name, calls):
        self.name = name
        self.calls = calls

    async def shutdown(self):
        self.calls.append(self.name)


class Sessions(Owner):
    def attach_health_service(self, _service):
        pass


async def test_application_shuts_sandbox_before_projects_and_resources():
    calls = []
    runtime = ApplicationRuntime(
        config_manager=SimpleNamespace(),
        resources=Owner("resources", calls),
        projects=Owner("projects", calls),
        sessions=Sessions("sessions", calls),
        agent_manager=SimpleNamespace(),
        agent_orchestrator=SimpleNamespace(),
        aac_runtime=Owner("aac", calls),
        sandbox=Owner("sandbox", calls),
    )
    await runtime.shutdown()
    assert calls == ["aac", "sessions", "sandbox", "projects", "resources"]


async def test_failed_startup_unwinds_sandbox_before_projects():
    calls = []
    cleanup = _StartupCleanup(Owner("resources", calls))
    cleanup.owners["projects"] = Owner("projects", calls)
    cleanup.owners["sandbox"] = Owner("sandbox", calls)
    await cleanup.shutdown()
    assert calls == ["sandbox", "projects", "resources"]


async def test_application_start_owns_and_attaches_sandbox(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setenv("KNOGGIN_HOST_LIBRARY_ROOT", "/host/projects")
    config = RootConfig()
    config_manager = SimpleNamespace(
        config=config,
        config_dir=tmp_path,
        resolve_path=lambda _path: tmp_path / "projects",
    )
    monkeypatch.setattr(
        application_module.ConfigManager,
        "initialize",
        staticmethod(lambda _config_dir: config_manager),
    )

    class KnowledgeStore:
        async def ensure_identity_entity(self, *_args):
            return None

        async def close_orphaned_open_exchanges(self, **_kwargs):
            return 0

    resources = Owner("resources", calls)
    resources.knowledge_store = KnowledgeStore()

    async def create_resources(**_kwargs):
        return resources

    monkeypatch.setattr(application_module.RuntimeResources, "create", create_resources)

    class Projects(Owner):
        def __init__(self, **_kwargs):
            super().__init__("projects", calls)
            self.entity_maintenance_service = object()
            self.maintenance_service = object()
            self.sandbox = None

        async def start(self):
            calls.append("projects_start")

        def attach_sandbox_manager(self, sandbox):
            self.sandbox = sandbox

    class Sandbox(Owner):
        def __init__(self, **kwargs):
            super().__init__("sandbox", calls)
            self.runtime_settings = kwargs["runtime_settings"]

        async def start(self):
            calls.append("sandbox_start")

    class Agents:
        def __init__(self, *_args):
            pass

        async def ensure_default_agent(self):
            pass

        async def ensure_maintenance_agent(self):
            pass

    class Sessions(Owner):
        def __init__(self, **_kwargs):
            super().__init__("sessions", calls)

        def attach_health_service(self, _service):
            pass

        async def resume_background_work(self):
            pass

    class AAC(Owner):
        @classmethod
        async def create(cls, **_kwargs):
            return cls("aac", calls)

        async def start(self):
            pass

    monkeypatch.setattr(application_module, "ProjectManager", Projects)
    monkeypatch.setattr(application_module, "SandboxManager", Sandbox)
    monkeypatch.setattr(application_module, "AgentManager", Agents)
    monkeypatch.setattr(application_module, "AgentOrchestrator", lambda *_a, **_k: object())
    monkeypatch.setattr(application_module, "SessionManager", Sessions)
    monkeypatch.setattr(application_module, "AACRuntime", AAC)

    runtime = await ApplicationRuntime.start(user_name="ada", config_dir=tmp_path)
    assert runtime.projects.sandbox is runtime.sandbox
    assert runtime.sandbox.runtime_settings.host_library_root == "/host/projects"
    assert runtime.health_service.sandbox is runtime.sandbox
    assert calls[:2] == ["projects_start", "sandbox_start"]
    await runtime.shutdown()
    assert calls[-5:] == ["aac", "sessions", "sandbox", "projects", "resources"]
