import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from common.schema.settings import DeveloperSettings, DocumentSettings, RootConfig
from core.project.domain_config_store import DomainActivation
from runtime.project_factory import ProjectRuntimeFactory
from tests.fixtures.factories import make_domain_config, make_project_state


class RecordingConfigManager:
    def __init__(self):
        self.config = RootConfig(developer_settings=DeveloperSettings())
        self.subscriptions = []

    def subscribe(self, callback, path):
        self.subscriptions.append((callback, path))
        value = self.config
        for part in path.split("."):
            value = getattr(value, part)
        callback(value)
        return lambda: None

    def resolve_path(self, configured_path):
        path = Path(configured_path)
        return path if path.is_absolute() else Path("/tmp/knoggin-config") / path

    def emit(self, path, value):
        for callback, subscribed_path in self.subscriptions:
            if subscribed_path == path:
                callback(value)


class RecordingScheduler:
    def __init__(self):
        self._jobs = {}

    def register(self, job):
        self._jobs[job.name] = job
        return self


class RecordingJob:
    def __init__(self, name, **kwargs):
        self.name = name
        self.kwargs = kwargs
        self.updates = []

    def update_settings(self, *settings):
        self.updates.append(settings if len(settings) > 1 else settings[0])

    def update_episode_settings(self, settings):
        self.updates.append(settings)


class RecordingProjectRuntime:
    def __init__(self):
        self.project_id = "project-1"
        self.scheduler = RecordingScheduler()
        self.document_service = SimpleNamespace()
        self.unsubscribers = []

    def add_config_unsubscriber(self, unsubscribe):
        self.unsubscribers.append(unsubscribe)


class RecordingProcessor:
    def __init__(self):
        self.updates = []

    def update_settings(self, settings):
        self.updates.append(settings)


class RecordingEntities:
    def __init__(self):
        self.updates = []

    def update_settings(self, settings):
        self.updates.append(settings)


class RecordingStartupScheduler:
    def __init__(self, _user_name, _project_id, **_kwargs):
        self.started = False

    async def start(self):
        self.started = True


class RecordingStartupRuntime:
    def __init__(self, **kwargs):
        self.project_id = kwargs["project_id"]
        self.scheduler = kwargs["scheduler"]
        self.document_service = kwargs["document_service"]
        self.project_semantic_processor = None

    async def shutdown(self):
        raise AssertionError("startup test should not require shutdown")


class RecordingStartupEntities:
    def get_known_aliases(self):
        return ()

    def get_alias_version(self):
        return 0

    def get_profile(self, _entity_id):
        return None


class RecordingStartupJob:
    def __init__(self, events):
        self.events = events
        self.calls = []

    async def synchronize_context_file(self, ctx, *, allow_user_edit):
        self.calls.append((ctx.user_name, ctx.project_id, allow_user_edit))
        self.events.append("sync")


class CapturedSemanticProcessor:
    def __init__(self, *_args, **kwargs):
        self.kwargs = kwargs


class RecordingIndexer:
    def __init__(self, events):
        self.events = events

    async def start(self):
        self.events.append("indexer")


@pytest.mark.runtime
@pytest.mark.no_network
def test_document_runtime_uses_typed_settings_and_shared_explicit_dependencies(
    monkeypatch,
):
    config_manager = RecordingConfigManager()
    config_manager.config = RootConfig(
        developer_settings=DeveloperSettings(
            documents=DocumentSettings(
                rerank_enabled=False,
                rerank_candidates=7,
            )
        )
    )
    resources = SimpleNamespace(
        postgres=object(),
        embedding=object(),
        background_work=None,
    )
    factory = ProjectRuntimeFactory(
        resources=resources,
        user_name="ada",
    )
    monkeypatch.setattr(
        "runtime.project_factory.ConfigManager.get",
        staticmethod(lambda: config_manager),
    )

    documents = factory._create_document_service(
        "project-1",
        readable_project_ids=["project-1"],
    )

    assert documents._document_rerank_enabled is False
    assert documents._document_rerank_candidates == 7


@pytest.mark.runtime
@pytest.mark.no_network
async def test_current_project_jobs_and_config_subscriptions_are_registered(
    monkeypatch,
):
    config_manager = RecordingConfigManager()
    resources = SimpleNamespace(
        postgres=object(),
        llm_service=object(),
        knowledge_store=object(),
        executor=object(),
        embedding=object(),
    )
    factory = ProjectRuntimeFactory(
        resources=resources,
        user_name="ada",
    )
    project_state = RecordingProjectRuntime()
    entities = RecordingEntities()
    processor = RecordingProcessor()
    semantic = RecordingJob("project_semantic")

    monkeypatch.setattr(
        "runtime.project_factory.ConfigManager.get",
        staticmethod(lambda: config_manager),
    )
    factory._register_background_jobs(
        project_state,
        entities=entities,
        processor=processor,
        project_semantic_processor=semantic,
    )

    assert list(project_state.scheduler._jobs) == ["project_semantic"]
    assert [path for _, path in config_manager.subscriptions] == [
        "developer_settings.entity_resolution",
        "developer_settings.nlp_pipeline",
        "developer_settings.ingestion",
        "developer_settings.jobs.episode",
    ]
    assert len(project_state.unsubscribers) == 4


@pytest.mark.runtime
@pytest.mark.no_network
async def test_config_updates_fan_out_only_to_current_runtime_components(
    monkeypatch,
):
    config_manager = RecordingConfigManager()
    resources = SimpleNamespace(
        postgres=object(),
        llm_service=object(),
        knowledge_store=object(),
        executor=object(),
        embedding=object(),
    )
    factory = ProjectRuntimeFactory(resources=resources, user_name="ada")
    state = RecordingProjectRuntime()
    entities = RecordingEntities()
    processor = RecordingProcessor()
    semantic = RecordingJob("project_semantic")

    monkeypatch.setattr(
        "runtime.project_factory.ConfigManager.get",
        staticmethod(lambda: config_manager),
    )
    factory._register_background_jobs(
        state,
        entities=entities,
        processor=processor,
        project_semantic_processor=semantic,
    )

    marker = object()
    config_manager.emit("developer_settings.entity_resolution", marker)
    config_manager.emit("developer_settings.nlp_pipeline", marker)
    config_manager.emit("developer_settings.ingestion", marker)
    config_manager.emit("developer_settings.jobs.episode", marker)

    assert entities.updates[-1] is marker
    assert processor.updates[-1] is marker
    assert semantic.updates[-2:] == [marker, marker]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_project_semantic_processor_is_registered_with_its_settings(
    monkeypatch,
):
    config_manager = RecordingConfigManager()
    factory = ProjectRuntimeFactory(
        resources=SimpleNamespace(),
        user_name="ada",
    )
    state = RecordingProjectRuntime()
    entities = RecordingEntities()
    processor = RecordingProcessor()
    semantic = RecordingJob("project_semantic")
    monkeypatch.setattr(
        "runtime.project_factory.ConfigManager.get",
        staticmethod(lambda: config_manager),
    )

    factory._register_background_jobs(
        state,
        entities=entities,
        processor=processor,
        project_semantic_processor=semantic,
    )

    assert list(state.scheduler._jobs) == ["project_semantic"]
    assert [path for _, path in config_manager.subscriptions] == [
        "developer_settings.entity_resolution",
        "developer_settings.nlp_pipeline",
        "developer_settings.ingestion",
        "developer_settings.jobs.episode",
    ]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_conflict_discovery_job_is_registered_with_its_policy_subscription(
    monkeypatch,
):
    config_manager = RecordingConfigManager()
    maintenance_service = object()
    resources = SimpleNamespace(llm_service=object())
    factory = ProjectRuntimeFactory(
        resources=resources,
        user_name="ada",
        maintenance_service=maintenance_service,
    )
    state = RecordingProjectRuntime()
    entities = RecordingEntities()
    processor = RecordingProcessor()
    semantic = RecordingJob("project_semantic")
    monkeypatch.setattr(
        "runtime.project_factory.ConfigManager.get",
        staticmethod(lambda: config_manager),
    )

    conflict_job = factory._create_conflict_discovery_job(resources=resources)
    assert conflict_job is not None
    assert conflict_job.maintenance_service is maintenance_service
    factory._register_background_jobs(
        state,
        entities=entities,
        processor=processor,
        project_semantic_processor=semantic,
        conflict_discovery_job=conflict_job,
    )

    assert list(state.scheduler._jobs) == ["project_semantic", "conflict_discovery"]
    assert [path for _, path in config_manager.subscriptions] == [
        "developer_settings.entity_resolution",
        "developer_settings.nlp_pipeline",
        "developer_settings.ingestion",
        "developer_settings.jobs.episode",
        "developer_settings.jobs.conflict_discovery",
    ]
    config_manager.emit(
        "developer_settings.jobs.conflict_discovery",
        config_manager.config.developer_settings.jobs.conflict_discovery.model_copy(
            update={"mode": "manual"}
        ),
    )
    assert conflict_job.enabled is False


@pytest.mark.runtime
@pytest.mark.no_network
def test_project_semantic_factory_wires_committed_entity_publication(monkeypatch):
    config_manager = RecordingConfigManager()

    async def publisher(_entity_ids):
        return None

    entities = SimpleNamespace(publish_committed_entity_ids=publisher)
    runtime = SimpleNamespace(
        project_id="project-1",
        capture_semantic_policy=lambda: None,
        text_processor=object(),
        entities=entities,
    )
    resources = SimpleNamespace(
        postgres=object(),
        llm_service=SimpleNamespace(count_tokens=lambda _text: 1),
        knowledge_store=SimpleNamespace(allocate_entity_id=lambda: None),
        embedding=object(),
    )
    factory = ProjectRuntimeFactory(resources=resources, user_name="ada")

    monkeypatch.setattr(
        "runtime.project_factory.ConfigManager.get",
        staticmethod(lambda: config_manager),
    )
    for symbol in (
        "SemanticWindowAdmission",
        "EpisodeGenerator",
        "ContextProjection",
        "ProjectContextReader",
        "ProjectContextWriter",
        "ContextUpdater",
        "ContextEntityBuildService",
        "ContextRelationshipExtractor",
    ):
        monkeypatch.setattr(
            "runtime.project_factory." + symbol, lambda *args, **kwargs: object()
        )
    monkeypatch.setattr(
        "runtime.project_factory.ProjectFilesystemFactory",
        lambda _root: SimpleNamespace(for_project=lambda _project_id: object()),
    )
    monkeypatch.setattr(
        "runtime.project_factory.ProjectSemanticProcessor", CapturedSemanticProcessor
    )

    job = factory._create_project_semantic_processor(runtime, resources=resources)

    assert job.kwargs["publish_committed_entity_ids"] is publisher
    assert job.kwargs["capture_semantic_policy"] is runtime.capture_semantic_policy


@pytest.mark.runtime
@pytest.mark.no_network
async def test_semantic_policy_capture_and_domain_activation_share_one_lock(
    monkeypatch,
):
    config_manager = RecordingConfigManager()
    initial = make_domain_config(version=2)
    activated = make_domain_config(version=3)
    activation_started = asyncio.Event()
    finish_activation = asyncio.Event()

    class Store:
        async def activate(self, **_kwargs):
            activation_started.set()
            await finish_activation.wait()
            return DomainActivation(
                config=activated,
                compiled=activated.compile(),
                previous_version=initial.version,
            )

    runtime = make_project_state(
        domain_config=initial,
        text_processor=SimpleNamespace(
            gliner_threshold=0.42,
            llm_ner_mode="fallback",
        ),
    )
    runtime.domain_config_store = Store()
    monkeypatch.setattr(
        "runtime.project_runtime.ConfigManager.get",
        staticmethod(lambda: config_manager),
    )

    before_activation = await runtime.capture_semantic_policy()
    activation = asyncio.create_task(
        runtime.activate_domain_config(make_domain_config(), expected_version=2)
    )
    await activation_started.wait()
    capture_after_activation = asyncio.create_task(runtime.capture_semantic_policy())
    await asyncio.sleep(0)
    assert not capture_after_activation.done()

    finish_activation.set()
    activation_result, after_activation = await asyncio.gather(
        activation,
        capture_after_activation,
    )

    assert activation_result.compiled.version == 3
    assert before_activation.domain.version == 2
    assert before_activation.domain is not runtime.compiled_domain
    assert after_activation.domain.version == 3
    assert after_activation.domain is runtime.compiled_domain


@pytest.mark.runtime
@pytest.mark.no_network
async def test_runtime_start_synchronizes_context_before_other_project_work(
    monkeypatch,
):
    config_manager = RecordingConfigManager()
    events = []

    async def get_vp01(_language):
        return object()

    resources = SimpleNamespace(
        postgres=object(),
        embedding=object(),
        knowledge_store=object(),
        llm_service=object(),
        executor=object(),
        background_work=None,
        model_work=object(),
        spacy=object(),
        get_vp01=get_vp01,
    )
    resources.require_ready = lambda: resources
    factory = ProjectRuntimeFactory(resources=resources, user_name="ada")
    indexer = RecordingIndexer(events)
    job = RecordingStartupJob(events)

    class DomainStore:
        def __init__(self, _postgres):
            pass

        async def load(self, _user_name, _project_id):
            return make_domain_config()

    class Loop:
        async def run_in_executor(self, _executor, _operation):
            return object()

    async def verify_user_entity(_entities):
        return None

    monkeypatch.setattr(
        "runtime.project_factory.ConfigManager.get",
        staticmethod(lambda: config_manager),
    )
    monkeypatch.setattr("runtime.project_factory.DomainConfigStore", DomainStore)
    monkeypatch.setattr(
        "runtime.project_factory.EntityResolver",
        lambda **_kwargs: RecordingStartupEntities(),
    )
    monkeypatch.setattr(
        "runtime.project_factory.KnowledgeRetrieval", lambda **_kwargs: object()
    )
    monkeypatch.setattr(
        "runtime.project_factory.ProjectRuntime", RecordingStartupRuntime
    )
    monkeypatch.setattr("runtime.project_factory.Scheduler", RecordingStartupScheduler)
    monkeypatch.setattr(
        "runtime.project_factory.asyncio.get_running_loop", lambda: Loop()
    )
    monkeypatch.setattr(factory, "_verify_user_entity", verify_user_entity)
    monkeypatch.setattr(
        factory,
        "_create_document_service",
        lambda *_args, **_kwargs: SimpleNamespace(indexer=indexer),
    )
    monkeypatch.setattr(
        factory,
        "_create_project_semantic_processor",
        lambda *_args, **_kwargs: job,
    )
    monkeypatch.setattr(
        factory,
        "_register_background_jobs",
        lambda *_args, **_kwargs: events.append("registered"),
    )

    runtime = await factory.create(
        project_id="project-1", readable_project_ids=["project-1"]
    )

    assert runtime.project_semantic_processor is job
    assert job.calls == [("ada", "project-1", True)]
    assert events == ["sync", "indexer", "registered"]
    assert runtime.scheduler.started is True
