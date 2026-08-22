from types import SimpleNamespace

import pytest

from common.schema.settings import DeveloperSettings, DocumentSettings, RootConfig
from runtime.project_factory import ProjectRuntimeFactory


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
        resource_profile=SimpleNamespace(workspace_prepare_concurrency=2),
    )
    factory = ProjectRuntimeFactory(
        resources=resources,
        user_name="ada",
        episode_window_size_provider=lambda _project_id: 8,
    )
    monkeypatch.setattr(
        "runtime.project_factory.ConfigManager.get",
        staticmethod(lambda: config_manager),
    )

    documents, workspace = factory._create_document_services(
        "project-1",
        readable_project_ids=["project-1"],
    )

    assert workspace._reader is documents._reader
    assert workspace._writer is documents._writer
    assert workspace._indexer is documents.indexer
    assert documents._document_rerank_enabled is False
    assert documents._document_rerank_candidates == 7
    assert documents.indexer.policy.workspace_prepare_concurrency == 2


@pytest.mark.runtime
@pytest.mark.no_network
async def test_current_project_jobs_and_config_subscriptions_are_registered(
    monkeypatch,
):
    config_manager = RecordingConfigManager()
    resources = SimpleNamespace(
        postgres=object(),
        redis=object(),
        llm_service=object(),
        knowledge_store=object(),
        executor=object(),
        embedding=object(),
    )
    factory = ProjectRuntimeFactory(
        resources=resources,
        user_name="ada",
        episode_window_size_provider=lambda _project_id: 8,
    )
    project_state = RecordingProjectRuntime()
    entities = RecordingEntities()
    processor = RecordingProcessor()
    episode = RecordingJob("episode")

    monkeypatch.setattr(
        "runtime.project_factory.ConfigManager.get",
        staticmethod(lambda: config_manager),
    )
    monkeypatch.setattr(
        "runtime.project_factory.MergeCleanupJob",
        lambda **kwargs: RecordingJob("merge_rollback_cleanup", **kwargs),
    )
    monkeypatch.setattr(
        "runtime.project_factory.AuditRetentionCleanupJob",
        lambda **kwargs: RecordingJob("audit_retention_cleanup", **kwargs),
    )
    monkeypatch.setattr(
        "runtime.project_factory.ConflictDiscoveryJob",
        lambda **kwargs: RecordingJob("conflict_discovery", **kwargs),
    )
    monkeypatch.setattr(
        "runtime.project_factory.AACJob",
        lambda state, deps: RecordingJob(
            "aac_discussion",
            state=state,
            resources=deps,
        ),
    )

    factory._register_background_jobs(
        project_state,
        entities=entities,
        processor=processor,
        episode_job=episode,
    )

    assert list(project_state.scheduler._jobs) == [
        "episode",
        "merge_rollback_cleanup",
        "audit_retention_cleanup",
        "conflict_discovery",
        "aac_discussion",
    ]
    assert [path for _, path in config_manager.subscriptions] == [
        "developer_settings.entity_resolution",
        "developer_settings.nlp_pipeline",
        "developer_settings.jobs.episode",
        "developer_settings.jobs.merge_rollback",
        "developer_settings.jobs.audit_retention",
        "developer_settings.jobs.conflict_discovery",
    ]
    assert len(project_state.unsubscribers) == 6


@pytest.mark.runtime
@pytest.mark.no_network
async def test_config_updates_fan_out_only_to_current_runtime_components(
    monkeypatch,
):
    config_manager = RecordingConfigManager()
    resources = SimpleNamespace(
        postgres=object(),
        redis=object(),
        llm_service=object(),
        knowledge_store=object(),
        executor=object(),
        embedding=object(),
    )
    factory = ProjectRuntimeFactory(
        resources=resources,
        user_name="ada",
        episode_window_size_provider=lambda _project_id: 8,
    )
    state = RecordingProjectRuntime()
    entities = RecordingEntities()
    processor = RecordingProcessor()
    episode = RecordingJob("episode")

    monkeypatch.setattr(
        "runtime.project_factory.ConfigManager.get",
        staticmethod(lambda: config_manager),
    )
    monkeypatch.setattr(
        "runtime.project_factory.MergeCleanupJob",
        lambda **kwargs: RecordingJob("merge_rollback_cleanup", **kwargs),
    )
    monkeypatch.setattr(
        "runtime.project_factory.AuditRetentionCleanupJob",
        lambda **kwargs: RecordingJob("audit_retention_cleanup", **kwargs),
    )
    monkeypatch.setattr(
        "runtime.project_factory.ConflictDiscoveryJob",
        lambda **kwargs: RecordingJob("conflict_discovery", **kwargs),
    )
    monkeypatch.setattr(
        "runtime.project_factory.AACJob",
        lambda *_: RecordingJob("aac_discussion"),
    )
    factory._register_background_jobs(
        state,
        entities=entities,
        processor=processor,
        episode_job=episode,
    )

    marker = object()
    config_manager.emit("developer_settings.entity_resolution", marker)
    config_manager.emit("developer_settings.nlp_pipeline", marker)
    config_manager.emit("developer_settings.jobs.episode", marker)
    config_manager.emit("developer_settings.jobs.merge_rollback", marker)
    config_manager.emit("developer_settings.jobs.audit_retention", marker)
    config_manager.emit("developer_settings.jobs.conflict_discovery", marker)

    assert entities.updates[-1] is marker
    assert processor.updates[-2:] == [marker, marker]
    assert episode.updates[-1] is marker
    assert state.scheduler._jobs["merge_rollback_cleanup"].updates[-1] is marker
    assert state.scheduler._jobs["audit_retention_cleanup"].updates[-1] is marker
    assert state.scheduler._jobs["conflict_discovery"].updates[-1] is marker
    assert "merge_detection" not in state.scheduler._jobs
