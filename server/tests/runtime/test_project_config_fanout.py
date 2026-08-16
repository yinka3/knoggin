from types import SimpleNamespace

import pytest

from common.schema.settings import DeveloperSettings, RootConfig
from core.project.project_manager import ProjectManager


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

class RecordingProjectState:
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
    manager = ProjectManager(resources=resources, user_name="ada")
    project_state = RecordingProjectState()
    entities = RecordingEntities()
    processor = RecordingProcessor()
    episode = RecordingJob("episode")

    monkeypatch.setattr(
        "core.project.project_manager.ConfigManager.get",
        staticmethod(lambda: config_manager),
    )
    monkeypatch.setattr(
        "core.project.project_manager.DLQReplayJob",
        lambda **kwargs: RecordingJob("dlq_auto_replay", **kwargs),
    )
    monkeypatch.setattr(
        "core.project.project_manager.EntityCleanupJob",
        lambda **kwargs: RecordingJob("entity_cleanup", **kwargs),
    )
    monkeypatch.setattr(
        "core.project.project_manager.MergeCleanupJob",
        lambda **kwargs: RecordingJob("merge_rollback_cleanup", **kwargs),
    )
    monkeypatch.setattr(
        "core.project.project_manager.AuditRetentionCleanupJob",
        lambda **kwargs: RecordingJob("audit_retention_cleanup", **kwargs),
    )
    monkeypatch.setattr(
        "core.project.project_manager.DocumentIndexingRecoveryJob",
        lambda *args, **kwargs: RecordingJob("document_index_recovery", **kwargs),
    )
    monkeypatch.setattr(
        "core.project.project_manager.AACJob",
        lambda state, deps: RecordingJob(
            "aac_discussion",
            state=state,
            resources=deps,
        ),
    )

    manager._register_background_jobs(
        project_state,
        entities,
        processor,
        episode,
    )

    assert list(project_state.scheduler._jobs) == [
        "episode",
        "document_index_recovery",
        "dlq_auto_replay",
        "entity_cleanup",
        "merge_rollback_cleanup",
        "audit_retention_cleanup",
        "aac_discussion",
    ]
    assert [path for _, path in config_manager.subscriptions] == [
        "developer_settings.entity_resolution",
        "developer_settings.nlp_pipeline",
        "developer_settings.jobs.episode",
        "developer_settings.jobs.document_indexing",
        "developer_settings.jobs.dlq",
        "developer_settings.jobs.cleaner",
        "developer_settings.jobs.merge_rollback",
        "developer_settings.jobs.audit_retention",
    ]
    assert len(project_state.unsubscribers) == 8


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
    manager = ProjectManager(resources=resources, user_name="ada")
    state = RecordingProjectState()
    entities = RecordingEntities()
    processor = RecordingProcessor()
    episode = RecordingJob("episode")

    monkeypatch.setattr(
        "core.project.project_manager.ConfigManager.get",
        staticmethod(lambda: config_manager),
    )
    monkeypatch.setattr(
        "core.project.project_manager.DLQReplayJob",
        lambda **kwargs: RecordingJob("dlq_auto_replay", **kwargs),
    )
    monkeypatch.setattr(
        "core.project.project_manager.EntityCleanupJob",
        lambda **kwargs: RecordingJob("entity_cleanup", **kwargs),
    )
    monkeypatch.setattr(
        "core.project.project_manager.MergeCleanupJob",
        lambda **kwargs: RecordingJob("merge_rollback_cleanup", **kwargs),
    )
    monkeypatch.setattr(
        "core.project.project_manager.AuditRetentionCleanupJob",
        lambda **kwargs: RecordingJob("audit_retention_cleanup", **kwargs),
    )
    monkeypatch.setattr(
        "core.project.project_manager.DocumentIndexingRecoveryJob",
        lambda *args, **kwargs: RecordingJob("document_index_recovery", **kwargs),
    )
    monkeypatch.setattr(
        "core.project.project_manager.AACJob",
        lambda *_: RecordingJob("aac_discussion"),
    )
    manager._register_background_jobs(state, entities, processor, episode)

    marker = object()
    config_manager.emit("developer_settings.entity_resolution", marker)
    config_manager.emit("developer_settings.nlp_pipeline", marker)
    config_manager.emit("developer_settings.jobs.episode", marker)
    config_manager.emit("developer_settings.jobs.document_indexing", marker)
    config_manager.emit("developer_settings.jobs.dlq", marker)
    config_manager.emit("developer_settings.jobs.cleaner", marker)
    config_manager.emit("developer_settings.jobs.merge_rollback", marker)
    config_manager.emit("developer_settings.jobs.audit_retention", marker)

    assert entities.updates[-1] is marker
    assert processor.updates[-2:] == [marker, marker]
    assert episode.updates[-1] is marker
    assert state.scheduler._jobs["document_index_recovery"].updates[-1] is marker
    assert state.scheduler._jobs["dlq_auto_replay"].updates[-1] is marker
    assert state.scheduler._jobs["entity_cleanup"].updates[-1] is marker
    assert state.scheduler._jobs["merge_rollback_cleanup"].updates[-1] is marker
    assert state.scheduler._jobs["audit_retention_cleanup"].updates[-1] is marker
    assert "merge_detection" not in state.scheduler._jobs
