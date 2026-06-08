import asyncio
import json
from dataclasses import dataclass

import pytest

from common.schema.settings import (
    ArchivalSettings,
    CleanerSettings,
    DeveloperSettings,
    DLQSettings,
    EntityResolutionSettings,
    MergerSettings,
    ProfileSettings,
    RootConfig,
    TextProcessorSettings,
    TopicConfigSettings,
    TopicSchema,
)
from common.utils.core_utils import safe_update
from infrastructure.redis_client import RedisKeys
from knoggin_server.project.project_manager import ProjectManager
from tests.fixtures.fakes import FakeResources

CONFIG_PATHS = [
    "default_topics",
    "developer_settings.entity_resolution",
    "developer_settings.nlp_pipeline",
    "developer_settings.jobs.profile",
    "developer_settings.jobs.merger",
    "developer_settings.jobs.dlq",
    "developer_settings.jobs.cleaner",
    "developer_settings.jobs.archival",
    "developer_settings.jobs.topic_config",
]


@dataclass
class Subscription:
    callback: object
    path: str
    active: bool = True
    calls: int = 0


class FanoutConfigManager:
    def __init__(self):
        self.config = RootConfig(developer_settings=DeveloperSettings())
        self.subscriptions: list[Subscription] = []

    def subscribe(self, callback, path):
        subscription = Subscription(callback=callback, path=path)
        self.subscriptions.append(subscription)
        safe_update(callback, self._get_path(path))
        subscription.calls += 1

        def unsubscribe():
            subscription.active = False

        return unsubscribe

    def emit(self, path, value):
        for subscription in self.subscriptions:
            if subscription.active and subscription.path == path:
                safe_update(subscription.callback, value)
                subscription.calls += 1

    def active_paths(self):
        return [
            subscription.path
            for subscription in self.subscriptions
            if subscription.active
        ]

    def _get_path(self, path):
        current = self.config
        for part in path.split("."):
            current = getattr(current, part)
        return current


class FanoutScheduler:
    instances = []

    def __init__(self, user_name, project_id, redis):
        self.user_name = user_name
        self.redis = redis
        self.project_id = project_id
        self._jobs = {}
        self.stopped = 0
        self.__class__.instances.append(self)

    @property
    def running(self):
        return False

    def register(self, job):
        self._jobs[job.name] = job
        return self

    async def stop(self):
        self.stopped += 1


class FanoutEntityManager:
    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.hierarchy_config = kwargs["hierarchy_config"]
        self.updated_settings = []
        self.registered_entities = []
        self.__class__.instances.append(self)

    async def get_id(self, name):
        return 1

    async def register_entity(self, *args):
        self.registered_entities.append(args)

    async def get_known_aliases(self):
        return {}

    async def get_profile(self, name):
        return None

    def update_settings(self, config):
        self.updated_settings.append(config)


class FanoutTextProcessor:
    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.refresh_count = 0
        self.__class__.instances.append(self)

    def refresh_topic_mappings(self):
        self.refresh_count += 1


class FanoutBatchProcessor:
    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.graph_client = kwargs["graph_client"]
        self.updated_settings = []
        self.refresh_count = 0
        self.__class__.instances.append(self)

    def update_settings(self, config):
        self.updated_settings.append(config)

    def refresh_topic_mappings(self):
        self.refresh_count += 1


async def boot_project(monkeypatch):
    FanoutScheduler.instances = []
    FanoutEntityManager.instances = []
    FanoutTextProcessor.instances = []
    FanoutBatchProcessor.instances = []

    config_manager = FanoutConfigManager()
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")

    monkeypatch.setattr(
        "knoggin_server.project.project_manager.ConfigManager.get",
        staticmethod(lambda: config_manager),
    )
    monkeypatch.setattr(
        "knoggin_server.project.project_manager.EntityManager",
        FanoutEntityManager,
    )
    monkeypatch.setattr(
        "knoggin_server.project.project_manager.TextProcessor",
        FanoutTextProcessor,
    )
    monkeypatch.setattr(
        "knoggin_server.project.project_manager.BatchProcessor",
        FanoutBatchProcessor,
    )
    monkeypatch.setattr(
        "knoggin_server.project.project_manager.Scheduler",
        FanoutScheduler,
    )

    project_state = await manager.get_or_start_project(
        "project-1",
        initial_topics_config={
            "DeepWork": TopicSchema(
                active=True,
                labels=["focus"],
                hierarchy={"DeepWork": ["Practice"]},
            )
        },
    )

    return project_state, config_manager


@pytest.mark.runtime
@pytest.mark.no_network
async def test_project_config_subscriptions_are_registered_and_initialized(
    monkeypatch,
):
    project_state, config_manager = await boot_project(monkeypatch)
    scheduler = project_state.scheduler

    assert config_manager.active_paths() == CONFIG_PATHS
    assert len(project_state.config_unsubscribers) == len(CONFIG_PATHS)
    assert len(scheduler._jobs) == 6

    assert len(project_state.entities.updated_settings) == 1
    assert isinstance(
        project_state.entities.updated_settings[-1], EntityResolutionSettings
    )

    processor = FanoutBatchProcessor.instances[0]
    assert len(processor.updated_settings) == 2
    assert isinstance(processor.updated_settings[0], EntityResolutionSettings)
    assert isinstance(processor.updated_settings[-1], TextProcessorSettings)

    assert scheduler._jobs["profile_refinement"].volume_threshold == 15
    assert scheduler._jobs["merge_detection"].auto_threshold == 0.93
    assert scheduler._jobs["dlq_auto_replay"].interval == 60
    assert scheduler._jobs["entity_cleanup"].run_interval_seconds == 24 * 3600
    assert scheduler._jobs["fact_archival"].retention_days == 14
    assert scheduler._jobs["topic_config"].interval_msgs == 40


@pytest.mark.runtime
@pytest.mark.no_network
async def test_project_config_updates_fan_out_to_runtime_services(monkeypatch):
    project_state, config_manager = await boot_project(monkeypatch)
    scheduler = project_state.scheduler
    processor = FanoutBatchProcessor.instances[0]

    entity_update = EntityResolutionSettings(
        fuzzy_substring_threshold=70,
        fuzzy_non_substring_threshold=88,
        generic_token_freq=7,
        candidate_fuzzy_threshold=82,
        candidate_vector_threshold=0.77,
    )
    nlp_update = TextProcessorSettings(
        gliner_threshold=0.71,
        vp01_min_confidence=0.66,
        llm_ner=False,
    )
    profile_update = ProfileSettings(
        msg_window=18,
        volume_threshold=3,
        idle_threshold=20,
        profile_batch_size=2,
    )
    merger_update = MergerSettings(
        auto_threshold=0.95,
        hitl_threshold=0.72,
        cosine_threshold=0.61,
    )
    dlq_update = DLQSettings(interval_seconds=17, batch_size=4, max_attempts=5)
    cleaner_update = CleanerSettings(
        interval_hours=2,
        orphan_age_hours=3,
        stale_junk_days=4,
    )
    archival_update = ArchivalSettings(
        retention_days=21,
        fallback_interval_hours=2.5,
    )
    topic_job_update = TopicConfigSettings(
        interval_msgs=25,
        conversation_window=35,
    )

    config_manager.emit("developer_settings.entity_resolution", entity_update)
    config_manager.emit("developer_settings.nlp_pipeline", nlp_update)
    config_manager.emit("developer_settings.jobs.profile", profile_update)
    config_manager.emit("developer_settings.jobs.merger", merger_update)
    config_manager.emit("developer_settings.jobs.dlq", dlq_update)
    config_manager.emit("developer_settings.jobs.cleaner", cleaner_update)
    config_manager.emit("developer_settings.jobs.archival", archival_update)
    config_manager.emit("developer_settings.jobs.topic_config", topic_job_update)
    config_manager.emit("developer_settings.search", object())

    assert project_state.entities.updated_settings[-1] is entity_update
    assert entity_update in processor.updated_settings
    assert processor.updated_settings[-1] is nlp_update

    profile_job = scheduler._jobs["profile_refinement"]
    assert profile_job.msg_window == 18
    assert profile_job.volume_threshold == 3
    assert profile_job.idle_threshold == 20
    assert profile_job.profile_batch_size == 2

    merge_job = scheduler._jobs["merge_detection"]
    assert merge_job.auto_threshold == 0.95
    assert merge_job.hitl_threshold == 0.72
    assert merge_job.cosine_threshold == 0.61

    dlq_job = scheduler._jobs["dlq_auto_replay"]
    assert dlq_job.interval == 17
    assert dlq_job.batch_size == 4
    assert dlq_job.max_attempts == 5

    cleaner_job = scheduler._jobs["entity_cleanup"]
    assert cleaner_job.run_interval_seconds == 2 * 3600
    assert cleaner_job.orphan_cutoff_ms == 3 * 3600 * 1000
    assert cleaner_job.stale_cutoff_ms == 4 * 24 * 3600 * 1000

    archival_job = scheduler._jobs["fact_archival"]
    assert archival_job.retention_days == 21
    assert archival_job._fallback_interval_seconds == 2.5 * 3600

    topic_job = scheduler._jobs["topic_config"]
    assert topic_job.interval_msgs == 25
    assert topic_job.conversation_window == 35

    assert len(project_state.entities.updated_settings) == 2
    assert len(processor.updated_settings) == 4


@pytest.mark.runtime
@pytest.mark.no_network
async def test_default_topics_fanout_updates_project_topics(monkeypatch):
    project_state, config_manager = await boot_project(monkeypatch)

    new_topics = {
        "NewTopic": {
            "active": True,
            "labels": ["new-label"],
            "hierarchy": {"NewTopic": ["Child"]},
            "aliases": ["fresh"],
        }
    }

    config_manager.emit("default_topics", new_topics)
    await asyncio.sleep(0)

    raw = await project_state.redis_client.hget(
        RedisKeys.project_topic_config("ada"),
        "project-1",
    )
    saved = json.loads(raw)

    assert "NewTopic" in project_state.topic_config.raw
    assert saved["NewTopic"]["labels"] == ["new-label"]
    assert project_state.entities.hierarchy_config == {
        "DeepWork": {"DeepWork": ["Practice"]},
        "NewTopic": {"NewTopic": ["Child"]},
    }
    assert project_state.pipeline.refresh_count == 1


@pytest.mark.runtime
@pytest.mark.no_network
async def test_topic_job_update_callback_refreshes_project_processor(monkeypatch):
    project_state, _ = await boot_project(monkeypatch)
    processor = FanoutBatchProcessor.instances[0]
    topic_job = project_state.scheduler._jobs["topic_config"]

    await topic_job.update_callback(
        {
            "ProcessorTopic": {
                "active": True,
                "labels": ["processor-label"],
                "hierarchy": {"ProcessorTopic": ["Child"]},
                "aliases": [],
            }
        }
    )

    assert "ProcessorTopic" in project_state.topic_config.raw
    assert project_state.entities.hierarchy_config == {
        "DeepWork": {"DeepWork": ["Practice"]},
        "ProcessorTopic": {"ProcessorTopic": ["Child"]},
    }
    assert processor.refresh_count == 1


@pytest.mark.runtime
@pytest.mark.no_network
async def test_project_state_shutdown_unsubscribes_config_fanout(monkeypatch):
    project_state, config_manager = await boot_project(monkeypatch)
    scheduler = project_state.scheduler
    initial_entity_updates = len(project_state.entities.updated_settings)

    await project_state.shutdown()

    assert project_state.config_unsubscribers == []
    assert config_manager.active_paths() == []
    assert scheduler.stopped == 1

    config_manager.emit(
        "developer_settings.entity_resolution",
        EntityResolutionSettings(fuzzy_substring_threshold=70),
    )

    assert len(project_state.entities.updated_settings) == initial_entity_updates
