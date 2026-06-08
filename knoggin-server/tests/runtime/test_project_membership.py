import json

import pytest

from common.schema.contracts import BatchResult
from common.schema.settings import DeveloperSettings, RootConfig, TopicSchema
from common.scoping import GLOBAL_PROJECT_SCOPE
from infrastructure.job.scheduler import Scheduler
from infrastructure.redis_client import RedisKeys
from knoggin_server.project.project_manager import ProjectManager
from tests.fixtures.fakes import FakeResources


class RecordingConfigManager:
    def __init__(self):
        self.config = RootConfig(developer_settings=DeveloperSettings())
        self.subscriptions = []

    def subscribe(self, callback, path):
        self.subscriptions.append((callback, path))

        def unsubscribe():
            pass

        return unsubscribe


class RecordingScheduler:
    instances = []

    def __init__(self, user_name, project_id, redis):
        self.user_name = user_name
        self.project_id = project_id
        self.redis = redis
        self._jobs = {}
        self.__class__.instances.append(self)

    @property
    def running(self):
        return False

    def register(self, job):
        self._jobs[job.name] = job
        return self


class RecordingEntityManager:
    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.hierarchy_config = kwargs["hierarchy_config"]
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
        self.updated_settings = config


class RecordingTextProcessor:
    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.refresh_count = 0
        self.__class__.instances.append(self)

    def refresh_topic_mappings(self):
        self.refresh_count += 1


class RecordingBatchProcessor:
    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.refresh_count = 0
        self.__class__.instances.append(self)

    def refresh_topic_mappings(self):
        self.refresh_count += 1

    def update_settings(self, config):
        self.updated_settings = config


class RecordingJob:
    def __init__(self, name, *args, **kwargs):
        self.name = name
        self.args = args
        self.kwargs = kwargs

    def update_settings(self, config):
        self.updated_settings = config


def recording_job_factory(name):
    return lambda *args, **kwargs: RecordingJob(name, *args, **kwargs)


@pytest.mark.runtime
@pytest.mark.no_network
async def test_acquire_project_for_session_records_durable_membership(monkeypatch):
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")
    project_state = object()

    seen = {}

    async def fake_get_or_start_project(project_id, initial_topics_config=None):
        seen["initial_topics_config"] = initial_topics_config
        return project_state

    monkeypatch.setattr(manager, "get_or_start_project", fake_get_or_start_project)

    topics_config = {"Custom": {"active": True}}
    result = await manager.acquire_project_for_session(
        "project-1", "session-1", topics_config=topics_config
    )

    assert result is project_state
    assert seen["initial_topics_config"] == topics_config
    assert await resources.redis.smembers(
        RedisKeys.project_sessions("ada", "project-1")
    ) == {"session-1"}


@pytest.mark.runtime
@pytest.mark.no_network
async def test_project_topic_config_is_seeded_from_session_topics_when_missing():
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")

    await manager._ensure_project_topics_config(
        "project-1",
        {"DeepWork": TopicSchema(active=True, labels=["practice"])},
    )

    raw = await resources.redis.hget(
        RedisKeys.project_topic_config("ada"), "project-1"
    )
    assert json.loads(raw) == {
        "DeepWork": {
            "active": True,
            "hot": False,
            "labels": ["practice"],
            "hierarchy": {},
            "aliases": [],
        }
    }


@pytest.mark.runtime
@pytest.mark.no_network
async def test_project_topic_config_seed_does_not_overwrite_existing_config():
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")
    await resources.redis.hset(
        RedisKeys.project_topic_config("ada"),
        "project-1",
        json.dumps({"Existing": {"active": True}}),
    )

    await manager._ensure_project_topics_config(
        "project-1",
        {"New": TopicSchema(active=True)},
    )

    raw = await resources.redis.hget(
        RedisKeys.project_topic_config("ada"), "project-1"
    )
    assert json.loads(raw) == {"Existing": {"active": True}}


@pytest.mark.runtime
@pytest.mark.no_network
async def test_get_or_start_project_caches_state_and_registers_project_jobs(
    monkeypatch,
):
    RecordingScheduler.instances = []
    RecordingEntityManager.instances = []
    RecordingTextProcessor.instances = []
    RecordingBatchProcessor.instances = []

    config_manager = RecordingConfigManager()
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")

    monkeypatch.setattr(
        "knoggin_server.project.project_manager.ConfigManager.get",
        staticmethod(lambda: config_manager),
    )
    monkeypatch.setattr(
        "knoggin_server.project.project_manager.EntityManager",
        RecordingEntityManager,
    )
    monkeypatch.setattr(
        "knoggin_server.project.project_manager.TextProcessor",
        RecordingTextProcessor,
    )
    monkeypatch.setattr(
        "knoggin_server.project.project_manager.BatchProcessor",
        RecordingBatchProcessor,
    )
    monkeypatch.setattr(
        "knoggin_server.project.project_manager.Scheduler",
        RecordingScheduler,
    )
    monkeypatch.setattr(
        "knoggin_server.project.project_manager.ProfileRefinementJob",
        recording_job_factory("profile_refinement"),
    )
    monkeypatch.setattr(
        "knoggin_server.project.project_manager.MergeDetectionJob",
        recording_job_factory("merge_detection"),
    )
    monkeypatch.setattr(
        "knoggin_server.project.project_manager.DLQReplayJob",
        recording_job_factory("dlq_auto_replay"),
    )
    monkeypatch.setattr(
        "knoggin_server.project.project_manager.EntityCleanupJob",
        recording_job_factory("entity_cleanup"),
    )
    monkeypatch.setattr(
        "knoggin_server.project.project_manager.FactArchivalJob",
        recording_job_factory("fact_archival"),
    )
    monkeypatch.setattr(
        "knoggin_server.project.project_manager.TopicConfigJob",
        recording_job_factory("topic_config"),
    )
    graph_write_calls = []

    async def fake_write_batch_callback(result, **kwargs):
        graph_write_calls.append((result, kwargs))
        return True, None

    monkeypatch.setattr(
        "knoggin_server.project.project_manager.write_batch_callback",
        fake_write_batch_callback,
    )

    project_state = await manager.get_or_start_project(
        "project-1",
        initial_topics_config={"DeepWork": TopicSchema(active=True)},
    )
    reused_state = await manager.get_or_start_project("project-1")

    assert reused_state is project_state
    assert project_state.active_runtime_sessions_count == 2
    assert manager.active_projects["project-1"] is project_state
    assert project_state.readable_project_ids == [GLOBAL_PROJECT_SCOPE, "project-1"]

    scheduler = RecordingScheduler.instances[0]
    assert scheduler.project_id == "project-1"
    assert project_state.scheduler is scheduler
    assert list(scheduler._jobs) == [
        "profile_refinement",
        "merge_detection",
        "dlq_auto_replay",
        "entity_cleanup",
        "fact_archival",
        "topic_config",
    ]

    assert RecordingEntityManager.instances[0].kwargs["project_id"] == "project-1"
    assert RecordingEntityManager.instances[0].kwargs["readable_project_ids"] == [
        GLOBAL_PROJECT_SCOPE,
        "project-1",
    ]
    assert RecordingBatchProcessor.instances[0].kwargs["project_id"] == "project-1"
    assert len(config_manager.subscriptions) == 9

    dlq_job = scheduler._jobs["dlq_auto_replay"]
    batch_result = BatchResult()
    batch_result.set_scope("ada", "session-1", "project-1")
    assert await dlq_job.kwargs["write_to_graph"](batch_result) == (True, None)
    assert graph_write_calls == [
        (
            batch_result,
            {
                "graph_client": resources.graph_client,
                "entities": project_state.entities,
                "session_id": "session-1",
                "project_id": "project-1",
                "user_name": "ada",
                "redis_client": resources.redis,
            },
        )
    ]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_project_scheduler_context_uses_project_id():
    redis = FakeResources().redis
    scheduler = Scheduler("ada", "project-1", redis)

    ctx = await scheduler._build_context()

    assert scheduler.project_id == "project-1"
    assert ctx.project_id == "project-1"
    assert not hasattr(scheduler, "session_id")
    assert not hasattr(ctx, "session_id")
    assert not hasattr(ctx, "scope_id")
