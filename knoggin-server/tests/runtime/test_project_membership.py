import asyncio
import json

import pytest

from common.schema.contracts import BatchResult
from common.schema.settings import DeveloperSettings, RootConfig, TopicSchema
from common.scoping import IDENTITY_SCOPE
from infrastructure.job.scheduler import Scheduler
from infrastructure.redis_client import RedisKeys
from knoggin_server.project.lifecycle import ProjectStatus
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
async def test_create_project_requires_non_empty_name():
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")

    with pytest.raises(ValueError, match="non-empty project name"):
        await manager.create_project("   ")

    assert await manager.list_projects() == []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_create_project_starts_active():
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")

    project = await manager.create_project("Research")

    assert project["status"] == ProjectStatus.ACTIVE.value
    assert project["archived_at"] is None
    assert project["deleted_at"] is None


@pytest.mark.runtime
@pytest.mark.no_network
async def test_search_repair_uses_retained_projects_for_identity():
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")
    active = await manager.create_project("Active")
    archived = await manager.create_project("Archive")
    deleted = await manager.create_project("Deleted")
    await manager.archive_project(archived["id"])
    await manager.delete_project(deleted["id"])

    summary = await manager.rebuild_project_search_indexes(active["id"])

    assert summary["identity"] == 1
    assert resources.knowledge_store.search_rebuild_calls == [
        {
            "project_id": active["id"],
            "user_name": "ada",
            "identity_project_ids": sorted([active["id"], archived["id"]]),
        }
    ]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_search_repair_allows_archived_and_deleted_targets():
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")
    archived = await manager.create_project("Archive")
    deleted = await manager.create_project("Deleted")
    await manager.archive_project(archived["id"])
    await manager.delete_project(deleted["id"])

    archived_summary = await manager.rebuild_project_search_indexes(archived["id"])
    deleted_summary = await manager.rebuild_project_search_indexes(deleted["id"])

    assert archived_summary["identity"] == 1
    assert deleted_summary["identity"] == 1
    assert [
        call["project_id"] for call in resources.knowledge_store.search_rebuild_calls
    ] == [archived["id"], deleted["id"]]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_search_repair_rejects_any_active_project_runtime():
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")
    project = await manager.create_project("Research")
    manager.active_projects["other-project"] = type(
        "ActiveProject",
        (),
        {"active_runtime_sessions_count": 1},
    )()

    with pytest.raises(RuntimeError, match="all project runtimes"):
        await manager.rebuild_project_search_indexes(project["id"])

    assert resources.knowledge_store.search_rebuild_calls == []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_search_repair_blocks_session_acquisition(monkeypatch):
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")
    project = await manager.create_project("Research")
    repair_started = asyncio.Event()
    finish_repair = asyncio.Event()
    acquired = object()

    async def slow_rebuild(project_id, user_name, identity_project_ids):
        repair_started.set()
        await finish_repair.wait()
        return {"messages": 0, "entities": 0, "facts": 0, "identity": 1}

    async def fake_acquire(project_id, session_id, topics_config=None):
        return acquired

    monkeypatch.setattr(
        resources.knowledge_store,
        "rebuild_project_search_indexes",
        slow_rebuild,
    )
    monkeypatch.setattr(manager, "_acquire_project_for_session", fake_acquire)

    repair_task = asyncio.create_task(
        manager.rebuild_project_search_indexes(project["id"])
    )
    await repair_started.wait()
    acquire_task = asyncio.create_task(
        manager.acquire_project_for_session(project["id"], "session-1")
    )
    await asyncio.sleep(0)

    assert not acquire_task.done()

    finish_repair.set()
    await repair_task
    assert await acquire_task is acquired


@pytest.mark.runtime
@pytest.mark.no_network
async def test_update_project_rejects_empty_name():
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")
    project = await manager.create_project("Research")

    with pytest.raises(ValueError, match="non-empty project name"):
        await manager.update_project(project["id"], name="  ")

    assert (await manager.get_project(project["id"]))["name"] == "Research"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_archive_project_preserves_metadata_sessions_and_runtime_data():
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")
    project = await manager.create_project("Research")
    await manager.add_session(project["id"], "session-1")
    await resources.redis.hset(
        RedisKeys.project_topic_config("ada"),
        project["id"],
        json.dumps({"General": {"active": True}}),
    )

    archived = await manager.archive_project(project["id"])

    assert archived["status"] == ProjectStatus.ARCHIVED.value
    assert archived["archived_at"]
    assert await manager.get_session_ids(project["id"]) == ["session-1"]
    assert await resources.redis.hget(
        RedisKeys.project_topic_config("ada"),
        project["id"],
    )
    assert await manager.get_project(project["id"]) is not None


@pytest.mark.runtime
@pytest.mark.no_network
async def test_archive_project_rejects_live_runtime_sessions():
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")
    project = await manager.create_project("Research")
    manager.active_projects[project["id"]] = type(
        "ActiveProject",
        (),
        {"active_runtime_sessions_count": 1},
    )()

    with pytest.raises(RuntimeError, match="active runtime sessions"):
        await manager.archive_project(project["id"])

    assert (await manager.get_project(project["id"]))["status"] == "active"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_archived_project_rejects_session_acquisition():
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")
    project = await manager.create_project("Research")
    await manager.archive_project(project["id"])

    with pytest.raises(ValueError, match="is archived"):
        await manager.acquire_project_for_session(project["id"], "session-1")

    assert await manager.get_session_ids(project["id"]) == []
    assert resources.knowledge_store.identity_calls == []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_reactivate_project_restores_session_eligibility(monkeypatch):
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")
    project = await manager.create_project("Research")
    await manager.archive_project(project["id"])
    reactivated = await manager.reactivate_project(project["id"])
    project_state = object()

    async def fake_get_or_start_project(project_id, initial_topics_config=None):
        return project_state

    monkeypatch.setattr(manager, "_get_or_start_project", fake_get_or_start_project)

    assert reactivated["status"] == ProjectStatus.ACTIVE.value
    assert reactivated["archived_at"] is None
    assert (
        await manager.acquire_project_for_session(project["id"], "session-1")
        is project_state
    )


@pytest.mark.runtime
@pytest.mark.no_network
async def test_delete_project_marks_metadata_deleted_and_preserves_data():
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")
    project = await manager.create_project("Research")
    await manager.add_session(project["id"], "session-1")
    await resources.redis.hset(
        RedisKeys.project_topic_config("ada"),
        project["id"],
        json.dumps({"General": {"active": True}}),
    )

    deleted = await manager.delete_project(project["id"])

    assert deleted["status"] == ProjectStatus.DELETED.value
    assert deleted["deleted_at"]
    assert await manager.get_session_ids(project["id"]) == ["session-1"]
    assert await resources.redis.hget(
        RedisKeys.project_topic_config("ada"),
        project["id"],
    )
    assert (await manager.get_project(project["id"]))["status"] == "deleted"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_delete_project_is_idempotent():
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")
    project = await manager.create_project("Research")

    first = await manager.delete_project(project["id"])
    second = await manager.delete_project(project["id"])

    assert second["status"] == ProjectStatus.DELETED.value
    assert second["deleted_at"] == first["deleted_at"]
    assert second["updated_at"] == first["updated_at"]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_deleted_project_remains_listed_with_metadata():
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")
    project = await manager.create_project("Research")
    await manager.add_session(project["id"], "session-1")
    await manager.delete_project(project["id"])

    projects = await manager.list_projects()

    assert len(projects) == 1
    assert projects[0]["id"] == project["id"]
    assert projects[0]["status"] == ProjectStatus.DELETED.value
    assert projects[0]["session_count"] == 1


@pytest.mark.runtime
@pytest.mark.no_network
async def test_archived_project_can_be_marked_deleted():
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")
    project = await manager.create_project("Research")
    archived = await manager.archive_project(project["id"])

    deleted = await manager.delete_project(project["id"])

    assert deleted["status"] == ProjectStatus.DELETED.value
    assert deleted["archived_at"] == archived["archived_at"]
    assert deleted["deleted_at"]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_delete_project_rejects_live_runtime_sessions():
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")
    project = await manager.create_project("Research")
    manager.active_projects[project["id"]] = type(
        "ActiveProject",
        (),
        {"active_runtime_sessions_count": 1},
    )()

    with pytest.raises(RuntimeError, match="active runtime sessions"):
        await manager.delete_project(project["id"])

    assert (await manager.get_project(project["id"]))["status"] == "active"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_deleted_project_cannot_resume_or_reactivate():
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")
    project = await manager.create_project("Research")
    await manager.delete_project(project["id"])

    with pytest.raises(ValueError, match="is deleted"):
        await manager.acquire_project_for_session(project["id"], "session-1")
    with pytest.raises(ValueError, match="cannot be reactivated"):
        await manager.reactivate_project(project["id"])
    with pytest.raises(ValueError, match="cannot be archived"):
        await manager.archive_project(project["id"])
    with pytest.raises(ValueError, match="cannot be updated"):
        await manager.update_project(project["id"], name="Restored")


@pytest.mark.runtime
@pytest.mark.no_network
async def test_deleted_and_missing_projects_have_no_readable_scope():
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")
    project = await manager.create_project("Research")
    await manager.delete_project(project["id"])

    assert await manager.get_readable_project_ids(project["id"]) == []
    assert await manager.get_readable_project_ids("missing-project") == []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_active_project_can_explicitly_read_archived_project():
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")
    archive = await manager.create_project("Archive")
    await manager.archive_project(archive["id"])
    active = await manager.create_project("Current")

    updated = await manager.update_project(
        active["id"],
        allowed_projects=[archive["id"], archive["id"], active["id"]],
    )

    assert updated["allowed_projects"] == [archive["id"]]
    assert await manager.get_readable_project_ids(active["id"]) == [
        IDENTITY_SCOPE,
        active["id"],
        archive["id"],
    ]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_allowed_project_scope_rejects_unknown_projects():
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")
    active = await manager.create_project("Current")

    with pytest.raises(ValueError, match="Unavailable allowed project IDs"):
        await manager.update_project(
            active["id"],
            allowed_projects=["missing-project"],
        )

    assert (await manager.get_project(active["id"]))["allowed_projects"] == []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_invalid_read_scope_does_not_shutdown_dormant_runtime():
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")
    active = await manager.create_project("Current")

    class DormantProject:
        active_runtime_sessions_count = 0

        def __init__(self):
            self.shutdown_calls = 0

        async def shutdown(self):
            self.shutdown_calls += 1

    state = DormantProject()
    manager.active_projects[active["id"]] = state

    with pytest.raises(ValueError, match="Unavailable allowed project IDs"):
        await manager.update_project(
            active["id"],
            allowed_projects=["missing-project"],
        )

    assert state.shutdown_calls == 0
    assert manager.active_projects[active["id"]] is state


@pytest.mark.runtime
@pytest.mark.no_network
async def test_deleted_allowed_project_is_removed_from_readable_scope():
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")
    retained = await manager.create_project("Retained")
    active = await manager.create_project(
        "Current",
        allowed_projects=[retained["id"]],
    )
    await manager.delete_project(retained["id"])

    assert await manager.get_readable_project_ids(active["id"]) == [
        IDENTITY_SCOPE,
        active["id"],
    ]

    with pytest.raises(ValueError, match="Unavailable allowed project IDs"):
        await manager.update_project(
            active["id"],
            allowed_projects=[retained["id"]],
        )


@pytest.mark.runtime
@pytest.mark.no_network
async def test_delete_project_invalidates_active_cross_project_read_scope():
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")
    retained = await manager.create_project("Retained")
    active = await manager.create_project(
        "Current",
        allowed_projects=[retained["id"]],
    )

    class ActiveEntities:
        def __init__(self):
            self.readable_project_ids = [
                IDENTITY_SCOPE,
                active["id"],
                retained["id"],
            ]
            self.removed = []

        def get_profiles(self):
            return {
                10: {"project_id": retained["id"]},
                11: {"project_id": active["id"]},
            }

        def remove_entities(self, entity_ids):
            self.removed.extend(entity_ids)

    entities = ActiveEntities()
    state = type(
        "ActiveProject",
        (),
        {
            "readable_project_ids": entities.readable_project_ids,
            "entities": entities,
            "active_runtime_sessions_count": 1,
        },
    )()
    manager.active_projects[active["id"]] = state

    await manager.delete_project(retained["id"])

    assert state.readable_project_ids == [IDENTITY_SCOPE, active["id"]]
    assert entities.readable_project_ids == [IDENTITY_SCOPE, active["id"]]
    assert entities.removed == [10]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_allowed_project_scope_rejects_live_runtime_change():
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")
    archive = await manager.create_project("Archive")
    active = await manager.create_project("Current")
    manager.active_projects[active["id"]] = type(
        "ActiveProject",
        (),
        {"active_runtime_sessions_count": 1},
    )()

    with pytest.raises(RuntimeError, match="cannot change its readable project scope"):
        await manager.update_project(
            active["id"],
            allowed_projects=[archive["id"]],
        )


@pytest.mark.runtime
@pytest.mark.no_network
async def test_acquire_project_for_session_records_durable_membership(monkeypatch):
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")
    project = await manager.create_project("Research")
    project_state = object()

    seen = {}

    async def fake_get_or_start_project(project_id, initial_topics_config=None):
        seen["initial_topics_config"] = initial_topics_config
        return project_state

    monkeypatch.setattr(manager, "_get_or_start_project", fake_get_or_start_project)

    topics_config = {"Custom": {"active": True}}
    result = await manager.acquire_project_for_session(
        project["id"], "session-1", topics_config=topics_config
    )

    assert result is project_state
    assert seen["initial_topics_config"] == topics_config
    assert await resources.redis.smembers(
        RedisKeys.project_sessions("ada", project["id"])
    ) == {"session-1"}
    assert resources.knowledge_store.identity_calls == [("ada", [])]
    assert resources.redis.evals == []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_acquire_project_initializes_identity_only_once(monkeypatch):
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")
    project = await manager.create_project("Research")

    async def fake_get_or_start_project(project_id, initial_topics_config=None):
        return object()

    monkeypatch.setattr(manager, "_get_or_start_project", fake_get_or_start_project)

    await manager.acquire_project_for_session(project["id"], "session-1")
    await manager.acquire_project_for_session(project["id"], "session-2")

    assert resources.knowledge_store.identity_calls == [("ada", [])]
    assert resources.redis.evals == []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_acquire_project_for_session_rejects_unknown_project():
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")

    with pytest.raises(ValueError, match="does not exist"):
        await manager.acquire_project_for_session("missing-project", "session-1")

    assert await resources.redis.smembers(
        RedisKeys.project_sessions("ada", "missing-project")
    ) == set()
    assert manager.active_projects == {}
    assert resources.knowledge_store.identity_calls == []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_get_or_start_project_requires_active_persisted_project():
    resources = FakeResources()
    manager = ProjectManager(resources=resources, user_name="ada")
    archived = await manager.create_project("Archived")
    deleted = await manager.create_project("Deleted")
    await manager.archive_project(archived["id"])
    await manager.delete_project(deleted["id"])

    with pytest.raises(ValueError, match="does not exist"):
        await manager.get_or_start_project("missing-project")
    with pytest.raises(ValueError, match="is archived"):
        await manager.get_or_start_project(archived["id"])
    with pytest.raises(ValueError, match="is deleted"):
        await manager.get_or_start_project(deleted["id"])

    assert manager.active_projects == {}


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
    await resources.redis.hset(
        RedisKeys.projects("ada"),
        "project-1",
        json.dumps(
            {
                "id": "project-1",
                "name": "Research",
                "description": None,
                "access_mode": "open",
                "allowed_projects": [],
                "status": ProjectStatus.ACTIVE.value,
                "archived_at": None,
                "deleted_at": None,
                "created_at": "2026-01-01T00:00:00+00:00",
                "updated_at": "2026-01-01T00:00:00+00:00",
            }
        ),
    )

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
    assert project_state.readable_project_ids == [IDENTITY_SCOPE, "project-1"]

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
        "aac_discussion",
    ]

    assert RecordingEntityManager.instances[0].kwargs["project_id"] == "project-1"
    assert RecordingEntityManager.instances[0].kwargs["readable_project_ids"] == [
        IDENTITY_SCOPE,
        "project-1",
    ]
    assert RecordingBatchProcessor.instances[0].kwargs["project_id"] == "project-1"
    assert project_state.batch_processor is RecordingBatchProcessor.instances[0]
    assert len(config_manager.subscriptions) == 9

    dlq_job = scheduler._jobs["dlq_auto_replay"]
    batch_result = BatchResult()
    batch_result.set_scope("ada", "session-1", "project-1")
    assert await dlq_job.kwargs["write_to_graph"](batch_result) == (True, None)
    assert graph_write_calls == [
        (
            batch_result,
            {
                "knowledge_store": resources.knowledge_store,
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
