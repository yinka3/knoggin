import json

import pytest

from common.schema.settings import TopicSchema
from infrastructure.job.scheduler import Scheduler
from infrastructure.redis_client import RedisKeys
from knoggin_server.project.project_manager import ProjectManager
from tests.fixtures.fakes import FakeResources


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

    raw = await resources.redis.hget(RedisKeys.session_config("ada"), "project-1")
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
        RedisKeys.session_config("ada"),
        "project-1",
        json.dumps({"Existing": {"active": True}}),
    )

    await manager._ensure_project_topics_config(
        "project-1",
        {"New": TopicSchema(active=True)},
    )

    raw = await resources.redis.hget(RedisKeys.session_config("ada"), "project-1")
    assert json.loads(raw) == {"Existing": {"active": True}}


@pytest.mark.runtime
@pytest.mark.no_network
async def test_project_scheduler_context_uses_project_scope_id():
    redis = FakeResources().redis
    scheduler = Scheduler("ada", "project-1", redis, project_id="project-1")

    ctx = await scheduler._build_context()

    assert ctx.session_id == "project-1"
    assert ctx.project_id == "project-1"
