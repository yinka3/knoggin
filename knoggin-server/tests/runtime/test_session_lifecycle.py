import json

import pytest

from common.utils.events import DebugEventEmitter
from infrastructure.redis_client import RedisKeys
from knoggin_server.session.context import Context
from knoggin_server.session.session_manager import SessionManager
from tests.fixtures.fakes import FakeContext, FakeProjectManager, FakeResources


@pytest.fixture
def session_manager():
    resources = FakeResources()
    project_manager = FakeProjectManager()
    active_sessions = {}
    manager = SessionManager(
        resources=resources,
        user_name="ada",
        active_sessions=active_sessions,
        project_manager=project_manager,
    )
    return manager, resources, project_manager, active_sessions


@pytest.mark.runtime
@pytest.mark.no_network
async def test_create_session_stores_metadata_and_active_context(
    monkeypatch, session_manager
):
    manager, resources, project_manager, active_sessions = session_manager

    async def fake_create(**kwargs):
        return FakeContext(session_id=kwargs["session_id"], project_id="project-1")

    monkeypatch.setattr(Context, "create", fake_create)

    ctx = await manager.create_session(
        topics_config={"General": {"active": True}},
        model="test-model",
        agent_id="agent-1",
        enabled_tools=["search"],
        project_id="project-1",
    )

    assert ctx.session_id in active_sessions
    assert active_sessions[ctx.session_id] is ctx
    assert project_manager.acquire_calls == [("project-1", ctx.session_id)]

    raw = await resources.redis.hget(RedisKeys.sessions("ada"), ctx.session_id)
    metadata = json.loads(raw)
    assert metadata["project_id"] == "project-1"
    assert metadata["model"] == "test-model"
    assert metadata["agent_id"] == "agent-1"
    assert metadata["enabled_tools"] == ["search"]
    assert metadata["topics_config"] == {"General": {"active": True}}


@pytest.mark.runtime
@pytest.mark.no_network
async def test_create_session_releases_project_state_when_context_create_fails(
    monkeypatch, session_manager
):
    manager, _, project_manager, active_sessions = session_manager

    async def failing_create(**kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(Context, "create", failing_create)

    with pytest.raises(RuntimeError, match="boom"):
        await manager.create_session(
            topics_config={"General": {"active": True}},
            project_id="project-1",
        )

    session_id = project_manager.acquire_calls[0][1]
    assert project_manager.remove_session_calls == [("project-1", session_id)]
    assert project_manager.release_calls == ["project-1"]
    assert active_sessions == {}


@pytest.mark.runtime
@pytest.mark.no_network
async def test_resume_session_uses_persisted_project_and_updates_last_active(
    monkeypatch, session_manager
):
    manager, resources, project_manager, active_sessions = session_manager
    await resources.redis.hset(
        RedisKeys.sessions("ada"),
        "session-1",
        json.dumps({"project_id": "project-1", "model": "resume-model"}),
    )

    async def fake_create(**kwargs):
        assert kwargs["session_id"] == "session-1"
        assert kwargs["model"] == "resume-model"
        return FakeContext(session_id="session-1", project_id="project-1")

    monkeypatch.setattr(Context, "create", fake_create)

    ctx = await manager.get_or_resume_session("session-1")

    assert ctx is active_sessions["session-1"]
    assert project_manager.acquire_calls == [("project-1", "session-1")]
    metadata = json.loads(
        await resources.redis.hget(RedisKeys.sessions("ada"), "session-1")
    )
    assert metadata["last_active"]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_resume_session_releases_project_state_when_context_create_fails(
    monkeypatch, session_manager
):
    manager, resources, project_manager, active_sessions = session_manager
    await resources.redis.hset(
        RedisKeys.sessions("ada"),
        "session-1",
        json.dumps({"project_id": "project-1", "model": "resume-model"}),
    )

    async def failing_create(**kwargs):
        raise RuntimeError("resume failed")

    monkeypatch.setattr(Context, "create", failing_create)

    with pytest.raises(RuntimeError, match="resume failed"):
        await manager.get_or_resume_session("session-1")

    assert project_manager.release_calls == ["project-1"]
    assert active_sessions == {}


@pytest.mark.runtime
@pytest.mark.no_network
async def test_close_session_releases_project_and_shuts_context_down(
    monkeypatch, session_manager
):
    manager, resources, project_manager, active_sessions = session_manager
    ctx = FakeContext(session_id="session-1", project_id="project-1")
    active_sessions["session-1"] = ctx
    await resources.redis.hset(
        RedisKeys.sessions("ada"),
        "session-1",
        json.dumps({"project_id": "project-1"}),
    )
    unregister_calls = []
    order = []

    async def shutdown():
        ctx.shutdown_count += 1
        order.append("shutdown")

    async def release_project(project_id):
        project_manager.release_calls.append(project_id)
        order.append("release_project")

    ctx.shutdown = shutdown
    project_manager.release_project = release_project

    class FakeEmitter:
        def unregister_session(self, project_id, session_id):
            unregister_calls.append((project_id, session_id))

    monkeypatch.setattr(DebugEventEmitter, "get", staticmethod(lambda: FakeEmitter()))

    assert await manager.close_session("session-1") is True

    assert active_sessions == {}
    assert ctx.shutdown_count == 1
    assert project_manager.release_calls == ["project-1"]
    assert order == ["shutdown", "release_project"]
    assert unregister_calls == [("project-1", "session-1")]
    metadata = json.loads(
        await resources.redis.hget(RedisKeys.sessions("ada"), "session-1")
    )
    assert metadata["last_active"]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_delete_session_data_cleans_keys_membership_and_filerag(
    session_manager,
):
    manager, resources, project_manager, active_sessions = session_manager
    ctx = FakeContext(session_id="session-1", project_id="project-1")
    active_sessions["session-1"] = ctx
    await resources.redis.hset(
        RedisKeys.sessions("ada"),
        "session-1",
        json.dumps({"project_id": "project-1"}),
    )
    await resources.redis.sadd(
        RedisKeys.project_sessions("ada", "project-1"), "session-1"
    )
    await resources.redis.set("memory:ada:session-1:notes", "remember me")
    await resources.redis.rpush(RedisKeys.buffer("ada", "session-1"), "pending")

    deleted = await manager.delete_session_data("session-1")

    assert deleted >= 1
    assert ctx.file_rag.cleanup_count == 1
    assert project_manager.remove_session_calls == [("project-1", "session-1")]
    assert await resources.redis.hget(RedisKeys.sessions("ada"), "session-1") is None
