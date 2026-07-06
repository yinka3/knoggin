import json
from datetime import datetime, timezone

import pytest

from common.schema.primitives import Message
from common.utils.events import EventEmitter
from infrastructure.redis_client import RedisKeys
from core.project.project_manager import ProjectManager
from core.session.context import Session
from core.session.session_manager import SessionManager
from tests.fixtures.factories import make_project_state
from tests.fixtures.fakes import (
    FakeConfigValue,
    FakeConsumer,
    FakeSession,
    FakeResources,
)


@pytest.mark.integration
@pytest.mark.no_network
async def test_session_create_add_history_and_close_flow(monkeypatch):
    resources = FakeResources()
    project_manager = ProjectManager(resources, user_name="ada")
    project = await project_manager.create_project("Research")
    active_sessions = {}
    manager = SessionManager(
        resources=resources,
        user_name="ada",
        active_sessions=active_sessions,
        project_manager=project_manager,
    )
    monkeypatch.setattr(
        Session,
        "current_config",
        property(lambda self: FakeConfigValue(conversation_context_turns=100)),
    )
    project_state = make_project_state(project["id"], redis=resources.redis)

    async def fake_get_or_start_project(project_id, initial_topics_config=None):
        project_state.active_runtime_sessions_count += 1
        project_manager.active_projects[project_id] = project_state
        return project_state

    async def fake_create(**kwargs):
        ctx = Session(kwargs["user_name"], ["General"], kwargs["resources"])
        ctx.session_id = kwargs["session_id"]
        ctx.project_id = kwargs["project_state"].project_id
        ctx.project = kwargs["project_state"]
        ctx.consumer = FakeConsumer()
        return ctx

    class FakeEmitter:
        def __init__(self):
            self.unregister_calls = []
            self.emit_calls = []

        async def emit(
            self,
            session_id,
            component,
            event,
            data=None,
            verbose_only=False,
        ):
            self.emit_calls.append((session_id, component, event, data, verbose_only))

        async def cleanup_scope(self, session_id):
            pass

        def unregister_session(self, project_id, session_id):
            self.unregister_calls.append((project_id, session_id))

    emitter = FakeEmitter()
    monkeypatch.setattr(
        project_manager,
        "_get_or_start_project",
        fake_get_or_start_project,
    )
    monkeypatch.setattr(Session, "create", fake_create)
    monkeypatch.setattr(EventEmitter, "get", staticmethod(lambda: emitter))

    ctx = await manager.create_session(
        topics_config={"General": {"active": True}},
        model="test-model",
        project_id=project["id"],
    )
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)
    msg = await ctx.add(
        Message(content="  hello from integration  ", timestamp=timestamp)
    )
    history = await manager.get_session_history_readonly(ctx.session_id)

    assert msg.id == 1
    assert history == [
        {
            "message_id": 1,
            "role": "user",
            "content": "hello from integration",
            "timestamp": timestamp.isoformat(),
        }
    ]
    assert await project_manager.get_session_ids(project["id"]) == [ctx.session_id]
    assert ctx.project.scheduler.activity_count == 1
    assert ctx.consumer.signaled == 1

    assert await manager.close_session(ctx.session_id) is True
    assert active_sessions == {}
    assert emitter.unregister_calls == [(project["id"], ctx.session_id)]
    assert project["id"] not in project_manager.active_projects

    metadata = json.loads(
        await resources.redis.hget(RedisKeys.sessions("ada"), ctx.session_id)
    )
    assert metadata["project_id"] == project["id"]
    assert metadata["last_active"]


@pytest.mark.integration
@pytest.mark.no_network
async def test_hard_project_delete_and_explicit_session_cleanup_are_separate():
    resources = FakeResources()
    project_manager = ProjectManager(resources, user_name="ada")
    project = await project_manager.create_project("Scratch")
    session_id = "session-1"
    active_sessions = {session_id: FakeSession(session_id, project["id"])}
    manager = SessionManager(
        resources=resources,
        user_name="ada",
        active_sessions=active_sessions,
        project_manager=project_manager,
    )

    await project_manager.add_session(project["id"], session_id)
    await resources.redis.hset(
        RedisKeys.sessions("ada"),
        session_id,
        json.dumps({"project_id": project["id"]}),
    )
    await resources.redis.rpush(RedisKeys.buffer("ada", session_id), "pending")
    await resources.redis.hset(
        RedisKeys.conversation("ada", session_id),
        "1",
        json.dumps(
            {
                "role": "user",
                "content": "delete me",
                "timestamp": "2026-01-02T03:04:00+00:00",
            }
        ),
    )

    deleted_project = await project_manager.delete_project(project["id"])
    assert await project_manager.get_session_ids(project["id"]) == []

    deleted_count = await manager.delete_session_data(session_id)

    assert deleted_project["status"] == "deleted"
    assert await project_manager.get_project(project["id"]) is None
    assert deleted_count >= 2
    assert await resources.redis.hget(RedisKeys.sessions("ada"), session_id) is None
    assert (
        await resources.redis.lrange(RedisKeys.buffer("ada", session_id), 0, -1)
        == []
    )
    assert (
        await resources.redis.hget(RedisKeys.conversation("ada", session_id), "1")
        is None
    )
