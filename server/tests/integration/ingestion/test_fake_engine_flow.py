import json
from datetime import datetime, timezone

import pytest

from common.schema.primitives import Message
from common.utils.events import EventEmitter
from core.project.project_manager import ProjectManager
from core.session.session_manager import SessionManager
from infrastructure.redis_client import RedisKeys
from runtime.session_runtime import SessionRuntime as Session
from tests.fixtures.factories import make_domain_config, make_project_state
from tests.fixtures.fakes import (
    FakeConfigValue,
    FakeConsumer,
    FakeResources,
)


@pytest.mark.integration
@pytest.mark.no_network
async def test_session_create_add_history_and_close_flow(monkeypatch):
    resources = FakeResources()
    project_manager = ProjectManager(resources, user_name="ada")
    project = await project_manager.create_project(
        "Research",
        domain_config=make_domain_config(version=0),
    )
    manager = SessionManager(
        resources=resources,
        user_name="ada",
        project_manager=project_manager,
    )
    active_sessions = manager._active_sessions
    monkeypatch.setattr(
        Session,
        "current_config",
        property(lambda self: FakeConfigValue(conversation_context_turns=100)),
    )
    project_state = make_project_state(project["id"], redis=resources.redis)

    async def create_project_runtime(*_args, **_kwargs):
        return project_state

    class RuntimeFactory:
        async def bootstrap(self, state, **kwargs):
            ctx = Session(resources=resources, user_name="ada")
            ctx.session_id = kwargs["session_id"]
            ctx.project_id = state.project_id
            ctx.project = state
            ctx.model = kwargs["model"]
            ctx.agent_id = kwargs["agent_id"]
            ctx.enabled_tools = kwargs["enabled_tools"]
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

    emitter = FakeEmitter()
    monkeypatch.setattr(
        project_manager,
        "project_factory",
        type("Factory", (), {"create": create_project_runtime})(),
    )
    monkeypatch.setattr(manager, "_session_runtime_factory", RuntimeFactory)
    monkeypatch.setattr(EventEmitter, "get", staticmethod(lambda: emitter))

    ctx = await manager.create_session(
        model="test-model",
        project_id=project["id"],
    )
    timestamp = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)
    msg = await ctx.add(
        Message(content="  hello from integration  ", timestamp=timestamp)
    )
    assert msg.id == 1
    assert await project_manager.get_session_ids(project["id"]) == [ctx.session_id]
    assert ctx.consumer.signaled == 1

    assert await manager.close_session(ctx.session_id) is True
    assert active_sessions == {}
    assert project["id"] not in project_manager.active_projects

    assert resources.postgres.sessions[ctx.session_id]["status"] == "deleted"


@pytest.mark.integration
@pytest.mark.no_network
async def test_hard_project_delete_makes_explicit_session_cleanup_idempotent():
    resources = FakeResources()
    project_manager = ProjectManager(resources, user_name="ada")

    class FakeAggregateDeletionWriter:
        async def delete_project(self, *, user_name, project_id):
            resources.postgres.projects.pop(project_id, None)
            resources.postgres.sessions = {
                session_id: row
                for session_id, row in resources.postgres.sessions.items()
                if row.get("project_id") != project_id
            }
            resources.postgres.messages = [
                row
                for row in resources.postgres.messages
                if row.get("project_id") != project_id
            ]
            return {"projects": 1}

    project_manager._project_deletion_writer = FakeAggregateDeletionWriter()
    project = await project_manager.create_project(
        "Scratch",
        domain_config=make_domain_config(version=0),
    )
    session_id = "session-1"
    resources.postgres.sessions[session_id] = {
        "session_id": session_id,
        "user_name": "ada",
        "project_id": project["id"],
        "status": "open",
    }
    manager = SessionManager(
        resources=resources,
        user_name="ada",
        project_manager=project_manager,
    )
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

    assert (
        await resources.redis.hget(RedisKeys.conversation("ada", session_id), "1")
        is None
    )

    deleted_count = await manager.delete_session_data(session_id)

    assert deleted_project["status"] == "deleted"
    assert await project_manager.get_project(project["id"]) is None
    assert deleted_count == 0
