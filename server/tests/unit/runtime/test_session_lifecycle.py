import json

import pytest

from core.session.context import Session
from core.session.session_manager import SessionManager
from infrastructure.redis_client import RedisKeys
from tests.fixtures.fakes import FakeProjectManager, FakeResources, FakeSession


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


async def activate_runtime_session(manager, project_manager, context):
    """Install a fully acquired runtime for tests that bypass session creation."""
    runtime = project_manager.project_runtime(
        context.project_id,
        context.session_id,
    )
    await runtime.__aenter__()
    manager.active_sessions[context.session_id] = context
    manager._project_runtime_contexts[context.session_id] = runtime


@pytest.mark.runtime
@pytest.mark.no_network
async def test_create_session_requires_project_id_without_side_effects(
    session_manager,
):
    manager, _, project_manager, active_sessions = session_manager

    with pytest.raises(ValueError, match="requires a project_id"):
        await manager.create_session(project_id="")

    assert project_manager.acquire_calls == []
    assert active_sessions == {}


@pytest.mark.runtime
@pytest.mark.no_network
async def test_create_session_stores_metadata_and_active_context(
    monkeypatch, session_manager
):
    manager, resources, project_manager, active_sessions = session_manager

    async def fake_create(**kwargs):
        return FakeSession(session_id=kwargs["session_id"], project_id="project-1")

    monkeypatch.setattr(Session, "create", fake_create)

    ctx = await manager.create_session(
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
    assert metadata["status"] == "open"
    assert "topics_config" not in metadata


@pytest.mark.runtime
@pytest.mark.no_network
async def test_create_session_releases_project_state_when_context_create_fails(
    monkeypatch, session_manager
):
    manager, _, project_manager, active_sessions = session_manager

    async def failing_create(**kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(Session, "create", failing_create)

    with pytest.raises(RuntimeError, match="boom"):
        await manager.create_session(
            project_id="project-1",
        )

    session_id = project_manager.acquire_calls[0][1]
    assert project_manager.remove_session_calls == [("project-1", session_id)]
    assert project_manager.release_calls == ["project-1"]
    assert active_sessions == {}


@pytest.mark.runtime
@pytest.mark.no_network
async def test_create_session_cleans_runtime_when_persistence_fails(
    monkeypatch, session_manager
):
    manager, resources, project_manager, active_sessions = session_manager
    context = FakeSession(session_id="session-1", project_id="project-1")

    async def fake_create(**kwargs):
        context.session_id = kwargs["session_id"]
        return context

    async def failing_execute(*_args, **_kwargs):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(Session, "create", fake_create)
    monkeypatch.setattr(resources.postgres, "execute", failing_execute)

    with pytest.raises(RuntimeError, match="database unavailable"):
        await manager.create_session(project_id="project-1")

    assert context.shutdown_count == 1
    assert project_manager.release_calls == ["project-1"]
    assert project_manager.remove_session_calls == [("project-1", context.session_id)]
    assert active_sessions == {}


@pytest.mark.runtime
@pytest.mark.no_network
async def test_resume_session_uses_persisted_project_and_updates_last_active(
    monkeypatch, session_manager
):
    manager, resources, project_manager, active_sessions = session_manager
    resources.postgres.sessions["session-1"] = {
        "session_id": "session-1",
        "user_name": "ada",
        "project_id": "project-1",
        "model": "resume-model",
    }
    await resources.redis.hset(
        RedisKeys.sessions("ada"),
        "session-1",
        json.dumps({"project_id": "project-1", "model": "resume-model"}),
    )

    async def fake_create(**kwargs):
        assert kwargs["session_id"] == "session-1"
        assert kwargs["model"] == "resume-model"
        return FakeSession(session_id="session-1", project_id="project-1")

    monkeypatch.setattr(Session, "create", fake_create)

    ctx = await manager.get_or_resume_session("session-1")

    assert ctx is active_sessions["session-1"]
    assert project_manager.acquire_calls == [("project-1", "session-1")]
    metadata = json.loads(
        await resources.redis.hget(RedisKeys.sessions("ada"), "session-1")
    )
    assert metadata["last_active"]
    assert metadata["status"] == "open"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_resume_session_does_not_trust_redis_only_metadata(
    monkeypatch,
    session_manager,
):
    manager, resources, project_manager, active_sessions = session_manager
    await resources.redis.hset(
        RedisKeys.sessions("ada"),
        "session-1",
        json.dumps({"project_id": "project-1", "model": "legacy-model"}),
    )

    async def should_not_create(**_kwargs):
        raise AssertionError("Redis-only session metadata must not resume a session")

    monkeypatch.setattr(Session, "create", should_not_create)

    assert await manager.get_or_resume_session("session-1") is None
    assert project_manager.acquire_calls == []
    assert active_sessions == {}


@pytest.mark.runtime
@pytest.mark.no_network
async def test_resume_session_releases_project_state_when_context_create_fails(
    monkeypatch, session_manager
):
    manager, resources, project_manager, active_sessions = session_manager
    resources.postgres.sessions["session-1"] = {
        "session_id": "session-1",
        "user_name": "ada",
        "project_id": "project-1",
        "model": "resume-model",
    }
    await resources.redis.hset(
        RedisKeys.sessions("ada"),
        "session-1",
        json.dumps({"project_id": "project-1", "model": "resume-model"}),
    )

    async def failing_create(**kwargs):
        raise RuntimeError("resume failed")

    monkeypatch.setattr(Session, "create", failing_create)

    with pytest.raises(RuntimeError, match="resume failed"):
        await manager.get_or_resume_session("session-1")

    assert project_manager.release_calls == ["project-1"]
    assert active_sessions == {}


@pytest.mark.runtime
@pytest.mark.no_network
async def test_resume_session_aborts_when_durable_session_disappears(
    monkeypatch,
    session_manager,
):
    manager, resources, project_manager, active_sessions = session_manager
    resources.postgres.sessions["session-1"] = {
        "session_id": "session-1",
        "user_name": "ada",
        "project_id": "project-1",
        "model": "resume-model",
    }
    context = FakeSession(session_id="session-1", project_id="project-1")
    resources.postgres.write_count = 0

    async def fake_create(**_kwargs):
        return context

    monkeypatch.setattr(Session, "create", fake_create)

    with pytest.raises(RuntimeError, match="Session disappeared while resuming"):
        await manager.get_or_resume_session("session-1")

    assert context.shutdown_count == 1
    assert project_manager.release_calls == ["project-1"]
    assert active_sessions == {}


@pytest.mark.runtime
@pytest.mark.no_network
async def test_close_session_alias_tombstones_and_shuts_context_down(
    monkeypatch, session_manager
):
    manager, resources, project_manager, active_sessions = session_manager
    ctx = FakeSession(session_id="session-1", project_id="project-1")
    resources.postgres.sessions["session-1"] = {
        "session_id": "session-1",
        "user_name": "ada",
        "project_id": "project-1",
        "model": None,
        "status": "open",
    }
    await activate_runtime_session(manager, project_manager, ctx)
    await resources.redis.hset(
        RedisKeys.sessions("ada"),
        "session-1",
        json.dumps({"project_id": "project-1"}),
    )
    order = []

    async def shutdown():
        ctx.shutdown_count += 1
        order.append("shutdown")

    async def release_project(project_id):
        project_manager.release_calls.append(project_id)
        order.append("release_project")

    ctx.shutdown = shutdown
    project_manager.release_project = release_project

    assert await manager.close_session("session-1") is True

    assert active_sessions == {}
    assert ctx.shutdown_count == 1
    assert project_manager.release_calls == ["project-1"]
    assert order == ["shutdown", "release_project"]
    assert resources.postgres.sessions["session-1"]["status"] == "deleted"
    assert resources.postgres.sessions["session-1"]["deleted_at"] is not None
    assert await resources.redis.hget(RedisKeys.sessions("ada"), "session-1") is None


@pytest.mark.runtime
@pytest.mark.no_network
async def test_delete_unloaded_session_is_durable_and_prevents_resume(
    monkeypatch, session_manager
):
    manager, resources, project_manager, active_sessions = session_manager
    resources.postgres.sessions["session-1"] = {
        "session_id": "session-1",
        "user_name": "ada",
        "project_id": "project-1",
        "model": "resume-model",
        "status": "open",
    }

    async def should_not_create(**_kwargs):
        raise AssertionError("deleted sessions must not be resumed")

    monkeypatch.setattr(Session, "create", should_not_create)

    assert await manager.close_session("session-1") is True
    assert resources.postgres.sessions["session-1"]["status"] == "deleted"
    assert await manager.get_or_resume_session("session-1") is None
    assert project_manager.acquire_calls == []
    assert active_sessions == {}


@pytest.mark.runtime
@pytest.mark.no_network
async def test_shutdown_deactivates_every_runtime_session(monkeypatch, session_manager):
    manager, _, project_manager, active_sessions = session_manager
    first = FakeSession(session_id="session-1", project_id="project-1")
    second = FakeSession(session_id="session-2", project_id="project-2")
    await activate_runtime_session(manager, project_manager, first)
    await activate_runtime_session(manager, project_manager, second)
    await manager.shutdown()

    assert active_sessions == {}
    assert first.shutdown_count == 1
    assert second.shutdown_count == 1
    assert set(project_manager.release_calls) == {"project-1", "project-2"}


@pytest.mark.runtime
@pytest.mark.no_network
async def test_delete_session_data_does_not_remove_project_documents(
    monkeypatch, session_manager,
):
    manager, resources, project_manager, active_sessions = session_manager
    ctx = FakeSession(session_id="session-1", project_id="project-1")
    project_document_service = object()
    ctx.document_service = project_document_service
    await activate_runtime_session(manager, project_manager, ctx)
    await resources.redis.hset(
        RedisKeys.sessions("ada"),
        "session-1",
        json.dumps({"project_id": "project-1"}),
    )
    await resources.redis.sadd(
        RedisKeys.project_sessions("ada", "project-1"), "session-1"
    )
    memory_key = RedisKeys.session_memory("ada", "session-1", "notes")
    dedup_key = RedisKeys.message_dedup("ada", "session-1", "digest")
    await resources.redis.set(memory_key, "remember me")
    await resources.redis.set(dedup_key, "42")
    await resources.redis.rpush(RedisKeys.buffer("ada", "session-1"), "pending")

    deleted = await manager.delete_session_data("session-1")

    assert deleted >= 1
    assert ctx.document_service is project_document_service
    assert ctx.shutdown_count == 1
    assert active_sessions == {}
    assert project_manager.release_calls == ["project-1"]
    assert project_manager.remove_session_calls == []
    assert await resources.redis.hget(RedisKeys.sessions("ada"), "session-1") is None
    assert await resources.redis.get(memory_key) is None
    assert await resources.redis.get(dedup_key) is None


@pytest.mark.runtime
@pytest.mark.no_network
async def test_delete_session_tombstone_preserves_readonly_message_history(
    session_manager,
):
    manager, resources, _, _ = session_manager
    resources.postgres.sessions["session-1"] = {
        "session_id": "session-1",
        "user_name": "ada",
        "project_id": "project-1",
        "status": "open",
        "model": "test-model",
    }
    resources.postgres.messages.append(
        {
            "message_id": 101,
            "user_name": "ada",
            "project_id": "project-1",
            "session_id": "session-1",
            "role": "user",
            "content": "Keep this as episode evidence.",
            "timestamp_ms": 1,
            "ingestion_state": "processed",
            "episode_eligible": True,
        }
    )

    await manager.delete_session_data("session-1")

    assert resources.postgres.sessions["session-1"]["status"] == "deleted"
    assert resources.postgres.messages[0]["content"] == "Keep this as episode evidence."
    assert await manager.get_session_history_readonly("session-1") == [
        {
            "message_id": 101,
            "role": "user",
            "content": "Keep this as episode evidence.",
            "timestamp": 1,
        }
    ]
    assert await manager.list_sessions() == {}
    assert await manager.get_or_resume_session("session-1") is None


@pytest.mark.runtime
@pytest.mark.no_network
async def test_delete_session_keeps_redis_state_when_durable_deletion_fails(
    monkeypatch,
    session_manager,
):
    manager, resources, project_manager, _ = session_manager
    context = FakeSession(
        session_id="session-1",
        project_id="project-1",
    )
    await activate_runtime_session(manager, project_manager, context)
    memory_key = RedisKeys.session_memory("ada", "session-1", "notes")
    await resources.redis.set(memory_key, "keep until durable delete succeeds")

    async def fail_durable_delete(**_kwargs):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(
        manager._session_deletion_writer,
        "delete_session",
        fail_durable_delete,
    )

    with pytest.raises(RuntimeError, match="database unavailable"):
        await manager.delete_session_data("session-1")

    assert await resources.redis.get(memory_key) == "keep until durable delete succeeds"
    assert "session-1" not in manager._closed_session_ids


@pytest.mark.runtime
@pytest.mark.no_network
async def test_delete_session_treats_redis_cleanup_failure_as_recoverable(
    monkeypatch,
    session_manager,
):
    manager, resources, _, _ = session_manager
    resources.postgres.sessions["session-1"] = {
        "session_id": "session-1",
        "user_name": "ada",
        "project_id": "project-1",
    }
    memory_key = RedisKeys.session_memory("ada", "session-1", "notes")
    await resources.redis.set(memory_key, "stale cache")

    async def fail_delete(*_keys):
        raise RuntimeError("redis unavailable")

    monkeypatch.setattr(resources.redis, "delete", fail_delete)

    assert await manager.delete_session_data("session-1") == 0
    assert resources.postgres.sessions["session-1"]["status"] == "deleted"
    assert await resources.redis.get(memory_key) == "stale cache"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_document_focus_persists_reads_and_clears(session_manager):
    manager, resources, _, active_sessions = session_manager
    ctx = FakeSession(session_id="session-1", project_id="project-1")

    class FocusDocumentService:
        async def resolve_focus_target(self, **kwargs):
            assert kwargs == {
                "session_id": "session-1",
                "document_id": "doc-1",
                "folder_root_id": None,
                "path_prefix": None,
            }
            return {
                "target_type": "document",
                "document_id": "doc-1",
                "relative_path": "docs/notes.md",
                "folder_root_id": None,
                "path_prefix": None,
            }

    ctx.document_service = FocusDocumentService()
    active_sessions["session-1"] = ctx
    await resources.redis.hset(
        RedisKeys.sessions("ada"),
        "session-1",
        json.dumps(
            {
                "project_id": "project-1",
                "document_focus": {
                    "mode": "pinned",
                    "target_type": "folder_upload",
                    "document_id": None,
                    "relative_path": None,
                    "folder_root_id": "folder-1",
                    "path_prefix": None,
                    "created_at": "2026-06-22T12:00:00+00:00",
                },
            }
        ),
    )

    focus = await manager.set_document_focus(
        "session-1",
        document_id="doc-1",
    )

    assert focus["target_type"] == "document"
    assert focus["mode"] == "pinned"
    assert focus["relative_path"] == "docs/notes.md"
    assert await manager.get_document_focus("session-1") == focus
    assert await manager.clear_document_focus("session-1") is True
    assert await manager.get_document_focus("session-1") is None
    assert await manager.clear_document_focus("session-1") is False
    metadata = json.loads(
        await resources.redis.hget(
            RedisKeys.sessions("ada"),
            "session-1",
        )
    )
    assert metadata["project_id"] == "project-1"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_document_focus_survives_session_resume(
    monkeypatch,
    session_manager,
):
    manager, resources, _, active_sessions = session_manager
    resources.postgres.sessions["session-1"] = {
        "session_id": "session-1",
        "user_name": "ada",
        "project_id": "project-1",
        "model": None,
    }
    focus = {
        "mode": "pinned",
        "target_type": "folder_upload",
        "document_id": None,
        "relative_path": None,
        "folder_root_id": "folder-1",
        "path_prefix": None,
        "created_at": "2026-06-22T12:00:00+00:00",
    }
    expected_focus = {
        "mode": "pinned",
        "target_type": "folder_upload",
        "folder_root_id": "folder-1",
        "created_at": "2026-06-22T12:00:00+00:00",
    }
    await resources.redis.hset(
        RedisKeys.sessions("ada"),
        "session-1",
        json.dumps(
            {
                "project_id": "project-1",
                "document_focus": focus,
            }
        ),
    )

    async def fake_create(**kwargs):
        return FakeSession(
            session_id=kwargs["session_id"],
            project_id="project-1",
        )

    monkeypatch.setattr(Session, "create", fake_create)

    await manager.get_or_resume_session("session-1")

    assert "session-1" in active_sessions
    assert await manager.get_document_focus("session-1") == expected_focus
