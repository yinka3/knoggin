import json

import pytest

from common.schema.document import dump_document_focus
from core.session.session_manager import SessionManager
from infrastructure.redis_client import RedisKeys
from tests.fixtures.fakes import FakeProjectManager, FakeResources, FakeSession


class RecordingSessionRuntimeFactory:
    def __init__(self, *, failure: Exception | None = None):
        self.calls: list[dict] = []
        self.failure = failure

    async def bootstrap(self, project_state, **kwargs):
        self.calls.append({"project_state": project_state, **kwargs})
        if self.failure is not None:
            raise self.failure
        context = FakeSession(
            session_id=kwargs["session_id"],
            project_id=getattr(project_state, "project_id", "project-1"),
        )
        context.model = kwargs["model"]
        context.agent_id = kwargs["agent_id"]
        context.enabled_tools = kwargs["enabled_tools"]
        context.document_focus = kwargs.get("document_focus")
        return context


@pytest.fixture
def session_manager(monkeypatch):
    resources = FakeResources()
    project_manager = FakeProjectManager()
    manager = SessionManager(
        resources=resources,
        user_name="ada",
        project_manager=project_manager,
    )
    factory = RecordingSessionRuntimeFactory()
    monkeypatch.setattr(manager, "_session_runtime_factory", lambda: factory)
    return manager, resources, project_manager, factory


@pytest.mark.runtime
@pytest.mark.no_network
async def test_create_session_requires_project_id_without_side_effects(
    session_manager,
):
    manager, _, project_manager, _ = session_manager

    with pytest.raises(ValueError, match="requires a project_id"):
        await manager.create_session(project_id="")

    assert project_manager.acquire_calls == []
    assert manager._active_sessions == {}


@pytest.mark.runtime
@pytest.mark.no_network
async def test_create_session_persists_metadata_then_bootstraps_runtime(
    session_manager,
):
    manager, resources, project_manager, factory = session_manager

    context = await manager.create_session(
        project_id="project-1",
        model="test-model",
        agent_id="agent-1",
        enabled_tools=[],
    )

    persisted = resources.postgres.sessions[context.session_id]
    assert persisted["project_id"] == "project-1"
    assert persisted["model"] == "test-model"
    assert persisted["agent_id"] == "agent-1"
    assert persisted["enabled_tools"] == []
    assert persisted["status"] == "open"
    assert project_manager.acquire_calls == [("project-1", context.session_id)]
    assert factory.calls[0]["enabled_tools"] == []
    assert manager._active_sessions[context.session_id] is context


@pytest.mark.runtime
@pytest.mark.no_network
async def test_create_failure_hard_deletes_durable_row_and_releases_exact_lease(
    monkeypatch,
    session_manager,
):
    manager, resources, project_manager, _ = session_manager
    factory = RecordingSessionRuntimeFactory(failure=RuntimeError("startup failed"))
    monkeypatch.setattr(manager, "_session_runtime_factory", lambda: factory)

    with pytest.raises(RuntimeError, match="startup failed"):
        await manager.create_session(project_id="project-1")

    session_id = project_manager.acquire_calls[0][1]
    assert session_id not in resources.postgres.sessions
    assert project_manager.release_calls == [("project-1", session_id)]
    assert manager._active_sessions == {}


@pytest.mark.runtime
@pytest.mark.no_network
async def test_persistence_failure_never_acquires_a_project_lease(
    monkeypatch,
    session_manager,
):
    manager, resources, project_manager, _ = session_manager

    async def unavailable(*_args, **_kwargs):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(resources.postgres, "execute", unavailable)

    with pytest.raises(RuntimeError, match="database unavailable"):
        await manager.create_session(project_id="project-1")

    assert project_manager.acquire_calls == []
    assert project_manager.release_calls == []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_resume_reconstructs_all_durable_session_configuration(
    session_manager,
):
    manager, resources, project_manager, factory = session_manager
    focus = {
        "mode": "pinned",
        "target_type": "folder_upload",
        "folder_root_id": "folder-1",
        "created_at": "2026-08-20T12:00:00+00:00",
    }
    resources.postgres.sessions["session-1"] = {
        "session_id": "session-1",
        "user_name": "ada",
        "project_id": "project-1",
        "model": "resume-model",
        "agent_id": "agent-1",
        "enabled_tools": json.dumps([]),
        "document_focus": json.dumps(focus),
        "status": "open",
    }

    context = await manager.get_or_resume_session("session-1")

    assert context is manager._active_sessions["session-1"]
    assert project_manager.acquire_calls == [("project-1", "session-1")]
    assert factory.calls[0]["model"] == "resume-model"
    assert factory.calls[0]["agent_id"] == "agent-1"
    assert factory.calls[0]["enabled_tools"] == []
    assert dump_document_focus(factory.calls[0]["document_focus"]) == focus


@pytest.mark.runtime
@pytest.mark.no_network
async def test_resume_failure_releases_the_exact_acquired_lease(
    monkeypatch,
    session_manager,
):
    manager, resources, project_manager, _ = session_manager
    resources.postgres.sessions["session-1"] = {
        "session_id": "session-1",
        "user_name": "ada",
        "project_id": "project-1",
        "model": "resume-model",
        "status": "open",
    }
    factory = RecordingSessionRuntimeFactory(failure=RuntimeError("resume failed"))
    monkeypatch.setattr(manager, "_session_runtime_factory", lambda: factory)

    with pytest.raises(RuntimeError, match="resume failed"):
        await manager.get_or_resume_session("session-1")

    assert project_manager.release_calls == [("project-1", "session-1")]
    assert manager._active_sessions == {}


@pytest.mark.runtime
@pytest.mark.no_network
async def test_deleted_or_missing_durable_sessions_do_not_resume(session_manager):
    manager, resources, project_manager, factory = session_manager
    resources.postgres.sessions["deleted"] = {
        "session_id": "deleted",
        "user_name": "ada",
        "project_id": "project-1",
        "status": "deleted",
    }

    assert await manager.get_or_resume_session("missing") is None
    assert await manager.get_or_resume_session("deleted") is None
    assert project_manager.acquire_calls == []
    assert factory.calls == []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_deactivate_releases_exact_session_lease(session_manager):
    manager, _, project_manager, _ = session_manager
    context = FakeSession(session_id="session-1", project_id="project-1")
    manager._active_sessions["session-1"] = context

    assert await manager.deactivate_runtime_session("session-1") is True

    assert context.shutdown_count == 1
    assert project_manager.release_calls == [("project-1", "session-1")]
    assert manager._active_sessions == {}


@pytest.mark.runtime
@pytest.mark.no_network
async def test_delete_marks_transient_cleanup_only_after_durable_delete(
    monkeypatch,
    session_manager,
):
    manager, resources, _, _ = session_manager
    calls = []

    async def delete_session(**kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(manager._session_deletion_writer, "delete_session", delete_session)
    key = RedisKeys.session_memory("ada", "session-1", "notes")
    await resources.redis.set(key, "cache")

    await manager.delete_session_data("session-1")

    assert calls == [{"user_name": "ada", "session_id": "session-1"}]
    assert await resources.redis.get(key) is None
    assert "session-1" not in manager._deleting_session_ids


@pytest.mark.runtime
@pytest.mark.no_network
async def test_failed_durable_delete_keeps_cache_and_clears_deleting_marker(
    monkeypatch,
    session_manager,
):
    manager, resources, _, _ = session_manager
    key = RedisKeys.session_memory("ada", "session-1", "notes")
    await resources.redis.set(key, "cache")

    async def unavailable(**_kwargs):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(manager._session_deletion_writer, "delete_session", unavailable)

    with pytest.raises(RuntimeError, match="database unavailable"):
        await manager.delete_session_data("session-1")

    assert await resources.redis.get(key) == "cache"
    assert "session-1" not in manager._deleting_session_ids


@pytest.mark.runtime
@pytest.mark.no_network
async def test_history_is_postgres_only(session_manager):
    manager, _, _, _ = session_manager

    assert await manager.get_session_history_readonly("session-1") == []
