from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from common.scoping import IDENTITY_SCOPE
from core.project.project_manager import ProjectManager, ProjectStatus
from infrastructure.redis_client import RedisKeys
from tests.fixtures.factories import make_domain_config
from tests.fixtures.fakes import FakeRedis


def project_row(
    project_id="project-1",
    *,
    status=ProjectStatus.ACTIVE.value,
    allowed_projects=None,
    name="Research",
):
    return {
        "project_id": project_id,
        "user_name": "ada",
        "name": name,
        "description": None,
        "access_mode": "open",
        "status": status,
        "topic_config": {},
        "allowed_projects": allowed_projects,
        "session_count": 0,
        "created_at": None,
        "updated_at": None,
        "archived_at": None,
        "deleted_at": None,
        "last_activity_at": None,
    }


class RecordingPostgres:
    def __init__(self, fetch_results=None, execute_results=None):
        self.fetch_results = list(fetch_results or [])
        self.execute_results = list(execute_results or [])
        self.calls = []
        self.transaction_enters = 0
        self.transaction_exits = 0

    @asynccontextmanager
    async def transaction(self):
        self.transaction_enters += 1
        try:
            yield _RecordingCursor(self)
        finally:
            self.transaction_exits += 1

    async def fetch_all(self, query, params=None):
        self.calls.append(("fetch_all", query, params))
        return self.fetch_results.pop(0) if self.fetch_results else []

    async def execute(self, query, params=None):
        self.calls.append(("execute", query, params))
        return self.execute_results.pop(0) if self.execute_results else 1


class _RecordingCursor:
    def __init__(self, postgres):
        self.postgres = postgres

    async def execute(self, query, params=None):
        return await self.postgres.execute(query, params)


class RecordingProjectDeletionWriter:
    def __init__(self):
        self.calls = []

    async def delete_project(self, *, user_name, project_id):
        self.calls.append((user_name, project_id))
        return {"projects": 1}


def make_manager(postgres):
    return ProjectManager(
        resources=SimpleNamespace(postgres=postgres),
        user_name="ada",
    )


@pytest.mark.runtime
@pytest.mark.no_network
async def test_project_runtime_releases_its_lease_when_session_work_fails(
    monkeypatch,
):
    manager = make_manager(RecordingPostgres())
    project_state = SimpleNamespace(project_id="project-1")
    calls = []

    async def acquire(project_id, session_id):
        calls.append(("acquire", project_id, session_id))
        return project_state

    async def release(project_id):
        calls.append(("release", project_id))

    monkeypatch.setattr(manager, "acquire_project_for_session", acquire)
    monkeypatch.setattr(manager, "release_project", release)

    with pytest.raises(RuntimeError, match="session startup failed"):
        async with manager.project_runtime("project-1", "session-1") as state:
            assert state is project_state
            raise RuntimeError("session startup failed")

    assert calls == [
        ("acquire", "project-1", "session-1"),
        ("release", "project-1"),
    ]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_create_project_persists_metadata_and_default_topics(monkeypatch):
    postgres = RecordingPostgres(
        [
            [project_row()],
        ]
    )
    manager = make_manager(postgres)
    monkeypatch.setattr(
        "core.project.project_manager.uuid.uuid4",
        lambda: "project-1",
    )

    result = await manager.create_project(
        "  Research  ",
        domain_config=make_domain_config(version=0),
    )

    insert = next(
        call
        for call in postgres.calls
        if call[0] == "execute" and "INSERT INTO public.projects" in call[1]
    )
    assert insert[2]["name"] == "Research"
    assert insert[2]["status"] == ProjectStatus.ACTIVE.value
    assert insert[2]["topic_config"]
    assert insert[2]["domain_config"]
    assert result["id"] == "project-1"
    assert "topic_config" not in result
    assert postgres.transaction_enters == 1
    assert postgres.transaction_exits == 1


@pytest.mark.runtime
@pytest.mark.no_network
async def test_create_project_rejects_empty_name_without_db_access():
    postgres = RecordingPostgres()

    with pytest.raises(ValueError, match="non-empty project name"):
        await make_manager(postgres).create_project(
            "   ",
            domain_config=make_domain_config(version=0),
        )

    assert postgres.calls == []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_create_project_requires_a_complete_identity_domain():
    postgres = RecordingPostgres()
    manager = make_manager(postgres)

    with pytest.raises(ValueError, match="at least one topic"):
        await manager.create_project("Research", domain_config={})

    with pytest.raises(ValueError, match="active 'Identity' topic"):
        await manager.create_project(
            "Research",
            domain_config={
                "topics": {"General": {"active": True}},
                "entity_types": {
                    "Concept": {"topic": "General", "labels": ["concept"]}
                },
            },
        )

    assert postgres.calls == []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_list_projects_normalizes_postgres_rows():
    postgres = RecordingPostgres(
        [[project_row("project-1"), project_row("project-2", name="Writing")]]
    )

    projects = await make_manager(postgres).list_projects()

    assert [project["id"] for project in projects] == [
        "project-1",
        "project-2",
    ]
    assert all("topic_config" not in project for project in projects)
    assert all(project["allowed_projects"] == [] for project in projects)


@pytest.mark.runtime
@pytest.mark.no_network
async def test_readable_scope_includes_identity_project_and_valid_allowed_projects():
    postgres = RecordingPostgres(
        [
            [project_row(allowed_projects=["project-2", "missing"])],
            [{"project_id": "project-2"}],
        ]
    )

    readable = await make_manager(postgres).get_readable_project_ids("project-1")

    assert readable == [IDENTITY_SCOPE, "project-1", "project-2"]
    assert "status IN ('active', 'archived')" in postgres.calls[1][1]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_update_project_rejects_scope_change_while_runtime_is_active():
    postgres = RecordingPostgres([[project_row()]])
    manager = make_manager(postgres)
    manager.active_projects["project-1"] = SimpleNamespace(
        active_runtime_sessions_count=1
    )

    with pytest.raises(RuntimeError, match="active runtime sessions"):
        await manager.update_project(
            "project-1",
            allowed_projects=["project-2"],
        )

    assert not any(call[0] == "execute" for call in postgres.calls)


@pytest.mark.runtime
@pytest.mark.no_network
async def test_update_project_replaces_read_scope_in_postgres():
    postgres = RecordingPostgres(
        [
            [project_row()],
            [{"project_id": "project-2", "status": "active"}],
            [project_row(allowed_projects=["project-2"])],
        ]
    )
    manager = make_manager(postgres)

    result = await manager.update_project(
        "project-1",
        name="Updated",
        allowed_projects=["project-2"],
    )

    writes = [call for call in postgres.calls if call[0] == "execute"]
    assert any("DELETE FROM public.project_read_scopes" in call[1] for call in writes)
    assert any("INSERT INTO public.project_read_scopes" in call[1] for call in writes)
    assert any("UPDATE public.projects SET name" in call[1] for call in writes)
    assert result["allowed_projects"] == ["project-2"]
    assert postgres.transaction_enters == 1
    assert postgres.transaction_exits == 1


@pytest.mark.runtime
@pytest.mark.no_network
async def test_archive_project_refuses_active_runtime_sessions():
    postgres = RecordingPostgres([[project_row()]])
    manager = make_manager(postgres)
    manager.active_projects["project-1"] = SimpleNamespace(
        active_runtime_sessions_count=1
    )

    with pytest.raises(RuntimeError, match="cannot be archived"):
        await manager.archive_project("project-1")


@pytest.mark.runtime
@pytest.mark.no_network
async def test_archive_and_reactivate_project_update_durable_status():
    postgres = RecordingPostgres(
        [
            [project_row()],
            [project_row(status=ProjectStatus.ARCHIVED.value)],
            [project_row(status=ProjectStatus.ARCHIVED.value)],
            [project_row(status=ProjectStatus.ACTIVE.value)],
        ]
    )
    manager = make_manager(postgres)

    archived = await manager.archive_project("project-1")
    reactivated = await manager.reactivate_project("project-1")

    statuses = [
        call[2]["status"]
        for call in postgres.calls
        if call[0] == "execute" and "status = %(status)s" in call[1]
    ]
    assert statuses == [
        ProjectStatus.ARCHIVED.value,
        ProjectStatus.ACTIVE.value,
    ]
    assert archived["status"] == ProjectStatus.ARCHIVED.value
    assert reactivated["status"] == ProjectStatus.ACTIVE.value


@pytest.mark.runtime
@pytest.mark.no_network
async def test_delete_project_uses_cascading_postgres_boundary():
    postgres = RecordingPostgres([[project_row()]])
    manager = make_manager(postgres)
    writer = RecordingProjectDeletionWriter()
    manager._project_deletion_writer = writer

    deleted = await manager.delete_project("project-1")

    assert writer.calls == [("ada", "project-1")]
    assert deleted["status"] == ProjectStatus.DELETED.value


@pytest.mark.runtime
@pytest.mark.no_network
async def test_delete_project_clears_project_session_and_agent_redis_state():
    postgres = RecordingPostgres(
        [
            [project_row()],
            [{"session_id": "session-1"}],
            [{"agent_id": "agent-1"}],
        ]
    )
    redis = FakeRedis()
    manager = ProjectManager(
        resources=SimpleNamespace(postgres=postgres, redis=redis),
        user_name="ada",
    )
    writer = RecordingProjectDeletionWriter()
    manager._project_deletion_writer = writer

    await redis.sadd(RedisKeys.dirty_entities("ada", "project-1"), "2")
    await redis.set(
        RedisKeys.last_profile_update("ada", "project-1", 2),
        "done",
    )
    await redis.set(RedisKeys.buffer("ada", "session-1"), "buffered")
    await redis.set(
        RedisKeys.message_dedup("ada", "session-1", "digest"),
        "accepted:1",
    )
    await redis.set(
        RedisKeys.agent_directives("ada", "agent-1"),
        "directives",
    )
    await redis.set(
        RedisKeys.dirty_entities("ada", "project-2"),
        "preserve",
    )
    await redis.hset(RedisKeys.projects("ada"), "project-1", "metadata")
    await redis.hset(RedisKeys.sessions("ada"), "session-1", "metadata")
    await redis.hset(RedisKeys.agents("ada"), "agent-1", "metadata")
    await redis.set(RedisKeys.agents_default("ada"), "agent-1")

    await manager.delete_project("project-1")

    assert writer.calls == [("ada", "project-1")]
    assert await redis.get(RedisKeys.dirty_entities("ada", "project-1")) is None
    assert await redis.get(RedisKeys.last_profile_update("ada", "project-1", 2)) is None
    assert await redis.get(RedisKeys.buffer("ada", "session-1")) is None
    assert (
        await redis.get(RedisKeys.message_dedup("ada", "session-1", "digest")) is None
    )
    assert await redis.get(RedisKeys.agent_directives("ada", "agent-1")) is None
    assert await redis.get(RedisKeys.agents_default("ada")) is None
    assert await redis.hget(RedisKeys.projects("ada"), "project-1") is None
    assert await redis.hget(RedisKeys.sessions("ada"), "session-1") is None
    assert await redis.hget(RedisKeys.agents("ada"), "agent-1") is None
    assert await redis.get(RedisKeys.dirty_entities("ada", "project-2")) == ("preserve")


@pytest.mark.runtime
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("rows", "message"),
    [
        ([], "does not exist"),
        ([project_row(status=ProjectStatus.ARCHIVED.value)], "archived"),
    ],
)
async def test_acquire_project_requires_existing_active_project(rows, message):
    manager = make_manager(RecordingPostgres([rows]))

    with pytest.raises(ValueError, match=message):
        await manager.acquire_project_for_session("project-1", "session-1")
