import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from api.app import create_app
from common.exceptions import (
    EpisodeEditConflictError,
    StorageWriteError,
    WorkspaceConflictError,
)
from common.schema.episode.models import Episode, MessageEpisode
from common.schema.public import UpdateEpisodeRequest
from common.schema.settings import RootConfig
from core.project.project_manager import ProjectManager
from runtime.api_port import ApplicationRuntimePort

pytestmark = [pytest.mark.unit, pytest.mark.no_network]

INITIAL = datetime(2026, 1, 1, tzinfo=timezone.utc)
EDITED = datetime(2026, 1, 2, tzinfo=timezone.utc)
PATH = "/v1/projects/p1/episodes/e1"
BODY = dict(
    summary=" Edited narrative ", new_developments=[" New development "],
    updates=[], unresolved=[" Remaining question "],
    expected_updated_at=INITIAL.isoformat(),
)


def make_port():
    episode = Episode(
        episode_id="e1", project_id="p1", summary="Original",
        messages=[MessageEpisode(message_id=1, session_id="deleted-session", message_position=0)],
        created_at=INITIAL, updated_at=INITIAL, embedding=[0.1] * 1024,
        generator_metadata={"private": "SECRET"},
    )
    store = SimpleNamespace(
        get_project_episode=AsyncMock(return_value=episode),
        edit_episode=AsyncMock(return_value=EDITED),
    )
    projects = SimpleNamespace(
        get_project=AsyncMock(return_value=dict(id="p1", status="active", allowed_projects=["p2"])),
        acquire_project_for_session=AsyncMock(return_value=object()),
        release_project_for_session=AsyncMock(),
    )
    sessions = SimpleNamespace(user_name="ada", get_or_resume_session=AsyncMock())
    runtime = SimpleNamespace(
        sessions=sessions, projects=projects, resources=SimpleNamespace(knowledge_store=store),
        config_manager=SimpleNamespace(config=RootConfig()),
    )
    return ApplicationRuntimePort(runtime), projects, store


async def call(port, method="PATCH", body=BODY, user="ada", path=PATH):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=create_app(port), raise_app_exceptions=False),
        base_url="http://test", headers={"X-User-Name": user},
    ) as client:
        return await client.request(method, path, **({"json": body} if method == "PATCH" else {}))


async def test_read_projects_only_narrative_and_revision_without_resuming_session():
    port, projects, store = make_port()
    response = await call(port, "GET")
    assert response.status_code == 200
    assert set(response.json()) == {
        "episode_id", "project_id", "summary", "new_developments", "updates",
        "unresolved", "user_modified", "created_at", "updated_at",
    }
    assert "SECRET" not in response.text
    assert "embedding" not in response.text
    store.get_project_episode.assert_awaited_once_with(
        "e1", user_name="ada", project_id="p1", visible_project_ids=["p1"],
    )
    projects.acquire_project_for_session.assert_not_awaited()
    port.runtime.sessions.get_or_resume_session.assert_not_awaited()


@pytest.mark.parametrize("revision", [INITIAL.isoformat(), "2025-12-31T19:00:00-05:00"])
async def test_edit_delegates_normalized_narrative_with_exact_project_lease(revision):
    port, projects, store = make_port()
    response = await call(port, body={**BODY, "expected_updated_at": revision})
    assert response.status_code == 200
    assert response.json() == dict(
        episode_id="e1", project_id="p1", user_modified=True, updated_at="2026-01-02T00:00:00Z",
    )
    store.edit_episode.assert_awaited_once_with(
        episode_id="e1", user_name="ada", project_id="p1", summary="Edited narrative",
        new_developments=["New development"], updates=[], unresolved=["Remaining question"],
        expected_updated_at=INITIAL,
    )
    project_id, lease_id = projects.acquire_project_for_session.await_args.args
    assert project_id == "p1" and lease_id.startswith("api-episode-edit:")
    projects.release_project_for_session.assert_awaited_once_with(project_id, lease_id)
    port.runtime.sessions.get_or_resume_session.assert_not_awaited()


@pytest.mark.parametrize("body", [
    {}, {**BODY, "summary": "   "}, {**BODY, "new_developments": [" "]},
    {**BODY, "updates": [1]}, {**BODY, "unresolved": None},
    {**BODY, "expected_updated_at": "2026-01-01T00:00:00"},
    {key: value for key, value in BODY.items() if key != "expected_updated_at"},
    {**BODY, "expected_updated_at": "not-a-date"},
    {**BODY, "summary": "x" * 20_001}, {**BODY, "updates": ["x"] * 101},
    {**BODY, "summary": "x" * 20_000, "new_developments": ["x"]},
    {**BODY, "embedding": [1]}, {**BODY, "user_modified": False},
    {**BODY, "messages": []}, {**BODY, "project_id": "p2"},
])
async def test_invalid_edit_does_not_reach_project_or_storage(body):
    port, projects, store = make_port()
    response = await call(port, body=body)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"
    projects.get_project.assert_not_awaited()
    store.edit_episode.assert_not_awaited()


async def test_configured_narrative_limit_is_checked_before_lease_or_embedding():
    port, projects, store = make_port()
    port.runtime.config_manager.config.developer_settings.jobs.episode.max_narrative_chars = 500
    response = await call(port, body={**BODY, "summary": "x" * 501})
    assert response.status_code == 422
    projects.acquire_project_for_session.assert_not_awaited()
    store.edit_episode.assert_not_awaited()


@pytest.mark.parametrize("method", ["GET", "PATCH"])
async def test_foreign_user_does_not_reach_any_owner(method):
    port, projects, store = make_port()
    response = await call(port, method, user="bob")
    assert response.status_code == 403
    projects.get_project.assert_not_awaited()
    store.get_project_episode.assert_not_awaited()
    store.edit_episode.assert_not_awaited()


@pytest.mark.parametrize("method", ["GET", "PATCH"])
@pytest.mark.parametrize("missing", ["project", "episode", "foreign_episode"])
async def test_missing_or_foreign_target_never_edits(method, missing):
    port, projects, store = make_port()
    if missing == "project":
        projects.get_project.return_value = None
    elif missing == "episode":
        store.get_project_episode.return_value = None
    else:
        store.get_project_episode.return_value.project_id = "p2"
    response = await call(port, method)
    assert response.status_code == 404
    store.edit_episode.assert_not_awaited()
    if method == "PATCH" and missing != "project":
        projects.release_project_for_session.assert_awaited_once_with(
            *projects.acquire_project_for_session.await_args.args,
        )


async def test_archived_project_is_readable_but_not_editable():
    port, projects, store = make_port()
    projects.get_project.return_value["status"] = "archived"
    assert (await call(port, "GET")).status_code == 200
    assert (await call(port)).status_code == 403
    projects.acquire_project_for_session.assert_not_awaited()
    store.edit_episode.assert_not_awaited()


@pytest.mark.parametrize("race", [False, True])
async def test_stale_revision_returns_safe_conflict_and_releases_lease(race):
    port, projects, store = make_port()
    if race:
        store.edit_episode.side_effect = EpisodeEditConflictError()
    else:
        store.get_project_episode.return_value.updated_at = EDITED
    response = await call(port)
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "episode_conflict"
    assert response.json()["error"]["retryable"] is False
    if not race:
        store.edit_episode.assert_not_awaited()
    projects.release_project_for_session.assert_awaited_once_with(
        *projects.acquire_project_for_session.await_args.args,
    )


@pytest.mark.parametrize("error,status", [
    (RuntimeError("SECRET embedding failure"), 500),
    (StorageWriteError("SECRET SQL"), 503),
])
async def test_failed_edit_releases_lease_and_never_exposes_raw_error(error, status):
    port, projects, store = make_port()
    store.edit_episode.side_effect = error
    response = await call(port)
    assert response.status_code == status
    assert "SECRET" not in response.text
    projects.release_project_for_session.assert_awaited_once_with(
        *projects.acquire_project_for_session.await_args.args,
    )


@pytest.mark.parametrize("worker_fails", [False, True])
async def test_cancelled_edit_settles_owned_worker_before_releasing_lease(worker_fails):
    port, projects, store = make_port()
    started, finish = asyncio.Event(), asyncio.Event()

    async def edit(**_kwargs):
        started.set()
        await finish.wait()
        if worker_fails:
            raise RuntimeError("SECRET worker failure")
        return EDITED

    store.edit_episode.side_effect = edit
    task = asyncio.create_task(port.update_episode(
        user_name="ada", project_id="p1", episode_id="e1",
        request=UpdateEpisodeRequest.model_validate(BODY),
    ))
    await asyncio.wait_for(started.wait(), timeout=2)
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    projects.release_project_for_session.assert_not_awaited()
    finish.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=2)
    projects.release_project_for_session.assert_awaited_once_with(
        *projects.acquire_project_for_session.await_args.args,
    )


async def test_edit_lease_blocks_real_project_archive_and_delete_until_worker_finishes():
    port, projects, store = make_port()
    manager = object.__new__(ProjectManager)
    manager.user_name = "ada"
    manager._closed = False
    manager.get_project = projects.get_project
    manager.get_readable_project_ids = AsyncMock(return_value=["p1", "p2"])
    manager.active_projects = {}
    manager._project_leases = {}
    manager.maintenance_service = SimpleNamespace(lock=asyncio.Lock())
    state = SimpleNamespace(shutdown=AsyncMock())
    manager.project_factory = SimpleNamespace(create=AsyncMock(return_value=state))
    port.runtime.projects = manager
    started, finish = asyncio.Event(), asyncio.Event()

    async def edit(**_kwargs):
        started.set()
        await finish.wait()
        return EDITED

    store.edit_episode.side_effect = edit
    task = asyncio.create_task(port.update_episode(
        user_name="ada", project_id="p1", episode_id="e1",
        request=UpdateEpisodeRequest.model_validate(BODY),
    ))
    try:
        await asyncio.wait_for(started.wait(), timeout=2)
        lease = set(manager._project_leases["p1"])
        assert len(lease) == 1
        for operation in (manager.archive_project, manager.delete_project):
            with pytest.raises(WorkspaceConflictError):
                await operation("p1")
            assert manager._project_leases["p1"] == lease
            state.shutdown.assert_not_awaited()
    finally:
        finish.set()
        await asyncio.wait_for(task, timeout=2)
    assert manager._project_leases == {}
    assert manager.active_projects == {}
    state.shutdown.assert_awaited_once()


async def test_repeated_cancellation_during_release_settles_exact_cleanup():
    port, projects, _ = make_port()
    started, finish = asyncio.Event(), asyncio.Event()

    async def release(*_args):
        started.set()
        await finish.wait()

    projects.release_project_for_session.side_effect = release
    task = asyncio.create_task(port.update_episode(
        user_name="ada", project_id="p1", episode_id="e1",
        request=UpdateEpisodeRequest.model_validate(BODY),
    ))
    await asyncio.wait_for(started.wait(), timeout=2)
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    finish.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=2)
    projects.release_project_for_session.assert_awaited_once_with(
        *projects.acquire_project_for_session.await_args.args,
    )


async def test_cleanup_failure_after_commit_is_safe_not_a_false_success():
    port, projects, store = make_port()
    projects.release_project_for_session.side_effect = StorageWriteError("SECRET cleanup")
    response = await call(port)
    assert response.status_code == 503
    assert "SECRET" not in response.text
    store.edit_episode.assert_awaited_once()
    projects.release_project_for_session.assert_awaited_once()


def test_openapi_exposes_strict_typed_episode_contracts():
    port, _, _ = make_port()
    schema = create_app(port).openapi()
    path = schema["paths"]["/v1/projects/{project_id}/episodes/{episode_id}"]
    assert path["get"]["responses"]["200"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/EpisodeResponse",
    }
    assert path["patch"]["responses"]["200"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/EpisodeEditedResponse",
    }
    request = schema["components"]["schemas"]["UpdateEpisodeRequest"]
    assert request["additionalProperties"] is False
    assert set(request["required"]) == set(BODY)
