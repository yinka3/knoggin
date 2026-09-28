import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import httpx
import pytest

from api.app import create_app
from common.exceptions import WorkspaceConflictError
from core.project.project_manager import ProjectManager
from runtime.api_port import ApplicationRuntimePort

pytestmark = [pytest.mark.unit, pytest.mark.no_network]


@pytest.mark.parametrize("operation", ["update_project", "archive_project", "delete_project"])
async def test_real_project_manager_keeps_active_lease_guard(operation):
    manager = object.__new__(ProjectManager)
    manager.get_project = AsyncMock(return_value={"id": "p1", "status": "active"})
    manager.active_projects = {}
    manager._project_leases = {"p1": {"session-owner"}}
    manager.maintenance_service = SimpleNamespace(lock=asyncio.Lock())
    with pytest.raises(WorkspaceConflictError):
        await getattr(manager, operation)("p1", **({"allowed_projects": []} if operation == "update_project" else {}))
    assert manager._project_leases == {"p1": {"session-owner"}}


def make_port():
    row = dict(id="p1", name="Project", status="active", allowed_projects=[],
               internal_path="secret", domain_config={"secret": "hidden"})
    manager = SimpleNamespace(
        list_projects=AsyncMock(return_value=[row]), get_project=AsyncMock(return_value=row),
        update_project=AsyncMock(return_value=row),
        archive_project=AsyncMock(return_value={**row, "status": "archived"}),
        delete_project=AsyncMock(return_value={"id": "p1", "file_cleanup_status": "pending"}),
    )
    port = ApplicationRuntimePort(SimpleNamespace(sessions=SimpleNamespace(user_name="ada"), projects=manager))
    return port, manager


async def test_project_routes_delegate_and_project_public_fields():
    port, manager = make_port()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(port), raise_app_exceptions=False),
                                 base_url="http://test", headers={"X-User-Name": "ada"}) as client:
        listed = await client.get("/v1/projects")
        read = await client.get("/v1/projects/p1")
        updated = await client.patch("/v1/projects/p1", json={"name": " New ", "allowed_projects": []})
        archived = await client.post("/v1/projects/p1/archive")
        deleted = await client.delete("/v1/projects/p1")
    assert all(r.status_code == 200 for r in (listed, read, updated, archived, deleted))
    assert all("secret" not in r.text for r in (listed, read, updated, archived, deleted))
    assert archived.json()["status"] == "archived"
    assert deleted.json() == {"project_id": "p1", "deleted": True, "file_cleanup_status": "pending"}
    manager.update_project.assert_awaited_once_with("p1", name="New", allowed_projects=[])
    manager.get_project.assert_awaited_once_with("p1")  # deletion retries do not require surviving metadata
    manager.delete_project.assert_awaited_once_with("p1")


async def test_project_projection_accepts_native_database_uuid():
    port, manager = make_port()
    identity = UUID("11111111-1111-1111-1111-111111111111")
    manager.get_project.return_value = dict(id=identity, name="Project", status="active")
    assert (await port.get_project(user_name="ada", project_id=str(identity))).id == str(identity)


@pytest.mark.parametrize("method,path,target,body", [
    ("GET", "/v1/projects", "list_projects", None),
    ("GET", "/v1/projects/p1", "get_project", None),
    ("PATCH", "/v1/projects/p1", "update_project", {"description": "notes"}),
    ("POST", "/v1/projects/p1/archive", "archive_project", None),
    ("DELETE", "/v1/projects/p1", "delete_project", None),
])
async def test_project_routes_check_scope_before_manager_calls(method, path, target, body):
    port, manager = make_port()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(port), raise_app_exceptions=False),
                                 base_url="http://test", headers={"X-User-Name": "other"}) as client:
        response = await client.request(method, path, json=body)
    assert response.status_code == 403
    getattr(manager, target).assert_not_awaited()


@pytest.mark.parametrize("method,path,target", [
    ("GET", "/v1/projects/p1", "get_project"),
    ("PATCH", "/v1/projects/p1", "update_project"),
    ("POST", "/v1/projects/p1/archive", "archive_project"),
    ("DELETE", "/v1/projects/p1", "delete_project"),
])
async def test_project_missing_and_lifecycle_conflicts(method, path, target):
    port, manager = make_port()
    operation = getattr(manager, target)
    operation.return_value = None
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(port), raise_app_exceptions=False),
                                 base_url="http://test", headers={"X-User-Name": "ada"}) as client:
        response = await client.request(method, path, json={} if method == "PATCH" else None)
        assert response.status_code == 404
        operation.side_effect = WorkspaceConflictError("secret active lease details")
        response = await client.request(method, path, json={} if method == "PATCH" else None)
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "workspace_conflict"
    assert "secret" not in response.text


@pytest.mark.parametrize("body", [{"status": "deleted"}, {"name": " "},
                                      {"allowed_projects": ["p1", " p1 "]}, {"allowed_projects": [" "]}])
async def test_project_update_rejects_invalid_fields(body):
    port, manager = make_port()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(port), raise_app_exceptions=False),
                                 base_url="http://test", headers={"X-User-Name": "ada"}) as client:
        response = await client.patch("/v1/projects/p1", json=body)
    assert response.status_code == 422
    manager.update_project.assert_not_awaited()
