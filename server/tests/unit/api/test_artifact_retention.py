from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from api.app import create_app
from runtime.api_port import ApplicationRuntimePort


@pytest.mark.no_network
@pytest.mark.parametrize("tail,target,args,status", [
    ("", "list_project_artifacts", (), 200),
    ("/artifact-1", "get_project_artifact", ("artifact-1",), 404),
    ("/artifact-1/revisions/1", "get_project_artifact_revision", ("artifact-1", 1), 404),
])
async def test_artifact_api_honors_session_filter_without_resuming_deleted_session(tail, target, args, status):
    method = AsyncMock(return_value=[] if target == "list_project_artifacts" else None)
    sessions = SimpleNamespace(user_name="ada", get_or_resume_session=AsyncMock())
    store = SimpleNamespace(**{target: method})
    port = ApplicationRuntimePort(SimpleNamespace(sessions=sessions, resources=SimpleNamespace(knowledge_store=store)))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=create_app(port), raise_app_exceptions=False),
        base_url="http://test", headers={"X-User-Name": "ada"},
    ) as client:
        response = await client.get(f"/v1/projects/p1/artifacts{tail}?session_id=deleted-session")
    assert response.status_code == status
    expected = dict(user_name="ada", project_id="p1", session_id="deleted-session")
    if target == "list_project_artifacts":
        expected["limit"] = 50
    method.assert_awaited_once_with(*args, **expected)
    sessions.get_or_resume_session.assert_not_awaited()
