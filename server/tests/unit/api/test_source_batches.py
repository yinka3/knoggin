import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from api.app import create_app
from common.schema.public import BatchSourceRequest
from runtime.api_port import ApplicationRuntimePort

pytestmark = [pytest.mark.unit, pytest.mark.no_network]


def make_port():
    document = dict(document_id="d1", project_id="p1", original_name="notes.md", relative_path="notes.md",
                    extension=".md", size_bytes=1, content_hash="hash", status="queued", error_message="SECRET")
    link = dict(link_id="l1", project_id="p1", url="https://example.com", internal="SECRET",
                created_at=datetime.now(timezone.utc), updated_at=datetime.now(timezone.utc))
    service = SimpleNamespace(submit_document=AsyncMock(return_value=document), save_web_link=AsyncMock(return_value=link))
    projects = SimpleNamespace(acquire_project_for_session=AsyncMock(return_value=SimpleNamespace(document_service=service)),
                               release_project_for_session=AsyncMock())
    port = ApplicationRuntimePort(SimpleNamespace(sessions=SimpleNamespace(user_name="ada"), projects=projects))
    return port, projects, service


DOC = {"source_type": "document", "original_name": "notes.md", "content_base64": "YQ=="}
LINK = {"source_type": "web_link", "url": "https://example.com"}


async def test_mixed_batch_partial_failures_preserve_order_and_exact_lease():
    port, projects, service = make_port()
    service.save_web_link.side_effect = [RuntimeError("SECRET database failure"), service.save_web_link.return_value]
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(port), raise_app_exceptions=False),
                                 base_url="http://test", headers={"X-User-Name": "ada"}) as client:
        response = await client.post("/v1/projects/p1/sources/batch", json={"items": [DOC, LINK, {**DOC, "content_base64": "invalid"}, LINK]})
    assert response.status_code == 200
    results = response.json()["results"]
    assert [r["status"] for r in results] == ["accepted_document", "failed", "failed", "accepted_web_link"]
    assert [r["index"] for r in results] == [0, 1, 2, 3]
    assert results[1]["error"]["code"] == "internal_error"
    assert results[2]["error"]["code"] == "invalid_request"
    assert results[1]["error"]["request_id"] == response.headers["x-request-id"]
    assert "SECRET" not in response.text
    service.submit_document.assert_awaited_once_with(content=b"a", original_name="notes.md", relative_path=None)
    service.save_web_link.assert_awaited()
    lease = projects.acquire_project_for_session.call_args.args
    projects.release_project_for_session.assert_awaited_once_with(*lease)


async def test_cancelled_batch_releases_lease_and_does_not_continue():
    port, projects, service = make_port()
    service.submit_document.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await port.admit_sources_batch(user_name="ada", project_id="p1", request=BatchSourceRequest(items=[DOC, LINK]))
    service.save_web_link.assert_not_awaited()
    projects.release_project_for_session.assert_awaited_once_with(*projects.acquire_project_for_session.call_args.args)


async def test_web_link_validation_is_safe_per_item():
    from common.schema.document import SavedWebLink
    port, projects, service = make_port()

    async def save(**kwargs):
        return SavedWebLink(link_id="l1", project_id="p1", created_at=datetime.now(timezone.utc),
                            updated_at=datetime.now(timezone.utc), **kwargs).model_dump()

    service.save_web_link.side_effect = save
    result = await port.admit_sources_batch(user_name="ada", project_id="p1", request=BatchSourceRequest(items=[
        {**LINK, "url": "file:///SECRET"}, LINK,
    ]))
    assert result.results[0].error.code == "invalid_request"
    assert "SECRET" not in result.model_dump_json()
    assert result.results[1].status == "accepted_web_link"


async def test_batch_denies_other_user_before_project_access():
    port, projects, service = make_port()
    with pytest.raises(PermissionError):
        await port.admit_sources_batch(user_name="other", project_id="p1", request=BatchSourceRequest(items=[LINK]))
    projects.acquire_project_for_session.assert_not_awaited()


@pytest.mark.parametrize("items", [[], [LINK] * 21, [{"source_type": "unknown"}]])
async def test_invalid_batch_shape_rejected_before_project_access(items):
    port, projects, service = make_port()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(port), raise_app_exceptions=False),
                                 base_url="http://test", headers={"X-User-Name": "ada"}) as client:
        assert (await client.post("/v1/projects/p1/sources/batch", json={"items": items})).status_code == 422
    projects.acquire_project_for_session.assert_not_awaited()


async def test_aggregate_encoded_limit_rejected_before_writes(monkeypatch):
    from common.schema import public
    monkeypatch.setattr(public, "MAX_DOCUMENT_BASE64_LENGTH", 4)
    port, projects, service = make_port()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(port), raise_app_exceptions=False),
                                 base_url="http://test", headers={"X-User-Name": "ada"}) as client:
        assert (await client.post("/v1/projects/p1/sources/batch", json={"items": [DOC, DOC]})).status_code == 413
    projects.acquire_project_for_session.assert_not_awaited()


async def test_aggregate_decoded_limit_rejected_before_lease(monkeypatch):
    from runtime import api_port
    monkeypatch.setattr(api_port, "MAX_DOCUMENT_SIZE", 1)
    port, projects, service = make_port()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(port), raise_app_exceptions=False),
                                 base_url="http://test", headers={"X-User-Name": "ada"}) as client:
        assert (await client.post("/v1/projects/p1/sources/batch", json={"items": [DOC, DOC]})).status_code == 413
    projects.acquire_project_for_session.assert_not_awaited()


async def test_batch_transport_limit_counts_actual_body_before_parsing(monkeypatch):
    from api import upload_limits
    monkeypatch.setattr(upload_limits, "MAX_DOCUMENT_UPLOAD_BODY_BYTES", 20)
    port, projects, service = make_port()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(port), raise_app_exceptions=False),
                                 base_url="http://test", headers={"X-User-Name": "ada"}) as client:
        response = await client.post("/v1/projects/p1/sources/batch", json={"items": [LINK]}, headers={"Content-Length": "1"})
    assert response.status_code == 413
    projects.acquire_project_for_session.assert_not_awaited()
