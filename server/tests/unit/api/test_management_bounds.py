import json
from unittest.mock import AsyncMock

import httpx
import pytest

from api.app import create_app
from api.upload_limits import DocumentUploadLimitMiddleware
from common.schema.public import DocumentResponse, project_public_model


@pytest.mark.parametrize("declared", [None, b"1", b"99"])
async def test_upload_body_limit_rejects_before_parsing_even_with_wrong_length(declared):
    downstream = AsyncMock()
    app = DocumentUploadLimitMiddleware(downstream, max_body_bytes=4)
    receive = AsyncMock(side_effect=[dict(type="http.request", body=b"abc", more_body=True),
                                    dict(type="http.request", body=b"de", more_body=False)])
    send = AsyncMock()
    scope = dict(type="http", method="POST", path="/v1/projects/p/documents",
                 headers=[] if declared is None else [(b"content-length", declared)])
    await app(scope, receive, send)
    downstream.assert_not_called()
    assert send.await_args_list[0].args[0]["status"] == 413
    assert json.loads(send.await_args_list[1].args[0]["body"])["error"]["code"] == "payload_too_large"


async def test_upload_body_at_limit_is_replayed_exactly():
    seen = []

    async def downstream(scope, receive, send):
        seen.append(await receive())

    app = DocumentUploadLimitMiddleware(downstream, max_body_bytes=4)
    message = dict(type="http.request", body=b"abcd", more_body=False)
    await app(dict(type="http", method="POST", path="/v1/projects/p/documents", headers=[]),
              AsyncMock(return_value=message), AsyncMock())
    assert seen == [message]


async def test_oversized_encoded_field_returns_413_without_port_call(monkeypatch):
    from common.schema import public
    monkeypatch.setattr(public, "MAX_DOCUMENT_BASE64_LENGTH", 4)
    port = AsyncMock()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(port), raise_app_exceptions=False),
                                base_url="http://test") as client:
        response = await client.post("/v1/projects/p/documents",
                                     json=dict(original_name="notes.md", content_base64="AAAAA"))
    assert response.status_code == 413
    port.upload_document.assert_not_called()


def test_document_projection_allowlist_and_openapi():
    value = dict(document_id="d", project_id="p", original_name="n.md", relative_path="n.md",
                 extension=".md", size_bytes=1, content_hash="hash", status="failed",
                 error_message="private-token", internal_path="private-token")
    result = project_public_model(DocumentResponse, value)
    assert "private-token" not in result.model_dump_json()
    schema = create_app(AsyncMock()).openapi()
    for path, method in (("/v1/projects/{project_id}/documents", "get"),
                         ("/v1/projects/{project_id}/documents", "post"),
                         ("/v1/projects/{project_id}/saved-web-links", "get"),
                         ("/v1/sessions", "post")):
        response = schema["paths"][path][method]["responses"]
        assert next(iter(response.values()))["content"]["application/json"]["schema"]
