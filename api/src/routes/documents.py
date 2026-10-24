"""Project document and saved-link routes."""

import asyncio
import base64
import json
from datetime import datetime
from uuid import UUID

from pydantic import ValidationError
from starlette.responses import JSONResponse
from starlette.routing import Route

from common.document_limits import MAX_DOCUMENT_BASE64_LENGTH, MAX_DOCUMENT_SIZE
from common.exceptions import SavedWebLinkAlreadyExistsError

from contracts.documents import (
    BatchDocument, BatchSources, OcrPreflight, PromoteSource, SaveWebLink, UpdateWebLink,
    UploadDocument,
)
from errors import handle_error
from project_lease import project_documents, project_runtime
from routes.project_settings import _error, _json, _project_error

DOCUMENT_FIELDS = {
    "document_id", "project_id", "original_name", "relative_path", "extension",
    "size_bytes", "content_hash", "extraction_mode", "extraction_strategy",
    "status", "current_snapshot_id", "chunk_count", "created_at", "updated_at",
    "indexed_at", "deleted_at", "next_index_retry_at", "index_attempt_count",
    "last_index_failure_kind",
}
CONTENT_FIELDS = DOCUMENT_FIELDS | {
    "document_name", "parse_snapshot_id", "chunk_index", "content", "start_line",
    "end_line", "total_lines", "truncated", "locator", "page_number",
}
LINK_FIELDS = {"link_id", "project_id", "url", "title", "summary", "created_at", "updated_at"}


def _public(row, fields):
    return {
        key: value.isoformat() if isinstance(value, datetime) else str(value) if isinstance(value, UUID) else value
        for key, value in row.items() if key in fields
    }


async def _project(request):
    return await _project_error(
        request.app.state.runtime, request.path_params["project_id"], write=True
    )


async def list_documents(request):
    if error := await _project(request):
        return error
    try:
        limit = int(request.query_params.get("limit", "100"))
    except ValueError:
        return _error("invalid_limit", "Invalid document limit", 422)
    if not 1 <= limit <= 1000:
        return _error("invalid_limit", "Invalid document limit", 422)
    async with project_documents(request.app.state.runtime, request.path_params["project_id"]) as service:
        rows = await service.list_documents(limit=limit)
    return JSONResponse([_public(row, DOCUMENT_FIELDS) for row in rows])


async def get_document(request):
    if error := await _project(request):
        return error
    async with project_documents(request.app.state.runtime, request.path_params["project_id"]) as service:
        try:
            row = await service.get_document_info(document_id=request.path_params["document_id"])
        except FileNotFoundError:
            return _error("document_not_found", "Document not found", 404)
    return JSONResponse(_public(row, DOCUMENT_FIELDS))


async def read_document(request):
    if error := await _project(request):
        return error
    try:
        start = int(request.query_params.get("start_line", "1"))
        end_value = request.query_params.get("end_line")
        end = int(end_value) if end_value is not None else None
    except ValueError:
        return _error("invalid_lines", "Invalid line range", 422)
    if start < 1 or (end is not None and end < start):
        return _error("invalid_lines", "Invalid line range", 422)
    async with project_documents(request.app.state.runtime, request.path_params["project_id"]) as service:
        try:
            row = await service.read_document(
                document_id=request.path_params["document_id"], start_line=start, end_line=end
            )
        except FileNotFoundError:
            return _error("document_not_found", "Document not found", 404)
    return JSONResponse(_public(row, CONTENT_FIELDS))


async def upload_document(request):
    if error := await _project(request):
        return error
    body = await _json(request)
    if isinstance(body, dict) and isinstance(body.get("content_base64"), str):
        if len(body["content_base64"]) > MAX_DOCUMENT_BASE64_LENGTH:
            return _error("payload_too_large", "Document is too large", 413)
    try:
        upload = UploadDocument.model_validate(body)
        content = base64.b64decode(upload.content_base64, validate=True)
    except (ValidationError, ValueError):
        return _error("invalid_document", "Invalid document upload", 422)
    if len(content) > MAX_DOCUMENT_SIZE:
        return _error("payload_too_large", "Document is too large", 413)
    async with project_documents(request.app.state.runtime, request.path_params["project_id"]) as service:
        try:
            row = await service.submit_document(
                content=content, original_name=upload.original_name,
                relative_path=upload.relative_path, extraction_mode=upload.extraction_mode,
                extraction_strategy=upload.extraction_strategy,
            )
        except ValueError:
            return _error("invalid_document", "Invalid document upload", 422)
    return JSONResponse(_public(row, DOCUMENT_FIELDS), status_code=201)


async def inspect_ocr(request):
    body = await _json(request)
    if isinstance(body, dict) and isinstance(body.get("content_base64"), str):
        if len(body["content_base64"]) > MAX_DOCUMENT_BASE64_LENGTH:
            return _error("payload_too_large", "Document is too large", 413)
    try:
        preflight = OcrPreflight.model_validate(body)
        if not preflight.original_name.lower().endswith(".pdf"):
            raise ValueError("PDF required")
        content = base64.b64decode(preflight.content_base64, validate=True)
    except (ValidationError, ValueError):
        return _error("invalid_document", "Invalid PDF preflight request", 422)
    if len(content) > MAX_DOCUMENT_SIZE:
        return _error("payload_too_large", "Document is too large", 413)
    from core.knowledge.documents.extraction import inspect_pdf_ocr_need

    result = await asyncio.to_thread(inspect_pdf_ocr_need, content)
    return JSONResponse(result.__dict__)


async def admit_batch(request):
    if error := await _project(request):
        return error
    payload = await _json(request)
    if isinstance(payload, dict) and isinstance(payload.get("items"), list):
        encoded_bytes = sum(
            len(item.get("content_base64", ""))
            for item in payload["items"]
            if isinstance(item, dict) and isinstance(item.get("content_base64"), str)
        )
        if encoded_bytes > MAX_DOCUMENT_BASE64_LENGTH:
            return _error("payload_too_large", "Batch is too large", 413)
    try:
        batch = BatchSources.model_validate(payload)
    except ValidationError:
        return _error("invalid_batch", "Invalid source batch", 422)
    decoded = {}
    failures = set()
    total_bytes = 0
    for index, item in enumerate(batch.items):
        if isinstance(item, BatchDocument):
            try:
                content = base64.b64decode(item.content_base64, validate=True)
            except ValueError:
                failures.add(index)
                continue
            total_bytes += len(content)
            if total_bytes > MAX_DOCUMENT_SIZE:
                return _error("payload_too_large", "Batch is too large", 413)
            decoded[index] = content
    results = []
    async with project_documents(request.app.state.runtime, request.path_params["project_id"]) as service:
        for index, item in enumerate(batch.items):
            try:
                if index in failures:
                    raise ValueError("Invalid document")
                if isinstance(item, BatchDocument):
                    row = await service.submit_document(
                        content=decoded[index], original_name=item.original_name,
                        relative_path=item.relative_path,
                        extraction_mode=item.extraction_mode,
                        extraction_strategy=item.extraction_strategy,
                    )
                    results.append({"index": index, "status": "accepted_document",
                                    "document": _public(row, DOCUMENT_FIELDS)})
                else:
                    row = await service.save_web_link(
                        url=item.url, title=item.title, summary=item.summary,
                    )
                    results.append({"index": index, "status": "accepted_web_link",
                                    "link": _public(row, LINK_FIELDS)})
            except Exception as exc:
                public_error = await handle_error(request, exc)
                results.append({"index": index, "status": "failed",
                                "error": json.loads(public_error.body)["error"]})
    return JSONResponse({"results": results})


async def reindex_document(request):
    if error := await _project(request):
        return error
    async with project_documents(request.app.state.runtime, request.path_params["project_id"]) as service:
        try:
            row = await service.reindex_document(document_id=request.path_params["document_id"])
        except FileNotFoundError:
            return _error("document_not_found", "Document not found", 404)
    return JSONResponse(_public(row, DOCUMENT_FIELDS))


async def delete_document(request):
    if error := await _project(request):
        return error
    async with project_documents(request.app.state.runtime, request.path_params["project_id"]) as service:
        try:
            row = await service.delete_document(document_id=request.path_params["document_id"])
        except FileNotFoundError:
            return _error("document_not_found", "Document not found", 404)
    return JSONResponse(_public(row, {"document_id", "deleted"}))


async def list_links(request):
    if error := await _project(request):
        return error
    try:
        limit = int(request.query_params.get("limit", "50"))
    except ValueError:
        return _error("invalid_limit", "Invalid link limit", 422)
    if not 1 <= limit <= 500:
        return _error("invalid_limit", "Invalid link limit", 422)
    async with project_documents(request.app.state.runtime, request.path_params["project_id"]) as service:
        rows = await service.list_saved_web_links(limit=limit)
    return JSONResponse([_public(row, LINK_FIELDS) for row in rows])


async def save_link(request):
    if error := await _project(request):
        return error
    try:
        body = SaveWebLink.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_link", "Invalid web link", 422)
    async with project_documents(request.app.state.runtime, request.path_params["project_id"]) as service:
        try:
            row = await service.save_web_link(**body.model_dump())
        except SavedWebLinkAlreadyExistsError:
            return _error("link_exists", "Web link already saved", 409)
        except (ValidationError, ValueError):
            return _error("invalid_link", "Invalid web link", 422)
    return JSONResponse(_public(row, LINK_FIELDS), status_code=201)


async def update_link(request):
    if error := await _project(request):
        return error
    try:
        body = UpdateWebLink.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_link", "Invalid web link", 422)
    if not body.model_fields_set:
        return _error("invalid_link", "Invalid web link", 422)
    async with project_documents(request.app.state.runtime, request.path_params["project_id"]) as service:
        try:
            row = await service.update_saved_web_link(
                link_id=request.path_params["link_id"], **body.model_dump(exclude_unset=True)
            )
        except FileNotFoundError:
            return _error("link_not_found", "Web link not found", 404)
        except ValueError:
            return _error("invalid_link", "Invalid web link", 422)
    return JSONResponse(_public(row, LINK_FIELDS))


async def delete_link(request):
    if error := await _project(request):
        return error
    async with project_documents(request.app.state.runtime, request.path_params["project_id"]) as service:
        try:
            row = await service.delete_saved_web_link(link_id=request.path_params["link_id"])
        except FileNotFoundError:
            return _error("link_not_found", "Web link not found", 404)
    return JSONResponse(_public(row, {"link_id", "deleted"}))


async def promote_source(request):
    if error := await _project(request):
        return error
    try:
        body = PromoteSource.model_validate(await _json(request))
    except ValidationError:
        return _error("invalid_source", "Invalid source", 422)
    runtime = request.app.state.runtime
    project_id = request.path_params["project_id"]
    async with project_runtime(runtime, project_id) as project:
        source = await runtime.resources.knowledge_store.get_source_reference(
            body.source_ref_id, user_name=runtime.sessions.user_name,
            project_id=project_id, session_id=body.session_id,
        )
        if source is None:
            return _error("source_not_found", "Source not found", 404)
        try:
            row = await project.document_service.promote_source(
                source, title=body.title, summary=body.summary,
            )
        except SavedWebLinkAlreadyExistsError:
            return _error("link_exists", "Web link already saved", 409)
        except (ValidationError, ValueError):
            return _error("invalid_source", "Invalid source", 422)
    return JSONResponse(_public(row, LINK_FIELDS), status_code=201)


routes = [
    Route("/v1/documents/ocr-preflight", inspect_ocr, methods=["POST"]),
    Route("/v1/projects/{project_id}/sources/batch", admit_batch, methods=["POST"]),
    Route("/v1/projects/{project_id}/documents", list_documents, methods=["GET"]),
    Route("/v1/projects/{project_id}/documents", upload_document, methods=["POST"]),
    Route("/v1/projects/{project_id}/documents/{document_id}", get_document, methods=["GET"]),
    Route("/v1/projects/{project_id}/documents/{document_id}", delete_document, methods=["DELETE"]),
    Route("/v1/projects/{project_id}/documents/{document_id}/content", read_document, methods=["GET"]),
    Route("/v1/projects/{project_id}/documents/{document_id}/reindex", reindex_document, methods=["POST"]),
    Route("/v1/projects/{project_id}/links", list_links, methods=["GET"]),
    Route("/v1/projects/{project_id}/links", save_link, methods=["POST"]),
    Route("/v1/projects/{project_id}/links/{link_id}", update_link, methods=["PATCH"]),
    Route("/v1/projects/{project_id}/links/{link_id}", delete_link, methods=["DELETE"]),
    Route("/v1/projects/{project_id}/sources/promote", promote_source, methods=["POST"]),
]
