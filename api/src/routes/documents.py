"""Project document and saved-link routes."""

import asyncio
import base64
import json
from dataclasses import asdict
from datetime import datetime
from uuid import UUID

from pydantic import ValidationError
from starlette.responses import JSONResponse
from starlette.routing import Route

from common.document_limits import (
    MAX_DOCUMENT_BASE64_LENGTH, MAX_DOCUMENT_SIZE, MAX_DOCUMENT_UPLOAD_BODY_BYTES,
)
from common.exceptions import SavedWebLinkAlreadyExistsError
from common.schema.document import FolderUploadEntry
from core.knowledge.documents.scanning import normalize_relative_path

from contracts.documents import (
    BatchDocument, BatchSources, FolderIntake, OcrPreflight, PromoteSource, SaveWebLink, UpdateWebLink,
    UploadDocument,
)
from errors import handle_error
from project_lease import project_documents, project_runtime, readable_project_documents
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
    "end_line", "total_lines", "truncated", "locator", "page_number", "total_pages",
}
LINK_FIELDS = {"link_id", "project_id", "url", "title", "summary", "created_at", "updated_at"}
SEARCH_FIELDS = {
    "chunk_id", "document_id", "project_id", "original_name", "document_name",
    "relative_path", "extension", "content_hash", "snapshot_id", "chunk_index",
    "content", "language", "chunk_kind", "symbol_name", "page_number",
    "start_line", "end_line", "start_row", "end_row", "section_path",
    "start_paragraph", "end_paragraph", "locator", "score",
}


def _public(row, fields):
    return {
        key: value.isoformat() if isinstance(value, datetime) else str(value) if isinstance(value, UUID) else value
        for key, value in row.items() if key in fields
    }


async def _project(request, *, write=True):
    return await _project_error(
        request.app.state.runtime, request.path_params["project_id"], write=write
    )


def _read_cursor(value):
    if value is None:
        return None
    if len(value) > 256:
        raise ValueError("Invalid cursor")
    try:
        timestamp, identifier = json.loads(base64.b64decode(value, altchars=b"-_", validate=True))
        created_at = datetime.fromisoformat(timestamp)
        if created_at.tzinfo is None:
            raise ValueError("Cursor timestamp needs a time zone")
        return created_at, UUID(identifier)
    except (TypeError, ValueError, UnicodeDecodeError):
        raise ValueError("Invalid cursor") from None


def _next_cursor(row, *, time_field, id_field):
    value = row[time_field]
    timestamp = value.isoformat() if isinstance(value, datetime) else value
    payload = json.dumps([timestamp, str(row[id_field])]).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii")


def _page(rows, *, limit, fields, time_field, id_field):
    items = rows[:limit]
    return {
        "items": [_public(row, fields) for row in items],
        "next_cursor": _next_cursor(items[-1], time_field=time_field, id_field=id_field)
        if len(rows) > limit else None,
    }


async def list_documents(request):
    if error := await _project(request, write=False):
        return error
    try:
        limit = int(request.query_params.get("limit", "100"))
        before = _read_cursor(request.query_params.get("cursor"))
    except ValueError:
        return _error("invalid_document_list", "Invalid document list options", 422)
    if not 1 <= limit <= 100:
        return _error("invalid_document_list", "Invalid document list options", 422)
    path_prefix = request.query_params.get("path_prefix")
    if path_prefix is not None and len(path_prefix) > 2048:
        return _error("invalid_document_list", "Invalid document list options", 422)
    async with readable_project_documents(request.app.state.runtime, request.path_params["project_id"]) as service:
        try:
            rows = await service.list_documents(
                limit=limit + 1, path_prefix=path_prefix, before=before,
            )
        except ValueError:
            return _error("invalid_document_list", "Invalid document list options", 422)
    return JSONResponse(_page(
        rows, limit=limit, fields=DOCUMENT_FIELDS,
        time_field="created_at", id_field="document_id",
    ))


async def search_documents(request):
    if error := await _project(request, write=False):
        return error
    query = request.query_params.get("query", "").strip()
    try:
        limit = int(request.query_params.get("limit", "10"))
        document_id = request.query_params.get("document_id")
        if document_id is not None:
            document_id = str(UUID(document_id))
    except ValueError:
        return _error("invalid_document_search", "Invalid document search", 422)
    if not query or len(query) > 500 or not 1 <= limit <= 50:
        return _error("invalid_document_search", "Invalid document search", 422)
    relative_path = request.query_params.get("relative_path")
    path_prefix = request.query_params.get("path_prefix")
    if any(value is not None and len(value) > 2048 for value in (relative_path, path_prefix)):
        return _error("invalid_document_search", "Invalid document search", 422)
    async with readable_project_documents(request.app.state.runtime, request.path_params["project_id"]) as service:
        try:
            rows = await service.search(
                query, n_results=limit, document_filter=document_id,
                relative_path=relative_path, path_prefix=path_prefix,
            )
        except ValueError:
            return _error("invalid_document_search", "Invalid document search", 422)
    return JSONResponse({"items": [_public(row, SEARCH_FIELDS) for row in rows]})


async def get_document(request):
    if error := await _project(request, write=False):
        return error
    async with readable_project_documents(request.app.state.runtime, request.path_params["project_id"]) as service:
        try:
            row = await service.get_document_info(document_id=request.path_params["document_id"])
        except FileNotFoundError:
            return _error("document_not_found", "Document not found", 404)
    return JSONResponse(_public(row, DOCUMENT_FIELDS))


async def read_document(request):
    if error := await _project(request, write=False):
        return error
    try:
        start = int(request.query_params.get("start_line", "1"))
        end_value = request.query_params.get("end_line")
        end = int(end_value) if end_value is not None else None
        page_value = request.query_params.get("page_number")
        page = int(page_value) if page_value is not None else None
    except ValueError:
        return _error("invalid_lines", "Invalid line range", 422)
    if start < 1 or (end is not None and end < start) or (page is not None and page < 1):
        return _error("invalid_lines", "Invalid line range", 422)
    async with readable_project_documents(request.app.state.runtime, request.path_params["project_id"]) as service:
        try:
            row = await service.read_document(
                document_id=request.path_params["document_id"],
                page_number=page, start_line=start, end_line=end,
            )
        except FileNotFoundError:
            return _error("document_not_found", "Document not found", 404)
        except ValueError:
            return _error("invalid_read_range", "Invalid document page or line range", 422)
        except RuntimeError as exc:
            if str(exc) == "Document has not been indexed":
                return _error("document_not_ready", "Document text is not ready", 409)
            raise
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


async def _folder_manifest(request):
    # A bounded JSON upload keeps folder intake comparable to existing document uploads.
    ceiling = MAX_DOCUMENT_UPLOAD_BODY_BYTES + 2 * 1024 * 1024
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > ceiling:
            return _error("payload_too_large", "Folder upload is too large", 413)
    try:
        payload = FolderIntake.model_validate(json.loads(body))
        files = []
        total_bytes = 0
        for item in payload.files:
            content = base64.b64decode(item.content_base64, validate=True)
            total_bytes += len(content)
            if total_bytes > MAX_DOCUMENT_SIZE:
                return _error("payload_too_large", "Folder upload is too large", 413)
            files.append(FolderUploadEntry(relative_path=item.relative_path, content=content))
    except (ValidationError, ValueError, TypeError):
        return _error("invalid_folder", "Invalid folder manifest", 422)
    return payload, files


def _folder_preview(preview):
    return {
        "folder_name": preview.folder_name,
        "settings": preview.settings.model_dump(mode="json"),
        "summary": asdict(preview.summary),
        "force_include_paths": preview.force_include_paths,
        "included": [asdict(entry) for entry in preview.included],
        "excluded": [asdict(entry) for entry in preview.excluded],
    }


async def folder_intake(request):
    if error := await _project(request):
        return error
    manifest = await _folder_manifest(request)
    if isinstance(manifest, JSONResponse):
        return manifest
    payload, files = manifest
    async with project_documents(request.app.state.runtime, request.path_params["project_id"]) as service:
        try:
            preview = await service.preview_folder(
                folder_name=payload.folder_name, entries=files,
                force_include_paths=payload.force_include_paths,
            )
        except ValueError:
            return _error("invalid_folder", "Invalid folder manifest", 422)
        result = _folder_preview(preview)
        if request.url.path.endswith("/preview"):
            return JSONResponse(result)

        content_by_path = {
            normalize_relative_path(entry.relative_path, entry.relative_path): entry.content
            for entry in files
        }
        accepted = []
        failed = []
        for entry in preview.included:
            try:
                row = await service.submit_document(
                    content=content_by_path[entry.relative_path],
                    original_name=entry.original_name,
                    relative_path=entry.relative_path,
                )
                accepted.append(_public(row, DOCUMENT_FIELDS))
            except Exception as exc:
                public_error = await handle_error(request, exc)
                failed.append({
                    "relative_path": entry.relative_path,
                    "error": json.loads(public_error.body)["error"],
                })
        result.update({"accepted": accepted, "failed": failed})
        return JSONResponse(result)


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
    if error := await _project(request, write=False):
        return error
    try:
        limit = int(request.query_params.get("limit", "50"))
        before = _read_cursor(request.query_params.get("cursor"))
    except ValueError:
        return _error("invalid_link_list", "Invalid link list options", 422)
    if not 1 <= limit <= 100:
        return _error("invalid_link_list", "Invalid link list options", 422)
    async with readable_project_documents(request.app.state.runtime, request.path_params["project_id"]) as service:
        rows = await service.list_saved_web_links(limit=limit + 1, before=before)
    return JSONResponse(_page(
        rows, limit=limit, fields=LINK_FIELDS,
        time_field="updated_at", id_field="link_id",
    ))


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
    Route("/v1/projects/{project_id}/folders/preview", folder_intake, methods=["POST"]),
    Route("/v1/projects/{project_id}/folders/intake", folder_intake, methods=["POST"]),
    Route("/v1/projects/{project_id}/documents", list_documents, methods=["GET"]),
    Route("/v1/projects/{project_id}/documents", upload_document, methods=["POST"]),
    Route("/v1/projects/{project_id}/documents/search", search_documents, methods=["GET"]),
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
