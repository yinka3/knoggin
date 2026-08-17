"""FastAPI entry point for Knoggin's UI HTTP and SSE API."""

from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from fastapi import APIRouter, FastAPI, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response, StreamingResponse
from loguru import logger

from knoggin import Knoggin, Turn

from .contracts import (
    MessageCreateRequest,
    ProjectCreateRequest,
    SessionCreateRequest,
    document_focus_to_sdk,
)
from .projection import (
    event_response,
    project_response,
    run_response,
    session_response,
)
from .runs import RunManager


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    knoggin = await Knoggin.start(
        user_name=os.environ.get("KNOGGIN_USER_NAME", "local")
    )
    app.state.knoggin = knoggin
    app.state.runs = RunManager(knoggin)
    try:
        yield
    finally:
        await app.state.runs.close()
        await knoggin.close()


def create_app() -> FastAPI:
    """Build the UI-specific API on top of the SDK facade."""

    app = FastAPI(title="Knoggin API", version="0.1.0", lifespan=lifespan)
    origins = [
        origin.strip()
        for origin in os.environ.get(
            "KNOGGIN_CORS_ORIGINS",
            "http://localhost:3000,http://127.0.0.1:3000",
        ).split(",")
        if origin.strip()
    ]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "DELETE"],
        allow_headers=["Content-Type", "Idempotency-Key", "X-Request-Id"],
    )
    app.include_router(_router())
    return app


def _router() -> APIRouter:
    router = APIRouter(prefix="/api/v1")

    @router.get("/health")
    async def health(request: Request) -> dict[str, Any]:
        engine = await request.app.state.knoggin.get_engine_health()
        return {"status": "ok", "engine": engine}

    @router.post("/projects", status_code=201)
    async def create_project(
        body: ProjectCreateRequest,
        request: Request,
    ) -> JSONResponse:
        request_id = _request_id(request)
        if not body.name.strip():
            return _error("INVALID_PROJECT", "name is required", 400, request_id)
        try:
            project = await request.app.state.knoggin.create_project(
                name=body.name,
                domain_config=body.domain_config,
                description=body.description,
            )
            return JSONResponse(project_response(project), status_code=201)
        except ValueError:
            return _error(
                "INVALID_PROJECT", "The project request is invalid.", 400, request_id
            )
        except Exception:
            logger.exception("Project creation failed")
            return _error(
                "PROJECT_CREATE_FAILED",
                "The project could not be created.",
                500,
                request_id,
            )

    @router.post("/projects/{project_id}/sessions", status_code=201)
    async def create_session(
        project_id: str,
        body: SessionCreateRequest,
        request: Request,
    ) -> JSONResponse:
        request_id = _request_id(request)
        try:
            session = await request.app.state.knoggin.create_session(
                project_id=project_id,
                model=body.model,
                agent_id=body.agent_id,
                enabled_tools=body.enabled_tools,
            )
            return JSONResponse(session_response(session), status_code=201)
        except ValueError:
            return _error(
                "INVALID_SESSION", "The session request is invalid.", 400, request_id
            )
        except Exception:
            logger.exception("Session creation failed")
            return _error(
                "SESSION_CREATE_FAILED",
                "The session could not be created.",
                500,
                request_id,
            )

    @router.post("/sessions/{session_id}/messages", status_code=202)
    async def submit_message(
        session_id: str,
        body: MessageCreateRequest,
        request: Request,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    ) -> JSONResponse:
        request_id = _request_id(request)
        if not idempotency_key or not idempotency_key.strip():
            return _error(
                "IDEMPOTENCY_KEY_REQUIRED",
                "Idempotency-Key is required.",
                400,
                request_id,
            )
        if not body.content.strip():
            return _error("INVALID_MESSAGE", "content is required", 400, request_id)
        try:
            run = await request.app.state.runs.submit_turn(
                session_id=session_id,
                turn=Turn(
                    content=body.content,
                    model=body.model,
                    agent_id=body.agent_id,
                    enabled_tools=(
                        tuple(body.enabled_tools) if body.enabled_tools else None
                    ),
                    document_focus=document_focus_to_sdk(body.document_focus),
                ),
                idempotency_key=idempotency_key,
            )
            return JSONResponse(run_response(run), status_code=202)
        except LookupError:
            return _error(
                "SESSION_NOT_FOUND", "The session could not be found.", 404, request_id
            )
        except ValueError:
            return _error(
                "INVALID_MESSAGE", "The message request is invalid.", 400, request_id
            )
        except Exception:
            logger.exception("Message submission failed")
            return _error(
                "RUN_SUBMIT_FAILED", "The run could not be started.", 500, request_id
            )

    @router.get("/runs/{run_id}")
    async def get_run(run_id: str, request: Request) -> JSONResponse:
        try:
            return JSONResponse(run_response(request.app.state.runs.get_run(run_id)))
        except LookupError:
            return _error(
                "RUN_NOT_FOUND",
                "The run could not be found.",
                404,
                _request_id(request),
            )

    @router.delete("/runs/{run_id}")
    async def cancel_run(run_id: str, request: Request) -> JSONResponse:
        try:
            run = await request.app.state.runs.cancel(run_id)
            return JSONResponse(run_response(run))
        except LookupError:
            return _error(
                "RUN_NOT_FOUND",
                "The run could not be found.",
                404,
                _request_id(request),
            )

    @router.get("/runs/{run_id}/events", response_model=None)
    async def run_events(run_id: str, request: Request) -> Response:
        try:
            request.app.state.runs.get_run(run_id)
            stream = request.app.state.runs.subscribe_events(run_id)
        except LookupError:
            return _error(
                "RUN_NOT_FOUND",
                "The run could not be found.",
                404,
                _request_id(request),
            )

        async def encode_events() -> AsyncIterator[str]:
            async for event in stream:
                if await request.is_disconnected():
                    return
                projected = event_response(event)
                if projected is None:
                    continue
                yield (
                    f"id: {projected['sequence']}\n"
                    f"event: {projected['type']}\n"
                    f"data: {json.dumps(projected, separators=(',', ':'), default=str)}\n\n"
                )

        return StreamingResponse(
            encode_events(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache, no-transform",
                "Connection": "keep-alive",
            },
        )

    return router


def _request_id(request: Request) -> str | None:
    return request.headers.get("x-request-id") or None


def _error(
    code: str,
    message: str,
    status_code: int,
    request_id: str | None,
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "code": code,
                "message": message,
                "retryable": status_code >= 500,
                "requestId": request_id,
            }
        },
    )


app = create_app()
