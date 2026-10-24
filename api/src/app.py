"""One HTTP lifespan owns one local engine runtime."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

from starlette.applications import Starlette
from starlette.middleware.cors import CORSMiddleware

from routes.health import routes as health_routes
from routes.agents import routes as agent_routes
from routes.artifacts import routes as artifact_routes
from routes.community import routes as community_routes
from routes.documents import routes as document_routes
from routes.episodes import routes as episode_routes
from routes.maintenance import routes as maintenance_routes
from routes.project_settings import routes as project_settings_routes
from routes.projects import routes as project_routes
from routes.settings import routes as settings_routes
from errors import handle_error
from routes.sessions import routes as session_routes
from routes.runs import routes as run_routes
from runtime.application import ApplicationRuntime


def create_app(
    *, user_name: str | None = None, config_dir: str | Path | None = None
) -> Starlette:
    """Bind this process to one local user and configuration directory."""
    user_name = (
        user_name if user_name is not None else os.environ.get("RUNTIME_USER_NAME")
    )
    if not user_name or not user_name.strip():
        raise ValueError("RUNTIME_USER_NAME must be set to a non-blank user name")
    user_name = user_name.strip()
    config_dir = config_dir or os.environ.get("SETTINGS_DIR", "~/.config/knoggin")
    config_path = Path(config_dir).expanduser()

    @asynccontextmanager
    async def lifespan(app: Starlette):
        runtime = await ApplicationRuntime.start(
            user_name=user_name, config_dir=config_path
        )
        app.state.runtime = runtime
        try:
            yield
        finally:
            try:
                await runtime.shutdown()
            finally:
                del app.state.runtime

    app = Starlette(
        routes=[*health_routes, *settings_routes, *project_routes, *project_settings_routes, *agent_routes, *session_routes, *run_routes, *community_routes, *document_routes, *artifact_routes, *maintenance_routes, *episode_routes],
        lifespan=lifespan,
        exception_handlers={Exception: handle_error},
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://127.0.0.1:3000", "http://localhost:3000"],
        allow_methods=["GET", "POST", "PATCH", "PUT", "DELETE"],
        allow_headers=["Content-Type"],
    )
    return app
