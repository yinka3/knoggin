"""One HTTP lifespan owns one local engine runtime."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

from starlette.applications import Starlette
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware

from local_access import ALLOWED_ORIGINS, SameOriginWrites

from routes.docs import routes as docs_routes
from routes.health import routes as health_routes
from routes.agents import routes as agent_routes
from routes.artifacts import routes as artifact_routes
from routes.community import routes as community_routes
from routes.documents import routes as document_routes
from routes.episodes import routes as episode_routes
from routes.maintenance import routes as maintenance_routes
from routes.knowledge import routes as knowledge_routes
from routes.workspace import routes as workspace_routes
from routes.context_notes import routes as context_note_routes
from routes.notebooks import routes as notebook_routes
from routes.project_settings import routes as project_settings_routes
from routes.project_control import routes as project_control_routes
from routes.projects import routes as project_routes
from routes.settings import routes as settings_routes
from errors import handle_error
from folder_staging import FolderStaging
from run_events import RunEvents
from routes.sessions import routes as session_routes
from routes.runs import routes as run_routes
from routes.research_jobs import routes as research_job_routes
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
        try:
            folder_staging = FolderStaging(
                lambda: runtime.config_manager.config.developer_settings.documents
            )
            run_events = RunEvents(
                lambda: runtime.config_manager.config.developer_settings.run_events
            )
        except Exception:
            await runtime.shutdown()
            raise
        app.state.runtime = runtime
        app.state.folder_staging = folder_staging
        app.state.run_events = run_events
        try:
            yield
        finally:
            try:
                await run_events.close()
            finally:
                try:
                    await runtime.shutdown()
                finally:
                    try:
                        await folder_staging.close()
                    finally:
                        del app.state.run_events
                        del app.state.folder_staging
                        del app.state.runtime

    app = Starlette(
        routes=[*docs_routes, *health_routes, *settings_routes, *project_routes, *project_settings_routes, *project_control_routes, *notebook_routes, *agent_routes, *session_routes, *run_routes, *research_job_routes, *community_routes, *document_routes, *artifact_routes, *maintenance_routes, *episode_routes, *knowledge_routes, *workspace_routes, *context_note_routes],
        lifespan=lifespan,
        exception_handlers={Exception: handle_error},
    )
    app.add_middleware(SameOriginWrites)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(ALLOWED_ORIGINS),
        allow_methods=["GET", "POST", "PATCH", "PUT", "DELETE"],
        allow_headers=["Content-Type"],
    )
    # A rebound domain (attacker.com resolving to 127.0.0.1) arrives with its
    # own name in Host; only local names are served.
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=allowed_hosts())
    return app


def allowed_hosts() -> list[str]:
    configured = os.environ.get("KNOGGIN_ALLOWED_HOSTS")
    if configured:
        return [host.strip() for host in configured.split(",") if host.strip()]
    return ["127.0.0.1", "localhost", "[::1]"]
