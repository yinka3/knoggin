"""One HTTP lifespan owns one local engine runtime."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

from starlette.applications import Starlette

from api.routes.health import routes as health_routes
from runtime.application import ApplicationRuntime


def create_app(
    *, user_name: str | None = None, config_dir: str | Path | None = None
) -> Starlette:
    """Bind this process to one local user and configuration directory."""
    user_name = user_name if user_name is not None else os.environ.get("RUNTIME_USER_NAME")
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

    return Starlette(routes=health_routes, lifespan=lifespan)
