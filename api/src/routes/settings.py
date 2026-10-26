"""Global settings routes for the process-bound local runtime."""

from pydantic import ValidationError
from starlette.responses import JSONResponse
from starlette.routing import Route

from contracts.settings import SettingsView, settings_status

SECTIONS = frozenset({"user_aliases", "llm", "search", "jev", "developer_settings"})


def _error(code: str, message: str, status: int) -> JSONResponse:
    return JSONResponse(
        {"error": {"code": code, "message": message}}, status_code=status
    )


async def _json(request):
    try:
        return await request.json()
    except (ValueError, UnicodeDecodeError):
        return None


def _status_response(runtime) -> dict:
    return settings_status(runtime).model_dump(mode="json")


async def get_settings(request):
    runtime = request.app.state.runtime
    return JSONResponse(
        {
            "settings": SettingsView.from_config(
                runtime.config_manager.config
            ).model_dump(mode="json"),
            "status": _status_response(runtime),
        }
    )


async def get_status(request):
    return JSONResponse(_status_response(request.app.state.runtime))


async def get_spending(request):
    snapshot = await request.app.state.runtime.resources.llm_service.spending_snapshot()
    return JSONResponse({key: snapshot[key] for key in (
        "configured_limit_usd", "spent_usd", "reserved_usd", "remaining_usd",
        "request_count", "enforced", "prompt_cache",
    )})


async def reload_settings(request):
    runtime = request.app.state.runtime
    if not runtime.config_manager.load():
        return _error("settings_reload_failed", "Settings could not be reloaded", 422)
    return JSONResponse({"accepted": True, "status": _status_response(runtime)})


async def retry_applies(request):
    runtime = request.app.state.runtime
    runtime.config_manager.retry_failed_applies()
    return JSONResponse(_status_response(runtime))


async def patch_section(request):
    section = request.path_params["section"]
    if section not in SECTIONS:
        return _error("unknown_settings_section", "Unknown settings section", 404)
    changes = await _json(request)
    if section == "user_aliases":
        valid_shape = isinstance(changes, list)
    else:
        valid_shape = isinstance(changes, dict) and bool(changes)
    if not valid_shape:
        return _error("invalid_settings", "Invalid settings update", 422)
    return _update(request.app.state.runtime, {section: changes})


def _update(runtime, changes: dict) -> JSONResponse:
    manager = runtime.config_manager
    try:
        manager.validate_updates(changes)
    except (ValidationError, ValueError, TypeError):
        return _error("invalid_settings", "Invalid settings update", 422)
    if not manager.update_settings(changes):
        return _error("settings_write_failed", "Settings could not be saved", 503)
    return JSONResponse({"accepted": True, "status": _status_response(runtime)})


routes = [
    Route("/v1/settings", get_settings, methods=["GET"]),
    Route("/v1/settings/status", get_status, methods=["GET"]),
    Route("/v1/settings/spending", get_spending, methods=["GET"]),
    Route("/v1/settings/reload", reload_settings, methods=["POST"]),
    Route("/v1/settings/retry-applies", retry_applies, methods=["POST"]),
    Route("/v1/settings/{section}", patch_section, methods=["PATCH"]),
]
