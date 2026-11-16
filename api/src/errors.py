"""Safe HTTP projection of engine failures."""

from pydantic import ValidationError
from starlette.responses import JSONResponse

from common.exceptions import (
    DependencyError,
    IdempotencyConflictError,
    LLMBudgetExceededError,
    MaintenanceConflictError,
    NotFoundError,
    RequestInProgressError,
    SavedWebLinkAlreadyExistsError,
    SessionBusyError,
    StorageError,
    WorkspaceConflictError,
)


async def handle_error(_request, error: Exception) -> JSONResponse:
    if isinstance(error, (NotFoundError, FileNotFoundError)):
        status, code, message = 404, "not_found", "Resource not found"
    elif isinstance(error, PermissionError):
        status, code, message = 403, "forbidden", "Operation is not allowed"
    elif isinstance(error, SessionBusyError):
        status, code, message = 409, "session_busy", "Session has an active run"
    elif isinstance(error, IdempotencyConflictError):
        status, code, message = 409, "idempotency_conflict", "Request key conflicts"
    elif isinstance(error, RequestInProgressError):
        status, code, message = 409, "request_in_progress", "Request is in progress"
    elif isinstance(error, SavedWebLinkAlreadyExistsError):
        status, code, message = 409, "link_exists", "Web link already saved"
    elif isinstance(error, WorkspaceConflictError):
        status, code, message = 409, "workspace_conflict", "Workspace changed"
    elif isinstance(error, MaintenanceConflictError):
        status, code, message = 409, "maintenance_conflict", "Review or knowledge changed"
    elif isinstance(error, LLMBudgetExceededError):
        status, code, message = 402, "llm_budget_exhausted", "Model budget is exhausted"
    elif isinstance(error, (ValidationError, ValueError, TypeError)):
        status, code, message = 422, "invalid_request", "Invalid request"
    elif isinstance(error, (DependencyError, StorageError)):
        status, code, message = 503, "dependency_unavailable", "Service is unavailable"
    else:
        status, code, message = 500, "internal_error", "Request could not be completed"
    return JSONResponse(
        {"error": {"code": code, "message": message}}, status_code=status
    )
