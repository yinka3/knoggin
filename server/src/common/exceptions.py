from typing import Dict, Optional


class EngineError(Exception):
    """Base class for all system-wide errors."""

    def __init__(
        self, message: str, code: str = "engine_error", details: Optional[Dict] = None
    ):
        super().__init__(message)
        self.message = message
        self.code = code
        self.details = details or {}


class PayloadTooLargeError(EngineError):
    def __init__(self):
        super().__init__("The upload exceeds the permitted size.", code="payload_too_large")


class ConfigurationError(EngineError):
    """Raised when the system is misconfigured or missing required settings."""

    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(message, code="configuration_error", details=details)


class DependencyError(EngineError):
    """Raised when a required service or dependency (LLM, etc.) is unavailable."""

    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(message, code="dependency_error", details=details)


class NotFoundError(EngineError):
    """Raised when a requested user-owned resource does not exist."""

    def __init__(self, resource: str = "resource"):
        super().__init__(f"Requested {resource} was not found", code="not_found")


class SessionBusyError(EngineError):
    """Raised when a normal agent run overlaps an active session run."""

    def __init__(self):
        super().__init__(
            "This session already has an active run",
            code="session_busy",
        )


class SavedWebLinkAlreadyExistsError(EngineError):
    """A project already contains the normalized bookmark URL."""

    def __init__(self):
        super().__init__("This project already has this web link", code="link_exists")


class IdempotencyConflictError(EngineError):
    """Raised when one request key is reused for a different submission."""

    def __init__(self):
        EngineError.__init__(
            self,
            "This idempotency key was already used for a different request",
            code="idempotency_conflict",
        )


class RequestInProgressError(EngineError):
    """Raised when the same accepted request is still executing locally."""

    def __init__(self):
        super().__init__(
            "This request is already in progress",
            code="request_in_progress",
        )


class RequestInterruptedError(EngineError):
    """Raised when a durable acceptance has no safe terminal result to replay."""

    def __init__(self):
        super().__init__(
            "This request was accepted but did not reach a durable outcome",
            code="request_interrupted",
        )


class StorageError(EngineError):
    """Base class for failures at a durable persistence boundary."""

    def __init__(
        self,
        operation: str,
        *,
        code: str,
        details: Optional[Dict] = None,
    ):
        super().__init__(
            f"Storage operation failed: {operation}",
            code=code,
            details={"operation": operation, **(details or {})},
        )


class StorageReadError(StorageError):
    """Raised when a durable read fails rather than returning normal absence."""

    def __init__(self, operation: str, details: Optional[Dict] = None):
        super().__init__(operation, code="storage_read_error", details=details)


class StorageWriteError(StorageError):
    """Raised when a durable mutation or transaction cannot commit."""

    def __init__(self, operation: str, details: Optional[Dict] = None):
        super().__init__(operation, code="storage_write_error", details=details)


class WorkspaceConflictError(EngineError, ValueError):
    """Raised when an optimistic-concurrency workspace write is stale."""

    def __init__(self, message: str = "Workspace file changed", details: Optional[Dict] = None):
        EngineError.__init__(
            self,
            message,
            code="workspace_conflict",
            details=details,
        )


class MaintenanceConflictError(EngineError, ValueError):
    """A reviewed correction no longer matches current project knowledge."""

    def __init__(self):
        EngineError.__init__(
            self, "Maintenance review or knowledge changed", code="maintenance_conflict"
        )


class EpisodeEditConflictError(EngineError, ValueError):
    """An optimistic episode edit no longer matches the owned revision."""

    def __init__(self):
        EngineError.__init__(
            self,
            "Episode is unavailable or has changed since it was read",
            code="episode_conflict",
        )


class LLMError(EngineError):
    """Base class for LLM request and response failures."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "llm_error",
        details: Optional[Dict] = None,
    ):
        super().__init__(message, code=code, details=details)


class LLMProviderError(LLMError):
    """Raised when the configured LLM provider cannot complete a request.

    ``transient`` marks outages, rate limits, and provider 5xx responses that
    are expected to clear without any change to the request.
    """

    def __init__(
        self,
        message: str,
        details: Optional[Dict] = None,
        *,
        transient: bool = False,
    ):
        super().__init__(message, code="llm_provider_error", details=details)
        self.transient = transient


class LLMContextTruncatedError(LLMProviderError):
    """The provider processed far fewer prompt tokens than were sent.

    Its context window is smaller than the prompt, so it silently dropped part
    of it. Retrying cannot help until the model or server is configured.
    """

    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(message, details=details, transient=False)


class LLMResponseError(LLMError):
    """Raised when an LLM response is empty, malformed, or fails validation."""

    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(message, code="llm_response_error", details=details)


class LLMBudgetExceededError(LLMError):
    """Raised before an external LLM attempt after the shared budget is spent."""

    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(message, code="llm_budget_exhausted", details=details)


class ToolExecutionError(EngineError):
    """Raised when a tool fails to execute correctly."""

    def __init__(
        self,
        tool_name: str,
        message: str,
        details: Optional[Dict] = None,
        *,
        retryable: bool = False,
    ):
        details = {**(details or {}), "tool": tool_name, "retryable": retryable}
        self.retryable = retryable
        super().__init__(
            f"Tool '{tool_name}' failed: {message}", code="tool_error", details=details
        )
