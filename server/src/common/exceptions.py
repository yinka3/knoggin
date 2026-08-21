from typing import Dict, Optional


class KnogginError(Exception):
    """Base class for all system-wide errors."""

    def __init__(
        self, message: str, code: str = "knoggin_error", details: Optional[Dict] = None
    ):
        super().__init__(message)
        self.message = message
        self.code = code
        self.details = details or {}


class ConfigurationError(KnogginError):
    """Raised when the system is misconfigured or missing required settings."""

    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(message, code="configuration_error", details=details)


class DependencyError(KnogginError):
    """Raised when a required service or dependency (Redis, LLM, etc.) is unavailable."""

    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(message, code="dependency_error", details=details)


class StorageError(KnogginError):
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


class WorkspaceConflictError(KnogginError, ValueError):
    """Raised when an optimistic-concurrency workspace write is stale."""

    def __init__(self, message: str = "Workspace file changed", details: Optional[Dict] = None):
        KnogginError.__init__(
            self,
            message,
            code="workspace_conflict",
            details=details,
        )


class LLMError(KnogginError):
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
    """Raised when the configured LLM provider cannot complete a request."""

    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(message, code="llm_provider_error", details=details)


class LLMResponseError(LLMError):
    """Raised when an LLM response is empty, malformed, or fails validation."""

    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(message, code="llm_response_error", details=details)


class LLMBudgetExceededError(LLMError):
    """Raised before an external LLM attempt after the shared budget is spent."""

    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(message, code="llm_budget_exhausted", details=details)


class ToolExecutionError(KnogginError):
    """Raised when a tool fails to execute correctly."""

    def __init__(self, tool_name: str, message: str, details: Optional[Dict] = None):
        details = details or {}
        details["tool"] = tool_name
        super().__init__(
            f"Tool '{tool_name}' failed: {message}", code="tool_error", details=details
        )
