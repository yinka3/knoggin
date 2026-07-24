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


class StorageUnavailableError(KnogginError):
    """Raised when a durable storage read cannot distinguish absence from failure."""

    def __init__(self, operation: str, details: Optional[Dict] = None):
        super().__init__(
            f"Storage query unavailable: {operation}",
            code="storage_unavailable",
            details={"operation": operation, **(details or {})},
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


class ToolExecutionError(KnogginError):
    """Raised when a tool fails to execute correctly."""

    def __init__(self, tool_name: str, message: str, details: Optional[Dict] = None):
        details = details or {}
        details["tool"] = tool_name
        super().__init__(
            f"Tool '{tool_name}' failed: {message}", code="tool_error", details=details
        )
