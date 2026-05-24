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


class ToolExecutionError(KnogginError):
    """Raised when a tool fails to execute correctly."""

    def __init__(self, tool_name: str, message: str, details: Optional[Dict] = None):
        details = details or {}
        details["tool"] = tool_name
        super().__init__(
            f"Tool '{tool_name}' failed: {message}", code="tool_error", details=details
        )
