from typing import Optional, Dict

class AgentError(Exception):
    """Base class for all agent-related errors."""
    def __init__(self, message: str, code: str = "agent_error", details: Optional[Dict] = None):
        super().__init__(message)
        self.message = message
        self.code = code
        self.details = details or {}

class ConfigurationError(AgentError):
    """Raised when the agent is misconfigured or missing required settings."""
    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(message, code="configuration_error", details=details)

class DependencyError(AgentError):
    """Raised when a required service or dependency (Redis, LLM, etc.) is unavailable."""
    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(message, code="dependency_error", details=details)

class ToolExecutionError(AgentError):
    """Raised when a tool fails to execute correctly."""
    def __init__(self, tool_name: str, message: str, details: Optional[Dict] = None):
        details = details or {}
        details["tool"] = tool_name
        super().__init__(f"Tool '{tool_name}' failed: {message}", code="tool_error", details=details)

class ContextOverflowError(AgentError):
    """Raised when the conversation context exceeds model limits."""
    def __init__(self, token_count: int, limit: int):
        super().__init__(
            f"Context overflow: {token_count} tokens exceed limit of {limit}",
            code="context_overflow",
            details={"tokens": token_count, "limit": limit}
        )

class GuardrailViolation(AgentError):
    """Raised when a request or response violates established safety or logic guardrails."""
    def __init__(self, rule: str, message: str):
        super().__init__(message, code="guardrail_violation", details={"rule": rule})
