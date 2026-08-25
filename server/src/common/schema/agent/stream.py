"""Internal agent execution-event contracts.

The application facade owns the SDK contract; FastAPI owns the UI HTTP
and SSE projection.
"""

from typing import (
    Annotated,
    Any,
    Dict,
    List,
    Literal,
    NotRequired,
    Optional,
    TypedDict,
    Union,
)

from pydantic import ConfigDict, Field, StrictBool, StrictInt, TypeAdapter

from common.schema.agent.research import ResearchMode


class _StrictStreamDict(TypedDict):
    """Typed dictionary that also forbids unknown keys during validation."""

    __pydantic_config__ = ConfigDict(extra="forbid")


NonNegativeInt = Annotated[StrictInt, Field(ge=0)]
NonBlankString = Annotated[str, Field(min_length=1)]


class StreamUsage(_StrictStreamDict):
    prompt_tokens: NonNegativeInt
    completion_tokens: NonNegativeInt
    total_tokens: NonNegativeInt
    approximate: StrictBool


class StreamToolCall(_StrictStreamDict):
    name: str
    arguments: str
    id: NotRequired[str]


class ContentData(_StrictStreamDict):
    content: str


class TokenEvent(_StrictStreamDict):
    event: Literal["token"]
    data: ContentData


class ThinkingEvent(_StrictStreamDict):
    event: Literal["thinking"]
    data: ContentData


class ToolCallsData(_StrictStreamDict):
    content: str
    calls: List[StreamToolCall]


class ToolCallsEvent(_StrictStreamDict):
    event: Literal["tool_calls"]
    data: ToolCallsData


class StepCompletedData(_StrictStreamDict):
    content: str
    usage: StreamUsage


class StepCompletedEvent(_StrictStreamDict):
    event: Literal["step_completed"]
    data: StepCompletedData


class StepErrorData(_StrictStreamDict):
    message: str
    kind: Literal["provider", "formatting"]
    usage: NotRequired[StreamUsage]


class StepErrorEvent(_StrictStreamDict):
    event: Literal["step_error"]
    data: StepErrorData


InternalAgentStreamEvent = Union[
    TokenEvent,
    ThinkingEvent,
    ToolCallsEvent,
    StepCompletedEvent,
    StepErrorEvent,
]


class ToolStartData(_StrictStreamDict):
    tool: NonBlankString
    args: Dict[str, Any]
    thinking: Optional[str]
    call_id: NonBlankString


class ToolStartEvent(_StrictStreamDict):
    event: Literal["tool_start"]
    data: ToolStartData


class ToolEndData(_StrictStreamDict):
    tool: NonBlankString
    result: Any
    call_id: NonBlankString


class ToolEndEvent(_StrictStreamDict):
    event: Literal["tool_end"]
    data: ToolEndData


class ToolErrorData(_StrictStreamDict):
    tool: NonBlankString
    error: NonBlankString
    call_id: NonBlankString


class ToolErrorEvent(_StrictStreamDict):
    event: Literal["tool_error"]
    data: ToolErrorData


class ResponseData(_StrictStreamDict):
    content: str
    usage: StreamUsage
    sources_consulted: NotRequired[List[Dict[str, Any]]]
    # Internal engine handoff.  Public run projections will expose a narrower
    # artifact reference/read contract once the API layer is added.
    artifact: NotRequired[Dict[str, Any]]
    research_mode: NotRequired[ResearchMode]
    fallback: NotRequired[StrictBool]


class ResponseEvent(_StrictStreamDict):
    event: Literal["response"]
    data: ResponseData


class ClarificationData(_StrictStreamDict):
    question: NonBlankString
    usage: NotRequired[StreamUsage]
    fallback: NotRequired[StrictBool]


class ClarificationEvent(_StrictStreamDict):
    event: Literal["clarification"]
    data: ClarificationData


class ErrorData(_StrictStreamDict):
    message: NonBlankString


class ErrorEvent(_StrictStreamDict):
    event: Literal["error"]
    data: ErrorData


# These events are engine-internal. The application facade wraps them in
# SDK events, and FastAPI projects browser-safe HTTP/SSE events. Neither
# boundary serializes this union directly.
AgentExecutionEvent = Union[
    TokenEvent,
    ThinkingEvent,
    ToolStartEvent,
    ToolEndEvent,
    ToolErrorEvent,
    ResponseEvent,
    ClarificationEvent,
    ErrorEvent,
]

AgentStreamEvent = Union[InternalAgentStreamEvent, AgentExecutionEvent]

_agent_execution_event_adapter = TypeAdapter(AgentExecutionEvent)


def validate_agent_execution_event(event: object) -> Dict[str, Any]:
    """Validate one event inside the engine execution boundary."""

    return _agent_execution_event_adapter.validate_python(event)
