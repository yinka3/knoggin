"""Internal and public agent stream-event contracts."""

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
    sources: Optional[List[Dict[str, Any]]]
    sources_consulted: NotRequired[List[Dict[str, Any]]]
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


PublicAgentStreamEvent = Union[
    TokenEvent,
    ThinkingEvent,
    ToolStartEvent,
    ToolEndEvent,
    ToolErrorEvent,
    ResponseEvent,
    ClarificationEvent,
    ErrorEvent,
]

AgentStreamEvent = Union[InternalAgentStreamEvent, PublicAgentStreamEvent]


_public_agent_stream_event_adapter = TypeAdapter(PublicAgentStreamEvent)


def validate_public_agent_stream_event(event: object) -> Dict[str, Any]:
    """Validate an event once as it leaves the agent subsystem."""

    return _public_agent_stream_event_adapter.validate_python(event)
