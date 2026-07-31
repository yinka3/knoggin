from typing import Any, Dict, List, Literal, NotRequired, Optional, TypedDict, Union

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, TypeAdapter


class StreamUsage(TypedDict):
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    approximate: bool


class StreamToolCall(TypedDict):
    name: str
    arguments: str
    id: NotRequired[str]


class ContentData(TypedDict):
    content: str


class TokenEvent(TypedDict):
    event: Literal["token"]
    data: ContentData


class ThinkingEvent(TypedDict):
    event: Literal["thinking"]
    data: ContentData


class ToolCallsData(TypedDict):
    content: str
    calls: List[StreamToolCall]


class ToolCallsEvent(TypedDict):
    event: Literal["tool_calls"]
    data: ToolCallsData


class StepCompletedData(TypedDict):
    content: str
    usage: StreamUsage


class StepCompletedEvent(TypedDict):
    event: Literal["step_completed"]
    data: StepCompletedData


class StepErrorData(TypedDict):
    message: str
    kind: Literal["provider", "formatting"]
    usage: NotRequired[StreamUsage]


class StepErrorEvent(TypedDict):
    event: Literal["step_error"]
    data: StepErrorData


InternalAgentStreamEvent = Union[
    TokenEvent,
    ThinkingEvent,
    ToolCallsEvent,
    StepCompletedEvent,
    StepErrorEvent,
]


class ToolStartData(TypedDict):
    tool: str
    args: Dict[str, Any]
    thinking: Optional[str]
    call_id: str


class ToolStartEvent(TypedDict):
    event: Literal["tool_start"]
    data: ToolStartData


class ToolEndData(TypedDict):
    tool: str
    result: Any
    call_id: str


class ToolEndEvent(TypedDict):
    event: Literal["tool_end"]
    data: ToolEndData


class ToolErrorData(TypedDict):
    tool: str
    error: str
    call_id: str


class ToolErrorEvent(TypedDict):
    event: Literal["tool_error"]
    data: ToolErrorData


class ResponseData(TypedDict):
    content: str
    usage: StreamUsage
    sources: Optional[List[Dict[str, Any]]]
    sources_consulted: NotRequired[List[Dict[str, Any]]]
    fallback: NotRequired[bool]


class ResponseEvent(TypedDict):
    event: Literal["response"]
    data: ResponseData


class ClarificationData(TypedDict):
    question: str
    usage: NotRequired[StreamUsage]
    fallback: NotRequired[bool]


class ClarificationEvent(TypedDict):
    event: Literal["clarification"]
    data: ClarificationData


class ErrorData(TypedDict):
    message: str


class ErrorEvent(TypedDict):
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


class _PublicStreamModel(BaseModel):
    """Runtime-validated shape shared by externally emitted stream events."""

    model_config = ConfigDict(extra="forbid")


class _PublicStreamUsage(_PublicStreamModel):
    prompt_tokens: StrictInt = Field(ge=0)
    completion_tokens: StrictInt = Field(ge=0)
    total_tokens: StrictInt = Field(ge=0)
    approximate: StrictBool


class _PublicContentData(_PublicStreamModel):
    content: str


class _PublicTokenEvent(_PublicStreamModel):
    event: Literal["token"]
    data: _PublicContentData


class _PublicThinkingEvent(_PublicStreamModel):
    event: Literal["thinking"]
    data: _PublicContentData


class _PublicToolStartData(_PublicStreamModel):
    tool: str = Field(min_length=1)
    args: Dict[str, Any]
    thinking: Optional[str]
    call_id: str = Field(min_length=1)


class _PublicToolStartEvent(_PublicStreamModel):
    event: Literal["tool_start"]
    data: _PublicToolStartData


class _PublicToolEndData(_PublicStreamModel):
    tool: str = Field(min_length=1)
    result: Any
    call_id: str = Field(min_length=1)


class _PublicToolEndEvent(_PublicStreamModel):
    event: Literal["tool_end"]
    data: _PublicToolEndData


class _PublicToolErrorData(_PublicStreamModel):
    tool: str = Field(min_length=1)
    error: str = Field(min_length=1)
    call_id: str = Field(min_length=1)


class _PublicToolErrorEvent(_PublicStreamModel):
    event: Literal["tool_error"]
    data: _PublicToolErrorData


class _PublicResponseData(_PublicStreamModel):
    content: str
    usage: _PublicStreamUsage
    sources: Optional[List[Dict[str, Any]]]
    sources_consulted: List[Dict[str, Any]] = Field(default_factory=list)
    fallback: bool = False


class _PublicResponseEvent(_PublicStreamModel):
    event: Literal["response"]
    data: _PublicResponseData


class _PublicClarificationData(_PublicStreamModel):
    question: str = Field(min_length=1)
    usage: Optional[_PublicStreamUsage] = None
    fallback: bool = False


class _PublicClarificationEvent(_PublicStreamModel):
    event: Literal["clarification"]
    data: _PublicClarificationData


class _PublicErrorData(_PublicStreamModel):
    message: str = Field(min_length=1)


class _PublicErrorEvent(_PublicStreamModel):
    event: Literal["error"]
    data: _PublicErrorData


PublicAgentStreamEventModel = Union[
    _PublicTokenEvent,
    _PublicThinkingEvent,
    _PublicToolStartEvent,
    _PublicToolEndEvent,
    _PublicToolErrorEvent,
    _PublicResponseEvent,
    _PublicClarificationEvent,
    _PublicErrorEvent,
]

_public_agent_stream_event_adapter = TypeAdapter(PublicAgentStreamEventModel)


def validate_public_agent_stream_event(event: object) -> Dict[str, Any]:
    """Validate an event once as it leaves the agent subsystem."""

    validated = _public_agent_stream_event_adapter.validate_python(event)
    return validated.model_dump(mode="json", exclude_unset=True)
