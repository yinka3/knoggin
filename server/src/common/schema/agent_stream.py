from typing import Any, Dict, List, Literal, NotRequired, Optional, TypedDict, Union


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
    call_id: Optional[str]


class ToolStartEvent(TypedDict):
    event: Literal["tool_start"]
    data: ToolStartData


class ToolEndData(TypedDict):
    tool: str
    result: Any
    call_id: NotRequired[Optional[str]]


class ToolEndEvent(TypedDict):
    event: Literal["tool_end"]
    data: ToolEndData


class ToolErrorData(TypedDict):
    tool: str
    error: str


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
