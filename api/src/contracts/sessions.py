"""Durable session preferences and run input."""

from typing import Annotated, Literal, Union
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator

from common.schema.session_input import normalise_enabled_tools
from common.schema.document import DocumentSelection
from common.schema.primitives import ImportedExchange


class SessionPreferences(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model: str | None = Field(default=None, min_length=1)
    agent_id: str | None = Field(default=None, min_length=1)
    enabled_tools: list[str] | None = None

    _tools = field_validator("enabled_tools")(normalise_enabled_tools)


class CreateSession(SessionPreferences):
    project_id: str = Field(min_length=1)


class StartResearchJob(BaseModel):
    model_config = ConfigDict(extra="forbid")

    question: str = Field(min_length=1, max_length=4000)
    links: list[str] = Field(default_factory=list)
    depth: Literal["research", "deep_research"] = "research"


class ResponseAnnotation(BaseModel):
    """A selected span of a completed assistant message in this session."""

    model_config = ConfigDict(extra="forbid")
    message_id: int = Field(gt=0)
    start: int = Field(ge=0)
    end: int = Field(gt=0)
    quote: str = Field(min_length=1, max_length=4000)


class StartRun(SessionPreferences):
    query: str = Field(min_length=1, max_length=100_000)
    annotations: list["ResponseAnnotation"] = Field(default_factory=list, max_length=5)
    research_mode: Literal["normal", "research", "deep_research"] = "normal"
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=200)
    notebook_id: UUID | None = None
    document_focus: Annotated[
        Union["RunDocumentFocus", "RunSubtreeFocus"], Field(discriminator="target_type")
    ] | None = None

    @field_validator("query")
    @classmethod
    def nonblank_query(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("blank query")
        return value


class SteerRun(BaseModel):
    model_config = ConfigDict(extra="forbid")

    instruction: str = Field(min_length=1, max_length=4000)
    annotations: list["ResponseAnnotation"] = Field(default_factory=list, max_length=5)

    @field_validator("instruction")
    @classmethod
    def nonblank_instruction(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("blank instruction")
        return value


class QueueFollowUp(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query: str = Field(min_length=1, max_length=100_000)
    annotations: list["ResponseAnnotation"] = Field(default_factory=list, max_length=5)

    @field_validator("query")
    @classmethod
    def nonblank_query(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("blank query")
        return value


class ImportExchange(ImportedExchange):
    model_config = ConfigDict(extra="forbid")

    user_content: str = Field(max_length=100_000)
    assistant_content: str | None = Field(default=None, max_length=100_000)


class ImportExchanges(BaseModel):
    """Recorded chat turns to add to a session at their original times."""

    model_config = ConfigDict(extra="forbid")

    exchanges: list[ImportExchange] = Field(min_length=1)


class DocumentFocus(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_type: Literal["document"]
    document_id: str = Field(min_length=1)
    behavior: Literal["prefer", "restrict"] = "prefer"


class SubtreeFocus(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_type: Literal["subtree"]
    path_prefix: str = Field(min_length=1)
    behavior: Literal["prefer", "restrict"] = "prefer"


focus_input = TypeAdapter(Annotated[Union[DocumentFocus, SubtreeFocus], Field(discriminator="target_type")])


class RunDocumentFocus(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_type: Literal["document"]
    document_id: str = Field(min_length=1)
    selection: DocumentSelection | None = None
    behavior: Literal["prefer", "restrict"] = "restrict"


class RunSubtreeFocus(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_type: Literal["subtree"]
    path_prefix: str = Field(min_length=1)
    behavior: Literal["prefer", "restrict"] = "prefer"


StartRun.model_rebuild()
SteerRun.model_rebuild()
QueueFollowUp.model_rebuild()
