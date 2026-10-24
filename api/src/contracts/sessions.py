"""Durable session preferences and run input."""

from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator

from common.schema.session_input import normalise_enabled_tools
from common.schema.document import DocumentSelection


class SessionPreferences(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model: str | None = Field(default=None, min_length=1)
    agent_id: str | None = Field(default=None, min_length=1)
    enabled_tools: list[str] | None = None

    _tools = field_validator("enabled_tools")(normalise_enabled_tools)


class CreateSession(SessionPreferences):
    project_id: str = Field(min_length=1)


class StartRun(SessionPreferences):
    query: str = Field(min_length=1, max_length=100_000)
    research_mode: Literal["normal", "research", "deep_research"] = "normal"
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=200)
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
