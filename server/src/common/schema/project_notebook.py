"""User-owned notebook entries, separate from an agent's temporary RunNotebook."""

from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from common.schema.evidence_board import BoardState

NotebookView = Literal["source_desk", "timeline", "evidence_board"]
NotebookSourceKind = Literal[
    "document", "saved_link", "message", "episode", "artifact", "source_ref", "finding"
]


class NotebookEntryInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    entry_id: UUID | None = None
    kind: NotebookSourceKind
    target_id: str = Field(min_length=1, max_length=200)
    note: str | None = Field(default=None, max_length=4000)
    event_at: datetime | None = None
    answer_excerpt: str | None = Field(default=None, max_length=4000)
    source_ref_ids: list[UUID] = Field(default_factory=list, max_length=20)

    @field_validator("target_id")
    @classmethod
    def nonblank_target(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("target_id must not be blank")
        return value

    @field_validator("event_at")
    @classmethod
    def aware_event_time(cls, value: datetime | None) -> datetime | None:
        if value is not None and value.tzinfo is None:
            raise ValueError("event_at must include a time zone")
        return value

    @model_validator(mode="after")
    def canonical_target(self):
        if self.kind in {"document", "saved_link", "artifact", "source_ref"}:
            self.target_id = str(UUID(self.target_id))
        elif self.kind in {"message", "finding"}:
            number = int(self.target_id)
            if number <= 0:
                raise ValueError("message ID must be positive")
            self.target_id = str(number)
        if self.kind == "finding":
            if self.answer_excerpt is None or not self.answer_excerpt.strip():
                raise ValueError("finding requires an answer excerpt")
            if len(set(self.source_ref_ids)) != len(self.source_ref_ids):
                raise ValueError("finding source references must be unique")
        elif self.answer_excerpt is not None or self.source_ref_ids:
            raise ValueError("answer excerpt and source references require a finding")
        return self


class NotebookEntry(NotebookEntryInput):
    entry_id: UUID
    source_version: str = Field(min_length=1, max_length=200)
    source_versions: dict[str, str] = Field(default_factory=dict)


class NotebookWrite(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str = Field(min_length=1, max_length=200)
    view: NotebookView
    entries: list[NotebookEntryInput] = Field(default_factory=list, max_length=100)

    @field_validator("title")
    @classmethod
    def nonblank_title(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("title must not be blank")
        return value

    @model_validator(mode="after")
    def unique_entry_ids(self):
        if self.view == "evidence_board" and self.entries:
            raise ValueError("Evidence boards use questions instead of source entries")
        ids = [entry.entry_id for entry in self.entries if entry.entry_id is not None]
        if len(ids) != len(set(ids)):
            raise ValueError("entry_id must be unique within a notebook")
        return self


class NotebookReplace(NotebookWrite):
    expected_revision: int = Field(ge=1)


class ProjectNotebook(BaseModel):
    model_config = ConfigDict(extra="forbid")

    notebook_id: UUID
    project_id: str
    user_name: str
    title: str
    view: NotebookView
    entries: list[NotebookEntry]
    board_state: BoardState | None = None
    revision: int = Field(ge=1)
    created_at: datetime
    updated_at: datetime
