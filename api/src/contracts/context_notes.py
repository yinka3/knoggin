"""Expected-revision writes to the controlled Context user notes."""

from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class NoteCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_revision_id: UUID | None
    section_key: str = Field(min_length=1)
    markdown: str = Field(min_length=1, max_length=50_000)


class NoteReplace(NoteCreate):
    pass


class NoteDelete(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_revision_id: UUID | None


class DiscardLocalEdits(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_file_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
