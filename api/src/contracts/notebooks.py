"""HTTP-only notebook request contracts."""

from pydantic import BaseModel, ConfigDict, Field
from uuid import UUID

from common.schema.project_notebook import NotebookEntryInput


class DeleteNotebook(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_revision: int = Field(ge=1)


class SaveFinding(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_revision: int = Field(ge=1)
    assistant_message_id: int = Field(gt=0)
    answer_excerpt: str = Field(min_length=1, max_length=4000)
    source_ref_ids: list[UUID] = Field(default_factory=list, max_length=20)
    note: str | None = Field(default=None, max_length=4000)

    def entry(self) -> NotebookEntryInput:
        return NotebookEntryInput(
            kind="finding", target_id=str(self.assistant_message_id),
            answer_excerpt=self.answer_excerpt,
            source_ref_ids=self.source_ref_ids, note=self.note,
        )


class EvaluateBoardClaim(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_revision: int = Field(ge=1)


class LookupRuledOut(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query: str = Field(min_length=1, max_length=200)
    limit: int = Field(default=20, ge=1, le=20)


class ProposeBoardClaim(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_revision: int = Field(ge=1)
    claim_id: UUID
    assistant_message_id: int = Field(gt=0)
    text: str = Field(min_length=1, max_length=2000)
    source_ref_ids: list[UUID] = Field(default_factory=list, max_length=20)


class AcceptBoardClaim(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_revision: int = Field(ge=1)
