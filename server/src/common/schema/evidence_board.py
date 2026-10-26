"""Bounded user-owned evidence-board questions and evaluated claims."""

from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator


class _BoardModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class BoardClaimInput(_BoardModel):
    claim_id: UUID
    text: str = Field(min_length=1, max_length=2000)
    source_ref_ids: list[UUID] = Field(default_factory=list, max_length=20)

    @model_validator(mode="after")
    def unique_sources(self):
        if not self.text.strip() or len(set(self.source_ref_ids)) != len(self.source_ref_ids):
            raise ValueError("Claim text and source references must be valid")
        return self


class BoardQuestionInput(_BoardModel):
    question_id: UUID
    text: str = Field(min_length=1, max_length=1000)
    depends_on: UUID | None = None
    claims: list[BoardClaimInput] = Field(default_factory=list, max_length=10)

    @model_validator(mode="after")
    def unique_claims(self):
        if not self.text.strip() or len({claim.claim_id for claim in self.claims}) != len(self.claims):
            raise ValueError("Question text and claim IDs must be valid")
        return self


class BoardWrite(_BoardModel):
    expected_revision: int = Field(ge=1)
    questions: list[BoardQuestionInput] = Field(default_factory=list, max_length=5)
    refresh_claim_ids: list[UUID] = Field(default_factory=list, max_length=50)

    @model_validator(mode="after")
    def valid_dependencies(self):
        seen: set[UUID] = set()
        claim_ids: set[UUID] = set()
        for question in self.questions:
            if question.question_id in seen:
                raise ValueError("Question IDs must be unique")
            if question.depends_on is not None and question.depends_on not in seen:
                raise ValueError("A dependent question must link to an earlier question")
            seen.add(question.question_id)
            for claim in question.claims:
                if claim.claim_id in claim_ids:
                    raise ValueError("Claim IDs must be unique across the board")
                claim_ids.add(claim.claim_id)
        if len(set(self.refresh_claim_ids)) != len(self.refresh_claim_ids) or not set(self.refresh_claim_ids) <= claim_ids:
            raise ValueError("Refresh IDs must identify claims on this board")
        return self


class BoardConfirmation(_BoardModel):
    expected_revision: int = Field(ge=1)
    rationale: str = Field(min_length=1, max_length=4000)
    scope_description: str = Field(min_length=1, max_length=2000)
    reconsider_if: str = Field(min_length=1, max_length=2000)


class BoardJudgment(_BoardModel):
    verdict: Literal["support", "conflict", "insufficient_evidence"]
    reason: str = Field(min_length=1, max_length=1000)
    confidence: float = Field(ge=0, le=1)
    evaluated_at: datetime
    fingerprint: str = Field(min_length=64, max_length=64)
    citation_versions: dict[str, str] = Field(default_factory=dict)
    dependency_fingerprint: str | None = None
    confirmed_at: datetime | None = None
    review_id: UUID | None = None
    stale: bool = False
    active_conclusion: bool = False


class BoardClaim(BoardClaimInput):
    source_versions: dict[str, str] = Field(default_factory=dict)
    citation_statuses: dict[str, Literal["current", "changed", "missing"]] = Field(
        default_factory=dict
    )
    proposed_by: Literal["user", "agent"] = "user"
    origin_message_id: int | None = Field(default=None, gt=0)
    accepted: bool = True
    judgment: BoardJudgment | None = None


class BoardQuestion(BoardQuestionInput):
    claims: list[BoardClaim] = Field(default_factory=list, max_length=10)


class BoardState(_BoardModel):
    questions: list[BoardQuestion] = Field(default_factory=list, max_length=5)
