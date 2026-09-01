"""The workflow-neutral contract for the human-review inbox."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

HumanReviewStatus = Literal["open", "resolved"]
HumanReviewPriority = Literal["low", "normal", "high"]


@dataclass(frozen=True, slots=True)
class HumanReview:
    """An inbox pointer; its subject workflow owns every actual decision."""

    review_id: str
    user_name: str
    project_id: str
    kind: str
    subject_type: str
    subject_id: str
    status: HumanReviewStatus
    priority: HumanReviewPriority
    title: str
    summary: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.status not in {"open", "resolved"}:
            raise ValueError("Human review status must be open or resolved")
        if self.priority not in {"low", "normal", "high"}:
            raise ValueError("Human review priority must be low, normal, or high")
