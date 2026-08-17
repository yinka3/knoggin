"""Immutable configuration and admission outcomes for community discussions."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import Enum

from common.schema.settings import CommunitySettings


@dataclass(frozen=True, slots=True)
class CommunityDiscussionPolicy:
    """Rules captured before one discussion claims its active-work lease."""

    version: str
    max_turns: int
    seeding_timeout_seconds: float
    turn_timeout_seconds: float

    @classmethod
    def capture(cls, settings: CommunitySettings) -> "CommunityDiscussionPolicy":
        values = {
            "max_turns": settings.max_turns,
            "seeding_timeout_seconds": float(settings.seeding_timeout_seconds),
            "turn_timeout_seconds": 20 * 60.0,
        }
        encoded = json.dumps(values, sort_keys=True, separators=(",", ":"))
        return cls(
            version=hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16],
            **values,
        )


class CommunityDiscussionAdmissionOutcome(str, Enum):
    """The finite outcomes of attempting to start a discussion."""

    STARTED = "started"
    SKIPPED = "skipped"


@dataclass(frozen=True, slots=True)
class CommunityDiscussionAdmission:
    """The scheduler-facing result of a discussion admission attempt."""

    outcome: CommunityDiscussionAdmissionOutcome
    reason: str
    discussion_id: str | None = None
    policy_version: str | None = None
