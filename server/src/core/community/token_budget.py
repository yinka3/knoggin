"""Shared approximate token accounting for one AAC discussion."""

from __future__ import annotations

from dataclasses import dataclass, field
from threading import Lock
from typing import Mapping


@dataclass
class AACTokenBudget:
    """A soft discussion cap shared by all AAC-caused model calls."""

    limit: int
    used: int = 0
    approximate: bool = False
    _lock: Lock = field(default_factory=Lock, init=False, repr=False)

    def __post_init__(self) -> None:
        if isinstance(self.limit, bool) or not isinstance(self.limit, int):
            raise ValueError("AAC token budget limit must be an integer")
        if self.limit < 0:
            raise ValueError("AAC token budget limit must be non-negative")
        if isinstance(self.used, bool) or not isinstance(self.used, int) or self.used < 0:
            raise ValueError("AAC token usage must be a non-negative integer")

    def allow_call(self) -> bool:
        """Return whether another model call may begin."""

        with self._lock:
            return self.used < self.limit

    @property
    def token_budget(self) -> int:
        return self.limit

    @property
    def tokens_used(self) -> int:
        return self.used

    def record(self, usage: Mapping[str, object]) -> int:
        """Add one provider or approximate usage record and return the total."""

        total = usage.get("total_tokens")
        if total is None:
            total = int(usage.get("prompt_tokens") or 0) + int(
                usage.get("completion_tokens") or 0
            )
        if isinstance(total, bool) or not isinstance(total, int) or total < 0:
            raise ValueError("AAC model usage must contain non-negative token counts")
        with self._lock:
            self.used += total
            self.approximate = self.approximate or bool(usage.get("approximate"))
            return self.used
