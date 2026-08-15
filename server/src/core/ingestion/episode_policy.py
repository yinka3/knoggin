"""Immutable rules captured for one episode-generation operation."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from common.schema.settings import EpisodeSettings


@dataclass(frozen=True, slots=True)
class EpisodeGenerationPolicy:
    """Settings that must remain stable while an episode window is processed."""

    version: str
    enabled: bool
    target_message_count: int
    max_message_count: int
    max_narrative_chars: int
    max_age_hours: float | None
    prior_episode_candidate_count: int

    @classmethod
    def capture(
        cls,
        *,
        settings: EpisodeSettings,
        episode_window_size: int,
    ) -> "EpisodeGenerationPolicy":
        if not 8 <= episode_window_size <= 72:
            raise ValueError("episode_window_size must be between 8 and 72")
        target_message_count = episode_window_size
        if settings.max_message_count < target_message_count:
            raise ValueError(
                "Episode max_message_count must be at least the target window size"
            )

        values = {
            "enabled": settings.enabled,
            "target_message_count": target_message_count,
            "max_message_count": settings.max_message_count,
            "max_narrative_chars": settings.max_narrative_chars,
            "max_age_hours": settings.max_age_hours,
            "prior_episode_candidate_count": settings.prior_episode_candidate_count,
        }
        encoded = json.dumps(values, sort_keys=True, separators=(",", ":"))
        return cls(
            version=hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16],
            **values,
        )

    def metadata(self) -> dict[str, object]:
        """Return the compact durable policy snapshot for an episode."""

        return {
            "version": self.version,
            "target_message_count": self.target_message_count,
            "max_message_count": self.max_message_count,
            "max_narrative_chars": self.max_narrative_chars,
            "prompt_narrative_chars": self.prompt_narrative_chars,
            "max_age_hours": self.max_age_hours,
            "prior_episode_candidate_count": self.prior_episode_candidate_count,
        }

    @property
    def prompt_narrative_chars(self) -> int:
        """Leave a deterministic 10% generation buffer below the hard cap."""

        return max(1, int(self.max_narrative_chars * 0.9))
