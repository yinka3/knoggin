"""Immutable rules captured for one episode-generation operation."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from common.schema.settings import EpisodeSettings, IngestionSettings


@dataclass(frozen=True, slots=True)
class EpisodeGenerationPolicy:
    """Settings that must remain stable while an episode window is processed."""

    version: str
    enabled: bool
    target_message_count: int
    max_message_count: int
    max_age_hours: float | None
    max_sessions_per_run: int
    prior_episode_candidate_count: int

    @classmethod
    def capture(
        cls,
        *,
        settings: EpisodeSettings,
        ingestion_settings: IngestionSettings,
    ) -> "EpisodeGenerationPolicy":
        target_message_count = ingestion_settings.batch_size * settings.batch_multiple
        if settings.max_message_count < target_message_count:
            raise ValueError(
                "Episode max_message_count must be at least the target window size"
            )

        values = {
            "enabled": settings.enabled,
            "target_message_count": target_message_count,
            "max_message_count": settings.max_message_count,
            "max_age_hours": settings.max_age_hours,
            "max_sessions_per_run": settings.max_sessions_per_run,
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
            "max_age_hours": self.max_age_hours,
            "prior_episode_candidate_count": self.prior_episode_candidate_count,
        }
