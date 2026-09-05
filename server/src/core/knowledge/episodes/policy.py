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
    max_episode_source_messages: int
    max_episode_source_tokens: int
    max_narrative_chars: int
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

        values = {
            "enabled": settings.enabled,
            "target_message_count": episode_window_size,
            "max_episode_source_messages": settings.max_episode_source_messages,
            "max_episode_source_tokens": settings.max_episode_source_tokens,
            "max_narrative_chars": settings.max_narrative_chars,
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
            "max_episode_source_messages": self.max_episode_source_messages,
            "max_episode_source_tokens": self.max_episode_source_tokens,
            "max_narrative_chars": self.max_narrative_chars,
            "prompt_narrative_chars": self.prompt_narrative_chars,
            "prior_episode_candidate_count": self.prior_episode_candidate_count,
        }

    def semantic_window_snapshot(self) -> dict[str, object]:
        """Return every generation control needed to replay a claimed window."""

        return {
            "version": self.version,
            "enabled": self.enabled,
            "target_message_count": self.target_message_count,
            "max_episode_source_messages": self.max_episode_source_messages,
            "max_episode_source_tokens": self.max_episode_source_tokens,
            "max_narrative_chars": self.max_narrative_chars,
            "prior_episode_candidate_count": self.prior_episode_candidate_count,
        }

    @classmethod
    def from_semantic_window_snapshot(
        cls, snapshot: object
    ) -> "EpisodeGenerationPolicy":
        """Rehydrate the immutable policy captured with a semantic window."""

        if not isinstance(snapshot, dict):
            raise ValueError("semantic window episode policy must be an object")
        expected = {
            "version",
            "enabled",
            "target_message_count",
            "max_episode_source_messages",
            "max_episode_source_tokens",
            "max_narrative_chars",
            "prior_episode_candidate_count",
        }
        if set(snapshot) != expected:
            raise ValueError("semantic window episode policy has an invalid shape")
        if (
            not isinstance(snapshot["version"], str)
            or not isinstance(snapshot["enabled"], bool)
            or any(
                not isinstance(snapshot[field], int)
                or isinstance(snapshot[field], bool)
                for field in expected - {"version", "enabled"}
            )
        ):
            raise ValueError("semantic window episode policy has invalid values")
        values = {
            "enabled": snapshot["enabled"],
            "target_message_count": snapshot["target_message_count"],
            "max_episode_source_messages": snapshot["max_episode_source_messages"],
            "max_episode_source_tokens": snapshot["max_episode_source_tokens"],
            "max_narrative_chars": snapshot["max_narrative_chars"],
            "prior_episode_candidate_count": snapshot[
                "prior_episode_candidate_count"
            ],
        }
        encoded = json.dumps(values, sort_keys=True, separators=(",", ":"))
        expected_version = hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]
        if snapshot["version"] != expected_version:
            raise ValueError("semantic window episode policy version does not match")
        try:
            policy = cls(
                version=snapshot["version"],
                **values,
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("semantic window episode policy has invalid values") from exc
        if (
            not policy.version
            or not 8 <= policy.target_message_count <= 72
            or policy.max_episode_source_messages <= 0
            or policy.max_episode_source_tokens <= 0
            or policy.max_narrative_chars <= 0
            or policy.prior_episode_candidate_count < 0
        ):
            raise ValueError("semantic window episode policy has invalid values")
        return policy

    @property
    def prompt_narrative_chars(self) -> int:
        """Leave a deterministic 10% generation buffer below the hard cap."""

        return max(1, int(self.max_narrative_chars * 0.9))


def estimate_source_tokens(messages: list[dict]) -> int:
    """Conservatively estimate tokens from canonical source-message text."""

    return sum(
        max(1, (len(str(message.get("content") or "")) + 2) // 3)
        for message in messages
    )
