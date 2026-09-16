"""Project-scoped, multi-proposal episode window construction.

This owns the model-facing representation. Database rows stay in persistence;
the model receives a compact evidence brief with stable local references only.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any

from common.schema.episode.generation import (
    EpisodeDecision,
    LLMEpisodeWindowDecision,
)
from common.schema.episode.models import Episode, MessageEpisode
from common.utils.local_references import resolve_local_id
from core.knowledge.episodes.policy import (
    EpisodeGenerationPolicy,
    estimate_source_tokens,
)


@dataclass(slots=True)
class ProjectEpisodeBuild:
    """Build new immutable episodes from one frozen semantic window."""

    project_id: str
    policy: EpisodeGenerationPolicy
    messages: list[dict[str, Any]]
    build_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    local_message_ids: dict[int, str] = field(default_factory=dict)
    message_ids_by_local: dict[str, int] = field(default_factory=dict)
    decisions: list[EpisodeDecision] = field(default_factory=list)
    final_episodes: list[Episode] = field(default_factory=list)

    @property
    def message_ids(self) -> list[int]:
        return [int(message["message_id"]) for message in self.messages]

    def prepare_local_references(self) -> None:
        self.messages.sort(key=self._source_order_key)
        ids = self.message_ids
        if len(ids) != len(set(ids)):
            raise ValueError("project episode window contains duplicate messages")
        if not all(message.get("session_id") for message in self.messages):
            raise ValueError("project episode messages require source session IDs")
        self.local_message_ids = {
            message_id: f"message:{position}"
            for position, message_id in enumerate(ids, start=1)
        }
        self.message_ids_by_local = {
            local: message_id for message_id, local in self.local_message_ids.items()
        }

    def evidence_brief(self) -> str:
        """Render readable window evidence instead of a persistence payload."""

        if not self.local_message_ids:
            raise ValueError("episode local references have not been prepared")
        lines = [
            "PROJECT EPISODE EVIDENCE CATALOG",
            "Use only catalog handles that appear below. Group only genuinely coherent material.",
            "A paired user/assistant turn is one conversational unit.",
            "",
            "SOURCE MESSAGES:",
        ]
        last_session: str | None = None
        for position, message in enumerate(self.messages, start=1):
            session_id = str(message["session_id"])
            if session_id != last_session:
                lines.extend(("", f"Session {session_id}:"))
                last_session = session_id
            message_id = int(message["message_id"])
            local = self.local_message_ids[message_id]
            role = str(message.get("role") or "message").upper()
            content = " ".join(str(message.get("content") or "").split())
            lines.append(f"[{local}] source-position={position} {role}: {content}")
            if message.get("user_msg_id"):
                paired_local = self.local_message_ids.get(int(message["user_msg_id"]))
                if paired_local:
                    lines.append(f"  evidence: paired-with {paired_local}")
        return "\n".join(lines)

    def apply_llm_output(
        self, output: LLMEpisodeWindowDecision
    ) -> list[EpisodeDecision]:
        if not isinstance(output, LLMEpisodeWindowDecision):
            raise TypeError("ProjectEpisodeBuild requires LLMEpisodeWindowDecision")
        decisions: list[EpisodeDecision] = []
        used_sources: set[int] = set()
        source_ids = set(self.message_ids)
        for proposal in output.proposals:
            decision = EpisodeDecision.model_validate(
                {
                    **proposal.model_dump(),
                    "message_influences": [
                        int(resolve_local_id(item, self.message_ids_by_local))
                        for item in proposal.message_influences
                    ],
                }
            )
            decision.validate_narrative_character_limit(
                self.policy.max_narrative_chars
            )
            selected = set(decision.message_influences)
            if not selected.issubset(source_ids):
                raise ValueError("episode proposal references a message outside the window")
            if selected.intersection(used_sources):
                raise ValueError("episode proposals cannot share source messages")
            selected_messages = [
                message
                for message in self.messages
                if int(message["message_id"]) in selected
            ]
            self._validate_source_limits(selected_messages)
            used_sources.update(selected)
            decisions.append(decision)
        self.decisions = decisions
        return decisions

    def _validate_source_limits(self, source_messages: list[dict[str, Any]]) -> None:
        if not source_messages:
            raise ValueError("episode proposals require source messages")
        if len(source_messages) > self.policy.max_episode_source_messages:
            raise ValueError(
                "episode proposal exceeds the configured source message limit"
            )
        if estimate_source_tokens(source_messages) > self.policy.max_episode_source_tokens:
            raise ValueError(
                "episode proposal exceeds the configured source token limit"
            )

    @staticmethod
    def _source_order_key(message: dict[str, Any]) -> tuple[bool, int, int]:
        return (
            message.get("timestamp_ms") is None,
            message.get("timestamp_ms") or 0,
            int(message["message_id"]),
        )

    def repair_brief(self, output: LLMEpisodeWindowDecision) -> str:
        """Render the rejected draft as readable repair input, never JSON."""

        lines = [self.evidence_brief(), "", "DRAFT PROPOSALS TO COMPRESS:"]
        for index, proposal in enumerate(output.proposals, start=1):
            lines.append(f"Proposal {index}:")
            lines.append("Messages: " + ", ".join(proposal.message_influences))
            lines.append("Summary: " + proposal.summary)
            for label, values in (
                ("New developments", proposal.new_developments),
                ("Updates", proposal.updates),
                ("Unresolved", proposal.unresolved),
            ):
                if values:
                    lines.append(f"{label}: " + " | ".join(values))
        return "\n".join(lines)

    def create_episodes(self) -> list[Episode]:
        """Materialize validated new episodes with stable source-derived IDs."""

        episodes: list[Episode] = []
        for decision in self.decisions:
            selected = set(decision.message_influences)
            source_messages = [
                message
                for message in self.messages
                if int(message["message_id"]) in selected
            ]
            if len(source_messages) != len(decision.message_influences):
                raise ValueError("episode proposal references a message outside the window")
            current = [
                MessageEpisode(
                    message_id=int(message["message_id"]),
                    session_id=str(message["session_id"]),
                    message_position=index,
                )
                for index, message in enumerate(source_messages)
            ]
            episode_id = str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    "knoggin:episode:"
                    f"{self.project_id}:"
                    f"{','.join(str(item.message_id) for item in current)}",
                )
            )
            episodes.append(
                Episode(
                    episode_id=episode_id,
                    project_id=self.project_id,
                    summary=decision.summary,
                    new_developments=decision.new_developments,
                    updates=decision.updates,
                    unresolved=decision.unresolved,
                    messages=current,
                    generator_metadata={"episode_policy": self.policy.metadata()},
                )
            )
        self.final_episodes = episodes
        return episodes

    def attach_embeddings(self, embeddings: list[list[float]]) -> list[Episode]:
        if len(embeddings) != len(self.final_episodes):
            raise RuntimeError("episode embedding service returned an invalid result")
        self.final_episodes = [
            episode.validated_copy(update={"embedding": embedding})
            for episode, embedding in zip(self.final_episodes, embeddings)
        ]
        return self.final_episodes
