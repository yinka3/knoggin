"""Project-scoped, multi-proposal episode window construction.

This deliberately owns the model-facing representation.  Database rows stay
inside the persistence layer; the model receives a compact evidence brief with
stable local references only.
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
from core.knowledge.episodes.policy import EpisodeGenerationPolicy


@dataclass(slots=True)
class ProjectEpisodeBuild:
    project_id: str
    policy: EpisodeGenerationPolicy
    messages: list[dict[str, Any]]
    prior_episodes: list[Episode]
    build_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    local_message_ids: dict[int, str] = field(default_factory=dict)
    message_ids_by_local: dict[str, int] = field(default_factory=dict)
    local_episode_ids: dict[str, str] = field(default_factory=dict)
    episode_ids_by_local: dict[str, str] = field(default_factory=dict)
    decisions: list[EpisodeDecision] = field(default_factory=list)
    final_episodes: list[Episode] = field(default_factory=list)

    @property
    def message_ids(self) -> list[int]:
        return [int(message["message_id"]) for message in self.messages]

    def prepare_local_references(self) -> None:
        self.messages.sort(
            key=lambda message: (
                message.get("timestamp_ms") is None,
                message.get("timestamp_ms") or 0,
                int(message["message_id"]),
            )
        )
        ids = self.message_ids
        if len(ids) != len(set(ids)):
            raise ValueError("project episode window contains duplicate messages")
        if not all(message.get("session_id") for message in self.messages):
            raise ValueError("project episode messages require source session IDs")
        self.local_message_ids, self.message_ids_by_local = self._catalog_references(
            ids, "message", preserve_order=True
        )
        candidates = [episode for episode in self.prior_episodes if not episode.user_modified]
        self.prior_episodes = candidates
        self.local_episode_ids, self.episode_ids_by_local = self._catalog_references(
            (episode.episode_id for episode in candidates), "episode"
        )

    @staticmethod
    def _catalog_references(
        identifiers, kind: str, *, preserve_order: bool = False
    ) -> tuple[dict[Any, str], dict[str, Any]]:
        values = list(identifiers)
        if not preserve_order:
            values = sorted(set(values), key=lambda value: (isinstance(value, str), value))
        else:
            values = list(dict.fromkeys(values))
        actual_to_local = {
            value: f"{kind}:{position}"
            for position, value in enumerate(values, start=1)
        }
        return actual_to_local, {
            local: value for value, local in actual_to_local.items()
        }

    def evidence_brief(self) -> str:
        """Render readable evidence rather than a serialized persistence payload."""

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
        for message in self.messages:
            session_id = str(message["session_id"])
            if session_id != last_session:
                lines.extend(("", f"Session {session_id}:"))
                last_session = session_id
            message_id = int(message["message_id"])
            local = self.local_message_ids[message_id]
            role = str(message.get("role") or "message").upper()
            content = " ".join(str(message.get("content") or "").split())
            position = self.message_ids.index(message_id) + 1
            lines.append(f"[{local}] source-position={position} {role}: {content}")
            hints = []
            if message.get("user_msg_id"):
                paired_local = self.local_message_ids.get(int(message["user_msg_id"]))
                if paired_local:
                    hints.append(f"paired-with {paired_local}")
            if hints:
                lines.append("  evidence: " + "; ".join(hints))
        if self.prior_episodes:
            lines.extend(("", "Prior project episodes available for revision:"))
            for episode in self.prior_episodes:
                local = self.local_episode_ids[episode.episode_id]
                lines.append(f"[{local}] {episode.summary}")
        return "\n".join(lines)

    def apply_llm_output(self, output: LLMEpisodeWindowDecision) -> list[EpisodeDecision]:
        if not isinstance(output, LLMEpisodeWindowDecision):
            raise TypeError("ProjectEpisodeBuild requires LLMEpisodeWindowDecision")
        decisions: list[EpisodeDecision] = []
        used_sources: set[int] = set()
        used_targets: set[str] = set()
        for proposal in output.proposals:
            payload = proposal.model_dump()
            if proposal.target_episode_id:
                payload["target_episode_id"] = str(resolve_local_id(
                    proposal.target_episode_id, self.episode_ids_by_local
                ))
            payload["message_influences"] = [
                int(resolve_local_id(item, self.message_ids_by_local))
                for item in proposal.message_influences
            ]
            decision = EpisodeDecision.model_validate(payload)
            decision.validate_narrative_character_limit(
                self.policy.max_narrative_chars
            )
            selected = set(decision.message_influences)
            if not selected.issubset(set(self.message_ids)):
                raise ValueError("episode proposal references a message outside the window")
            if selected.intersection(used_sources):
                raise ValueError("episode proposals cannot share source messages")
            if decision.action == "consolidate":
                target = next((item for item in self.prior_episodes if item.episode_id == decision.target_episode_id), None)
                if target is None or target.user_modified:
                    raise ValueError("episode proposal cannot revise this target")
                if target.episode_id in used_targets:
                    raise ValueError("episode proposals must target distinct episodes")
                used_targets.add(target.episode_id)
            used_sources.update(selected)
            decisions.append(decision)
        self.decisions = decisions
        return decisions

    def repair_brief(self, output: LLMEpisodeWindowDecision) -> str:
        """Render the rejected draft as readable repair input, never JSON."""

        lines = [self.evidence_brief(), "", "DRAFT PROPOSALS TO COMPRESS:"]
        for index, proposal in enumerate(output.proposals, start=1):
            lines.append(f"Proposal {index}: {proposal.action}")
            if proposal.target_episode_id:
                lines.append(f"Revision target: {proposal.target_episode_id}")
            lines.append("Messages: " + ", ".join(
                item for item in proposal.message_influences
            ))
            lines.append("Summary: " + (proposal.summary or ""))
            for label, values in (
                ("New developments", proposal.new_developments),
                ("Updates", proposal.updates),
                ("Unresolved", proposal.unresolved),
            ):
                if values:
                    lines.append(f"{label}: " + " | ".join(values))
        return "\n".join(lines)

    def create_episodes(self) -> list[Episode]:
        message_by_id = {int(message["message_id"]): message for message in self.messages}
        episodes: list[Episode] = []
        for decision in self.decisions:
            selected_in_source_order = [
                message_id
                for message_id in self.message_ids
                if message_id in set(decision.message_influences)
            ]
            current = [
                MessageEpisode(
                    message_id=message_id,
                    session_id=str(message_by_id[message_id]["session_id"]),
                    message_position=index,
                )
                for index, message_id in enumerate(selected_in_source_order)
            ]
            target = next((item for item in self.prior_episodes if item.episode_id == decision.target_episode_id), None)
            effective_action = decision.action
            messages = current
            if target is not None:
                merged = { (item.session_id, item.message_id): item for item in target.messages }
                merged.update({(item.session_id, item.message_id): item for item in current})
                messages = [
                    item.model_copy(update={"message_position": position})
                    for position, item in enumerate(merged.values())
                ]
            episode_id = target.episode_id if target else str(uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"knoggin:episode:{self.project_id}:{','.join(str(item.message_id) for item in current)}",
            ))
            episodes.append(Episode(
                episode_id=episode_id,
                project_id=self.project_id,
                summary=decision.summary,
                new_developments=decision.new_developments,
                updates=decision.updates,
                unresolved=decision.unresolved,
                messages=messages,
                generator_metadata={"decision_action": decision.action, "effective_action": effective_action, "episode_policy": self.policy.metadata()},
            ))
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
