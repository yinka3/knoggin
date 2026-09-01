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
from core.knowledge.episodes.policy import (
    EpisodeGenerationPolicy,
    estimate_source_tokens,
)


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
    consolidation_evidence: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

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
            selected_messages = [
                message for message in self.messages
                if int(message["message_id"]) in selected
            ]
            if (
                decision.action == "create"
                and (
                    len(selected_messages) > self.policy.max_episode_source_messages
                    or estimate_source_tokens(selected_messages)
                    > self.policy.max_episode_source_tokens
                )
            ):
                # An over-cap proposal cannot be persisted.  Leaving it out is
                # still a successful evaluation, so the source checkpoint can
                # advance without creating an oversized Episode.
                continue
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

    def preflight_consolidation(
        self,
        decision: EpisodeDecision,
        source_messages: list[dict[str, Any]],
    ) -> bool:
        """Validate and retain complete canonical evidence for one target."""

        if decision.action != "consolidate" or not decision.target_episode_id:
            return False
        target = next(
            (
                episode
                for episode in self.prior_episodes
                if episode.episode_id == decision.target_episode_id
            ),
            None,
        )
        if target is None or target.user_modified:
            return False
        target_ids = {message.message_id for message in target.messages}
        loaded_ids = {int(message["message_id"]) for message in source_messages}
        if target_ids != loaded_ids:
            return False
        current_ids = set(decision.message_influences)
        current_by_id = {
            int(message["message_id"]): message for message in self.messages
        }
        if not current_ids.issubset(current_by_id):
            return False
        combined_by_id = {
            int(message["message_id"]): dict(message) for message in source_messages
        }
        for message_id in current_ids:
            combined_by_id[message_id] = dict(current_by_id[message_id])
        combined = sorted(combined_by_id.values(), key=self._source_order_key)
        if len(combined) > self.policy.max_episode_source_messages:
            return False
        if estimate_source_tokens(combined) > self.policy.max_episode_source_tokens:
            return False
        self.consolidation_evidence[target.episode_id] = combined
        return True

    def keep_consolidation_separate(self, decision: EpisodeDecision) -> None:
        """Turn an invalid consolidation hypothesis into a new Episode proposal."""

        decision.action = "create"
        decision.target_episode_id = None

    def consolidation_brief(self, decision: EpisodeDecision) -> str:
        """Render the complete canonical packet for a second-pass decision."""

        if not decision.target_episode_id:
            raise ValueError("consolidation brief requires a target Episode")
        evidence = self.consolidation_evidence.get(decision.target_episode_id)
        if not evidence:
            raise ValueError("consolidation evidence has not been prepared")
        refs, _ = self._catalog_references(
            (int(message["message_id"]) for message in evidence),
            "message",
            preserve_order=True,
        )
        lines = [
            "COMPLETE EPISODE CONSOLIDATION EVIDENCE",
            "Every source message below is canonical evidence for this decision.",
            "Return every message reference exactly once when consolidating.",
            "",
        ]
        for message in evidence:
            message_id = int(message["message_id"])
            role = str(message.get("role") or "message").upper()
            content = " ".join(str(message.get("content") or "").split())
            lines.append(
                f"[{refs[message_id]}] {role} "
                f"(session={message.get('session_id', '')}): {content}"
            )
        return "\n".join(lines)

    def resolve_consolidation_references(
        self, decision: EpisodeDecision, references: list[str]
    ) -> list[int]:
        """Resolve second-pass local handles against the complete packet."""

        if not decision.target_episode_id:
            raise ValueError("consolidation references require a target Episode")
        evidence = self.consolidation_evidence.get(decision.target_episode_id)
        if not evidence:
            raise ValueError("consolidation evidence has not been prepared")
        _, local_to_actual = self._catalog_references(
            (int(message["message_id"]) for message in evidence),
            "message",
            preserve_order=True,
        )
        return [int(resolve_local_id(reference, local_to_actual)) for reference in references]

    def apply_consolidation_output(
        self,
        decision: EpisodeDecision,
        *,
        action: str,
        summary: str | None,
        new_developments: list[str],
        updates: list[str],
        unresolved: list[str],
        message_ids: list[int],
    ) -> bool:
        """Apply a full-evidence result, or keep the new units separate."""

        if action == "keep_separate":
            self.keep_consolidation_separate(decision)
            return False
        if action != "consolidate" or not decision.target_episode_id:
            self.keep_consolidation_separate(decision)
            return False
        evidence = self.consolidation_evidence.get(decision.target_episode_id)
        if not evidence or set(message_ids) != {
            int(message["message_id"]) for message in evidence
        }:
            self.keep_consolidation_separate(decision)
            return False
        if not summary or not summary.strip():
            self.keep_consolidation_separate(decision)
            return False
        decision.summary = summary.strip()
        decision.new_developments = new_developments
        decision.updates = updates
        decision.unresolved = unresolved
        decision.message_influences = list(message_ids)
        decision.validate_narrative_character_limit(self.policy.max_narrative_chars)
        return True

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
        message_by_id = {
            int(message["message_id"]): message for message in self.messages
        }
        for source_messages in self.consolidation_evidence.values():
            message_by_id.update(
                {int(message["message_id"]): message for message in source_messages}
            )
        episodes: list[Episode] = []
        for decision in self.decisions:
            target = next(
                (
                    item
                    for item in self.prior_episodes
                    if item.episode_id == decision.target_episode_id
                ),
                None,
            )
            complete_source = (
                self.consolidation_evidence.get(target.episode_id, [])
                if target is not None
                else [
                    message
                    for message in self.messages
                    if int(message["message_id"]) in set(decision.message_influences)
                ]
            )
            complete_source = sorted(complete_source, key=self._source_order_key)
            if (
                not complete_source
                or len(complete_source) > self.policy.max_episode_source_messages
                or estimate_source_tokens(complete_source)
                > self.policy.max_episode_source_tokens
            ):
                continue
            selected_in_source_order = [
                int(message["message_id"]) for message in complete_source
            ]
            current = [
                MessageEpisode(
                    message_id=message_id,
                    session_id=str(message_by_id[message_id]["session_id"]),
                    message_position=index,
                )
                for index, message_id in enumerate(selected_in_source_order)
            ]
            effective_action = decision.action
            messages = current
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
