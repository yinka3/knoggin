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
from common.schema.episode.models import (
    EntityEpisode,
    Episode,
    MessageEpisode,
    RelationshipEpisode,
)
from common.utils.local_references import build_local_id_maps, resolve_local_id
from core.ingestion.episode_policy import EpisodeGenerationPolicy


@dataclass(slots=True)
class ProjectEpisodeBuild:
    project_id: str
    policy: EpisodeGenerationPolicy
    messages: list[dict[str, Any]]
    entity_ids_by_message: dict[int, list[int]]
    relationship_ids_by_message: dict[int, list[str]]
    entity_catalog: list[dict[str, Any]]
    relationship_catalog: list[dict[str, Any]]
    prior_episodes: list[Episode]
    build_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    local_message_ids: dict[int, str] = field(default_factory=dict)
    message_ids_by_local: dict[str, int] = field(default_factory=dict)
    local_entity_ids: dict[int, str] = field(default_factory=dict)
    entity_ids_by_local: dict[str, int] = field(default_factory=dict)
    local_relationship_ids: dict[str, str] = field(default_factory=dict)
    relationship_ids_by_local: dict[str, str] = field(default_factory=dict)
    local_episode_ids: dict[str, str] = field(default_factory=dict)
    episode_ids_by_local: dict[str, str] = field(default_factory=dict)
    decisions: list[EpisodeDecision] = field(default_factory=list)
    final_episodes: list[Episode] = field(default_factory=list)

    @property
    def message_ids(self) -> list[int]:
        return [int(message["message_id"]) for message in self.messages]

    def prepare_local_references(self) -> None:
        ids = self.message_ids
        if len(ids) != len(set(ids)):
            raise ValueError("project episode window contains duplicate messages")
        if not all(message.get("session_id") for message in self.messages):
            raise ValueError("project episode messages require source session IDs")
        self.local_message_ids, self.message_ids_by_local = build_local_id_maps(ids, "m")
        entity_ids = sorted({item for values in self.entity_ids_by_message.values() for item in values})
        relationship_ids = sorted({item for values in self.relationship_ids_by_message.values() for item in values})
        self.local_entity_ids, self.entity_ids_by_local = build_local_id_maps(entity_ids, "e")
        self.local_relationship_ids, self.relationship_ids_by_local = build_local_id_maps(relationship_ids, "r")
        candidates = [episode for episode in self.prior_episodes if not episode.user_modified]
        self.prior_episodes = candidates
        self.local_episode_ids, self.episode_ids_by_local = build_local_id_maps(
            (episode.episode_id for episode in candidates), "ep"
        )

    def evidence_brief(self) -> str:
        """Render readable evidence rather than a serialized persistence payload."""

        if not self.local_message_ids:
            raise ValueError("episode local references have not been prepared")
        lines = [
            "PROJECT EPISODE WINDOW",
            "Use only the local references below. Group only genuinely coherent material.",
            "A paired user/assistant turn is one conversational unit.",
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
            lines.append(f"[{local}] {role}: {content}")
            hints = []
            if message.get("user_msg_id"):
                paired_local = self.local_message_ids.get(int(message["user_msg_id"]))
                if paired_local:
                    hints.append(f"paired-with {paired_local}")
            entities = [self.local_entity_ids[item] for item in self.entity_ids_by_message.get(message_id, [])]
            relationships = [self.local_relationship_ids[item] for item in self.relationship_ids_by_message.get(message_id, [])]
            if entities:
                hints.append("entities " + ", ".join(entities))
            if relationships:
                hints.append("relationships " + ", ".join(relationships))
            if hints:
                lines.append("  hints: " + "; ".join(hints))
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
                {
                    **item.model_dump(),
                    "message_id": int(resolve_local_id(item.message_id, self.message_ids_by_local)),
                }
                for item in proposal.message_influences
            ]
            payload["focus_entities"] = [
                {**item.model_dump(), "entity_id": int(resolve_local_id(item.entity_id, self.entity_ids_by_local))}
                for item in proposal.focus_entities
            ]
            payload["central_relationships"] = [
                {**item.model_dump(), "relationship_id": str(resolve_local_id(item.relationship_id, self.relationship_ids_by_local))}
                for item in proposal.central_relationships
            ]
            decision = EpisodeDecision.model_validate(payload)
            decision.validate_narrative_character_limit(
                self.policy.max_narrative_chars
            )
            selected = {item.message_id for item in decision.message_influences}
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
                item.message_id for item in proposal.message_influences
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
            current = [
                MessageEpisode(
                    message_id=message_id,
                    session_id=str(message_by_id[message_id]["session_id"]),
                    influence_weight=influence.influence_weight,
                    influence_reason=influence.influence_reason,
                    message_position=index,
                )
                for index, (message_id, influence) in enumerate(
                    (item.message_id, item) for item in decision.message_influences
                )
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
                session_id=current[0].session_id,
                summary=decision.summary,
                new_developments=decision.new_developments,
                updates=decision.updates,
                unresolved=decision.unresolved,
                importance=decision.importance,
                messages=messages,
                entities=[EntityEpisode(entity_id=item.entity_id, prominence_weight=item.prominence_weight, role=item.role, is_focus_entity=True) for item in decision.focus_entities],
                relationships=[RelationshipEpisode(relationship_id=item.relationship_id, prominence_weight=item.prominence_weight, is_central_relationship=True) for item in decision.central_relationships],
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
