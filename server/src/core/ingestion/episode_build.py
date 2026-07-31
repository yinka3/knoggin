"""Workflow-owned state for constructing one episodic-memory window."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from typing import Optional

from common.schema.episode import (
    EntityEpisode,
    Episode,
    MessageEpisode,
    RelationshipEpisode,
)
from common.schema.episode_output import (
    EpisodeConsolidation,
    EpisodeDecision,
    LLMEpisodeConsolidation,
    LLMEpisodeDecision,
)
from common.utils.local_references import build_local_id_maps, resolve_local_id


@dataclass(slots=True)
class EpisodeBuild:
    """Mutable owner of all pre-persistence state for one episode window."""

    project_id: str
    session_id: str
    messages: list[dict]
    entity_ids_by_message: dict[int, list[int]]
    relationship_ids_by_message: dict[int, list[str]]
    entity_catalog: list[dict]
    relationship_catalog: list[dict]
    prior_episodes: list[Episode]
    local_message_ids: dict[int, str] = field(default_factory=dict)
    message_ids_by_local: dict[str, int] = field(default_factory=dict)
    local_entity_ids: dict[int, str] = field(default_factory=dict)
    entity_ids_by_local: dict[str, int] = field(default_factory=dict)
    local_relationship_ids: dict[str, str] = field(default_factory=dict)
    relationship_ids_by_local: dict[str, str] = field(default_factory=dict)
    local_episode_ids: dict[str, str] = field(default_factory=dict)
    episode_ids_by_local: dict[str, str] = field(default_factory=dict)
    decision: Optional[EpisodeDecision] = None
    consolidation: Optional[EpisodeConsolidation] = None
    final_episode: Optional[Episode] = None
    action: Optional[str] = None
    consolidation_limit_hit: bool = False
    persisted: bool = False
    issues: list[str] = field(default_factory=list)
    sealed: bool = False
    released: bool = False

    @classmethod
    def from_window(
        cls,
        *,
        project_id: str,
        session_id: str,
        messages: list[dict],
        entity_ids_by_message: dict[int, list[int]],
        relationship_ids_by_message: dict[int, list[str]],
        entity_catalog: list[dict],
        relationship_catalog: list[dict],
        prior_episodes: list[Episode],
    ) -> "EpisodeBuild":
        """Allocate one build with ownership of a bounded episode window."""

        return cls(
            project_id=project_id,
            session_id=session_id,
            messages=[dict(message) for message in messages],
            entity_ids_by_message={
                message_id: list(entity_ids)
                for message_id, entity_ids in entity_ids_by_message.items()
            },
            relationship_ids_by_message={
                message_id: list(relationship_ids)
                for message_id, relationship_ids in relationship_ids_by_message.items()
            },
            entity_catalog=[dict(entity) for entity in entity_catalog],
            relationship_catalog=[dict(relationship) for relationship in relationship_catalog],
            prior_episodes=list(prior_episodes),
        )

    @property
    def message_ids(self) -> list[int]:
        return [int(message["message_id"]) for message in self.messages]

    @property
    def entity_ids(self) -> list[int]:
        return sorted(
            {
                entity_id
                for message_entity_ids in self.entity_ids_by_message.values()
                for entity_id in message_entity_ids
            }
        )

    @property
    def relationship_ids(self) -> list[str]:
        return sorted(
            {
                relationship_id
                for message_relationship_ids in self.relationship_ids_by_message.values()
                for relationship_id in message_relationship_ids
            }
        )

    @property
    def outcome_action(self) -> str:
        """Return the durable action after episode creation or persistence."""

        if self.final_episode is None:
            return "skip"
        return str(self.final_episode.generator_metadata["effective_action"])

    @property
    def outcome_episode_id(self) -> str | None:
        return self.final_episode.episode_id if self.final_episode else None

    @property
    def source_message_count(self) -> int:
        return len(self.messages)

    @property
    def episode_source_message_count(self) -> int:
        return len(self.final_episode.messages) if self.final_episode else 0

    @property
    def entity_link_count(self) -> int:
        return len(self.final_episode.entities) if self.final_episode else 0

    @property
    def relationship_link_count(self) -> int:
        return len(self.final_episode.relationships) if self.final_episode else 0

    def _require_active(self) -> None:
        if self.released:
            raise RuntimeError("EpisodeBuild has been released")
        if self.sealed:
            raise RuntimeError("EpisodeBuild has been sealed")

    def validate_source_window(self) -> None:
        """Verify the bounded source window before model-visible IDs are made."""

        self._require_active()
        if not self.project_id or not self.session_id:
            raise ValueError("EpisodeBuild requires project and session scope")
        if not self.messages:
            raise ValueError("EpisodeBuild requires at least one source message")
        message_ids = self.message_ids
        if any(message_id <= 0 for message_id in message_ids):
            raise ValueError("EpisodeBuild source message IDs must be positive")
        if len(message_ids) != len(set(message_ids)):
            raise ValueError("EpisodeBuild source message IDs must be unique")
        expected_message_ids = set(message_ids)
        if set(self.entity_ids_by_message) != expected_message_ids:
            raise ValueError("EpisodeBuild entity references must cover source messages")
        if set(self.relationship_ids_by_message) != expected_message_ids:
            raise ValueError(
                "EpisodeBuild relationship references must cover source messages"
            )

    def prepare_local_references(self) -> None:
        """Build the only identifiers exposed to the LLM for this build."""

        self.validate_source_window()
        (
            self.local_message_ids,
            self.message_ids_by_local,
        ) = build_local_id_maps(self.message_ids, "m")
        (
            self.local_entity_ids,
            self.entity_ids_by_local,
        ) = build_local_id_maps(self.entity_ids, "e")
        (
            self.local_relationship_ids,
            self.relationship_ids_by_local,
        ) = build_local_id_maps(self.relationship_ids, "r")
        (
            self.local_episode_ids,
            self.episode_ids_by_local,
        ) = build_local_id_maps(
            (episode.episode_id for episode in self.prior_episodes),
            "ep",
        )

    def generation_payload(self) -> str:
        """Render canonical window evidence using only local identifiers."""

        self._require_prepared_references()
        payload = self._localized_context_payload(message_key="messages")
        payload["prior_episodes"] = [
            {
                "episode_id": self.local_episode_ids[episode.episode_id],
                "summary": episode.summary,
                "new_developments": episode.new_developments,
                "updates": episode.updates,
                "unresolved": episode.unresolved,
                "importance": episode.importance,
            }
            for episode in self.prior_episodes
        ]
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)

    def consolidation_payload(self, target_episode_id: str) -> str:
        """Render complete consolidation evidence for one selected episode."""

        self._require_prepared_references()
        try:
            target_local_id = self.local_episode_ids[target_episode_id]
        except KeyError as exc:
            raise ValueError(
                "Episode consolidation target is not in the supplied context"
            ) from exc
        payload = self._localized_context_payload(message_key="source_messages")
        payload["target_episode_id"] = target_local_id
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)

    def apply_llm_decision(self, output: LLMEpisodeDecision) -> EpisodeDecision:
        """Resolve, validate, and retain strict model output in this build."""

        self._require_active()
        self._require_prepared_references()
        if not isinstance(output, LLMEpisodeDecision):
            raise TypeError("EpisodeBuild requires an LLMEpisodeDecision")
        payload = output.model_dump()
        if output.target_episode_id is not None:
            payload["target_episode_id"] = str(
                resolve_local_id(output.target_episode_id, self.episode_ids_by_local)
            )
        payload.update(self._resolve_ranked_references(output))
        decision = EpisodeDecision.model_validate(payload)
        self.decision = decision
        self.action = decision.action
        self.validate_resolved_decision()
        return decision

    def apply_llm_consolidation(
        self,
        output: LLMEpisodeConsolidation,
        *,
        target_episode_id: str,
    ) -> EpisodeDecision:
        """Resolve a consolidation response into the build's final decision."""

        self._require_active()
        self._require_prepared_references()
        if not isinstance(output, LLMEpisodeConsolidation):
            raise TypeError("EpisodeBuild requires an LLMEpisodeConsolidation")
        payload = output.model_dump()
        payload.update(self._resolve_ranked_references(output))
        consolidation = EpisodeConsolidation.model_validate(payload)
        self._validate_ranked_output(consolidation, "consolidation")
        self.consolidation = consolidation
        self.decision = EpisodeDecision(
            action="consolidate",
            target_episode_id=target_episode_id,
            **consolidation.model_dump(),
        )
        self.action = self.decision.action
        self.validate_resolved_decision()
        return self.decision

    def validate_resolved_decision(self) -> None:
        """Ensure resolved output exactly covers canonical build input."""

        self._require_active()
        if self.decision is None:
            raise ValueError("EpisodeBuild has no resolved decision")
        if self.decision.action == "skip":
            return
        self._validate_ranked_output(self.decision, "decision")
        if self.decision.action == "consolidate":
            candidate_ids = {episode.episode_id for episode in self.prior_episodes}
            if self.decision.target_episode_id not in candidate_ids:
                raise ValueError(
                    "Episode decision consolidation target must be a prior candidate"
                )

    def create_episode(
        self,
        *,
        max_message_count: int,
        max_age_hours: Optional[float],
    ) -> Optional[Episode]:
        """Build the strict persisted episode after all decisions are resolved."""

        self._require_active()
        self.validate_resolved_decision()
        assert self.decision is not None
        if self.decision.action == "skip":
            self.final_episode = None
            return None

        current_messages = self._messages_from_decision(self.decision)
        target_episode = next(
            (
                episode
                for episode in self.prior_episodes
                if episode.episode_id == self.decision.target_episode_id
            ),
            None,
        )
        should_create = self.decision.action == "create" or target_episode is None
        messages = current_messages
        self.consolidation_limit_hit = False
        if not should_create:
            messages = self._combine_messages(target_episode.messages, current_messages)
            if self._exceeds_consolidation_limits(
                target_episode,
                message_count=len(messages),
                max_message_count=max_message_count,
                max_age_hours=max_age_hours,
            ):
                should_create = True
                self.consolidation_limit_hit = True
                messages = current_messages

        episode_id = (
            self._episode_id_for_window(current_messages)
            if should_create
            else target_episode.episode_id
        )
        self.final_episode = Episode(
            episode_id=episode_id,
            project_id=self.project_id,
            session_id=self.session_id,
            summary=self.decision.summary,
            new_developments=self.decision.new_developments,
            updates=self.decision.updates,
            unresolved=self.decision.unresolved,
            importance=self.decision.importance,
            messages=messages,
            entities=[
                EntityEpisode(
                    entity_id=focus.entity_id,
                    prominence_weight=focus.prominence_weight,
                    role=focus.role,
                    is_focus_entity=True,
                )
                for focus in self.decision.focus_entities
            ],
            relationships=[
                RelationshipEpisode(
                    relationship_id=relationship.relationship_id,
                    prominence_weight=relationship.prominence_weight,
                    is_central_relationship=True,
                )
                for relationship in self.decision.central_relationships
            ],
            generator_metadata={
                "decision_action": self.decision.action,
                "effective_action": "create" if should_create else "consolidate",
                "consolidated": (
                    not should_create and self.decision.action == "consolidate"
                ),
                "consolidation_limit_hit": self.consolidation_limit_hit,
            },
        )
        return self.final_episode

    def attach_embedding(self, embedding: list[float]) -> Episode:
        self._require_active()
        if self.final_episode is None:
            raise ValueError("EpisodeBuild cannot embed a skipped episode")
        self.final_episode = Episode.model_validate(
            {**self.final_episode.model_dump(), "embedding": embedding}
        )
        return self.final_episode

    def validate_for_persistence(self) -> None:
        self._require_active()
        if self.decision is None:
            raise ValueError("EpisodeBuild requires a resolved decision")
        if self.decision.action != "skip" and self.final_episode is None:
            raise ValueError("EpisodeBuild requires a final episode before persistence")

    def mark_persisted(self) -> None:
        self.validate_for_persistence()
        self.persisted = True
        self.sealed = True

    def release(self) -> None:
        if self.released:
            return
        self.local_message_ids.clear()
        self.message_ids_by_local.clear()
        self.local_entity_ids.clear()
        self.entity_ids_by_local.clear()
        self.local_relationship_ids.clear()
        self.relationship_ids_by_local.clear()
        self.local_episode_ids.clear()
        self.episode_ids_by_local.clear()
        self.released = True

    def _require_prepared_references(self) -> None:
        if not self.local_message_ids:
            raise ValueError("EpisodeBuild local references have not been prepared")

    def _localized_context_payload(self, *, message_key: str) -> dict:
        return {
            message_key: [
                {
                    "message_id": self.local_message_ids[int(message["message_id"])],
                    "role": message.get("role"),
                    "content": message.get("content"),
                    "timestamp_ms": message.get("timestamp_ms"),
                }
                for message in self.messages
            ],
            "entity_refs_by_message": {
                self.local_message_ids[int(message["message_id"])]: [
                    self.local_entity_ids[entity_id]
                    for entity_id in self.entity_ids_by_message.get(
                        int(message["message_id"]), []
                    )
                ]
                for message in self.messages
            },
            "relationship_refs_by_message": {
                self.local_message_ids[int(message["message_id"])]: [
                    self.local_relationship_ids[relationship_id]
                    for relationship_id in self.relationship_ids_by_message.get(
                        int(message["message_id"]), []
                    )
                ]
                for message in self.messages
            },
            "entity_catalog": [
                {
                    "entity_id": self.local_entity_ids[int(entity["entity_id"])],
                    "canonical_name": entity.get("canonical_name"),
                    "type": entity.get("type"),
                    "aliases": entity.get("aliases", []),
                }
                for entity in self.entity_catalog
            ],
            "relationship_catalog": [
                {
                    "relationship_id": self.local_relationship_ids[
                        str(relationship["relationship_id"])
                    ],
                    "entity_a": self._render_relationship_endpoint(
                        relationship["entity_a"]
                    ),
                    "entity_b": self._render_relationship_endpoint(
                        relationship["entity_b"]
                    ),
                    "relationship_type": relationship.get("relationship_type"),
                    "confidence": relationship.get("confidence"),
                    "context": relationship.get("context"),
                    "evidence_message_ids": [
                        self.local_message_ids[int(message_id)]
                        for message_id in relationship.get("evidence_message_ids", [])
                    ],
                }
                for relationship in self.relationship_catalog
            ],
        }

    def _render_relationship_endpoint(self, endpoint: dict) -> dict:
        rendered = {
            "canonical_name": endpoint.get("canonical_name"),
            "type": endpoint.get("type"),
        }
        local_entity_id = self.local_entity_ids.get(int(endpoint["entity_id"]))
        if local_entity_id is not None:
            rendered["entity_id"] = local_entity_id
        return rendered

    def _resolve_ranked_references(
        self, output: LLMEpisodeDecision | LLMEpisodeConsolidation
    ) -> dict:
        return {
            "message_influences": [
                {
                    **influence.model_dump(),
                    "message_id": int(
                        resolve_local_id(influence.message_id, self.message_ids_by_local)
                    ),
                }
                for influence in output.message_influences
            ],
            "focus_entities": [
                {
                    **focus.model_dump(),
                    "entity_id": int(
                        resolve_local_id(focus.entity_id, self.entity_ids_by_local)
                    ),
                }
                for focus in output.focus_entities
            ],
            "central_relationships": [
                {
                    **relationship.model_dump(),
                    "relationship_id": str(
                        resolve_local_id(
                            relationship.relationship_id,
                            self.relationship_ids_by_local,
                        )
                    ),
                }
                for relationship in output.central_relationships
            ],
        }

    def _validate_ranked_output(
        self,
        output: EpisodeDecision | EpisodeConsolidation,
        output_name: str,
    ) -> None:
        source_message_ids = set(self.message_ids)
        influence_message_ids = [
            influence.message_id for influence in output.message_influences
        ]
        if (
            len(influence_message_ids) != len(set(influence_message_ids))
            or set(influence_message_ids) != source_message_ids
        ):
            raise ValueError(
                f"Episode {output_name} message influences must cover each source "
                "message exactly once"
            )

        focus_entity_ids = [focus.entity_id for focus in output.focus_entities]
        if len(focus_entity_ids) != len(set(focus_entity_ids)):
            raise ValueError(f"Episode {output_name} focus entities must be unique")
        if len(focus_entity_ids) > 2:
            raise ValueError(
                f"Episode {output_name} may select at most two focus entities"
            )
        if not set(focus_entity_ids).issubset(self.entity_ids):
            raise ValueError(
                f"Episode {output_name} focus entities must belong to the source window"
            )

        central_relationship_ids = [
            relationship.relationship_id for relationship in output.central_relationships
        ]
        if len(central_relationship_ids) != len(set(central_relationship_ids)):
            raise ValueError(
                f"Episode {output_name} central relationships must be unique"
            )
        if not set(central_relationship_ids).issubset(self.relationship_ids):
            raise ValueError(
                f"Episode {output_name} central relationships must belong to "
                "the source window"
            )

    def _messages_from_decision(
        self, decision: EpisodeDecision
    ) -> list[MessageEpisode]:
        influences_by_message = {
            influence.message_id: influence for influence in decision.message_influences
        }
        return [
            MessageEpisode(
                message_id=message_id,
                influence_weight=influences_by_message[message_id].influence_weight,
                influence_reason=influences_by_message[message_id].influence_reason,
                message_position=position,
            )
            for position, message_id in enumerate(self.message_ids)
        ]

    @staticmethod
    def _combine_messages(
        existing_messages: list[MessageEpisode],
        current_messages: list[MessageEpisode],
    ) -> list[MessageEpisode]:
        messages_by_id = {message.message_id: message for message in existing_messages}
        messages_by_id.update(
            {message.message_id: message for message in current_messages}
        )
        return [
            message.model_copy(update={"message_position": position})
            for position, message in enumerate(
                sorted(messages_by_id.values(), key=lambda item: item.message_id)
            )
        ]

    def _exceeds_consolidation_limits(
        self,
        target_episode: Episode,
        *,
        message_count: int,
        max_message_count: int,
        max_age_hours: Optional[float],
    ) -> bool:
        if message_count > max_message_count:
            return True
        if max_age_hours is None:
            return False
        timestamp_values = [
            int(message["timestamp_ms"])
            for message in self.messages
            if message.get("timestamp_ms") is not None
        ]
        if not timestamp_values:
            return False
        age_hours = (
            max(timestamp_values) / 1000 - target_episode.created_at.timestamp()
        ) / 3600
        return age_hours > max_age_hours

    def _episode_id_for_window(self, messages: list[MessageEpisode]) -> str:
        source_message_ids = ",".join(str(message.message_id) for message in messages)
        return str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"knoggin:episode:{self.project_id}:{self.session_id}:{source_message_ids}",
            )
        )
