"""Episode primitives and persisted aggregates."""

import math
from collections.abc import Mapping
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator

from common.utils.time_utils import get_now

EPISODE_EMBEDDING_DIMENSION = 1024


class EpisodeNarrativeLimitError(ValueError):
    """The model produced a valid shape that exceeds the server hard limit."""


class EpisodeNarrative(BaseModel):
    """Narrative content independent of an episode's reference representation."""

    summary: Optional[str] = Field(None, min_length=1)
    new_developments: List[str] = Field(default_factory=list)
    updates: List[str] = Field(default_factory=list)
    unresolved: List[str] = Field(default_factory=list)
    importance: float = Field(0.0, ge=0.0, le=1.0)

    def narrative_character_count(self) -> int:
        """Count the exact text persisted in the narrative fields."""

        return sum(
            len(value)
            for value in (
                self.summary or "",
                *self.new_developments,
                *self.updates,
                *self.unresolved,
            )
        )

    def validate_narrative_character_limit(self, maximum: int) -> None:
        if maximum <= 0:
            raise ValueError("episode narrative character limit must be positive")
        count = self.narrative_character_count()
        if count > maximum:
            raise EpisodeNarrativeLimitError(
                f"episode narrative is {count} characters; limit is {maximum}"
            )


class MessageEpisode(BaseModel):
    """A canonical source message and its contribution to an episode."""

    message_id: int = Field(..., gt=0)
    session_id: str = Field(..., min_length=1)
    influence_weight: float = Field(0.0, ge=0.0)
    influence_reason: Optional[str] = None
    message_position: int = Field(..., ge=0)
    attached_at: Optional[datetime] = None


class EntityEpisode(BaseModel):
    """An entity observed in an episode's source messages."""

    entity_id: int = Field(..., gt=0)
    prominence_weight: float = Field(0.0, ge=0.0)
    role: Optional[str] = None
    is_focus_entity: bool = False
    source_message_count: int = Field(0, ge=0)
    first_seen_at: Optional[datetime] = None
    last_seen_at: Optional[datetime] = None


class RelationshipEpisode(BaseModel):
    """A relationship evidenced by an episode's source messages."""

    relationship_id: str = Field(..., min_length=1)
    prominence_weight: float = Field(0.0, ge=0.0)
    is_central_relationship: bool = False
    source_message_count: int = Field(0, ge=0)


class EpisodeCheckpoint(BaseModel):
    """Chronological cursor for episode processing within one session."""

    last_evaluated_message_id: int = Field(0, ge=0)
    last_evaluated_timestamp_ms: Optional[int] = None


class EpisodeVersion(EpisodeNarrative):
    """One bounded snapshot of an episode before consolidation."""

    version: int = Field(..., gt=0)
    saved_at: datetime
    summary: str = Field(..., min_length=1)
    first_message_at: Optional[datetime] = None
    last_message_at: Optional[datetime] = None
    source_message_ids: List[int] = Field(..., min_length=1)
    generator_metadata: Dict[str, Any] = Field(default_factory=dict)


class Episode(EpisodeNarrative):
    """A complete episodic memory and its source-linked graph context."""

    episode_id: str = Field(..., min_length=1)
    project_id: str = Field(..., min_length=1)
    session_id: str = Field(..., min_length=1)
    summary: str = Field(..., min_length=1)
    source_message_count: int = Field(0, ge=0)
    first_message_at: Optional[datetime] = None
    last_message_at: Optional[datetime] = None
    embedding: Optional[List[float]] = Field(default=None, exclude=True)
    messages: List[MessageEpisode] = Field(..., min_length=1)
    entities: List[EntityEpisode] = Field(default_factory=list)
    relationships: List[RelationshipEpisode] = Field(default_factory=list)
    version_history: List[EpisodeVersion] = Field(default_factory=list)
    # Automation must never silently replace an episode that a person curated.
    user_modified: bool = False
    created_at: datetime = Field(default_factory=get_now)
    updated_at: datetime = Field(default_factory=get_now)
    generator_metadata: Dict[str, Any] = Field(default_factory=dict)

    def validated_copy(
        self, *, update: Mapping[str, Any] | None = None
    ) -> "Episode":
        """Create a copy after validating any requested domain changes."""

        data = self.model_dump()
        data["embedding"] = self.embedding
        if update:
            data.update(update)
        return type(self).model_validate(data)

    def model_copy(
        self,
        *,
        update: Mapping[str, Any] | None = None,
        deep: bool = False,
    ) -> "Episode":
        """Preserve Pydantic copy semantics while protecting vector integrity.

        ``model_copy`` intentionally leaves general model updates unchecked.  The
        embedding is the exception: it crosses a fixed-width storage boundary,
        so validate it before accepting an updated vector.
        """

        if update and "embedding" in update:
            self.validate_embedding(update["embedding"])
        return super().model_copy(update=update, deep=deep)

    @field_validator("embedding")
    @classmethod
    def validate_embedding(
        cls, embedding: Optional[List[float]]
    ) -> Optional[List[float]]:
        if embedding is None:
            return None
        if len(embedding) != EPISODE_EMBEDDING_DIMENSION:
            raise ValueError(
                "episode embedding must contain exactly "
                f"{EPISODE_EMBEDDING_DIMENSION} dimensions"
            )
        if not all(math.isfinite(value) for value in embedding):
            raise ValueError("episode embedding must contain only finite values")
        return embedding

    @field_validator("messages")
    @classmethod
    def validate_messages(cls, messages: List[MessageEpisode]) -> List[MessageEpisode]:
        message_ids = [message.message_id for message in messages]
        if len(set(message_ids)) != len(message_ids):
            raise ValueError("messages must not contain duplicate message IDs")
        message_positions = [message.message_position for message in messages]
        if len(set(message_positions)) != len(message_positions):
            raise ValueError("messages must not contain duplicate message positions")
        return messages

    @field_validator("entities")
    @classmethod
    def validate_entities(cls, entities: List[EntityEpisode]) -> List[EntityEpisode]:
        entity_ids = [entity.entity_id for entity in entities]
        if len(set(entity_ids)) != len(entity_ids):
            raise ValueError("entities must not contain duplicate entity IDs")
        if sum(entity.is_focus_entity for entity in entities) > 2:
            raise ValueError("episodes may contain at most two focus entities")
        return entities

    @field_validator("relationships")
    @classmethod
    def validate_relationships(
        cls, relationships: List[RelationshipEpisode]
    ) -> List[RelationshipEpisode]:
        relationship_ids = [
            relationship.relationship_id for relationship in relationships
        ]
        if len(set(relationship_ids)) != len(relationship_ids):
            raise ValueError(
                "relationships must not contain duplicate relationship IDs"
            )
        return relationships
