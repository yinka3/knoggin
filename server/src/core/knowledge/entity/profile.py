from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional


@dataclass(frozen=True, slots=True)
class EntityIdentity:
    """User-global entity identity, independent of any project classification."""

    entity_id: int
    user_name: str
    canonical_name: str
    aliases: tuple[str, ...] = ()
    embedding: Optional[list[float]] = None
    status: str = "active"
    redirect_entity_id: Optional[int] = None


@dataclass(frozen=True, slots=True)
class ProjectEntityContext:
    """One project's domain classification and activity for a global identity."""

    project_id: str
    entity_id: int
    user_name: str
    entity_type: str
    topic: str = "General"
    last_mentioned_ms: Optional[int] = None


@dataclass
class EntityProfile:
    canonical_name: str
    entity_type: str = ""
    topic: str = "General"
    project_id: Optional[str] = None
    embedding: Optional[list[float]] = None

    @classmethod
    def from_entity_record(cls, entity: Mapping[str, Any]) -> "EntityProfile":
        return cls(
            canonical_name=entity.get("canonical_name") or entity.get("name") or "",
            entity_type=entity.get("type") or entity.get("entity_type") or "",
            topic=entity.get("topic") or "General",
            project_id=entity.get("project_id"),
            embedding=entity.get("embedding"),
        )

    @classmethod
    def registered(
        cls,
        canonical_name: str,
        entity_type: str,
        topic: Optional[str],
        project_id: Optional[str],
        embedding: Optional[list[float]],
    ) -> "EntityProfile":
        return cls(
            canonical_name=canonical_name,
            entity_type=entity_type,
            topic=topic or "General",
            project_id=project_id,
            embedding=embedding,
        )

    @property
    def canonical_lower(self) -> str:
        return self.canonical_name.lower()

    def set_embedding(self, embedding: list[float]) -> None:
        self.embedding = embedding

    def apply_updates(self, updates: Mapping[str, Any]) -> None:
        if "canonical_name" in updates:
            self.canonical_name = updates["canonical_name"]
        if "type" in updates:
            self.entity_type = updates["type"]
        if "entity_type" in updates:
            self.entity_type = updates["entity_type"]
        if "topic" in updates:
            self.topic = updates["topic"] or "General"
        if "project_id" in updates:
            self.project_id = updates["project_id"]
        if "embedding" in updates:
            self.embedding = updates["embedding"]

    def to_dict(self) -> dict[str, Any]:
        return {
            "canonical_name": self.canonical_name,
            "type": self.entity_type,
            "topic": self.topic,
            "project_id": self.project_id,
            "embedding": self.embedding,
        }
