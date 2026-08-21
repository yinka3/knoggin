"""Focused subsystem contracts used by ingestion workflows.

These protocols document the dependency surface a workflow needs without
turning the full persistence or model services into dependencies everywhere.
"""

from __future__ import annotations

from typing import Any, Protocol, TypeVar, runtime_checkable

from common.schema.episode.models import Episode, EpisodeCheckpoint
from common.schema.ingestion.contracts import (
    CandidateSuggestion,
    EntityWrite,
    EpisodeEligibility,
    ExecutionScope,
    GraphWriteSummary,
    IngestionCommit,
    MessageEntityRef,
    RelationshipWrite,
)

ResponseT = TypeVar("ResponseT")


@runtime_checkable
class IngestionPersistence(Protocol):
    """Durable operations owned by the ingestion worker."""

    async def save_message_logs(self, messages: list[dict[str, Any]]) -> bool: ...

    async def save_candidate_suggestions(
        self,
        scope: ExecutionScope,
        suggestions: list[CandidateSuggestion],
    ) -> int: ...


@runtime_checkable
class IngestionGraphPersistence(Protocol):
    """Graph validation and commit operations required by one sealed batch."""

    async def validate_existing_ids(
        self,
        ids: list[int],
        *,
        visible_project_ids: list[str],
    ) -> set[int] | None: ...

    async def commit_ingestion(self, commit: IngestionCommit) -> GraphWriteSummary: ...

    async def update_entity_aliases(
        self,
        alias_updates: dict[int, list[str]],
        *,
        project_id: str,
    ) -> None: ...

    async def write_batch(
        self,
        entities: list[EntityWrite],
        relationships: list[RelationshipWrite],
        *,
        message_entity_refs: list[MessageEntityRef],
        eligible_messages: list[EpisodeEligibility],
        scope: ExecutionScope,
    ) -> bool: ...


@runtime_checkable
class EpisodeStore(Protocol):
    """Episode persistence and retrieval required by ``EpisodeJob``."""

    async def get_episode_checkpoint(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> EpisodeCheckpoint: ...

    async def get_next_project_episode_window(
        self, *, user_name: str, project_id: str, message_count: int
    ) -> list[dict[str, Any]]: ...

    async def get_recent_project_episodes(
        self, *, user_name: str, project_id: str, limit: int
    ) -> list[Episode]: ...

    async def get_project_episodes_for_entities(
        self, entity_ids: list[int], *, user_name: str, project_id: str, limit: int
    ) -> list[Episode]: ...

    async def write_project_episode_window(
        self,
        episodes: list[Episode],
        window_messages: list[dict[str, Any]],
        *, user_name: str, project_id: str,
    ) -> bool: ...

    async def get_next_episode_window(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        checkpoint: EpisodeCheckpoint,
        message_count: int,
    ) -> list[dict[str, Any]]: ...

    async def get_entity_ids_for_messages(
        self,
        message_ids: list[int],
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> dict[int, list[int]]: ...

    async def get_relationship_ids_for_messages(
        self,
        message_ids: list[int],
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> dict[int, list[str]]: ...

    async def get_episode_generation_catalog(
        self,
        message_ids: list[int],
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]: ...

    async def get_recent_episodes(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        limit: int,
    ) -> list[Episode]: ...

    async def get_episodes_for_entities(
        self,
        entity_ids: list[int],
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        limit: int,
    ) -> list[Episode]: ...

    async def get_episode_source_messages(
        self,
        episode_id: str,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> list[dict[str, Any]]: ...


@runtime_checkable
class StructuredGenerator(Protocol):
    """One typed structured-generation operation."""

    async def generate_structured(
        self,
        *,
        response_model: type[ResponseT],
        system: str,
        user: str,
        temperature: float = 1.0,
    ) -> ResponseT: ...


@runtime_checkable
class EmbeddingEncoder(Protocol):
    """Batch text-to-vector encoding required by episode construction."""

    async def encode(self, texts: list[str]) -> list[list[float]]: ...
