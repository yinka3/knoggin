"""Narrow dependencies required by project episode generation."""

from __future__ import annotations

from typing import Any, Protocol, TypeVar, runtime_checkable

from common.schema.episode.models import Episode

ResponseT = TypeVar("ResponseT")


@runtime_checkable
class EpisodeStore(Protocol):
    """Project-scoped durable operations required by EpisodeJob."""

    async def has_ready_project_episode_window(
        self, *, user_name: str, project_id: str, message_count: int
    ) -> bool: ...

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
        *,
        user_name: str,
        project_id: str,
    ) -> bool: ...

    async def get_entity_ids_for_messages(
        self,
        message_ids: list[int],
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> dict[int, list[int]]: ...



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
