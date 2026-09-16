"""Generation-only service for project episode narratives.

Window selection and durable checkpoint ownership deliberately live outside of
this module. Callers provide frozen canonical messages and choose how their
result is persisted.
"""

from __future__ import annotations

from typing import Protocol, TypeVar

from common.schema.episode.generation import LLMEpisodeWindowDecision
from common.schema.episode.models import EpisodeNarrativeLimitError
from common.utils.diagnostic_context import diagnostic_scope
from core.knowledge.episodes.build import ProjectEpisodeBuild
from core.knowledge.episodes.embedding import build_episode_embedding_text
from core.knowledge.episodes.policy import EpisodeGenerationPolicy
from core.knowledge.episodes.prompts import (
    get_episode_generation_prompt,
    get_episode_narrative_repair_prompt,
)

ResponseT = TypeVar("ResponseT")


class StructuredGenerator(Protocol):
    async def generate_structured(
        self,
        *,
        response_model: type[ResponseT],
        system: str,
        user: str,
        temperature: float = 1.0,
    ) -> ResponseT: ...


class EmbeddingEncoder(Protocol):
    async def encode(self, texts: list[str]) -> list[list[float]]: ...


class EpisodeGenerator:
    """Generate validated, immutable episodes from one frozen window."""

    def __init__(
        self,
        *,
        llm: StructuredGenerator | None,
        embedding_service: EmbeddingEncoder | None,
    ) -> None:
        self.llm = llm
        self.embedding_service = embedding_service

    def build_for_messages(
        self,
        *,
        project_id: str,
        messages: list[dict[str, object]],
        policy: EpisodeGenerationPolicy,
    ) -> ProjectEpisodeBuild:
        """Create a stable, validated model input from supplied messages only."""

        if not messages:
            raise ValueError("Episode generation requires frozen source messages")
        build = ProjectEpisodeBuild(
            project_id=project_id,
            policy=policy,
            messages=[dict(message) for message in messages],
        )
        build.prepare_local_references()
        return build

    async def generate(
        self,
        *,
        user_name: str,
        project_id: str,
        messages: list[dict[str, object]],
        policy: EpisodeGenerationPolicy,
    ) -> ProjectEpisodeBuild:
        """Generate from one frozen message set without selecting or checkpointing."""

        build = self.build_for_messages(
            project_id=project_id,
            messages=messages,
            policy=policy,
        )
        return await self.generate_build(
            build,
            user_name=user_name,
            project_id=project_id,
        )

    async def generate_build(
        self,
        build: ProjectEpisodeBuild,
        *,
        user_name: str,
        project_id: str,
    ) -> ProjectEpisodeBuild:
        """Evaluate an already prepared build; persistence remains caller-owned."""

        if self.llm is None or self.embedding_service is None:
            raise RuntimeError("EpisodeGenerator requires an LLM and embedding service")
        policy = build.policy
        with diagnostic_scope(
            user_name=user_name,
            project_id=project_id,
            episode_build_id=build.build_id,
        ):
            output = await self.llm.generate_structured(
                response_model=LLMEpisodeWindowDecision,
                system=get_episode_generation_prompt(
                    user_name,
                    prompt_narrative_chars=policy.prompt_narrative_chars,
                    max_narrative_chars=policy.max_narrative_chars,
                    max_episode_source_messages=policy.max_episode_source_messages,
                    max_episode_source_tokens=policy.max_episode_source_tokens,
                ),
                user=build.evidence_brief(),
                temperature=0.0,
            )
            try:
                build.apply_llm_output(output)
            except EpisodeNarrativeLimitError:
                output = await self.llm.generate_structured(
                    response_model=LLMEpisodeWindowDecision,
                    system=get_episode_narrative_repair_prompt(
                        user_name,
                        max_narrative_chars=policy.max_narrative_chars,
                    ),
                    user=build.repair_brief(output),
                    temperature=0.0,
                )
                build.apply_llm_output(output)
            episodes = build.create_episodes()
            if episodes:
                build.attach_embeddings(
                    await self.embedding_service.encode(
                        [build_episode_embedding_text(episode) for episode in episodes]
                    )
                )
        return build
