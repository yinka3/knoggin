"""Generation-only service for project episode narratives.

Window selection and durable checkpoint ownership deliberately live outside of
this module.  Callers provide frozen canonical messages and choose how their
result is persisted.
"""

from __future__ import annotations

from typing import Any

from loguru import logger

from common.schema.episode.generation import (
    LLMEpisodeConsolidation,
    LLMEpisodeWindowDecision,
)
from common.schema.episode.models import EpisodeNarrativeLimitError
from common.utils.diagnostic_context import diagnostic_scope
from core.knowledge.episodes.build import ProjectEpisodeBuild
from core.knowledge.episodes.embedding import build_episode_embedding_text
from core.knowledge.episodes.policy import EpisodeGenerationPolicy
from core.knowledge.episodes.ports import (
    EmbeddingEncoder,
    EpisodeStore,
    StructuredGenerator,
)
from core.knowledge.episodes.prompts import (
    get_episode_consolidation_prompt,
    get_episode_generation_prompt,
    get_episode_narrative_repair_prompt,
)


class EpisodeGenerator:
    """Generate validated episode narratives from caller-owned evidence."""

    def __init__(
        self,
        knowledge_store: EpisodeStore,
        *,
        llm: StructuredGenerator | None,
        embedding_service: EmbeddingEncoder | None,
    ) -> None:
        self.knowledge_store = knowledge_store
        self.llm = llm
        self.embedding_service = embedding_service

    async def build_for_messages(
        self,
        *,
        user_name: str,
        project_id: str,
        messages: list[dict[str, Any]],
        policy: EpisodeGenerationPolicy,
    ) -> ProjectEpisodeBuild:
        """Create a stable, validated model input from supplied messages only."""

        if not messages:
            raise ValueError("Episode generation requires frozen source messages")
        first_message = min(messages, key=ProjectEpisodeBuild._source_order_key)
        prior = await self.knowledge_store.get_nearby_project_episodes(
            user_name=user_name,
            project_id=project_id,
            session_ids=sorted({str(item["session_id"]) for item in messages}),
            before_message_id=int(first_message["message_id"]),
            before_timestamp_ms=first_message.get("timestamp_ms"),
            limit=policy.prior_episode_candidate_count,
        )
        build = ProjectEpisodeBuild(
            project_id=project_id,
            policy=policy,
            messages=[dict(message) for message in messages],
            prior_episodes=prior[: policy.prior_episode_candidate_count],
        )
        build.prepare_local_references()
        return build

    async def generate(
        self,
        *,
        user_name: str,
        project_id: str,
        messages: list[dict[str, Any]],
        policy: EpisodeGenerationPolicy,
    ) -> ProjectEpisodeBuild:
        """Generate from one frozen message set without selecting or checkpointing."""

        build = await self.build_for_messages(
            user_name=user_name,
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
            await self.regenerate_consolidations(
                build,
                user_name=user_name,
                project_id=project_id,
            )
            episodes = build.create_episodes()
            if episodes:
                build.attach_embeddings(
                    await self.embedding_service.encode(
                        [build_episode_embedding_text(episode) for episode in episodes]
                    )
                )
        return build

    async def regenerate_consolidations(
        self,
        build: ProjectEpisodeBuild,
        *,
        user_name: str,
        project_id: str,
    ) -> None:
        """Re-ground each consolidation proposal in complete source evidence."""

        if self.llm is None:
            raise RuntimeError("EpisodeGenerator requires an LLM")
        for decision in build.decisions:
            if decision.action != "consolidate" or not decision.target_episode_id:
                continue
            try:
                source_messages = (
                    await self.knowledge_store.get_project_episode_source_messages(
                        decision.target_episode_id,
                        user_name=user_name,
                        project_id=project_id,
                    )
                )
                if not build.preflight_consolidation(decision, source_messages):
                    build.keep_consolidation_separate(decision)
                    continue
                output = await self.llm.generate_structured(
                    response_model=LLMEpisodeConsolidation,
                    system=get_episode_consolidation_prompt(user_name),
                    user=build.consolidation_brief(decision),
                    temperature=0.0,
                )
                message_ids = build.resolve_consolidation_references(
                    decision, output.message_influences
                )
                build.apply_consolidation_output(
                    decision,
                    action=output.action,
                    summary=output.summary,
                    new_developments=output.new_developments,
                    updates=output.updates,
                    unresolved=output.unresolved,
                    message_ids=message_ids,
                )
            except Exception as exc:
                # A stale packet, capacity change, provider failure, or invalid
                # second-pass response must not block the new completed units.
                logger.warning(
                    "Episode consolidation fell back to a separate Episode: {}", exc
                )
                build.keep_consolidation_separate(decision)
