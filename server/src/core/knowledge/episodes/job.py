"""Project-scoped episode generation job."""

from __future__ import annotations

from time import perf_counter
from typing import Awaitable, Callable

from loguru import logger

from common.schema.episode.generation import LLMEpisodeWindowDecision
from common.schema.episode.models import EpisodeNarrativeLimitError
from common.schema.settings import EpisodeSettings
from common.utils.diagnostic_context import diagnostic_scope
from common.utils.events import emit
from core.knowledge.episodes.build import ProjectEpisodeBuild
from core.knowledge.episodes.embedding import build_episode_embedding_text
from core.knowledge.episodes.policy import EpisodeGenerationPolicy
from core.knowledge.episodes.ports import (
    EmbeddingEncoder,
    EpisodeStore,
    StructuredGenerator,
)
from core.knowledge.episodes.prompts import (
    get_episode_generation_prompt,
    get_episode_narrative_repair_prompt,
)
from infrastructure.job.base import BaseJob, JobContext, JobResult


class EpisodeJob(BaseJob):
    """Create up to three project memories from one merged source window."""

    def __init__(
        self,
        knowledge_store: EpisodeStore,
        settings: EpisodeSettings,
        *,
        episode_window_size: int = 24,
        episode_window_size_provider: Callable[[], Awaitable[int]] | None = None,
        llm: StructuredGenerator | None = None,
        embedding_service: EmbeddingEncoder | None = None,
    ) -> None:
        self.knowledge_store = knowledge_store
        self.llm = llm
        self.embedding_service = embedding_service
        self.episode_window_size_provider = episode_window_size_provider
        self.update_settings(settings, episode_window_size=episode_window_size)

    @property
    def name(self) -> str:
        return "episode"

    def update_settings(self, settings: EpisodeSettings, *, episode_window_size: int | None = None) -> None:
        self._settings = settings
        current_window_size = (
            episode_window_size
            if episode_window_size is not None
            else (self._policy.target_message_count if hasattr(self, "_policy") else 24)
        )
        self._policy = EpisodeGenerationPolicy.capture(
            settings=settings,
            episode_window_size=current_window_size,
        )

    def update_episode_window_size(self, episode_window_size: int) -> None:
        self._policy = EpisodeGenerationPolicy.capture(
            settings=self._settings, episode_window_size=episode_window_size
        )

    async def _refresh_project_window_size(self) -> None:
        if self.episode_window_size_provider is not None:
            self.update_episode_window_size(await self.episode_window_size_provider())

    @property
    def policy(self) -> EpisodeGenerationPolicy:
        return self._policy

    async def should_run(self, ctx: JobContext) -> bool:
        if not self._policy.enabled or self.llm is None or self.embedding_service is None:
            return False
        await self._refresh_project_window_size()
        return await self.knowledge_store.has_ready_project_episode_window(
            user_name=ctx.user_name,
            project_id=ctx.project_id,
            message_count=self._policy.target_message_count,
        )

    async def load_build(self, *, user_name: str, project_id: str) -> ProjectEpisodeBuild | None:
        messages = await self.knowledge_store.get_next_project_episode_window(
            user_name=user_name, project_id=project_id,
            message_count=self._policy.target_message_count,
        )
        if not messages:
            return None
        entity_ids_by_message: dict[int, list[int]] = {}
        relationship_ids_by_message: dict[int, list[str]] = {}
        entity_catalog: list[dict] = []
        relationship_catalog: list[dict] = []
        for session_id in sorted({str(item["session_id"]) for item in messages}):
            ids = [int(item["message_id"]) for item in messages if item["session_id"] == session_id]
            entity_ids_by_message.update(await self.knowledge_store.get_entity_ids_for_messages(
                ids, user_name=user_name, project_id=project_id, session_id=session_id
            ))
            relationship_ids_by_message.update(await self.knowledge_store.get_relationship_ids_for_messages(
                ids, user_name=user_name, project_id=project_id, session_id=session_id
            ))
            entities, relationships = await self.knowledge_store.get_episode_generation_catalog(
                ids, user_name=user_name, project_id=project_id, session_id=session_id
            )
            entity_catalog.extend(entities)
            relationship_catalog.extend(relationships)
        source_entities = sorted({value for values in entity_ids_by_message.values() for value in values})
        prior = await self.knowledge_store.get_recent_project_episodes(
            user_name=user_name, project_id=project_id, limit=1
        )
        if source_entities:
            prior.extend(await self.knowledge_store.get_project_episodes_for_entities(
                source_entities, user_name=user_name, project_id=project_id,
                limit=self._policy.prior_episode_candidate_count,
            ))
        deduped = {episode.episode_id: episode for episode in prior}
        return ProjectEpisodeBuild(
            project_id=project_id, policy=self._policy, messages=messages,
            entity_ids_by_message=entity_ids_by_message,
            relationship_ids_by_message=relationship_ids_by_message,
            entity_catalog=list({int(item["entity_id"]): item for item in entity_catalog}.values()),
            relationship_catalog=list({str(item["relationship_id"]): item for item in relationship_catalog}.values()),
            prior_episodes=list(deduped.values())[:self._policy.prior_episode_candidate_count],
        )

    async def process_next_window(self, *, user_name: str, project_id: str) -> ProjectEpisodeBuild | None:
        await self._refresh_project_window_size()
        build = await self.load_build(user_name=user_name, project_id=project_id)
        if build is None:
            return None
        if self.llm is None or self.embedding_service is None:
            raise RuntimeError("EpisodeJob requires an LLM and embedding service")
        with diagnostic_scope(user_name=user_name, project_id=project_id, episode_build_id=build.build_id):
            build.prepare_local_references()
            output = await self.llm.generate_structured(
                response_model=LLMEpisodeWindowDecision,
                system=get_episode_generation_prompt(
                    user_name,
                    prompt_narrative_chars=self._policy.prompt_narrative_chars,
                    max_narrative_chars=self._policy.max_narrative_chars,
                ),
                user=build.evidence_brief(), temperature=0.0,
            )
            try:
                build.apply_llm_output(output)
            except EpisodeNarrativeLimitError:
                output = await self.llm.generate_structured(
                    response_model=LLMEpisodeWindowDecision,
                    system=get_episode_narrative_repair_prompt(
                        user_name,
                        max_narrative_chars=self._policy.max_narrative_chars,
                    ),
                    user=build.repair_brief(output),
                    temperature=0.0,
                )
                build.apply_llm_output(output)
            episodes = build.create_episodes()
            if episodes:
                build.attach_embeddings(await self.embedding_service.encode([
                    build_episode_embedding_text(episode) for episode in episodes
                ]))
            persisted = await self.knowledge_store.write_project_episode_window(
                build.final_episodes, build.messages,
                user_name=user_name, project_id=project_id,
            )
            if not persisted:
                return None
            return build

    async def execute(self, ctx: JobContext) -> JobResult:
        started = perf_counter()
        try:
            build = await self.process_next_window(user_name=ctx.user_name, project_id=ctx.project_id)
        except Exception as exc:
            logger.exception("Project episode processing failed: %s", exc)
            await emit(ctx.project_id, "job", "episode_processing_failed", {
                "user_name": ctx.user_name, "project_id": ctx.project_id, "error": str(exc),
            })
            return JobResult(success=False, summary="EpisodeJob failed")
        if build is None:
            return JobResult(success=True, summary="EpisodeJob found no ready project window")
        await emit(ctx.project_id, "job", "episode_processed", {
            "user_name": ctx.user_name, "project_id": ctx.project_id,
            "source_message_count": len(build.messages),
            "episode_count": len(build.final_episodes),
            "processing_latency_ms": round((perf_counter() - started) * 1000, 3),
            "policy_version": self._policy.version,
        })
        return JobResult(success=True, summary=f"EpisodeJob persisted {len(build.final_episodes)} project episodes")
