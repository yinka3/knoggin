"""Project-scoped episode generation job."""

from __future__ import annotations

from time import perf_counter
from typing import Awaitable, Callable

from loguru import logger

from common.schema.episode.generation import (
    LLMEpisodeConsolidation,
    LLMEpisodeWindowDecision,
)
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
    get_episode_consolidation_prompt,
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
        policy = self._policy
        return await self.knowledge_store.has_ready_project_episode_window(
            user_name=ctx.user_name,
            project_id=ctx.project_id,
            message_count=policy.target_message_count,
        )

    async def load_build(
        self,
        *,
        user_name: str,
        project_id: str,
        policy: EpisodeGenerationPolicy | None = None,
    ) -> ProjectEpisodeBuild | None:
        policy = policy or self._policy
        messages = await self.knowledge_store.get_next_project_episode_window(
            user_name=user_name, project_id=project_id,
            message_count=policy.target_message_count,
        )
        if not messages:
            return None
        first_message = min(
            messages,
            key=lambda item: (
                item.get("timestamp_ms") is None,
                item.get("timestamp_ms") or 0,
                int(item["message_id"]),
            ),
        )
        prior = await self.knowledge_store.get_nearby_project_episodes(
            user_name=user_name,
            project_id=project_id,
            session_ids=sorted({str(item["session_id"]) for item in messages}),
            before_message_id=int(first_message["message_id"]),
            before_timestamp_ms=first_message.get("timestamp_ms"),
            limit=policy.prior_episode_candidate_count,
        )
        return ProjectEpisodeBuild(
            project_id=project_id, policy=policy, messages=messages,
            prior_episodes=prior[:policy.prior_episode_candidate_count],
        )

    async def process_next_window(self, *, user_name: str, project_id: str) -> ProjectEpisodeBuild | None:
        await self._refresh_project_window_size()
        policy = self._policy
        build = await self.load_build(
            user_name=user_name, project_id=project_id, policy=policy
        )
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
                    prompt_narrative_chars=policy.prompt_narrative_chars,
                    max_narrative_chars=policy.max_narrative_chars,
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
                        max_narrative_chars=policy.max_narrative_chars,
                    ),
                    user=build.repair_brief(output),
                    temperature=0.0,
                )
                build.apply_llm_output(output)
            await self._regenerate_consolidations(
                build,
                user_name=user_name,
                project_id=project_id,
            )
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

    async def _regenerate_consolidations(
        self,
        build: ProjectEpisodeBuild,
        *,
        user_name: str,
        project_id: str,
    ) -> None:
        """Re-ground each consolidation proposal in complete source evidence."""

        if self.llm is None:
            raise RuntimeError("EpisodeJob requires an LLM")
        for decision in build.decisions:
            if decision.action != "consolidate" or not decision.target_episode_id:
                continue
            try:
                source_messages = await self.knowledge_store.get_project_episode_source_messages(
                    decision.target_episode_id,
                    user_name=user_name,
                    project_id=project_id,
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
                    "Episode consolidation fell back to a separate Episode: {}",
                    exc,
                )
                build.keep_consolidation_separate(decision)

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
            "policy_version": build.policy.version,
        })
        return JobResult(success=True, summary=f"EpisodeJob persisted {len(build.final_episodes)} project episodes")
