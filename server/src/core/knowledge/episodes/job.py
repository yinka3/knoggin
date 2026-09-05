"""Project-scoped episode generation job."""

from __future__ import annotations

from time import perf_counter
from typing import Awaitable, Callable

from loguru import logger

from common.schema.settings import EpisodeSettings
from common.utils.events import emit
from core.knowledge.episodes.build import ProjectEpisodeBuild
from core.knowledge.episodes.generator import EpisodeGenerator
from core.knowledge.episodes.policy import EpisodeGenerationPolicy
from core.knowledge.episodes.ports import (
    EmbeddingEncoder,
    EpisodeStore,
    StructuredGenerator,
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
        self.generator = EpisodeGenerator(
            knowledge_store,
            llm=llm,
            embedding_service=embedding_service,
        )
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
        return await self.generator.build_for_messages(
            user_name=user_name,
            project_id=project_id,
            messages=messages,
            policy=policy,
        )

    async def process_next_window(self, *, user_name: str, project_id: str) -> ProjectEpisodeBuild | None:
        await self._refresh_project_window_size()
        policy = self._policy
        build = await self.load_build(
            user_name=user_name, project_id=project_id, policy=policy
        )
        if build is None:
            return None
        build = await self.generator.generate_build(
            build,
            user_name=user_name,
            project_id=project_id,
        )
        persisted = await self.knowledge_store.write_project_episode_window(
            build.final_episodes,
            build.messages,
            user_name=user_name,
            project_id=project_id,
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
            "policy_version": build.policy.version,
        })
        return JobResult(success=True, summary=f"EpisodeJob persisted {len(build.final_episodes)} project episodes")
