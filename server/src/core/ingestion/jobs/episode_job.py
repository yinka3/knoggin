from time import perf_counter
from typing import Awaitable, Callable, Optional

from loguru import logger

from common.schema.episode import Episode
from common.schema.episode_output import (
    EpisodeConsolidation,
    EpisodeDecision,
    LLMEpisodeConsolidation,
    LLMEpisodeDecision,
)
from common.schema.settings import (
    EpisodeSettings,
    IngestionSettings,
)
from common.utils.events import emit
from core.ingestion.episode_build import EpisodeBuild
from core.ingestion.prompts import (
    get_episode_consolidation_prompt,
    get_episode_generation_prompt,
)
from core.knowledge.episode_embedding import build_episode_embedding_text
from core.knowledge.services.embedding_service import EmbeddingService
from infrastructure.job.base import BaseJob, JobContext, JobResult
from infrastructure.knowledge_store import KnowledgeStore
from infrastructure.llm_client import LLMService


class EpisodeJob(BaseJob):
    """Generates bounded episodic memory for eligible project sessions."""

    def __init__(
        self,
        knowledge_store: KnowledgeStore,
        settings: EpisodeSettings,
        ingestion_settings: IngestionSettings,
        llm: LLMService | None = None,
        embedding_service: EmbeddingService | None = None,
        session_ids_provider: Optional[Callable[[], Awaitable[list[str]]]] = None,
    ) -> None:
        self.knowledge_store = knowledge_store
        self.llm = llm
        self.embedding_service = embedding_service
        self.session_ids_provider = session_ids_provider
        self.update_settings(settings, ingestion_settings)

    @property
    def name(self) -> str:
        return "episode"

    def update_settings(
        self,
        settings: EpisodeSettings,
        ingestion_settings: IngestionSettings,
    ) -> None:
        target_message_count = ingestion_settings.batch_size * settings.batch_multiple
        if settings.max_message_count < target_message_count:
            raise ValueError(
                "Episode max_message_count must be at least the target window size"
            )

        self.enabled = settings.enabled
        self.batch_multiple = settings.batch_multiple
        self.target_message_count = target_message_count
        self.max_message_count = settings.max_message_count
        self.max_age_hours = settings.max_age_hours
        self.max_sessions_per_run = settings.max_sessions_per_run
        self.prior_episode_candidate_count = settings.prior_episode_candidate_count
        self.retrieval_episode_limit = settings.retrieval_episode_limit
        logger.info(
            "EpisodeJob settings updated: "
            f"target_messages={self.target_message_count}, "
            f"max_messages={self.max_message_count}, "
            f"max_sessions={self.max_sessions_per_run}"
        )

    async def should_run(self, ctx: JobContext) -> bool:
        """Run when any durable project session has one ready episode window."""

        if (
            not self.enabled
            or self.llm is None
            or self.embedding_service is None
            or self.session_ids_provider is None
        ):
            return False
        session_ids = await self.session_ids_provider()
        for session_id in session_ids[: self.max_sessions_per_run]:
            window = await self.load_next_window(
                user_name=ctx.user_name,
                project_id=ctx.project_id,
                session_id=session_id,
            )
            if window:
                return True
        return False

    async def load_next_window(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> list[dict]:
        """Load one complete candidate window for a specific conversation."""

        checkpoint = await self.knowledge_store.get_episode_checkpoint(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )
        return await self.knowledge_store.get_next_episode_window(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            checkpoint=checkpoint,
            message_count=self.target_message_count,
        )

    async def load_candidate_build(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> EpisodeBuild | None:
        """Load one eligible window into its workflow-owned aggregate."""

        messages = await self.load_next_window(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )
        if not messages:
            return None

        message_ids = [int(message["message_id"]) for message in messages]
        (
            entity_ids_by_message,
            relationship_ids_by_message,
            entity_catalog,
            relationship_catalog,
        ) = await self._load_context(
            message_ids,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )
        source_entity_ids = sorted(
            {
                entity_id
                for entity_ids in entity_ids_by_message.values()
                for entity_id in entity_ids
            }
        )
        prior_episodes = await self._select_prior_episodes(
            source_entity_ids,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )
        return EpisodeBuild.from_window(
            project_id=project_id,
            session_id=session_id,
            messages=messages,
            entity_ids_by_message=entity_ids_by_message,
            relationship_ids_by_message=relationship_ids_by_message,
            entity_catalog=entity_catalog,
            relationship_catalog=relationship_catalog,
            prior_episodes=prior_episodes,
        )

    async def _load_context(
        self,
        message_ids: list[int],
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> tuple[
        dict[int, list[int]],
        dict[int, list[str]],
        list[dict],
        list[dict],
    ]:
        entity_ids_by_message = await self.knowledge_store.get_entity_ids_for_messages(
            message_ids,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )
        relationship_ids_by_message = (
            await self.knowledge_store.get_relationship_ids_for_messages(
                message_ids,
                user_name=user_name,
                project_id=project_id,
                session_id=session_id,
            )
        )
        (
            entity_catalog,
            relationship_catalog,
        ) = await self.knowledge_store.get_episode_generation_catalog(
            message_ids,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )
        return (
            {
                message_id: sorted(
                    {
                        int(entity_id)
                        for entity_id in entity_ids_by_message.get(message_id, [])
                    }
                )
                for message_id in message_ids
            },
            {
                message_id: sorted(
                    {
                        str(relationship_id)
                        for relationship_id in relationship_ids_by_message.get(
                            message_id, []
                        )
                    }
                )
                for message_id in message_ids
            },
            entity_catalog,
            relationship_catalog,
        )

    async def _select_prior_episodes(
        self,
        source_entity_ids: list[int],
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> list[Episode]:
        """Keep the immediate prior episode plus the highest-overlap matches."""

        recent_episodes = await self.knowledge_store.get_recent_episodes(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            limit=1,
        )
        overlapping_episodes = []
        if source_entity_ids:
            overlapping_episodes = await self.knowledge_store.get_episodes_for_entities(
                source_entity_ids,
                user_name=user_name,
                project_id=project_id,
                session_id=session_id,
                limit=self.prior_episode_candidate_count,
            )

        selected = []
        seen_episode_ids = set()
        for episode in [*recent_episodes, *overlapping_episodes]:
            if episode.episode_id in seen_episode_ids:
                continue
            selected.append(episode)
            seen_episode_ids.add(episode.episode_id)
            if len(selected) == self.prior_episode_candidate_count:
                break
        return selected

    async def generate_decision(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> EpisodeDecision | None:
        """Generate one grounded episode decision for a ready candidate window."""

        build = await self.load_candidate_build(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )
        if build is None:
            return None
        try:
            return await self._generate_decision_for_build(build, user_name=user_name)
        finally:
            build.release()

    async def _generate_decision_for_build(
        self,
        build: EpisodeBuild,
        *,
        user_name: str,
    ) -> EpisodeDecision:
        """Generate a strict decision directly into one episode build."""

        if self.llm is None:
            raise RuntimeError("EpisodeJob requires an LLM to generate a decision")
        build.prepare_local_references()
        output = await self.llm.generate_structured(
            response_model=LLMEpisodeDecision,
            system=get_episode_generation_prompt(user_name),
            user=build.generation_payload(),
            temperature=0.0,
        )
        return build.apply_llm_decision(output)

    async def process_next_window(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> EpisodeBuild | None:
        """Generate and persist one episode window in its owning aggregate."""

        build = await self.load_candidate_build(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )
        if build is None:
            return None
        builds = [build]
        try:
            decision = await self._generate_decision_for_build(
                build,
                user_name=user_name,
            )
        except ValueError as exc:
            await self._emit_validation_failure(
                exc,
                stage="decision",
                user_name=user_name,
                project_id=project_id,
                session_id=session_id,
                source_message_count=len(build.messages),
            )
            build.release()
            raise
        try:
            window_message_ids = build.message_ids
            episode = build.create_episode(
                max_message_count=self.max_message_count,
                max_age_hours=self.max_age_hours,
            )
            if (
                episode is not None
                and decision.action == "consolidate"
                and episode.generator_metadata["effective_action"] == "consolidate"
            ):
                build = await self._load_consolidation_build(
                    decision.target_episode_id,
                    build,
                    user_name=user_name,
                    project_id=project_id,
                    session_id=session_id,
                )
                builds.append(build)
                decision = await self._regenerate_consolidation_for_build(
                    decision.target_episode_id,
                    build,
                    user_name=user_name,
                )
                episode = build.create_episode(
                    max_message_count=self.max_message_count,
                    max_age_hours=self.max_age_hours,
                )
            if episode is not None:
                episode = await self._embed_build_episode(build)
            persisted = await self.knowledge_store.write_episode_window(
                episode,
                window_message_ids,
                user_name=user_name,
                project_id=project_id,
                session_id=session_id,
            )
            if not persisted:
                return None
            build.mark_persisted()
            return build
        except ValueError as exc:
            await self._emit_validation_failure(
                exc,
                stage="consolidation" if decision.action == "consolidate" else "episode",
                user_name=user_name,
                project_id=project_id,
                session_id=session_id,
                source_message_count=len(build.messages),
            )
            raise
        finally:
            for owned_build in builds:
                owned_build.release()

    async def _load_consolidation_build(
        self,
        target_episode_id: str | None,
        window_build: EpisodeBuild,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> EpisodeBuild:
        """Load a replacement aggregate containing the full consolidation source."""

        target_episode = next(
            (
                episode
                for episode in window_build.prior_episodes
                if episode.episode_id == target_episode_id
            ),
            None,
        )
        if target_episode is None:
            raise ValueError("Episode consolidation target is not a prior candidate")
        target_sources = await self.knowledge_store.get_episode_source_messages(
            target_episode.episode_id,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )
        expected_source_message_ids = {
            message.message_id for message in target_episode.messages
        }
        actual_source_message_ids = {
            int(message["message_id"]) for message in target_sources
        }
        if actual_source_message_ids != expected_source_message_ids:
            raise ValueError(
                "Episode consolidation target source messages no longer match "
                "its persisted provenance"
            )
        messages_by_id = {
            int(message["message_id"]): dict(message) for message in target_sources
        }
        messages_by_id.update(
            {
                int(message["message_id"]): dict(message)
                for message in window_build.messages
            }
        )
        messages = sorted(
            messages_by_id.values(),
            key=lambda message: (
                message.get("timestamp_ms") is None,
                message.get("timestamp_ms") or 0,
                int(message["message_id"]),
            ),
        )
        message_ids = [int(message["message_id"]) for message in messages]
        (
            entity_ids_by_message,
            relationship_ids_by_message,
            entity_catalog,
            relationship_catalog,
        ) = await self._load_context(
            message_ids,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )
        return EpisodeBuild.from_window(
            project_id=project_id,
            session_id=session_id,
            messages=messages,
            entity_ids_by_message=entity_ids_by_message,
            relationship_ids_by_message=relationship_ids_by_message,
            entity_catalog=entity_catalog,
            relationship_catalog=relationship_catalog,
            prior_episodes=[target_episode],
        )

    async def _regenerate_consolidation_for_build(
        self,
        target_episode_id: str | None,
        build: EpisodeBuild,
        *,
        user_name: str,
    ) -> EpisodeDecision:
        """Regenerate and resolve a consolidation response into one build."""

        if self.llm is None:
            raise RuntimeError("EpisodeJob requires an LLM to regenerate an episode")
        if not target_episode_id:
            raise ValueError("Episode consolidation requires a target episode ID")
        build.prepare_local_references()
        output = await self.llm.generate_structured(
            response_model=LLMEpisodeConsolidation,
            system=get_episode_consolidation_prompt(user_name),
            user=build.consolidation_payload(target_episode_id),
            temperature=0.0,
        )
        return build.apply_llm_consolidation(
            output,
            target_episode_id=target_episode_id,
        )

    async def _embed_build_episode(self, build: EpisodeBuild) -> Episode:
        """Attach the embedding to the aggregate-owned final episode."""

        if self.embedding_service is None:
            raise RuntimeError("EpisodeJob requires an embedding service")
        if build.final_episode is None:
            raise ValueError("EpisodeBuild has no episode to embed")
        embeddings = await self.embedding_service.encode(
            [build_episode_embedding_text(build.final_episode)]
        )
        if len(embeddings) != 1:
            raise RuntimeError("Episode embedding service returned an invalid result")
        return build.attach_embedding(embeddings[0])

    @staticmethod
    async def _emit_validation_failure(
        exc: ValueError,
        *,
        stage: str,
        user_name: str,
        project_id: str,
        session_id: str,
        source_message_count: int,
    ) -> None:
        message = str(exc)
        invalid_identifier = any(
            marker in message
            for marker in (
                "Unknown local ID",
                "must belong",
                "target must be",
                "invent IDs",
            )
        )
        await emit(
            project_id,
            "job",
            "episode_validation_failed",
            {
                "user_name": user_name,
                "project_id": project_id,
                "session_id": session_id,
                "stage": stage,
                "source_message_count": source_message_count,
                "invalid_identifier": invalid_identifier,
                "reason": (
                    "invalid_identifier" if invalid_identifier else "validation_failed"
                ),
                "error": "invalid identifier" if invalid_identifier else message,
            },
        )
        if invalid_identifier:
            await emit(
                project_id,
                "job",
                "local_reference_resolution_failed",
                {
                    "pipeline": "episode",
                    "reference_type": "episode_decision",
                    "reason": "validation_rejected",
                    "stage": stage,
                },
            )

    async def execute(self, ctx: JobContext) -> JobResult:
        """Process one ready window in each bounded project-session slice."""

        if not self.enabled:
            return JobResult(success=True, summary="EpisodeJob is disabled")
        if self.llm is None:
            return JobResult(success=False, summary="EpisodeJob has no LLM")
        if self.embedding_service is None:
            return JobResult(
                success=False,
                summary="EpisodeJob has no embedding service",
            )
        if self.session_ids_provider is None:
            return JobResult(
                success=False,
                summary="EpisodeJob has no session provider",
            )

        outcomes = []
        failures = []
        session_ids = await self.session_ids_provider()
        for session_id in session_ids[: self.max_sessions_per_run]:
            started_at = perf_counter()
            try:
                outcome = await self.process_next_window(
                    user_name=ctx.user_name,
                    project_id=ctx.project_id,
                    session_id=session_id,
                )
            except Exception as exc:
                failures.append(session_id)
                logger.exception(f"EpisodeJob failed for session {session_id}: {exc}")
                await emit(
                    ctx.project_id,
                    "job",
                    "episode_processing_failed",
                    {
                        "user_name": ctx.user_name,
                        "project_id": ctx.project_id,
                        "session_id": session_id,
                        "processing_latency_ms": round(
                            (perf_counter() - started_at) * 1000, 3
                        ),
                        "error": str(exc),
                    },
                )
                continue
            if outcome is None:
                continue
            outcomes.append(outcome)
            await emit(
                ctx.project_id,
                "job",
                "episode_processed",
                {
                    "user_name": ctx.user_name,
                    "project_id": ctx.project_id,
                    "session_id": session_id,
                    "action": outcome.outcome_action,
                    "episode_id": outcome.outcome_episode_id,
                    "source_message_count": outcome.source_message_count,
                    "episode_source_message_count": (
                        outcome.episode_source_message_count
                    ),
                    "entity_link_count": outcome.entity_link_count,
                    "relationship_link_count": outcome.relationship_link_count,
                    "consolidation_limit_hit": outcome.consolidation_limit_hit,
                    "episode_at_max_size": (
                        outcome.episode_source_message_count >= self.max_message_count
                    ),
                    "processing_latency_ms": round(
                        (perf_counter() - started_at) * 1000, 3
                    ),
                },
            )

        return JobResult(
            success=not failures,
            summary=(
                f"EpisodeJob processed {len(outcomes)} windows "
                f"({', '.join(outcome.outcome_action for outcome in outcomes) or 'none'}); "
                f"failures={len(failures)}"
            ),
        )
