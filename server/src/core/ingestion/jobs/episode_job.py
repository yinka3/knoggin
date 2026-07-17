import json
import uuid
from dataclasses import dataclass
from time import perf_counter
from typing import Awaitable, Callable, Optional

from loguru import logger

from common.schema.contracts import (
    EpisodeConsolidation,
    EpisodeDecision,
    LLMEpisodeConsolidation,
    LLMEpisodeDecision,
)
from common.schema.primitives import (
    EntityEpisode,
    Episode,
    MessageEpisode,
    RelationshipEpisode,
)
from common.schema.settings import (
    EpisodeSettings,
    IngestionSettings,
    LocalReferenceSettings,
)
from common.utils.events import emit
from common.utils.local_references import build_local_id_maps, resolve_local_id
from core.ingestion.prompts import (
    get_episode_consolidation_prompt,
    get_episode_generation_prompt,
)
from core.knowledge.episode_embedding import build_episode_embedding_text
from core.knowledge.services.embedding_service import EmbeddingService
from infrastructure.job.base import BaseJob, JobContext, JobResult
from infrastructure.knowledge_store import KnowledgeStore
from infrastructure.llm_client import LLMService


@dataclass(frozen=True)
class EpisodeCandidateContext:
    """One ready-to-generate window and the bounded context around it."""

    messages: list[dict]
    entity_ids_by_message: dict[int, list[int]]
    relationship_ids_by_message: dict[int, list[str]]
    entity_catalog: list[dict]
    relationship_catalog: list[dict]
    prior_episodes: list[Episode]

    @property
    def entity_ids(self) -> list[int]:
        return sorted(
            {
                entity_id
                for message_entity_ids in self.entity_ids_by_message.values()
                for entity_id in message_entity_ids
            }
        )

    @property
    def relationship_ids(self) -> list[str]:
        return sorted(
            {
                relationship_id
                for message_relationship_ids in self.relationship_ids_by_message.values()
                for relationship_id in message_relationship_ids
            }
        )


@dataclass(frozen=True)
class EpisodeProcessingResult:
    """One durable episode outcome, including a checkpointed skip."""

    action: str
    episode_id: str | None
    source_message_count: int
    episode_source_message_count: int = 0
    entity_link_count: int = 0
    relationship_link_count: int = 0
    consolidation_limit_hit: bool = False


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
        local_reference_settings: LocalReferenceSettings | None = None,
    ) -> None:
        self.knowledge_store = knowledge_store
        self.llm = llm
        self.embedding_service = embedding_service
        self.session_ids_provider = session_ids_provider
        self.local_references_enabled = (
            local_reference_settings.enabled
            if local_reference_settings is not None
            else True
        )
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

    def update_local_reference_settings(
        self,
        config: LocalReferenceSettings,
    ) -> None:
        self.local_references_enabled = config.enabled

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

    async def load_candidate_context(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> EpisodeCandidateContext | None:
        """Load one eligible window with all canonical graph context."""

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
        return EpisodeCandidateContext(
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

        context = await self.load_candidate_context(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )
        if context is None:
            return None
        return await self._generate_decision_for_context(context, user_name=user_name)

    async def _generate_decision_for_context(
        self,
        context: EpisodeCandidateContext,
        *,
        user_name: str,
    ) -> EpisodeDecision:
        """Ask the LLM for a decision after candidate context is already loaded."""

        if self.llm is None:
            raise RuntimeError("EpisodeJob requires an LLM to generate a decision")

        message_local_ids, message_ids_by_local = build_local_id_maps(
            (int(message["message_id"]) for message in context.messages),
            "m",
            use_local_references=self.local_references_enabled,
        )
        entity_local_ids, entity_ids_by_local = build_local_id_maps(
            context.entity_ids,
            "e",
            use_local_references=self.local_references_enabled,
        )
        relationship_local_ids, relationship_ids_by_local = build_local_id_maps(
            context.relationship_ids,
            "r",
            use_local_references=self.local_references_enabled,
        )
        episode_local_ids, episode_ids_by_local = build_local_id_maps(
            (episode.episode_id for episode in context.prior_episodes),
            "ep",
            use_local_references=self.local_references_enabled,
        )
        system_prompt = get_episode_generation_prompt(user_name)
        if not self.local_references_enabled:
            system_prompt += (
                "\n\nLegacy ID mode is active. Return only IDs supplied in this "
                "call; ignore local-reference examples."
            )
        output = await self.llm.generate_structured(
            response_model=LLMEpisodeDecision,
            system=system_prompt,
            user=self._build_generation_input(
                context,
                message_local_ids=message_local_ids,
                entity_local_ids=entity_local_ids,
                relationship_local_ids=relationship_local_ids,
                episode_local_ids=episode_local_ids,
            ),
            temperature=0.0,
        )
        decision = self._resolve_decision(
            output,
            message_ids_by_local=message_ids_by_local,
            entity_ids_by_local=entity_ids_by_local,
            relationship_ids_by_local=relationship_ids_by_local,
            episode_ids_by_local=episode_ids_by_local,
        )
        self._validate_decision(decision, context)
        return decision

    async def process_next_window(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> EpisodeProcessingResult | None:
        """Generate and atomically persist one episode decision for a session."""

        context = await self.load_candidate_context(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )
        if context is None:
            return None
        try:
            decision = await self._generate_decision_for_context(
                context,
                user_name=user_name,
            )
        except ValueError as exc:
            await self._emit_validation_failure(
                exc,
                stage="decision",
                user_name=user_name,
                project_id=project_id,
                session_id=session_id,
                source_message_count=len(context.messages),
            )
            raise
        window_message_ids = [
            int(message["message_id"]) for message in context.messages
        ]
        episode = self._build_episode(
            decision,
            context,
            project_id=project_id,
            session_id=session_id,
        )
        if (
            episode is not None
            and decision.action == "consolidate"
            and episode.generator_metadata["effective_action"] == "consolidate"
        ):
            consolidation_context = context
            try:
                consolidation_context = await self._load_consolidation_context(
                    decision.target_episode_id,
                    context,
                    user_name=user_name,
                    project_id=project_id,
                    session_id=session_id,
                )
                consolidation = await self._regenerate_consolidation(
                    decision.target_episode_id,
                    consolidation_context,
                    user_name=user_name,
                )
            except ValueError as exc:
                await self._emit_validation_failure(
                    exc,
                    stage="consolidation",
                    user_name=user_name,
                    project_id=project_id,
                    session_id=session_id,
                    source_message_count=len(consolidation_context.messages),
                )
                raise
            decision = EpisodeDecision(
                action="consolidate",
                target_episode_id=decision.target_episode_id,
                **consolidation.model_dump(),
            )
            episode = self._build_episode(
                decision,
                consolidation_context,
                project_id=project_id,
                session_id=session_id,
            )
        if episode is not None:
            episode = await self._embed_episode(episode)
        persisted = await self.knowledge_store.write_episode_window(
            episode,
            window_message_ids,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )
        if not persisted:
            return None
        return EpisodeProcessingResult(
            action=(
                "skip"
                if episode is None
                else str(episode.generator_metadata["effective_action"])
            ),
            episode_id=episode.episode_id if episode else None,
            source_message_count=len(window_message_ids),
            episode_source_message_count=len(episode.messages) if episode else 0,
            entity_link_count=len(episode.entities) if episode else 0,
            relationship_link_count=len(episode.relationships) if episode else 0,
            consolidation_limit_hit=(
                bool(episode.generator_metadata.get("consolidation_limit_hit"))
                if episode
                else False
            ),
        )

    async def _load_consolidation_context(
        self,
        target_episode_id: str | None,
        window_context: EpisodeCandidateContext,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> EpisodeCandidateContext:
        """Load the complete bounded source set for one selected consolidation."""

        target_episode = next(
            (
                episode
                for episode in window_context.prior_episodes
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
                for message in window_context.messages
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
        return EpisodeCandidateContext(
            messages=messages,
            entity_ids_by_message=entity_ids_by_message,
            relationship_ids_by_message=relationship_ids_by_message,
            entity_catalog=entity_catalog,
            relationship_catalog=relationship_catalog,
            prior_episodes=[target_episode],
        )

    async def _regenerate_consolidation(
        self,
        target_episode_id: str | None,
        context: EpisodeCandidateContext,
        *,
        user_name: str,
    ) -> EpisodeConsolidation:
        """Regenerate one selected episode against all of its source evidence."""

        if self.llm is None:
            raise RuntimeError("EpisodeJob requires an LLM to regenerate an episode")
        if not target_episode_id:
            raise ValueError("Episode consolidation requires a target episode ID")
        message_local_ids, message_ids_by_local = build_local_id_maps(
            (int(message["message_id"]) for message in context.messages),
            "m",
            use_local_references=self.local_references_enabled,
        )
        entity_local_ids, entity_ids_by_local = build_local_id_maps(
            context.entity_ids,
            "e",
            use_local_references=self.local_references_enabled,
        )
        relationship_local_ids, relationship_ids_by_local = build_local_id_maps(
            context.relationship_ids,
            "r",
            use_local_references=self.local_references_enabled,
        )
        episode_local_ids, _ = build_local_id_maps(
            (episode.episode_id for episode in context.prior_episodes),
            "ep",
            use_local_references=self.local_references_enabled,
        )
        try:
            target_episode_local_id = episode_local_ids[target_episode_id]
        except KeyError as exc:
            raise ValueError(
                "Episode consolidation target is not in the supplied context"
            ) from exc
        system_prompt = get_episode_consolidation_prompt(user_name)
        if not self.local_references_enabled:
            system_prompt += (
                "\n\nLegacy ID mode is active. Return only IDs supplied in this "
                "call; ignore local-reference examples."
            )
        output = await self.llm.generate_structured(
            response_model=LLMEpisodeConsolidation,
            system=system_prompt,
            user=self._build_consolidation_input(
                target_episode_local_id,
                context,
                message_local_ids=message_local_ids,
                entity_local_ids=entity_local_ids,
                relationship_local_ids=relationship_local_ids,
            ),
            temperature=0.0,
        )
        consolidation = self._resolve_consolidation(
            output,
            message_ids_by_local=message_ids_by_local,
            entity_ids_by_local=entity_ids_by_local,
            relationship_ids_by_local=relationship_ids_by_local,
        )
        self._validate_ranked_output(consolidation, context, "consolidation")
        return consolidation

    async def _embed_episode(self, episode: Episode) -> Episode:
        """Embed the current episode narrative before it is persisted."""

        if self.embedding_service is None:
            raise RuntimeError("EpisodeJob requires an embedding service")
        embeddings = await self.embedding_service.encode(
            [build_episode_embedding_text(episode)]
        )
        if len(embeddings) != 1:
            raise RuntimeError("Episode embedding service returned an invalid result")
        return Episode.model_validate(
            {**episode.model_dump(), "embedding": embeddings[0]}
        )

    def _build_episode(
        self,
        decision: EpisodeDecision,
        context: EpisodeCandidateContext,
        *,
        project_id: str,
        session_id: str,
    ) -> Episode | None:
        """Translate validated output into the aggregate persisted by the writer."""

        if decision.action == "skip":
            return None

        current_messages = self._messages_from_decision(decision, context)
        target_episode = next(
            (
                episode
                for episode in context.prior_episodes
                if episode.episode_id == decision.target_episode_id
            ),
            None,
        )
        should_create = decision.action == "create" or target_episode is None
        consolidation_limit_hit = False
        messages = current_messages
        if not should_create:
            messages = self._combine_messages(target_episode.messages, current_messages)
            if self._exceeds_consolidation_limits(
                target_episode,
                context,
                message_count=len(messages),
            ):
                should_create = True
                consolidation_limit_hit = True
                messages = current_messages

        episode_id = (
            self._episode_id_for_window(project_id, session_id, current_messages)
            if should_create
            else target_episode.episode_id
        )
        return Episode(
            episode_id=episode_id,
            project_id=project_id,
            session_id=session_id,
            summary=decision.summary,
            new_developments=decision.new_developments,
            updates=decision.updates,
            unresolved=decision.unresolved,
            importance=decision.importance,
            messages=messages,
            entities=[
                EntityEpisode(
                    entity_id=focus.entity_id,
                    prominence_weight=focus.prominence_weight,
                    role=focus.role,
                    is_focus_entity=True,
                )
                for focus in decision.focus_entities
            ],
            relationships=[
                RelationshipEpisode(
                    relationship_id=relationship.relationship_id,
                    prominence_weight=relationship.prominence_weight,
                    is_central_relationship=True,
                )
                for relationship in decision.central_relationships
            ],
            generator_metadata={
                "decision_action": decision.action,
                "effective_action": "create" if should_create else "consolidate",
                "consolidated": not should_create and decision.action == "consolidate",
                "consolidation_limit_hit": consolidation_limit_hit,
            },
        )

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

    def _exceeds_consolidation_limits(
        self,
        target_episode: Episode,
        context: EpisodeCandidateContext,
        *,
        message_count: int,
    ) -> bool:
        if message_count > self.max_message_count:
            return True
        if self.max_age_hours is None:
            return False
        timestamp_values = [
            int(message["timestamp_ms"])
            for message in context.messages
            if message.get("timestamp_ms") is not None
        ]
        if not timestamp_values:
            return False
        latest_timestamp_ms = max(timestamp_values)
        age_hours = (
            latest_timestamp_ms / 1000 - target_episode.created_at.timestamp()
        ) / 3600
        return age_hours > self.max_age_hours

    @staticmethod
    def _messages_from_decision(
        decision: EpisodeDecision,
        context: EpisodeCandidateContext,
    ) -> list[MessageEpisode]:
        influences_by_message = {
            influence.message_id: influence for influence in decision.message_influences
        }
        return [
            MessageEpisode(
                message_id=int(message["message_id"]),
                influence_weight=influences_by_message[
                    int(message["message_id"])
                ].influence_weight,
                influence_reason=influences_by_message[
                    int(message["message_id"])
                ].influence_reason,
                message_position=position,
            )
            for position, message in enumerate(context.messages)
        ]

    @staticmethod
    def _combine_messages(
        existing_messages: list[MessageEpisode],
        current_messages: list[MessageEpisode],
    ) -> list[MessageEpisode]:
        messages_by_id = {message.message_id: message for message in existing_messages}
        messages_by_id.update(
            {message.message_id: message for message in current_messages}
        )
        return [
            message.model_copy(update={"message_position": position})
            for position, message in enumerate(
                sorted(messages_by_id.values(), key=lambda item: item.message_id)
            )
        ]

    @staticmethod
    def _episode_id_for_window(
        project_id: str,
        session_id: str,
        messages: list[MessageEpisode],
    ) -> str:
        source_message_ids = ",".join(str(message.message_id) for message in messages)
        return str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"knoggin:episode:{project_id}:{session_id}:{source_message_ids}",
            )
        )

    @staticmethod
    def _build_generation_input(
        context: EpisodeCandidateContext,
        *,
        message_local_ids: dict[int, str],
        entity_local_ids: dict[int, str],
        relationship_local_ids: dict[str, str],
        episode_local_ids: dict[str, str],
    ) -> str:
        """Render bounded episode evidence using this call's local references."""

        payload = EpisodeJob._build_localized_context_payload(
            context,
            message_key="messages",
            message_local_ids=message_local_ids,
            entity_local_ids=entity_local_ids,
            relationship_local_ids=relationship_local_ids,
        )
        payload["prior_episodes"] = [
            {
                "episode_id": episode_local_ids[episode.episode_id],
                "summary": episode.summary,
                "new_developments": episode.new_developments,
                "updates": episode.updates,
                "unresolved": episode.unresolved,
                "importance": episode.importance,
            }
            for episode in context.prior_episodes
        ]
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)

    @staticmethod
    def _build_consolidation_input(
        target_episode_local_id: str,
        context: EpisodeCandidateContext,
        *,
        message_local_ids: dict[int, str],
        entity_local_ids: dict[int, str],
        relationship_local_ids: dict[str, str],
    ) -> str:
        """Render the complete regeneration source set with local references."""

        payload = EpisodeJob._build_localized_context_payload(
            context,
            message_key="source_messages",
            message_local_ids=message_local_ids,
            entity_local_ids=entity_local_ids,
            relationship_local_ids=relationship_local_ids,
        )
        payload["target_episode_id"] = target_episode_local_id
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)

    @staticmethod
    def _build_localized_context_payload(
        context: EpisodeCandidateContext,
        *,
        message_key: str,
        message_local_ids: dict[int, str],
        entity_local_ids: dict[int, str],
        relationship_local_ids: dict[str, str],
    ) -> dict:
        """Render canonical episode evidence without exposing system identifiers."""

        payload = {
            message_key: [
                {
                    "message_id": message_local_ids[int(message["message_id"])],
                    "role": message.get("role"),
                    "content": message.get("content"),
                    "timestamp_ms": message.get("timestamp_ms"),
                }
                for message in context.messages
            ],
            "entity_refs_by_message": {
                message_local_ids[int(message["message_id"])]: [
                    entity_local_ids[entity_id]
                    for entity_id in context.entity_ids_by_message.get(
                        int(message["message_id"]), []
                    )
                ]
                for message in context.messages
            },
            "relationship_refs_by_message": {
                message_local_ids[int(message["message_id"])]: [
                    relationship_local_ids[relationship_id]
                    for relationship_id in context.relationship_ids_by_message.get(
                        int(message["message_id"]), []
                    )
                ]
                for message in context.messages
            },
            "entity_catalog": [
                {
                    "entity_id": entity_local_ids[int(entity["entity_id"])],
                    "canonical_name": entity.get("canonical_name"),
                    "type": entity.get("type"),
                    "aliases": entity.get("aliases", []),
                }
                for entity in context.entity_catalog
            ],
            "relationship_catalog": [
                {
                    "relationship_id": relationship_local_ids[
                        str(relationship["relationship_id"])
                    ],
                    "entity_a": EpisodeJob._render_relationship_endpoint(
                        relationship["entity_a"], entity_local_ids
                    ),
                    "entity_b": EpisodeJob._render_relationship_endpoint(
                        relationship["entity_b"], entity_local_ids
                    ),
                    "relationship_type": relationship.get("relationship_type"),
                    "confidence": relationship.get("confidence"),
                    "context": relationship.get("context"),
                    "evidence_message_ids": [
                        message_local_ids[int(message_id)]
                        for message_id in relationship.get("evidence_message_ids", [])
                    ],
                }
                for relationship in context.relationship_catalog
            ],
        }
        return payload

    @staticmethod
    def _render_relationship_endpoint(
        endpoint: dict,
        entity_local_ids: dict[int, str],
    ) -> dict:
        """Render an endpoint without leaking an ID that is not selectable."""

        rendered = {
            "canonical_name": endpoint.get("canonical_name"),
            "type": endpoint.get("type"),
        }
        local_entity_id = entity_local_ids.get(int(endpoint["entity_id"]))
        if local_entity_id is not None:
            rendered["entity_id"] = local_entity_id
        return rendered

    @staticmethod
    def _resolve_decision(
        decision: LLMEpisodeDecision,
        *,
        message_ids_by_local: dict[str, int],
        entity_ids_by_local: dict[str, int],
        relationship_ids_by_local: dict[str, str],
        episode_ids_by_local: dict[str, str],
    ) -> EpisodeDecision:
        """Resolve a model decision into the internal real-ID contract."""

        payload = decision.model_dump()
        if decision.target_episode_id is not None:
            payload["target_episode_id"] = str(
                resolve_local_id(decision.target_episode_id, episode_ids_by_local)
            )
        payload.update(
            EpisodeJob._resolve_ranked_references(
                decision,
                message_ids_by_local=message_ids_by_local,
                entity_ids_by_local=entity_ids_by_local,
                relationship_ids_by_local=relationship_ids_by_local,
            )
        )
        return EpisodeDecision.model_validate(payload)

    @staticmethod
    def _resolve_consolidation(
        consolidation: LLMEpisodeConsolidation,
        *,
        message_ids_by_local: dict[str, int],
        entity_ids_by_local: dict[str, int],
        relationship_ids_by_local: dict[str, str],
    ) -> EpisodeConsolidation:
        """Resolve a model regeneration into the internal real-ID contract."""

        payload = consolidation.model_dump()
        payload.update(
            EpisodeJob._resolve_ranked_references(
                consolidation,
                message_ids_by_local=message_ids_by_local,
                entity_ids_by_local=entity_ids_by_local,
                relationship_ids_by_local=relationship_ids_by_local,
            )
        )
        return EpisodeConsolidation.model_validate(payload)

    @staticmethod
    def _resolve_ranked_references(
        output: LLMEpisodeDecision | LLMEpisodeConsolidation,
        *,
        message_ids_by_local: dict[str, int],
        entity_ids_by_local: dict[str, int],
        relationship_ids_by_local: dict[str, str],
    ) -> dict:
        """Resolve all local ranked selections before validation or persistence."""

        return {
            "message_influences": [
                {
                    **influence.model_dump(),
                    "message_id": int(
                        resolve_local_id(
                            influence.message_id,
                            message_ids_by_local,
                        )
                    ),
                }
                for influence in output.message_influences
            ],
            "focus_entities": [
                {
                    **focus.model_dump(),
                    "entity_id": int(
                        resolve_local_id(focus.entity_id, entity_ids_by_local)
                    ),
                }
                for focus in output.focus_entities
            ],
            "central_relationships": [
                {
                    **relationship.model_dump(),
                    "relationship_id": str(
                        resolve_local_id(
                            relationship.relationship_id,
                            relationship_ids_by_local,
                        )
                    ),
                }
                for relationship in output.central_relationships
            ],
        }

    @staticmethod
    def _validate_decision(
        decision: EpisodeDecision,
        context: EpisodeCandidateContext,
    ) -> None:
        """Reject identifiers or coverage that do not match canonical context."""

        if decision.action == "skip":
            return

        EpisodeJob._validate_ranked_output(decision, context, "decision")

        if decision.action == "consolidate":
            candidate_episode_ids = {
                episode.episode_id for episode in context.prior_episodes
            }
            if decision.target_episode_id not in candidate_episode_ids:
                raise ValueError(
                    "Episode decision consolidation target must be a prior candidate"
                )

    @staticmethod
    def _validate_ranked_output(
        output: EpisodeDecision | EpisodeConsolidation,
        context: EpisodeCandidateContext,
        output_name: str,
    ) -> None:
        """Reject rankings that do not exactly match the supplied source set."""

        source_message_ids = {
            int(message["message_id"]) for message in context.messages
        }
        influence_message_ids = [
            influence.message_id for influence in output.message_influences
        ]
        if (
            len(influence_message_ids) != len(set(influence_message_ids))
            or set(influence_message_ids) != source_message_ids
        ):
            raise ValueError(
                f"Episode {output_name} message influences must cover each "
                "source message "
                "exactly once"
            )

        focus_entity_ids = [focus.entity_id for focus in output.focus_entities]
        if len(focus_entity_ids) != len(set(focus_entity_ids)):
            raise ValueError(f"Episode {output_name} focus entities must be unique")
        if len(focus_entity_ids) > 2:
            raise ValueError(
                f"Episode {output_name} may select at most two focus entities"
            )
        if not set(focus_entity_ids).issubset(context.entity_ids):
            raise ValueError(
                f"Episode {output_name} focus entities must belong to the source window"
            )

        central_relationship_ids = [
            relationship.relationship_id
            for relationship in output.central_relationships
        ]
        if len(central_relationship_ids) != len(set(central_relationship_ids)):
            raise ValueError(
                f"Episode {output_name} central relationships must be unique"
            )
        if not set(central_relationship_ids).issubset(context.relationship_ids):
            raise ValueError(
                f"Episode {output_name} central relationships must belong to "
                "the source window"
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
                    "action": outcome.action,
                    "episode_id": outcome.episode_id,
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
                f"({', '.join(outcome.action for outcome in outcomes) or 'none'}); "
                f"failures={len(failures)}"
            ),
        )
