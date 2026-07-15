from dataclasses import dataclass
import json
import uuid
from typing import Awaitable, Callable, Optional

from loguru import logger

from common.schema.contracts import EpisodeDecision
from common.schema.primitives import (
    EntityEpisode,
    Episode,
    MessageEpisode,
    RelationshipEpisode,
)
from common.schema.settings import EpisodeSettings, IngestionSettings
from common.utils.events import emit
from core.ingestion.prompts import get_episode_generation_prompt
from infrastructure.job.base import BaseJob, JobContext, JobResult
from infrastructure.knowledge_store import KnowledgeStore
from infrastructure.llm_client import LLMService


@dataclass(frozen=True)
class EpisodeCandidateContext:
    """One ready-to-generate window and the bounded context around it."""

    messages: list[dict]
    entity_ids_by_message: dict[int, list[int]]
    relationship_ids_by_message: dict[int, list[str]]
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
                for message_relationship_ids
                in self.relationship_ids_by_message.values()
                for relationship_id in message_relationship_ids
            }
        )


@dataclass(frozen=True)
class EpisodeProcessingResult:
    """One durable episode outcome, including a checkpointed skip."""

    action: str
    episode_id: str | None
    source_message_count: int


class EpisodeJob(BaseJob):
    """Generates bounded episodic memory for eligible project sessions."""

    def __init__(
        self,
        knowledge_store: KnowledgeStore,
        settings: EpisodeSettings,
        ingestion_settings: IngestionSettings,
        llm: LLMService | None = None,
        session_ids_provider: Optional[Callable[[], Awaitable[list[str]]]] = None,
    ) -> None:
        self.knowledge_store = knowledge_store
        self.llm = llm
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
        self.retrieval_source_message_limit = settings.retrieval_source_message_limit
        logger.info(
            "EpisodeJob settings updated: "
            f"target_messages={self.target_message_count}, "
            f"max_messages={self.max_message_count}, "
            f"max_sessions={self.max_sessions_per_run}"
        )

    async def should_run(self, ctx: JobContext) -> bool:
        """Run when any durable project session has one ready episode window."""

        if not self.enabled or self.llm is None or self.session_ids_provider is None:
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

        checkpoint = await self.knowledge_store.get_last_evaluated_episode_message_id(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )
        return await self.knowledge_store.get_next_episode_window(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            after_message_id=checkpoint,
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
        entity_ids_by_message, relationship_ids_by_message = await self._load_context(
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
            prior_episodes=prior_episodes,
        )

    async def _load_context(
        self,
        message_ids: list[int],
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> tuple[dict[int, list[int]], dict[int, list[str]]]:
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
            overlapping_episodes = (
                await self.knowledge_store.get_episodes_for_entities(
                    source_entity_ids,
                    user_name=user_name,
                    project_id=project_id,
                    session_id=session_id,
                    limit=self.prior_episode_candidate_count,
                )
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
    ) -> EpisodeProcessingResult | None:
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

        decision = await self.llm.generate_structured(
            response_model=EpisodeDecision,
            system=get_episode_generation_prompt(user_name),
            user=self._build_generation_input(context),
            temperature=0.0,
        )
        self._validate_decision(decision, context)
        return decision

    async def process_next_window(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> EpisodeDecision | None:
        """Generate and atomically persist one episode decision for a session."""

        context = await self.load_candidate_context(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )
        if context is None:
            return None
        decision = await self._generate_decision_for_context(
            context,
            user_name=user_name,
        )
        episode = self._build_episode(
            decision,
            context,
            project_id=project_id,
            session_id=session_id,
        )
        window_message_ids = [
            int(message["message_id"]) for message in context.messages
        ]
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
        messages = current_messages
        if not should_create:
            messages = self._combine_messages(target_episode.messages, current_messages)
            if self._exceeds_consolidation_limits(
                target_episode,
                context,
                message_count=len(messages),
            ):
                should_create = True
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
            influence.message_id: influence
            for influence in decision.message_influences
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
        source_message_ids = ",".join(
            str(message.message_id) for message in messages
        )
        return str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"knoggin:episode:{project_id}:{session_id}:{source_message_ids}",
            )
        )

    @staticmethod
    def _build_generation_input(context: EpisodeCandidateContext) -> str:
        """Render only bounded, canonical evidence for the episode generator."""

        payload = {
            "messages": [
                {
                    "message_id": int(message["message_id"]),
                    "role": message.get("role"),
                    "content": message.get("content"),
                    "timestamp_ms": message.get("timestamp_ms"),
                }
                for message in context.messages
            ],
            "entity_ids_by_message": context.entity_ids_by_message,
            "relationship_ids_by_message": context.relationship_ids_by_message,
            "prior_episodes": [
                {
                    "episode_id": episode.episode_id,
                    "summary": episode.summary,
                    "new_developments": episode.new_developments,
                    "updates": episode.updates,
                    "unresolved": episode.unresolved,
                    "importance": episode.importance,
                }
                for episode in context.prior_episodes
            ],
        }
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)

    @staticmethod
    def _validate_decision(
        decision: EpisodeDecision,
        context: EpisodeCandidateContext,
    ) -> None:
        """Reject identifiers or coverage that do not match canonical context."""

        if decision.action == "skip":
            return

        source_message_ids = {
            int(message["message_id"]) for message in context.messages
        }
        influence_message_ids = [
            influence.message_id for influence in decision.message_influences
        ]
        if (
            len(influence_message_ids) != len(set(influence_message_ids))
            or set(influence_message_ids) != source_message_ids
        ):
            raise ValueError(
                "Episode decision message influences must cover each source message "
                "exactly once"
            )

        focus_entity_ids = [focus.entity_id for focus in decision.focus_entities]
        if len(focus_entity_ids) != len(set(focus_entity_ids)):
            raise ValueError("Episode decision focus entities must be unique")
        if len(focus_entity_ids) > 2:
            raise ValueError("Episode decision may select at most two focus entities")
        if not set(focus_entity_ids).issubset(context.entity_ids):
            raise ValueError(
                "Episode decision focus entities must belong to the source window"
            )

        central_relationship_ids = [
            relationship.relationship_id
            for relationship in decision.central_relationships
        ]
        if len(central_relationship_ids) != len(set(central_relationship_ids)):
            raise ValueError("Episode decision central relationships must be unique")
        if not set(central_relationship_ids).issubset(context.relationship_ids):
            raise ValueError(
                "Episode decision central relationships must belong to the source window"
            )

        if decision.action == "consolidate":
            candidate_episode_ids = {
                episode.episode_id for episode in context.prior_episodes
            }
            if decision.target_episode_id not in candidate_episode_ids:
                raise ValueError(
                    "Episode decision consolidation target must be a prior candidate"
                )

    async def execute(self, ctx: JobContext) -> JobResult:
        """Process one ready window in each bounded project-session slice."""

        if not self.enabled:
            return JobResult(success=True, summary="EpisodeJob is disabled")
        if self.llm is None:
            return JobResult(success=False, summary="EpisodeJob has no LLM")
        if self.session_ids_provider is None:
            return JobResult(
                success=False,
                summary="EpisodeJob has no session provider",
            )

        outcomes = []
        failures = []
        session_ids = await self.session_ids_provider()
        for session_id in session_ids[: self.max_sessions_per_run]:
            try:
                outcome = await self.process_next_window(
                    user_name=ctx.user_name,
                    project_id=ctx.project_id,
                    session_id=session_id,
                )
            except Exception as exc:
                failures.append(session_id)
                logger.exception(
                    "EpisodeJob failed for session "
                    f"{session_id}: {exc}"
                )
                await emit(
                    ctx.project_id,
                    "job",
                    "episode_processing_failed",
                    {
                        "user_name": ctx.user_name,
                        "project_id": ctx.project_id,
                        "session_id": session_id,
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
