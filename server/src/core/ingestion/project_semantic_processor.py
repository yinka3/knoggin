"""Episode stage for durable project semantic windows."""

from __future__ import annotations

from dataclasses import replace
from time import monotonic
from typing import Awaitable, Callable, Protocol
from uuid import UUID

from loguru import logger

from common.conf.domain_config import CompiledDomain
from common.exceptions import (
    LLMBudgetExceededError,
    LLMProviderError,
    LLMRefusalError,
)
from common.schema.context import ContextRevisionOrigin, ContextSnapshot
from common.schema.episode.models import Episode, EpisodeCard
from common.schema.semantic_window import (
    SemanticWindowOrigin,
    SemanticWindowRecord,
    SemanticWindowStage,
)
from common.schema.settings import EpisodeSettings, IngestionSettings
from common.utils.time_utils import get_now_ms
from core.ingestion.batch import EpisodeEvidence, EvidenceMessage, SemanticWindowBuild
from core.ingestion.entity_build import EntityBuildService
from core.ingestion.policy import IngestionPolicy
from core.ingestion.relationship_extractor import RelationshipExtractor
from core.ingestion.semantic_window_admission import SemanticWindowAdmission
from core.knowledge.context.maintainer import (
    ContextMaintainer,
    ContextMaintenanceBuild,
    combine_maintenance,
    maintenance_snapshot,
)
from core.knowledge.context.models import (
    ContextMaintenanceState,
    ContextMaterialization,
    ContextUserEditSynchronizationError,
)
from core.knowledge.context.projection import (
    ContextProjection,
    ContextSynchronizationOutcome,
    ContextSynchronizationStatus,
)
from core.knowledge.context.render import context_document_hash
from core.knowledge.context.updater import ContextUpdater, ContextUpdateResult
from core.knowledge.episodes.embedding import (
    EpisodeEmbeddingEncoder,
    encode_episode_embedding_texts,
    exchange_embedding_text,
)
from core.knowledge.episodes.generator import EpisodeGenerator
from core.knowledge.episodes.policy import EpisodeGenerationPolicy
from infrastructure.job.base import BaseJob, JobContext, JobResult


def _is_transient_failure(exc: Exception) -> bool:
    if isinstance(exc, LLMBudgetExceededError):
        return True
    return isinstance(exc, LLMProviderError) and exc.transient


class SemanticEpisodeStore(Protocol):
    """Durable operations owned by project semantic stages."""

    async def get_active_project_semantic_window(
        self, *, user_name: str, project_id: str
    ) -> SemanticWindowRecord | None: ...

    async def get_project_semantic_window_evidence_messages(
        self, window_id: str, *, user_name: str, project_id: str
    ) -> list[dict]: ...

    async def get_project_semantic_window_episode_result(
        self, window_id: str, *, user_name: str, project_id: str
    ) -> list[Episode] | None: ...

    async def get_project_semantic_window_assistant_source_refs(
        self, window_id: str, *, user_name: str, project_id: str
    ) -> list[dict]: ...

    async def get_recent_project_episodes(
        self, *, user_name: str, project_id: str, limit: int
    ) -> list[EpisodeCard]: ...

    async def get_project_context_block_supports(
        self, block_ids: list[str], *, user_name: str, project_id: str
    ) -> dict: ...

    async def get_current_project_context_revision(
        self, *, user_name: str, project_id: str
    ): ...

    async def get_project_context_snapshot(
        self, revision_id: str, *, user_name: str, project_id: str
    ) -> ContextSnapshot | None: ...

    async def get_project_semantic_window_context_snapshot(
        self, window_id: str, *, user_name: str, project_id: str
    ) -> ContextSnapshot | None: ...

    async def commit_project_context_revision(
        self,
        *,
        user_name: str,
        project_id: str,
        expected_parent_revision_id: str | None,
        window_id: str | None,
        origin: ContextRevisionOrigin,
        domain_version: int,
        edit_summary: str,
        materialization: ContextMaterialization,
        maintained: bool = False,
    ) -> ContextSnapshot: ...

    async def forget_messages(
        self,
        *,
        user_name: str,
        project_id: str,
        values: tuple[str, ...],
        message_ids: tuple[int, ...],
        block_ids: tuple[UUID, ...],
    ) -> list[int]: ...

    async def get_episodes_with_forgotten_sources(
        self, *, user_name: str, project_id: str
    ) -> list[dict]: ...

    async def forget_in_episode(self, **kwargs) -> bool: ...

    async def forget_graph(self, **kwargs) -> list[int]: ...

    async def complete_forget_cleanup(self, **kwargs) -> None: ...

    async def get_project_context_maintenance_state(
        self, revision_id: str, *, user_name: str, project_id: str
    ) -> ContextMaintenanceState: ...

    async def advance_project_semantic_window_stage(
        self,
        *,
        window_id: str,
        user_name: str,
        project_id: str,
        expected_stage: SemanticWindowStage,
        next_stage: SemanticWindowStage,
        context_revision_id: str | None = None,
    ) -> bool: ...

    async def write_project_semantic_window_episodes(
        self,
        *,
        window_id: str,
        episodes: list[Episode],
        window_messages: list[dict],
        user_name: str,
        project_id: str,
    ) -> bool: ...

    async def commit_project_semantic_knowledge(self, build: SemanticWindowBuild): ...

    async def get_exchanges_without_embeddings(
        self, *, user_name: str, project_id: str, limit: int
    ) -> list[dict]: ...

    async def write_exchange_embeddings(
        self, *, project_id: str, rows: list[dict]
    ) -> int: ...

    async def get_project_semantic_window_committed_entity_ids(
        self, window_id: str, *, user_name: str, project_id: str
    ) -> tuple[int, ...]: ...

    async def enrich_project_semantic_window_episodes(
        self, *, window_id: str, user_name: str, project_id: str
    ) -> dict[str, int]: ...

    async def record_project_semantic_window_failure(
        self,
        *,
        window_id: str,
        user_name: str,
        project_id: str,
        expected_stage: SemanticWindowStage,
        failure_stage: str,
        failure_code: str,
        error_summary: str,
        failed_at_ms: int,
        next_retry_at_ms: int | None,
        expected_attempt_count: int | None = None,
        expected_episode_result_recorded: bool | None = None,
        consume_attempt: bool = True,
    ) -> SemanticWindowRecord | None: ...


class ProjectSemanticProcessor(BaseJob):
    """Drain one durable project semantic window through its checkpoints.

    The scheduler supplies bounded execution and periodic recovery. Once this
    owner starts a due window, it reloads durable state after each successful
    checkpoint and advances Episode -> Context -> Knowledge -> completion
    without waiting for another scheduler cadence.
    """

    enabled = True
    cadence_seconds = 30
    EXCHANGE_EMBEDDING_BATCH = 64
    run_immediately_on_first_check = True

    def __init__(
        self,
        admission: SemanticWindowAdmission,
        knowledge_store: SemanticEpisodeStore,
        episode_generator: EpisodeGenerator,
        *,
        settings: IngestionSettings,
        exchange_embedding_service: EpisodeEmbeddingEncoder | None = None,
        capture_semantic_policy: Callable[[], Awaitable[IngestionPolicy]],
        context_updater: ContextUpdater,
        context_projection: ContextProjection,
        entity_builder: EntityBuildService,
        relationship_extractor: RelationshipExtractor,
        publish_committed_entity_ids: Callable[[tuple[int, ...]], Awaitable[None]],
        signal_relationship_advisories: Callable[[], bool] | None = None,
        now_ms: Callable[[], int] | None = None,
        context_maintainer: ContextMaintainer | None = None,
    ) -> None:
        if not callable(capture_semantic_policy):
            raise TypeError("ProjectSemanticProcessor requires a semantic policy callback")
        required_collaborators = {
            "context_updater": context_updater,
            "context_projection": context_projection,
            "entity_builder": entity_builder,
            "relationship_extractor": relationship_extractor,
        }
        missing = [
            name for name, collaborator in required_collaborators.items()
            if collaborator is None
        ]
        if missing:
            raise TypeError(
                "ProjectSemanticProcessor requires " + ", ".join(missing)
            )
        if not callable(publish_committed_entity_ids):
            raise TypeError("publish_committed_entity_ids must be callable")
        if not isinstance(settings, IngestionSettings):
            raise TypeError("ProjectSemanticProcessor requires IngestionSettings")
        self.admission = admission
        self.knowledge_store = knowledge_store
        self.episode_generator = episode_generator
        self._exchange_embedding_service = exchange_embedding_service
        self._context_updater = context_updater
        self._context_maintainer = context_maintainer
        self._context_projection = context_projection
        self._entity_builder = entity_builder
        self._relationship_extractor = relationship_extractor
        self._publish_committed_entity_ids = publish_committed_entity_ids
        self._signal_relationship_advisories = signal_relationship_advisories
        self._capture_semantic_policy = capture_semantic_policy
        self._now_ms = now_ms or get_now_ms
        self._timeout_stage: tuple[SemanticWindowRecord, str] | None = None
        self._flush_requested = False
        self.update_settings(settings)

    @property
    def name(self) -> str:
        return "project_semantic"

    def update_settings(self, settings: IngestionSettings) -> None:
        if not isinstance(settings, IngestionSettings):
            raise TypeError("settings must be IngestionSettings")
        self._settings = settings
        self.admission.update_settings(settings)

    def update_episode_settings(self, settings: EpisodeSettings) -> None:
        self.admission.update_episode_settings(settings)

    def request_flush(self) -> None:
        """Turn finished conversation into memory now instead of after the
        idle wait. The request holds until no finished exchange is left."""

        self._flush_requested = True

    @property
    def flush_pending(self) -> bool:
        """True until a run finds nothing left to claim after a flush request."""

        return self._flush_requested

    async def should_run(self, ctx: JobContext) -> bool:
        active = await self.knowledge_store.get_active_project_semantic_window(
            user_name=ctx.user_name,
            project_id=ctx.project_id,
        )
        if active is not None:
            return (
                self._episode_is_due(active)
                or self._context_is_due(active)
                or self._knowledge_is_due(active)
                or self._finalization_is_due(active)
            )
        # New conversation windows use this job's existing immediate/30-second
        # cadence. Selecting here would build and tokenize an unclaimed proposal
        # only for execute() to select it again before the durable claim.
        return False

    async def execute(self, ctx: JobContext) -> JobResult:
        self._timeout_stage = None
        await self._embed_closed_exchanges(ctx)
        window = await self.knowledge_store.get_active_project_semantic_window(
            user_name=ctx.user_name,
            project_id=ctx.project_id,
        )
        policy: IngestionPolicy | None = None
        if window is None:
            policy = await self._capture_semantic_policy()
            synchronization = await self.synchronize_context_file(
                ctx,
                allow_user_edit=True,
                ingestion_policy=policy,
            )
            if not synchronization.allows_semantic_admission:
                return JobResult(
                    success=True,
                    summary=(
                        "Semantic admission deferred while CONTEXT.md user edits "
                        "await synchronization"
                    ),
                )
            window = await self.knowledge_store.get_active_project_semantic_window(
                user_name=ctx.user_name,
                project_id=ctx.project_id,
            )
        elif window.stage is not SemanticWindowStage.CLAIMED:
            await self.synchronize_context_file(ctx, allow_user_edit=False)
        if window is None:
            policy = policy or await self._capture_semantic_policy()
            force_flush = self._flush_requested
            claimed = await self.admission.claim_next(
                user_name=ctx.user_name,
                project_id=ctx.project_id,
                domain=policy.domain,
                ingestion_policy=policy,
                force_flush=force_flush,
            )
            if claimed is None:
                if force_flush:
                    # Only a flushed run may consume the request; one that
                    # started before it may have missed what it asked for.
                    self._flush_requested = False
                return JobResult(success=True, summary="No semantic window is due")
            window = claimed.window
            if claimed.claimed:
                logger.info(
                    "Semantic window claimed project={} window={} tokens={} reason={}",
                    ctx.project_id,
                    window.window_id,
                    window.source_token_count,
                    window.policy_snapshot.get("admission_policy", {}).get("close_reason"),
                )
        return await self._drain_window(window, ctx)

    async def _embed_closed_exchanges(self, ctx: JobContext) -> None:
        """Give each closed exchange its own search vector, ahead of windowing.

        Exchange closure wakes this job, so new conversation becomes
        semantically searchable without waiting for an Episode. Failure only
        delays search; it never blocks semantic windows.
        """

        if self._exchange_embedding_service is None:
            return
        try:
            pending = await self.knowledge_store.get_exchanges_without_embeddings(
                user_name=ctx.user_name,
                project_id=ctx.project_id,
                limit=self.EXCHANGE_EMBEDDING_BATCH,
            )
            if not pending:
                return
            vectors = await encode_episode_embedding_texts(
                self._exchange_embedding_service,
                [exchange_embedding_text(row) for row in pending],
            )
            await self.knowledge_store.write_exchange_embeddings(
                project_id=ctx.project_id,
                rows=[
                    {**row, "embedding": vector}
                    for row, vector in zip(pending, vectors, strict=True)
                ],
            )
        except Exception as exc:
            logger.warning("Exchange embedding deferred: {}", exc)

    async def _drain_window(
        self,
        window: SemanticWindowRecord,
        ctx: JobContext,
    ) -> JobResult:
        """Advance one active window until it fails, waits, or completes.

        The durable row is reloaded after every successful stage. This keeps
        recovery at checkpoint boundaries and prevents this in-memory run from
        assuming that a prior write succeeded or that another owner did not
        change the active window.
        """

        completed_stages: list[str] = []
        while True:
            stage = self._next_due_stage(window)
            if stage is None:
                if completed_stages:
                    return JobResult(
                        success=True,
                        summary=(
                            "Semantic processor stopped after durable stages: "
                            + " -> ".join(completed_stages)
                        ),
                    )
                return JobResult(success=True, summary="No semantic window stage is due")

            stage_name, execute_stage = stage
            self._timeout_stage = (window, stage_name)
            stage_started = monotonic()
            result = await execute_stage(window, ctx)
            self._timeout_stage = None
            if not result.success:
                return result
            completed_stages.append(stage_name)

            reloaded = await self.knowledge_store.get_active_project_semantic_window(
                user_name=ctx.user_name,
                project_id=ctx.project_id,
            )
            if reloaded is None:
                logger.info(
                    "Semantic stage committed project={} window={} stage={} duration_s={:.2f} "
                    "next=completed recovered={} result={}",
                    ctx.project_id, window.window_id, stage_name,
                    monotonic() - stage_started,
                    window.last_failure_code is not None, result.summary,
                )
                return JobResult(
                    success=True,
                    summary=(
                        "Semantic processor completed durable stages: "
                        + " -> ".join(completed_stages)
                    ),
                )
            if reloaded.window_id != window.window_id:
                logger.warning(
                    "Semantic processor yielded because active window {} replaced {}",
                    reloaded.window_id,
                    window.window_id,
                )
                return JobResult(
                    success=True,
                    summary="Semantic processor yielded after the active window changed",
                )
            if reloaded == window:
                logger.warning(
                    "Semantic processor stopped because {} reported success without a durable checkpoint",
                    stage_name,
                )
                return JobResult(
                    success=True,
                    summary="Semantic processor stopped without durable progress",
                )
            logger.info(
                "Semantic stage committed project={} window={} stage={} duration_s={:.2f} "
                "next={} recovered={} result={}",
                ctx.project_id, window.window_id, stage_name,
                monotonic() - stage_started, reloaded.stage.value,
                window.last_failure_code is not None, result.summary,
            )
            window = reloaded

    async def on_timeout(self, ctx: JobContext) -> None:
        """Charge a cancelled stage only if its durable checkpoint is unchanged."""
        attempt = self._timeout_stage
        self._timeout_stage = None
        if attempt is None:
            return
        window, stage_name = attempt
        current = await self.knowledge_store.get_active_project_semantic_window(
            user_name=ctx.user_name,
            project_id=ctx.project_id,
        )
        if (
            current is None
            or current.window_id != window.window_id
            or current.stage is not window.stage
            or current.episode_result_recorded != window.episode_result_recorded
            or current.attempt_count != window.attempt_count
        ):
            return
        await self._record_failure(
            window,
            ctx,
            TimeoutError("Semantic stage exceeded scheduler execution timeout"),
            failure_stage=stage_name.lower(),
            expected_attempt_count=window.attempt_count,
            expected_episode_result_recorded=window.episode_result_recorded,
        )

    def _next_due_stage(
        self,
        window: SemanticWindowRecord,
    ) -> tuple[str, Callable[[SemanticWindowRecord, JobContext], Awaitable[JobResult]]] | None:
        if self._episode_is_due(window):
            return "Episode", self._execute_episode_stage
        if self._context_is_due(window):
            return "Context", self._execute_context_stage
        if self._knowledge_is_due(window):
            return "Knowledge", self._execute_knowledge_stage
        if self._finalization_is_due(window):
            return "finalization", self._execute_finalization_stage
        return None

    async def synchronize_context_file(
        self,
        ctx: JobContext,
        *,
        allow_user_edit: bool,
        ingestion_policy: IngestionPolicy | None = None,
    ) -> ContextSynchronizationOutcome:
        """Run the sole project-owned Context file synchronization boundary."""

        try:
            policy = ingestion_policy or await self._capture_semantic_policy()
            projection = await self._context_projection.synchronize(
                user_name=ctx.user_name,
                project_id=ctx.project_id,
                ingestion_policy=policy,
                allow_user_edit=allow_user_edit,
            )
            return ContextSynchronizationOutcome(
                status=ContextSynchronizationStatus.SYNCED,
                projection=projection,
            )
        except ContextUserEditSynchronizationError as exc:
            logger.warning("Context user-edit synchronization is blocked: {}", exc)
            return ContextSynchronizationOutcome(
                status=ContextSynchronizationStatus.USER_EDIT_BLOCKED,
            )
        except Exception as exc:
            logger.warning("Context file synchronization is pending repair: {}", exc)
            return ContextSynchronizationOutcome(
                status=ContextSynchronizationStatus.PROJECTION_PENDING,
            )

    def read_context_file(self) -> tuple[str, str] | None:
        return self._context_projection.read_local_file()

    async def discard_context_edits(
        self, *, user_name: str, project_id: str, expected_file_hash: str
    ) -> str | None:
        policy = await self._capture_semantic_policy()
        return await self._context_projection.discard_local_edits(
            user_name=user_name, project_id=project_id,
            domain=policy.domain, expected_file_hash=expected_file_hash,
        )

    async def change_user_note(self, *, user_name: str, project_id: str, **change):
        policy = await self._capture_semantic_policy()
        return await self._context_projection.change_user_note(
            user_name=user_name, project_id=project_id,
            ingestion_policy=policy, **change,
        )

    async def _execute_episode_stage(
        self, window: SemanticWindowRecord, ctx: JobContext
    ) -> JobResult:
        try:
            existing = await self.knowledge_store.get_project_semantic_window_episode_result(
                str(window.window_id),
                user_name=ctx.user_name,
                project_id=ctx.project_id,
            )
            if existing is not None:
                return JobResult(
                    success=True,
                    summary=(
                        f"Semantic Episode result already records {len(existing)} episodes"
                    ),
                )

            messages = await self.knowledge_store.get_project_semantic_window_evidence_messages(
                str(window.window_id),
                user_name=ctx.user_name,
                project_id=ctx.project_id,
            )
            if window.origin is SemanticWindowOrigin.HUMAN_EDIT:
                episodes: list[Episode] = []
            else:
                if not messages:
                    raise ValueError("Conversation semantic window has no frozen evidence")
                policy = EpisodeGenerationPolicy.from_semantic_window_snapshot(
                    window.policy_snapshot.get("episode_generation_policy")
                )
                if not policy.enabled:
                    episodes = []
                else:
                    try:
                        build = await self.episode_generator.generate(
                            user_name=ctx.user_name,
                            project_id=ctx.project_id,
                            messages=messages,
                            policy=policy,
                        )
                        episodes = build.final_episodes
                    except LLMRefusalError:
                        if not self._is_last_attempt(window):
                            raise
                        logger.warning(
                            "The model refused window {} on its last attempt; "
                            "recording no Episodes", window.window_id,
                        )
                        episodes = []
            persisted = await self.knowledge_store.write_project_semantic_window_episodes(
                window_id=str(window.window_id),
                episodes=episodes,
                window_messages=messages,
                user_name=ctx.user_name,
                project_id=ctx.project_id,
            )
            if not persisted:
                existing = (
                    await self.knowledge_store.get_project_semantic_window_episode_result(
                        str(window.window_id),
                        user_name=ctx.user_name,
                        project_id=ctx.project_id,
                    )
                )
                if existing is None:
                    raise RuntimeError("Semantic Episode result was not readable after retry")
                episodes = existing
            return JobResult(
                success=True,
                summary=f"Semantic Episode stage recorded {len(episodes)} episodes",
            )
        except Exception as exc:
            logger.exception("Semantic Episode stage failed: {}", exc)
            await self._record_failure(
                window,
                ctx,
                exc,
                failure_stage="episode_generation",
            )
            return JobResult(success=False, summary="Semantic Episode stage failed")

    async def _execute_context_stage(
        self, window: SemanticWindowRecord, ctx: JobContext
    ) -> JobResult:
        """Commit or resume Context without treating its file as canonical."""

        try:
            existing = await self.knowledge_store.get_project_semantic_window_context_snapshot(
                str(window.window_id),
                user_name=ctx.user_name,
                project_id=ctx.project_id,
            )
            if existing is not None:
                await self._forget_in_episodes(ctx)
                await self._advance_context_stage(window, ctx, existing)
                return JobResult(
                    success=True,
                    summary="Semantic Context stage resumed its committed revision",
                )

            current_revision = await self.knowledge_store.get_current_project_context_revision(
                user_name=ctx.user_name,
                project_id=ctx.project_id,
            )
            snapshot = None
            if current_revision is not None:
                snapshot = await self.knowledge_store.get_project_context_snapshot(
                    str(current_revision.revision_id),
                    user_name=ctx.user_name,
                    project_id=ctx.project_id,
                )
                if snapshot is None:
                    raise RuntimeError("Current Context revision was not materialized")

            episodes = await self.knowledge_store.get_project_semantic_window_episode_result(
                str(window.window_id),
                user_name=ctx.user_name,
                project_id=ctx.project_id,
            )
            if episodes is None:
                raise RuntimeError("Context stage requires a durable Episode result")
            messages = await self.knowledge_store.get_project_semantic_window_evidence_messages(
                str(window.window_id),
                user_name=ctx.user_name,
                project_id=ctx.project_id,
            )
            if window.origin is SemanticWindowOrigin.CONVERSATION and not messages:
                raise ValueError("Conversation Context stage has no frozen evidence")
            domain = self._frozen_domain(window)
            context_settings = self._settings.context
            # Only one window per project is open, so every other Episode is
            # from an earlier window.
            this_window = {episode.episode_id for episode in episodes}
            recent = (
                await self.knowledge_store.get_recent_project_episodes(
                    user_name=ctx.user_name,
                    project_id=ctx.project_id,
                    limit=context_settings.earlier_episodes + len(this_window),
                )
                if context_settings.earlier_episodes
                else []
            )
            earlier = [
                episode for episode in recent if episode.episode_id not in this_window
            ][: context_settings.earlier_episodes]
            try:
                result = await self._context_updater.update(
                    user_name=ctx.user_name,
                    project_id=ctx.project_id,
                    domain=domain,
                    snapshot=snapshot,
                    messages=messages,
                    assistant_source_refs=(
                        await self.knowledge_store.get_project_semantic_window_assistant_source_refs(
                            str(window.window_id),
                            user_name=ctx.user_name,
                            project_id=ctx.project_id,
                        )
                    ),
                    earlier_episodes=earlier[::-1],
                    budget_tokens=context_settings.max_tokens,
                )
            except LLMRefusalError:
                if not self._is_last_attempt(window):
                    raise
                logger.warning(
                    "The model refused window {} on its last attempt; "
                    "leaving Context unchanged", window.window_id,
                )
                result = ContextUpdateResult(
                    materialization=None,
                    edit_summary="The model refused this window",
                    operation_count=0,
                )
            # Before the commit, so a retry that resumes the committed revision
            # cannot skip it. Forgetting the same messages again is a no-op.
            for request in result.forget_requests:
                await self.knowledge_store.forget_messages(
                    user_name=ctx.user_name,
                    project_id=ctx.project_id,
                    values=request.values,
                    message_ids=request.message_ids,
                    block_ids=request.block_ids,
                )
            if result.materialization is not None:
                result, maintained = await self._maintain_context(
                    ctx, domain, snapshot, result, episodes=episodes, earlier=earlier[::-1]
                )
                assert result.materialization is not None
                committed = await self.knowledge_store.commit_project_context_revision(
                    user_name=ctx.user_name,
                    project_id=ctx.project_id,
                    expected_parent_revision_id=(
                        None if snapshot is None else str(snapshot.revision_id)
                    ),
                    window_id=str(window.window_id),
                    origin=ContextRevisionOrigin.CONVERSATION,
                    domain_version=domain.version,
                    edit_summary=result.edit_summary,
                    materialization=result.materialization,
                    maintained=maintained,
                )
            elif snapshot is None:
                # A no-op first window still needs an immutable, addressable
                # Context revision for the durable stage checkpoint.
                committed = await self.knowledge_store.commit_project_context_revision(
                    user_name=ctx.user_name,
                    project_id=ctx.project_id,
                    expected_parent_revision_id=None,
                    window_id=str(window.window_id),
                    origin=ContextRevisionOrigin.CONVERSATION,
                    domain_version=domain.version,
                    edit_summary=result.edit_summary,
                    materialization=ContextMaterialization(
                        blocks=(),
                        content_hash=context_document_hash((), domain),
                        new_block_ids=frozenset(),
                    ),
                )
            elif snapshot.domain_version != domain.version:
                # A domain activation changes the interpretation rules even
                # when Context text stays the same. Give this window a
                # revision at its frozen version with an empty impact closure;
                # the Knowledge stage can then checkpoint the no-op safely.
                committed = await self.knowledge_store.commit_project_context_revision(
                    user_name=ctx.user_name,
                    project_id=ctx.project_id,
                    expected_parent_revision_id=str(snapshot.revision_id),
                    window_id=str(window.window_id),
                    origin=ContextRevisionOrigin.CONVERSATION,
                    domain_version=domain.version,
                    edit_summary="Context rules updated; no block changes",
                    materialization=ContextMaterialization(
                        blocks=tuple(snapshot.blocks),
                        content_hash=context_document_hash(snapshot.blocks, domain),
                        new_block_ids=frozenset(),
                        impacted_block_ids=frozenset(),
                    ),
                )
            else:
                committed = snapshot

            await self._forget_in_episodes(ctx)
            await self._advance_context_stage(window, ctx, committed)
            return JobResult(
                success=True,
                summary=(
                    "Semantic Context stage committed "
                    f"revision {committed.revision_number}"
                ),
            )
        except Exception as exc:
            logger.exception("Semantic Context stage failed: {}", exc)
            await self._record_failure(
                window,
                ctx,
                exc,
                failure_stage="context_update",
            )
            return JobResult(success=False, summary="Semantic Context stage failed")

    async def _forget_in_episodes(self, ctx: JobContext) -> None:
        """Rewrite every Episode linked to a message forgotten since its last
        update. A failed window's retry finds and finishes the rest; an
        Episode edited meanwhile is read again on that retry.
        """

        episodes = await self.knowledge_store.get_episodes_with_forgotten_sources(
            user_name=ctx.user_name, project_id=ctx.project_id
        )
        stale = 0
        for episode in episodes:
            rewrite, embedding = await self.episode_generator.rewrite_without(
                user_name=ctx.user_name,
                episode=episode,
                forgotten_texts=list(episode["forgotten_texts"]),
            )
            stored = await self.knowledge_store.forget_in_episode(
                episode_id=episode["episode_id"],
                user_name=ctx.user_name,
                project_id=ctx.project_id,
                summary=rewrite.summary,
                new_developments=rewrite.new_developments,
                updates=rewrite.updates,
                unresolved=rewrite.unresolved,
                embedding=embedding,
                forgotten_message_ids=list(episode["forgotten_ids"]),
                expected_updated_at=episode["updated_at"],
            )
            stale += not stored
        if stale:
            raise RuntimeError(f"{stale} Episodes changed while forgetting; retrying")
        deleted_ids = await self.knowledge_store.forget_graph(
            user_name=ctx.user_name, project_id=ctx.project_id,
        )
        if deleted_ids:
            await self._publish_committed_entity_ids(tuple(deleted_ids))
        await self.knowledge_store.complete_forget_cleanup(
            user_name=ctx.user_name, project_id=ctx.project_id,
        )

    async def _maintain_context(
        self,
        ctx: JobContext,
        domain: CompiledDomain,
        snapshot: ContextSnapshot | None,
        result: ContextUpdateResult,
        *,
        episodes: list[Episode],
        earlier: list[EpisodeCard],
    ) -> tuple[ContextUpdateResult, bool]:
        """Run the maintainer on the updated Context when it is due.

        Due after maintain_every_episodes Episodes from Context-changing
        windows, or when Context is over budget. Any failure skips it; the
        per-window update still commits.
        """

        update = result.materialization
        if self._context_maintainer is None or update is None:
            return result, False
        settings = self._settings.context
        try:
            state = (
                await self.knowledge_store.get_project_context_maintenance_state(
                    str(snapshot.revision_id),
                    user_name=ctx.user_name,
                    project_id=ctx.project_id,
                )
                if snapshot is not None
                else ContextMaintenanceState(0, {})
            )
            # This window changed Context, so its Episodes age every block it
            # did not write.
            ages = {
                block.block_id: (
                    0 if block.block_id in update.new_block_ids
                    else state.block_ages.get(block.block_id, 0) + len(episodes)
                )
                for block in update.blocks
            }
            supports: dict = {}
            for support in update.supports:
                supports.setdefault(support.block_id, []).append(support)
            older = [str(b.block_id) for b in update.blocks if b.block_id not in supports]
            supports.update(
                await self.knowledge_store.get_project_context_block_supports(
                    older, user_name=ctx.user_name, project_id=ctx.project_id
                )
            )
            build = ContextMaintenanceBuild(
                domain=domain,
                snapshot=maintenance_snapshot(
                    update, project_id=ctx.project_id, domain=domain
                ),
                ages=ages,
                supports=supports,
                budget_tokens=settings.max_tokens,
                max_removed_share=settings.maintain_max_removed_share,
                earlier_episodes=earlier,
                count_tokens=self._context_maintainer.count_tokens,
            )
            due = state.episodes_since_maintenance + len(episodes)
            if (
                due < settings.maintain_every_episodes
                and build.context_tokens() <= settings.max_tokens
            ):
                return result, False
            maintenance = await self._context_maintainer.maintain(
                user_name=ctx.user_name, build=build
            )
        except Exception as exc:
            logger.warning("Context maintenance skipped: {}", exc)
            return result, False
        if maintenance.materialization is None:
            return result, True
        return replace(
            result,
            materialization=combine_maintenance(update, maintenance.materialization),
            edit_summary=f"{result.edit_summary}; {maintenance.summary}"[:2_000],
        ), True

    def _is_last_attempt(self, window: SemanticWindowRecord) -> bool:
        # A refusal can be chance, so it retries like any failure. On the last
        # attempt the stage records nothing instead: the window's messages stay
        # searchable and later windows are no longer blocked behind it.
        return window.attempt_count + 1 >= self._settings.semantic_window_retry.max_attempts

    def _episode_is_due(self, window: SemanticWindowRecord) -> bool:
        if window.stage is not SemanticWindowStage.CLAIMED:
            return False
        if window.episode_result_recorded:
            return False
        if window.attempt_count >= self._settings.semantic_window_retry.max_attempts:
            return False
        return window.next_retry_at_ms is None or self._now_ms() >= window.next_retry_at_ms

    def _context_is_due(self, window: SemanticWindowRecord) -> bool:
        if window.stage is not SemanticWindowStage.CLAIMED:
            return False
        if not window.episode_result_recorded:
            return False
        if window.attempt_count >= self._settings.semantic_window_retry.max_attempts:
            return False
        return window.next_retry_at_ms is None or self._now_ms() >= window.next_retry_at_ms

    def _knowledge_is_due(self, window: SemanticWindowRecord) -> bool:
        if window.stage is not SemanticWindowStage.CONTEXT_COMMITTED:
            return False
        if window.attempt_count >= self._settings.semantic_window_retry.max_attempts:
            return False
        return window.next_retry_at_ms is None or self._now_ms() >= window.next_retry_at_ms

    def _finalization_is_due(self, window: SemanticWindowRecord) -> bool:
        if window.stage is not SemanticWindowStage.KNOWLEDGE_COMMITTED:
            return False
        if window.attempt_count >= self._settings.semantic_window_retry.max_attempts:
            return False
        return window.next_retry_at_ms is None or self._now_ms() >= window.next_retry_at_ms

    async def _execute_knowledge_stage(
        self, window: SemanticWindowRecord, ctx: JobContext
    ) -> JobResult:
        """Extract this window's Episodes into Knowledge and commit it atomically."""

        try:
            build = SemanticWindowBuild.from_committed_window(
                window=window, **await self._knowledge_input(window, ctx),
            )
            if build.episodes:
                await self._entity_builder.build(build)
                await self._relationship_extractor.extract(build)
            else:
                build.set_empty_knowledge_result()
            summary = await self.knowledge_store.commit_project_semantic_knowledge(build)
            if self._signal_relationship_advisories is not None:
                try:
                    self._signal_relationship_advisories()
                except Exception:
                    logger.exception("Could not wake relationship advisory refresh")
            return JobResult(
                success=True,
                summary=(
                    "Semantic Knowledge stage resumed its committed graph"
                    if summary.resumed
                    else "Semantic Knowledge stage committed "
                    f"{summary.relationships_written} relationships"
                ),
            )
        except Exception as exc:
            logger.exception("Semantic Knowledge stage failed: {}", exc)
            await self._record_failure(
                window,
                ctx,
                exc,
                failure_stage="knowledge_reconciliation",
            )
            return JobResult(success=False, summary="Semantic Knowledge stage failed")

    async def _knowledge_input(self, window: SemanticWindowRecord, ctx: JobContext) -> dict:
        """The window's Episodes with their messages, earlier summaries, and hints."""

        scope = {"user_name": ctx.user_name, "project_id": ctx.project_id}
        episodes = await self.knowledge_store.get_project_semantic_window_episode_result(
            str(window.window_id), **scope
        ) or []
        if not episodes:
            return {"episodes": ()}
        # What the user asked to forget never becomes Knowledge.
        messages = {
            int(row["message_id"]): row
            for row in await self.knowledge_store.get_project_semantic_window_evidence_messages(
                str(window.window_id), **scope
            )
            if row.get("lifecycle_state") != "forgotten"
        }
        evidence = []
        for episode in episodes:
            covered = tuple(
                EvidenceMessage(
                    message_id=item.message_id,
                    role=str(messages[item.message_id]["role"]),
                    content=str(messages[item.message_id]["content"]),
                    timestamp_ms=int(messages[item.message_id]["timestamp_ms"] or 0),
                )
                for item in sorted(episode.messages, key=lambda m: m.message_position)
                if item.message_id in messages
            )
            if covered:
                evidence.append(EpisodeEvidence(
                    episode_id=episode.episode_id, summary=episode.summary, messages=covered,
                ))
        policy = self._frozen_policy(window)
        lookback = max(policy.vp01_episode_lookback, policy.vp02_episode_lookback)
        earlier: list[str] = []
        if lookback:
            own = {episode.episode_id for episode in episodes}
            recent = await self.knowledge_store.get_recent_project_episodes(
                limit=lookback + len(own), **scope
            )
            earlier = [card.summary for card in recent if card.episode_id not in own][
                :lookback
            ][::-1]
        return {
            "episodes": tuple(evidence),
            "earlier_summaries": earlier,
            "context_hints": await self._context_hints(window, ctx, set(messages)),
        }

    async def _context_hints(
        self, window: SemanticWindowRecord, ctx: JobContext, message_ids: set[int]
    ) -> dict[int, tuple[str, ...]]:
        """For each message, the current Context blocks it supports."""

        if window.context_revision_id is None:
            return {}
        scope = {"user_name": ctx.user_name, "project_id": ctx.project_id}
        context = await self.knowledge_store.get_project_context_snapshot(
            str(window.context_revision_id), **scope
        )
        if context is None or not context.blocks:
            return {}
        supports = await self.knowledge_store.get_project_context_block_supports(
            [str(block.block_id) for block in context.blocks], **scope
        )
        markdown = {block.block_id: block.markdown for block in context.blocks}
        hints: dict[int, list[str]] = {}
        for block_id, rows in supports.items():
            for support in rows:
                if support.message_id in message_ids:
                    hints.setdefault(support.message_id, []).append(markdown[block_id])
        return {message_id: tuple(values) for message_id, values in hints.items()}

    @staticmethod
    def _frozen_policy(window: SemanticWindowRecord) -> IngestionPolicy:
        return IngestionPolicy.from_semantic_window_snapshot(
            window.policy_snapshot.get("ingestion_policy")
        )

    async def _execute_finalization_stage(
        self, window: SemanticWindowRecord, ctx: JobContext
    ) -> JobResult:
        """Publish committed resolver state, enrich Episodes, then complete."""

        try:
            entity_ids = (
                await self.knowledge_store.get_project_semantic_window_committed_entity_ids(
                    str(window.window_id),
                    user_name=ctx.user_name,
                    project_id=ctx.project_id,
                )
            )
            if entity_ids:
                await self._publish_committed_entity_ids(entity_ids)
        except Exception as exc:
            logger.exception("Semantic resolver publication stage failed: {}", exc)
            await self._record_failure(
                window,
                ctx,
                exc,
                failure_stage="resolver_publication",
            )
            return JobResult(
                success=False,
                summary="Semantic resolver publication stage failed",
            )

        try:
            enriched = await self.knowledge_store.enrich_project_semantic_window_episodes(
                window_id=str(window.window_id),
                user_name=ctx.user_name,
                project_id=ctx.project_id,
            )
            advanced = await self.knowledge_store.advance_project_semantic_window_stage(
                window_id=str(window.window_id),
                user_name=ctx.user_name,
                project_id=ctx.project_id,
                expected_stage=SemanticWindowStage.KNOWLEDGE_COMMITTED,
                next_stage=SemanticWindowStage.COMPLETED,
            )
            if not advanced:
                current = await self.knowledge_store.get_active_project_semantic_window(
                    user_name=ctx.user_name,
                    project_id=ctx.project_id,
                )
                if current is not None:
                    raise RuntimeError("Semantic completion checkpoint changed unexpectedly")
            return JobResult(
                success=True,
                summary=(
                    "Semantic window completed after enriching "
                    f"{enriched['entities']} entities and {enriched['relationships']} relationships"
                ),
            )
        except Exception as exc:
            logger.exception("Semantic finalization stage failed: {}", exc)
            await self._record_failure(
                window,
                ctx,
                exc,
                failure_stage="episode_enrichment",
            )
            return JobResult(success=False, summary="Semantic finalization stage failed")

    @staticmethod
    def _frozen_domain(window: SemanticWindowRecord) -> CompiledDomain:
        payload = window.policy_snapshot.get("compiled_domain")
        domain = CompiledDomain.from_dict(payload)
        if domain.version != window.domain_version:
            raise ValueError("semantic window domain snapshot does not match its version")
        return domain

    async def _advance_context_stage(
        self,
        window: SemanticWindowRecord,
        ctx: JobContext,
        committed: ContextSnapshot,
    ) -> None:
        advanced = await self.knowledge_store.advance_project_semantic_window_stage(
            window_id=str(window.window_id),
            user_name=ctx.user_name,
            project_id=ctx.project_id,
            expected_stage=SemanticWindowStage.CLAIMED,
            next_stage=SemanticWindowStage.CONTEXT_COMMITTED,
            context_revision_id=str(committed.revision_id),
        )
        if not advanced:
            active = await self.knowledge_store.get_active_project_semantic_window(
                user_name=ctx.user_name,
                project_id=ctx.project_id,
            )
            if (
                active is None
                or active.window_id != window.window_id
                or active.stage is not SemanticWindowStage.CONTEXT_COMMITTED
                or active.context_revision_id != committed.revision_id
            ):
                raise RuntimeError("Context checkpoint changed before it could be recorded")
        await self.synchronize_context_file(ctx, allow_user_edit=False)

    async def _record_failure(
        self,
        window: SemanticWindowRecord,
        ctx: JobContext,
        exc: Exception,
        *,
        failure_stage: str,
        expected_attempt_count: int | None = None,
        expected_episode_result_recorded: bool | None = None,
    ) -> None:
        failed_at_ms = self._now_ms()
        retry = self._settings.semantic_window_retry
        # Provider outages and a spent budget say nothing about this window's
        # content. Retry them at the slowest cadence without spending the
        # bounded attempts reserved for failures that may never succeed.
        consume_attempt = not _is_transient_failure(exc)
        attempt = window.attempt_count + 1
        delay_seconds = (
            min(
                retry.initial_backoff_seconds * (2 ** max(0, attempt - 1)),
                retry.max_backoff_seconds,
            )
            if consume_attempt
            else retry.max_backoff_seconds
        )
        next_retry_at_ms = (
            failed_at_ms + (delay_seconds * 1000)
            if attempt < retry.max_attempts or not consume_attempt
            else None
        )
        attempt_guard = (
            {"expected_attempt_count": expected_attempt_count}
            if expected_attempt_count is not None
            else {}
        )
        checkpoint_guard = (
            {"expected_episode_result_recorded": expected_episode_result_recorded}
            if expected_episode_result_recorded is not None
            else {}
        )
        await self.knowledge_store.record_project_semantic_window_failure(
            window_id=str(window.window_id),
            user_name=ctx.user_name,
            project_id=ctx.project_id,
            expected_stage=window.stage,
            failure_stage=failure_stage,
            failure_code=type(exc).__name__,
            error_summary=str(exc)[:2_000] or type(exc).__name__,
            failed_at_ms=failed_at_ms,
            next_retry_at_ms=next_retry_at_ms,
            consume_attempt=consume_attempt,
            **attempt_guard,
            **checkpoint_guard,
        )
        logger.warning(
            "Semantic window failed project={} window={} stage={} error={} "
            "attempt={} charged={} retry_at_ms={} terminal={}",
            ctx.project_id, window.window_id, failure_stage,
            type(exc).__name__, attempt, consume_attempt,
            next_retry_at_ms, next_retry_at_ms is None,
        )
