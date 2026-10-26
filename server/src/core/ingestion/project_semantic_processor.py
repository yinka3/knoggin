"""Episode stage for durable project semantic windows."""

from __future__ import annotations

from time import time
from typing import Awaitable, Callable, Protocol

from loguru import logger

from common.conf.domain_config import CompiledDomain
from common.exceptions import LLMBudgetExceededError, LLMProviderError
from common.schema.context import ContextRevisionOrigin, ContextSnapshot
from common.schema.episode.models import Episode
from common.schema.semantic_window import (
    SemanticWindowOrigin,
    SemanticWindowRecord,
    SemanticWindowStage,
)
from common.schema.settings import EpisodeSettings, IngestionSettings
from core.ingestion.batch import SemanticWindowBuild
from core.ingestion.context_entity_build import ContextEntityBuildService
from core.ingestion.policy import IngestionPolicy
from core.ingestion.relationship_extractor import ContextRelationshipExtractor
from core.ingestion.semantic_window_admission import SemanticWindowAdmission
from core.knowledge.context.models import (
    ContextMaterialization,
    ContextUserEditSynchronizationError,
)
from core.knowledge.context.projection import (
    ContextProjection,
    ContextSynchronizationOutcome,
    ContextSynchronizationStatus,
)
from core.knowledge.context.render import context_document_hash
from core.knowledge.context.updater import ContextUpdater
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

    async def get_project_context_revision_impact_block_ids(
        self, revision_id: str, *, user_name: str, project_id: str
    ) -> frozenset: ...

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
    ) -> ContextSnapshot: ...

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
        context_entity_builder: ContextEntityBuildService,
        context_relationship_extractor: ContextRelationshipExtractor,
        publish_committed_entity_ids: Callable[[tuple[int, ...]], Awaitable[None]],
        signal_relationship_advisories: Callable[[], bool] | None = None,
        now_ms: Callable[[], int] | None = None,
    ) -> None:
        if not callable(capture_semantic_policy):
            raise TypeError("ProjectSemanticProcessor requires a semantic policy callback")
        required_collaborators = {
            "context_updater": context_updater,
            "context_projection": context_projection,
            "context_entity_builder": context_entity_builder,
            "context_relationship_extractor": context_relationship_extractor,
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
        self._context_projection = context_projection
        self._context_entity_builder = context_entity_builder
        self._context_relationship_extractor = context_relationship_extractor
        self._publish_committed_entity_ids = publish_committed_entity_ids
        self._signal_relationship_advisories = signal_relationship_advisories
        self._capture_semantic_policy = capture_semantic_policy
        self._now_ms = now_ms or (lambda: int(time() * 1000))
        self._timeout_stage: tuple[SemanticWindowRecord, str] | None = None
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
            claimed = await self.admission.claim_next(
                user_name=ctx.user_name,
                project_id=ctx.project_id,
                domain=policy.domain,
                ingestion_policy=policy,
            )
            if claimed is None:
                return JobResult(success=True, summary="No semantic window is due")
            window = claimed.window
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
                    build = await self.episode_generator.generate(
                        user_name=ctx.user_name,
                        project_id=ctx.project_id,
                        messages=messages,
                        policy=policy,
                    )
                    episodes = build.final_episodes
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
                episodes=episodes,
            )
            if result.materialization is not None:
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
        """Rebuild one frozen Context impact closure and atomically commit it."""

        try:
            if window.context_revision_id is None:
                raise RuntimeError("Context-committed window has no Context revision")
            context = await self.knowledge_store.get_project_context_snapshot(
                str(window.context_revision_id),
                user_name=ctx.user_name,
                project_id=ctx.project_id,
            )
            if context is None:
                raise RuntimeError("Context checkpoint was not materialized")
            # The revision-owned window is the only window allowed to replay
            # its persisted impact.  A later no-op window checkpoints the
            # same immutable revision with an empty effective impact; the
            # writer rechecks that durable ownership before advancing it.
            impact_block_ids = (
                await self.knowledge_store.get_project_context_revision_impact_block_ids(
                    str(context.revision_id),
                    user_name=ctx.user_name,
                    project_id=ctx.project_id,
                )
                if context.window_id == window.window_id
                else frozenset()
            )
            if impact_block_ids:
                supports = await self.knowledge_store.get_project_context_block_supports(
                    [str(block.block_id) for block in context.blocks],
                    user_name=ctx.user_name,
                    project_id=ctx.project_id,
                )
                messages = (
                    await self.knowledge_store.get_project_semantic_window_evidence_messages(
                        str(window.window_id),
                        user_name=ctx.user_name,
                        project_id=ctx.project_id,
                    )
                )
            else:
                supports = {}
                messages = []
            build = SemanticWindowBuild.from_committed_window(
                window=window,
                context=context,
                impact_block_ids=impact_block_ids,
                block_supports=supports,
                message_text_by_id={
                    int(message["message_id"]): str(message["content"])
                    for message in messages
                },
            )
            if build.impact_block_ids:
                await self._context_entity_builder.build(build)
                await self._context_relationship_extractor.extract(build)
                if build.unknown_endpoint_diagnostics:
                    await self._context_entity_builder.build(build)
                    await self._context_relationship_extractor.extract(build)
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
