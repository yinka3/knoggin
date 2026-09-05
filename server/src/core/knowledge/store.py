from typing import Dict, List, Optional, Set, Tuple

from loguru import logger

from common.conf.domain_config import CompiledDomain
from common.schema.artifacts import ArtifactDraft, ArtifactReference, ArtifactRevision
from common.schema.context import (
    ContextProjectionState,
    ContextRevisionOrigin,
    ContextRevisionRecord,
    ContextSnapshot,
)
from common.schema.episode.models import Episode, EpisodeCard
from common.schema.evidence import EvidenceBundle, EvidenceTraversalLimits
from common.schema.semantic_window import (
    SemanticWindowClaimResult,
    SemanticWindowMessage,
    SemanticWindowRecord,
    SemanticWindowStage,
)
from common.schema.source.references import (
    AssistantMessageWithSources,
    SourceConsulted,
    SourceReference,
    SourceReferenceCandidate,
)
from core.knowledge.context.models import ContextMaterialization
from core.knowledge.db.embedding_rebuilder import EmbeddingRebuilder
from core.knowledge.db.id_allocator import IdAllocator
from core.knowledge.db.projection_rebuilder import GraphBuilder
from core.knowledge.db.readers.artifact_reader import ArtifactReader
from core.knowledge.db.readers.entity_reader import EntityReader
from core.knowledge.db.readers.episode_reader import EpisodeReader
from core.knowledge.db.readers.evidence_traversal_reader import EvidenceTraversalReader
from core.knowledge.db.readers.graph_reader import GraphReader
from core.knowledge.db.readers.knowledge_query_reader import KnowledgeQueryReader
from core.knowledge.db.readers.message_reader import MessageReader
from core.knowledge.db.readers.project_context_reader import ProjectContextReader
from core.knowledge.db.readers.relationship_observation_reader import (
    RelationshipObservationReader,
)
from core.knowledge.db.readers.semantic_window_reader import SemanticWindowReader
from core.knowledge.db.readers.source_reference_reader import SourceReferenceReader
from core.knowledge.db.writers.artifact_writer import ArtifactWriter
from core.knowledge.db.writers.entity_reclassification_writer import (
    EntityReclassificationWriter,
    HistoricalReclassificationResult,
)
from core.knowledge.db.writers.episode_writer import EpisodeWriter
from core.knowledge.db.writers.graph_writer import GraphWriter
from core.knowledge.db.writers.message_lifecycle_writer import (
    ExchangeClosure,
    MessageAcceptance,
    MessageLifecycleWriter,
)
from core.knowledge.db.writers.message_writer import MessageWriter
from core.knowledge.db.writers.project_context_writer import ProjectContextWriter
from core.knowledge.db.writers.relationship_reclassification_writer import (
    HistoricalRelationshipReclassificationResult,
    RelationshipReclassificationWriter,
)
from core.knowledge.db.writers.semantic_commit_writer import (
    SemanticCommitSummary,
    SemanticCommitWriter,
)
from core.knowledge.db.writers.semantic_window_writer import SemanticWindowWriter
from core.knowledge.db.writers.source_reference_writer import SourceReferenceWriter
from core.knowledge.evidence_service import EvidenceService
from core.knowledge.services.embedding_service import EmbeddingService
from infrastructure.postgres_client import PostgresClient


class KnowledgeStore:
    """
    Core-facing facade over durable knowledge persistence.

    Composes focused readers, writers, rebuilders, and tool queries over the
    runtime-owned PostgresClient. Callers use this
    boundary without depending on the underlying SQL, AGE, or index layout.
    """

    def __init__(
        self,
        postgres_client: PostgresClient,
        embedding_service: EmbeddingService,
    ):
        self._postgres_client = postgres_client
        self._id_allocator = IdAllocator(self._postgres_client)
        self._graph_writer = GraphWriter(self._postgres_client)
        self._entity_reclassification_writer = EntityReclassificationWriter(
            self._postgres_client
        )
        self._relationship_reclassification_writer = RelationshipReclassificationWriter(
            self._postgres_client
        )
        self._episode_writer = EpisodeWriter(self._postgres_client)
        self._message_writer = MessageWriter(self._postgres_client)
        self._message_lifecycle_writer = MessageLifecycleWriter(
            self._postgres_client, self._message_writer
        )
        self._source_reference_writer = SourceReferenceWriter(self._postgres_client)
        self._artifact_writer = ArtifactWriter(self._postgres_client)
        self._project_context_writer = ProjectContextWriter(self._postgres_client)
        self._semantic_window_writer = SemanticWindowWriter(self._postgres_client)
        self._semantic_commit_writer = SemanticCommitWriter(self._postgres_client)
        self._entity_reader = EntityReader(self._postgres_client)
        self._evidence_service = EvidenceService(
            EvidenceTraversalReader(self._postgres_client)
        )
        self._episode_reader = EpisodeReader(self._postgres_client)
        self._graph_reader = GraphReader(self._postgres_client)
        self._message_reader = MessageReader(self._postgres_client)
        self._knowledge_query_reader = KnowledgeQueryReader(self._postgres_client)
        self._source_reference_reader = SourceReferenceReader(self._postgres_client)
        self._artifact_reader = ArtifactReader(self._postgres_client)
        self._project_context_reader = ProjectContextReader(self._postgres_client)
        self._relationship_observation_reader = RelationshipObservationReader(
            self._postgres_client
        )
        self._semantic_window_reader = SemanticWindowReader(self._postgres_client)
        self._projection_rebuilder = GraphBuilder(self._postgres_client)
        self._embedding_rebuilder = EmbeddingRebuilder(
            self._postgres_client,
            embedding_service,
        )
        logger.info("KnowledgeStore initialized with internal Postgres/AGE backend")

    async def get_relationship_observation_evidence(
        self,
        observation_id: int,
        *,
        user_name: str,
        project_id: str,
        limits: EvidenceTraversalLimits | None = None,
    ) -> EvidenceBundle:
        """Return bounded provenance for one project-owned observation."""

        return await self._evidence_service.for_relationship_observation(
            observation_id,
            user_name=user_name,
            project_id=project_id,
            limits=limits,
        )

    async def get_context_block_evidence(
        self,
        block_id: str,
        *,
        user_name: str,
        project_id: str,
        limits: EvidenceTraversalLimits | None = None,
    ) -> EvidenceBundle:
        """Return bounded provenance for one project-owned Context block."""

        return await self._evidence_service.for_context_block(
            block_id,
            user_name=user_name,
            project_id=project_id,
            limits=limits,
        )

    async def get_relationship_observations_evidence(
        self,
        observation_ids: list[int],
        *,
        user_name: str,
        project_id: str,
        limits: EvidenceTraversalLimits | None = None,
    ) -> tuple[EvidenceBundle, ...]:
        """Return bounded provenance for a set of project observations."""

        return await self._evidence_service.for_relationship_observations(
            observation_ids,
            user_name=user_name,
            project_id=project_id,
            limits=limits,
        )

    async def save_message_logs(self, messages: List[Dict]) -> bool:
        return await self._message_writer.save_message_logs(messages)

    async def create_editable_user_message(
        self, message: Dict, *, edit_window_seconds: int
    ) -> MessageAcceptance:
        return await self._message_lifecycle_writer.create_editable_user_message(
            message, edit_window_seconds=edit_window_seconds
        )

    async def ensure_project_context(
        self,
        *,
        user_name: str,
        project_id: str,
    ) -> None:
        """Initialize the project-owned Context root without creating a revision."""

        await self._project_context_writer.ensure_context(
            user_name=user_name,
            project_id=project_id,
        )

    async def get_current_project_context_revision(
        self,
        *,
        user_name: str,
        project_id: str,
    ) -> ContextRevisionRecord | None:
        return await self._project_context_reader.get_current_revision(
            user_name=user_name,
            project_id=project_id,
        )

    async def get_project_context_projection_state(
        self,
        *,
        user_name: str,
        project_id: str,
    ) -> ContextProjectionState | None:
        return await self._project_context_reader.get_projection_state(
            user_name=user_name,
            project_id=project_id,
        )

    async def get_project_context_snapshot(
        self,
        revision_id: str,
        *,
        user_name: str,
        project_id: str,
    ) -> ContextSnapshot | None:
        return await self._project_context_reader.get_snapshot(
            revision_id,
            user_name=user_name,
            project_id=project_id,
        )

    async def get_project_context_revision_impact_block_ids(
        self,
        revision_id: str,
        *,
        user_name: str,
        project_id: str,
    ) -> frozenset:
        return await self._project_context_reader.get_revision_impact_block_ids(
            revision_id,
            user_name=user_name,
            project_id=project_id,
        )

    async def get_project_context_block_supports(
        self,
        block_ids: list[str],
        *,
        user_name: str,
        project_id: str,
    ) -> dict:
        return await self._project_context_reader.get_block_supports(
            block_ids,
            user_name=user_name,
            project_id=project_id,
        )

    async def get_project_semantic_window_context_snapshot(
        self,
        window_id: str,
        *,
        user_name: str,
        project_id: str,
    ) -> ContextSnapshot | None:
        """Reload a Context revision committed before its window checkpoint CAS."""

        return await self._project_context_reader.get_window_snapshot(
            window_id,
            user_name=user_name,
            project_id=project_id,
        )

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
    ) -> ContextSnapshot:
        return await self._project_context_writer.commit_revision(
            user_name=user_name,
            project_id=project_id,
            expected_parent_revision_id=expected_parent_revision_id,
            window_id=window_id,
            origin=origin,
            domain_version=domain_version,
            edit_summary=edit_summary,
            materialization=materialization,
        )

    async def record_project_context_projection(
        self,
        *,
        user_name: str,
        project_id: str,
        revision_id: str,
        projection_hash: str,
    ) -> bool:
        return await self._project_context_writer.record_projection(
            user_name=user_name,
            project_id=project_id,
            revision_id=revision_id,
            projection_hash=projection_hash,
        )

    async def claim_project_semantic_window(
        self,
        window: SemanticWindowRecord,
        messages: list[SemanticWindowMessage],
    ) -> SemanticWindowClaimResult:
        return await self._semantic_window_writer.claim_window(window, messages)

    async def get_project_semantic_window(
        self,
        window_id: str,
        *,
        user_name: str,
        project_id: str,
    ) -> SemanticWindowRecord | None:
        return await self._semantic_window_reader.get_window(
            window_id,
            user_name=user_name,
            project_id=project_id,
        )

    async def get_project_semantic_window_messages(
        self,
        window_id: str,
        *,
        user_name: str,
        project_id: str,
    ) -> list[SemanticWindowMessage]:
        return await self._semantic_window_reader.get_window_messages(
            window_id,
            user_name=user_name,
            project_id=project_id,
        )

    async def get_active_project_semantic_window(
        self,
        *,
        user_name: str,
        project_id: str,
    ) -> SemanticWindowRecord | None:
        return await self._semantic_window_reader.get_active_window(
            user_name=user_name,
            project_id=project_id,
        )

    async def get_project_semantic_window_evidence_messages(
        self,
        window_id: str,
        *,
        user_name: str,
        project_id: str,
    ) -> list[dict]:
        return await self._semantic_window_reader.get_window_evidence_messages(
            window_id,
            user_name=user_name,
            project_id=project_id,
        )

    async def get_project_semantic_window_assistant_source_refs(
        self,
        window_id: str,
        *,
        user_name: str,
        project_id: str,
    ) -> list[dict]:
        """Load assistant-owned source refs from frozen window membership only."""

        return await self._semantic_window_reader.get_window_assistant_source_refs(
            window_id,
            user_name=user_name,
            project_id=project_id,
        )

    async def get_project_semantic_window_episode_result(
        self,
        window_id: str,
        *,
        user_name: str,
        project_id: str,
    ) -> list[Episode] | None:
        episode_ids = await self._semantic_window_reader.get_window_episode_ids(
            window_id,
            user_name=user_name,
            project_id=project_id,
        )
        if episode_ids is None:
            return None
        episodes: list[Episode] = []
        for episode_id in episode_ids:
            episode = await self._episode_reader.get_project_episode(
                episode_id,
                user_name=user_name,
                project_id=project_id,
            )
            if episode is None:
                raise RuntimeError("Semantic window references an unavailable episode")
            episodes.append(episode)
        return episodes

    async def write_project_semantic_window_episodes(
        self,
        *,
        window_id: str,
        episodes: list[Episode],
        window_messages: list[dict],
        user_name: str,
        project_id: str,
    ) -> bool:
        return await self._episode_writer.write_project_semantic_window_episodes(
            window_id=window_id,
            episodes=episodes,
            window_messages=window_messages,
            user_name=user_name,
            project_id=project_id,
        )

    async def enrich_project_semantic_window_episodes(
        self,
        *,
        window_id: str,
        user_name: str,
        project_id: str,
    ) -> dict[str, int]:
        """Attach active Context-backed Knowledge links after semantic commit."""

        return await self._episode_writer.enrich_project_semantic_window_episodes(
            window_id=window_id,
            user_name=user_name,
            project_id=project_id,
        )

    async def get_unclaimed_project_semantic_exchange_rows(
        self,
        *,
        user_name: str,
        project_id: str,
    ) -> list[dict]:
        """Load whole canonical exchanges for project-level semantic admission."""

        return await self._semantic_window_reader.get_unclaimed_project_exchange_rows(
            user_name=user_name,
            project_id=project_id,
        )

    async def advance_project_semantic_window_stage(
        self,
        *,
        window_id: str,
        user_name: str,
        project_id: str,
        expected_stage: SemanticWindowStage,
        next_stage: SemanticWindowStage,
        context_revision_id: str | None = None,
    ) -> bool:
        return await self._semantic_window_writer.advance_stage(
            window_id=window_id,
            user_name=user_name,
            project_id=project_id,
            expected_stage=expected_stage,
            next_stage=next_stage,
            context_revision_id=context_revision_id,
        )

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
    ) -> SemanticWindowRecord | None:
        return await self._semantic_window_writer.record_failure(
            window_id=window_id,
            user_name=user_name,
            project_id=project_id,
            expected_stage=expected_stage,
            failure_stage=failure_stage,
            failure_code=failure_code,
            error_summary=error_summary,
            failed_at_ms=failed_at_ms,
            next_retry_at_ms=next_retry_at_ms,
        )

    async def retry_project_semantic_window(
        self,
        *,
        window_id: str,
        user_name: str,
        project_id: str,
    ) -> SemanticWindowRecord | None:
        """Make one failed active window eligible for an operator retry."""

        return await self._semantic_window_writer.retry_window(
            window_id=window_id,
            user_name=user_name,
            project_id=project_id,
        )

    async def commit_project_semantic_knowledge(self, build) -> SemanticCommitSummary:
        """Atomically reconcile a Context-committed window into Knowledge."""

        return await self._semantic_commit_writer.commit(build)

    async def get_active_context_relationship_supports(
        self,
        relationship_id: str,
        *,
        user_name: str,
        project_id: str,
    ) -> list[dict]:
        return await self._relationship_observation_reader.get_active_context_supports(
            relationship_id,
            user_name=user_name,
            project_id=project_id,
        )

    async def get_semantic_window_health(
        self,
        *,
        user_name: str,
        project_id: str,
    ) -> Dict[str, int | None]:
        """Return project semantic-window health without exposing messages."""

        row = await self._postgres_client.fetch_one(
            """
            SELECT
                count(*) FILTER (WHERE stage <> 'completed') AS pending_count,
                count(*) FILTER (WHERE stage = 'claimed') AS claimed_count,
                count(*) FILTER (WHERE last_failure_at_ms IS NOT NULL) AS failed_count,
                count(*) FILTER (
                    WHERE stage <> 'completed'
                      AND last_failure_at_ms IS NOT NULL
                      AND next_retry_at_ms IS NULL
                ) AS exhausted_count,
                min((EXTRACT(EPOCH FROM claimed_at) * 1000)::BIGINT)
                    FILTER (WHERE stage <> 'completed') AS oldest_pending_ms,
                max((EXTRACT(EPOCH FROM completed_at) * 1000)::BIGINT)
                    AS last_processed_ms
            FROM public.project_semantic_windows
            WHERE user_name = %s AND project_id = %s
            """,
            (user_name, project_id),
        )
        return {
            key: row.get(key) if row else None
            for key in (
                "pending_count",
                "claimed_count",
                "failed_count",
                "exhausted_count",
                "oldest_pending_ms",
                "last_processed_ms",
            )
        }

    async def finalize_assistant_exchange(
        self,
        message: Dict,
        candidates: List[SourceReferenceCandidate],
        *,
        readable_project_ids: List[str],
        artifact: ArtifactDraft | None = None,
    ) -> tuple[int, list[str], bool]:
        """Commit one final assistant response and its exchange closure together.

        The result is ``(assistant_message_id, source_ref_ids, created)``.
        Retries of the same finalization return the original assistant instead
        of creating a second answer.
        """

        if message.get("role") != "assistant":
            raise ValueError("Only assistant messages can finalize an exchange")
        user_message_id = message.get("user_msg_id")
        if not isinstance(user_message_id, int) or isinstance(user_message_id, bool):
            raise ValueError("Assistant exchange finalization requires user_msg_id")
        scope = ("user_name", "project_id", "session_id", "id")
        missing = [name for name in scope if not message.get(name)]
        if missing:
            raise ValueError(
                "Assistant exchange finalization missing scope fields: "
                + ", ".join(missing)
            )

        async with self._postgres_client.transaction() as cur:
            existing = await self._message_lifecycle_writer.prepare_assistant_exchange_finalization(
                user_name=message["user_name"],
                project_id=message["project_id"],
                session_id=message["session_id"],
                user_message_id=user_message_id,
                cur=cur,
            )
            if existing is not None:
                await cur.execute(
                    """
                    SELECT source_ref_id
                    FROM public.message_source_refs
                    WHERE project_id = %s
                      AND session_id = %s
                      AND message_id = %s
                    ORDER BY created_at ASC, result_position ASC, source_ref_id ASC
                    """,
                    (
                        message["project_id"],
                        message["session_id"],
                        existing.assistant_message_id,
                    ),
                )
                rows = await cur.fetchall()
                return (
                    int(existing.assistant_message_id),
                    [str(row["source_ref_id"]) for row in rows],
                    False,
                )

            await self._message_writer.save_message_logs([message], cur=cur)
            references = await self._source_reference_writer.write_for_assistant_message(
                message["id"],
                candidates,
                user_name=message["user_name"],
                project_id=message["project_id"],
                session_id=message["session_id"],
                readable_project_ids=readable_project_ids,
                cursor=cur,
            )
            if artifact is not None:
                await self._artifact_writer.write_for_assistant_message(
                    message["id"],
                    artifact,
                    user_name=message["user_name"],
                    project_id=message["project_id"],
                    session_id=message["session_id"],
                    cursor=cur,
                )
            await self._message_lifecycle_writer.close_user_exchange(
                user_name=message["user_name"],
                project_id=message["project_id"],
                session_id=message["session_id"],
                user_message_id=user_message_id,
                outcome="assistant_final",
                closed_at_ms=int(message.get("sealed_at_ms") or 0),
                cur=cur,
            )
            return (
                int(message["id"]),
                [
                    str(reference.source_ref_id)
                    for reference in references
                    if getattr(reference, "source_ref_id", None)
                ],
                True,
            )

    async def close_user_exchange(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        user_message_id: int,
        outcome: str,
        closed_at_ms: int | None = None,
    ) -> ExchangeClosure:
        """Close a clarification, failure, cancellation, or user-only turn."""

        return await self._message_lifecycle_writer.close_user_exchange(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            user_message_id=user_message_id,
            outcome=outcome,
            closed_at_ms=closed_at_ms,
        )

    async def get_project_artifact(
        self,
        artifact_id: str,
        *,
        user_name: str,
        project_id: str,
        session_id: str | None = None,
    ) -> ArtifactReference | None:
        return await self._artifact_reader.get_artifact(
            artifact_id,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )

    async def list_project_artifacts(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str | None = None,
        limit: int = 50,
    ) -> List[ArtifactReference]:
        return await self._artifact_reader.list_project_artifacts(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            limit=limit,
        )

    async def get_message_artifact(
        self,
        message_id: int,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> ArtifactReference | None:
        """Read the artifact attached to one committed assistant message."""

        return await self._artifact_reader.get_for_assistant_message(
            message_id,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )

    async def get_project_artifact_revision(
        self,
        artifact_id: str,
        revision: int,
        *,
        user_name: str,
        project_id: str,
        session_id: str | None = None,
    ) -> ArtifactRevision | None:
        return await self._artifact_reader.get_revision(
            artifact_id,
            revision,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )

    async def get_message_source_refs(
        self,
        message_id: int,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> List[SourceConsulted]:
        return await self._source_reference_reader.get_message_source_refs(
            message_id,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )

    async def get_source_reference(
        self,
        source_ref_id: str,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> SourceReference | None:
        """Read one assistant-owned provenance row for explicit promotion."""
        return await self._source_reference_reader.get_source_reference(
            source_ref_id,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )

    async def get_assistant_message_with_sources(
        self,
        message_id: int,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> Optional[AssistantMessageWithSources]:
        return await self._source_reference_reader.get_assistant_message_with_sources(
            message_id,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )

    async def get_episode_source_refs(
        self,
        episode_id: str,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> List[SourceConsulted]:
        return await self._source_reference_reader.get_episode_source_refs(
            episode_id,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )

    async def allocate_entity_id(self) -> int:
        return await self._id_allocator.allocate_entity_id()

    async def allocate_message_id(self) -> int:
        return await self._id_allocator.allocate_message_id()

    async def get_project_episode_source_refs(
        self, episode_id: str, *, user_name: str, project_id: str
    ) -> List[SourceConsulted]:
        return await self._source_reference_reader.get_project_episode_source_refs(
            episode_id, user_name=user_name, project_id=project_id
        )

    async def edit_episode(
        self,
        *,
        episode_id: str,
        user_name: str,
        project_id: str,
        summary: str,
        new_developments: List[str],
        updates: List[str],
        unresolved: List[str],
    ) -> None:
        await self._episode_writer.edit_episode(
            episode_id=episode_id,
            user_name=user_name,
            project_id=project_id,
            summary=summary,
            new_developments=new_developments,
            updates=updates,
            unresolved=unresolved,
        )

    async def get_project_episode(
        self,
        episode_id: str,
        *,
        user_name: str,
        project_id: str,
        visible_project_ids: Optional[List[str]] = None,
    ) -> Optional[Episode]:
        return await self._episode_reader.get_project_episode(
            episode_id,
            user_name=user_name,
            project_id=project_id,
            visible_project_ids=visible_project_ids,
        )

    async def get_recent_project_episodes(
        self,
        *,
        user_name: str,
        project_id: str,
        limit: int,
        visible_project_ids: Optional[List[str]] = None,
    ) -> List[EpisodeCard]:
        return await self._episode_reader.get_recent_project_episodes(
            user_name=user_name,
            project_id=project_id,
            limit=limit,
            visible_project_ids=visible_project_ids,
        )

    async def get_nearby_project_episodes(
        self,
        *,
        user_name: str,
        project_id: str,
        session_ids: List[str],
        before_message_id: int,
        before_timestamp_ms: int | None,
        limit: int,
    ) -> List[Episode]:
        return await self._episode_reader.get_nearby_project_episodes(
            user_name=user_name,
            project_id=project_id,
            session_ids=session_ids,
            before_message_id=before_message_id,
            before_timestamp_ms=before_timestamp_ms,
            limit=limit,
        )

    async def search_project_episodes(
        self,
        query: str,
        *,
        user_name: str,
        project_id: str,
        limit: int,
        visible_project_ids: Optional[List[str]] = None,
    ) -> List[EpisodeCard]:
        return await self._episode_reader.search_project_episodes(
            query,
            user_name=user_name,
            project_id=project_id,
            limit=limit,
            visible_project_ids=visible_project_ids,
        )

    async def search_project_episodes_by_embedding(
        self,
        embedding: List[float],
        *,
        user_name: str,
        project_id: str,
        limit: int = 10,
        score_threshold: float = 0.35,
        visible_project_ids: Optional[List[str]] = None,
    ) -> List[tuple[EpisodeCard, float]]:
        return await self._episode_reader.search_project_episodes_by_embedding(
            embedding,
            user_name=user_name,
            project_id=project_id,
            limit=limit,
            score_threshold=score_threshold,
            visible_project_ids=visible_project_ids,
        )

    async def get_project_episode_source_messages(
        self,
        episode_id: str,
        *,
        user_name: str,
        project_id: str,
        visible_project_ids: Optional[List[str]] = None,
    ) -> List[Dict]:
        return await self._episode_reader.get_project_episode_source_messages(
            episode_id,
            user_name=user_name,
            project_id=project_id,
            visible_project_ids=visible_project_ids,
        )

    async def get_project_episodes_for_entities(
        self,
        entity_ids: List[int],
        *,
        user_name: str,
        project_id: str,
        limit: int,
        visible_project_ids: Optional[List[str]] = None,
    ) -> List[EpisodeCard]:
        return await self._episode_reader.get_project_episodes_for_entities(
            entity_ids,
            user_name=user_name,
            project_id=project_id,
            limit=limit,
            visible_project_ids=visible_project_ids,
        )

    async def get_episode(
        self,
        episode_id: str,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> Optional[Episode]:
        return await self._episode_reader.get_episode(
            episode_id,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )

    async def get_episodes_for_entity(
        self,
        entity_id: int,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        limit: int = 10,
    ) -> List[EpisodeCard]:
        return await self._episode_reader.get_episodes_for_entity(
            entity_id,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            limit=limit,
        )

    async def get_episodes_for_entities(
        self,
        entity_ids: List[int],
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        limit: int = 10,
    ) -> List[EpisodeCard]:
        return await self._episode_reader.get_episodes_for_entities(
            entity_ids,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            limit=limit,
        )

    async def get_merge_evidence_for_entities(
        self,
        entity_ids: List[int],
        *,
        project_id: str,
        evidence_limit: int = 4,
        source_message_limit: int = 2,
    ) -> Dict[int, List[Dict]]:
        return await self._episode_reader.get_merge_evidence_for_entities(
            entity_ids,
            project_id=project_id,
            evidence_limit=evidence_limit,
            source_message_limit=source_message_limit,
        )

    async def search_episodes(
        self,
        query: str,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        limit: int = 10,
    ) -> List[EpisodeCard]:
        return await self._episode_reader.search_episodes(
            query,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            limit=limit,
        )

    async def search_episodes_by_embedding(
        self,
        embedding: List[float],
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        limit: int = 10,
        score_threshold: float = 0.35,
    ) -> List[tuple[EpisodeCard, float]]:
        return await self._episode_reader.search_episodes_by_embedding(
            embedding,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            limit=limit,
            score_threshold=score_threshold,
        )

    async def get_recent_episodes(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        limit: int = 1,
    ) -> List[EpisodeCard]:
        return await self._episode_reader.get_recent_episodes(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            limit=limit,
        )

    async def get_episode_source_messages(
        self,
        episode_id: str,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> List[Dict]:
        return await self._episode_reader.get_episode_source_messages(
            episode_id,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )

    async def ensure_identity_entity(
        self, user_name: str, aliases: Optional[List[str]] = None
    ) -> Dict:
        return await self._graph_writer.ensure_identity_entity(user_name, aliases)

    async def update_entity_embedding(
        self, entity_id: int, embedding: List[float], *, project_id: str
    ):
        return await self._graph_writer.update_entity_embedding(
            entity_id, embedding, project_id=project_id
        )

    async def update_entity_aliases(
        self, alias_updates: Dict[int, List[str]], *, project_id: str
    ) -> None:
        return await self._graph_writer.update_entity_aliases(
            alias_updates, project_id=project_id
        )

    async def preview_historical_reclassification(
        self,
        *,
        user_name: str,
        project_id: str,
        domain: CompiledDomain,
        limit: int | None = None,
    ) -> Dict:
        plan = await self._entity_reclassification_writer.preview(
            user_name=user_name,
            project_id=project_id,
            domain=domain,
            limit=limit,
        )
        return plan.to_dict()

    async def reclassify_historical_entities(
        self,
        *,
        user_name: str,
        project_id: str,
        domain: CompiledDomain,
        batch_size: int = 100,
        max_entities: int | None = None,
    ) -> HistoricalReclassificationResult:
        return await self._entity_reclassification_writer.reclassify(
            user_name=user_name,
            project_id=project_id,
            domain=domain,
            batch_size=batch_size,
            max_entities=max_entities,
        )

    async def preview_historical_relationship_normalization(
        self,
        *,
        user_name: str,
        project_id: str,
        domain: CompiledDomain,
        limit: int | None = None,
    ) -> Dict:
        plan = await self._relationship_reclassification_writer.preview(
            user_name=user_name,
            project_id=project_id,
            domain=domain,
            limit=limit,
        )
        return plan.to_dict()

    async def normalize_historical_relationships(
        self,
        *,
        user_name: str,
        project_id: str,
        domain: CompiledDomain,
        batch_size: int = 100,
        max_relationships: int | None = None,
    ) -> HistoricalRelationshipReclassificationResult:
        return await self._relationship_reclassification_writer.reclassify(
            user_name=user_name,
            project_id=project_id,
            domain=domain,
            batch_size=batch_size,
            max_relationships=max_relationships,
        )

    async def preview_project_entity_cleanup(
        self,
        *,
        user_name: str,
        project_id: str,
        limit: int = 100,
    ) -> List[Dict]:
        return await self._entity_reader.preview_project_entity_cleanup(
            user_name=user_name,
            project_id=project_id,
            limit=limit,
        )

    async def delete_selected_project_entities(
        self,
        entity_ids: List[int],
        *,
        user_name: str,
        project_id: str,
    ) -> List[int]:
        return await self._graph_writer.delete_selected_project_entities(
            entity_ids,
            user_name=user_name,
            project_id=project_id,
        )

    async def rebuild_project_projection(
        self,
        project_id: str,
        user_name: str,
    ) -> Dict[str, int]:
        return await self._projection_rebuilder.rebuild_project_projection(
            project_id,
            user_name=user_name,
        )

    async def rebuild_project_embeddings(
        self,
        project_id: str,
        user_name: str,
    ) -> Dict[str, int]:
        return await self._embedding_rebuilder.rebuild_project_embeddings(
            project_id,
            user_name,
        )

    async def get_entity_embedding(
        self, entity_id: int, *, visible_project_ids: List[str]
    ) -> List[float]:
        return await self._entity_reader.get_entity_embedding(
            entity_id,
            visible_project_ids=visible_project_ids,
        )

    async def get_message_text(
        self,
        message_id: int,
        *,
        user_name: str,
        session_id: str,
        visible_project_ids: List[str],
    ) -> str:
        return await self._graph_reader.get_message_text(
            message_id,
            user_name=user_name,
            session_id=session_id,
            visible_project_ids=visible_project_ids,
        )

    async def get_messages_by_ids(
        self,
        ids: List[int],
        *,
        user_name: str,
        session_ids: List[str],
        visible_project_ids: List[str],
    ) -> List[Dict]:
        return await self._graph_reader.get_messages_by_ids(
            ids,
            user_name=user_name,
            session_ids=session_ids,
            visible_project_ids=visible_project_ids,
        )

    async def get_recent_project_messages(
        self,
        user_name: str,
        project_id: str,
        limit: int,
        before_message_id: Optional[int] = None,
    ) -> List[Dict]:
        return await self._graph_reader.get_recent_project_messages(
            user_name,
            project_id,
            limit,
            before_message_id=before_message_id,
        )

    async def get_surrounding_messages(
        self,
        message_id: int,
        *,
        user_name: str,
        session_id: str,
        visible_project_ids: List[str],
        forward: int = 3,
        target_total: int = 10,
        discoverable_only: bool = False,
    ) -> List[Dict]:
        return await self._graph_reader.get_surrounding_messages(
            message_id,
            user_name=user_name,
            session_id=session_id,
            visible_project_ids=visible_project_ids,
            forward=forward,
            target_total=target_total,
            discoverable_only=discoverable_only,
        )

    async def validate_existing_ids(
        self, ids: List[int], *, visible_project_ids: List[str]
    ) -> Set[int]:
        return await self._entity_reader.validate_existing_ids(
            ids,
            visible_project_ids=visible_project_ids,
        )

    async def get_entities_by_names(
        self, names: List[str], *, visible_project_ids: List[str]
    ) -> List[Dict]:
        return await self._entity_reader.get_entities_by_names(
            names,
            visible_project_ids=visible_project_ids,
        )

    async def search_entities_by_embedding(
        self,
        embedding: List[float],
        *,
        visible_project_ids: List[str],
        limit: int = 10,
        score_threshold: float = 0.8,
    ) -> List[Tuple[int, float]]:
        return await self._entity_reader.search_entities_by_embedding(
            embedding,
            visible_project_ids=visible_project_ids,
            limit=limit,
            score_threshold=score_threshold,
        )

    async def list_entities(
        self,
        limit: int = 20,
        offset: int = 0,
        *,
        visible_project_ids: List[str],
        topic: str = None,
        entity_type: str = None,
        search: str = None,
    ) -> Tuple[List[Dict], int]:
        return await self._entity_reader.list_entities(
            limit,
            offset,
            visible_project_ids=visible_project_ids,
            topic=topic,
            entity_type=entity_type,
            search=search,
        )

    async def get_entity_by_id(
        self, entity_id: int, *, visible_project_ids: List[str]
    ) -> Optional[Dict]:
        return await self._entity_reader.get_entity_by_id(
            entity_id,
            visible_project_ids=visible_project_ids,
        )

    async def get_entities_by_ids(
        self, entity_ids: List[int], *, visible_project_ids: List[str]
    ) -> List[Dict]:
        return await self._entity_reader.get_entities_by_ids(
            entity_ids,
            visible_project_ids=visible_project_ids,
        )

    async def get_top_connected_entities(
        self, *, visible_project_ids: List[str], limit: int = 10
    ) -> List[Dict]:
        return await self._entity_reader.get_top_connected_entities(
            visible_project_ids=visible_project_ids,
            limit=limit,
        )

    async def get_recently_active_entities(
        self, *, visible_project_ids: List[str], days: int = 7, limit: int = 10
    ) -> List[Dict]:
        return await self._entity_reader.get_recently_active_entities(
            visible_project_ids=visible_project_ids,
            days=days,
            limit=limit,
        )

    async def get_neighbor_ids_batch(
        self, entity_ids: List[int], *, visible_project_ids: List[str]
    ) -> Dict[int, Set[int]]:
        return await self._graph_reader.get_neighbor_ids_batch(
            entity_ids,
            visible_project_ids=visible_project_ids,
        )

    async def get_hot_topic_context_with_messages(
        self,
        hot_topic_names: List[str],
        *,
        project_id: str,
        msg_limit: int = 5,
    ) -> Dict:
        return await self._knowledge_query_reader.get_hot_topic_context_with_messages(
            hot_topic_names,
            project_id=project_id,
            msg_limit=msg_limit,
        )

    async def search_messages_fts(
        self,
        query: str,
        *,
        user_name: str,
        session_ids: List[str],
        visible_project_ids: List[str],
        limit: int = 50,
    ) -> List[Tuple[int, float, str]]:
        return await self._message_reader.search_fts(
            query,
            user_name=user_name,
            session_ids=session_ids,
            visible_project_ids=visible_project_ids,
            limit=limit,
        )

    async def search_entity(
        self,
        query: str,
        *,
        visible_project_ids: List[str],
        limit: int = 5,
        connections_limit: int = 5,
        evidence_limit: int = 5,
    ) -> List[Dict]:
        return await self._entity_reader.search_by_name(
            query,
            visible_project_ids=visible_project_ids,
            limit=limit,
            connections_limit=connections_limit,
            evidence_limit=evidence_limit,
        )

    async def get_related_entities(
        self,
        entity_ids: List[int],
        *,
        visible_project_ids: List[str],
        limit: int = 50,
    ) -> List[Dict]:
        return await self._entity_reader.get_related_entities(
            entity_ids,
            visible_project_ids=visible_project_ids,
            limit=limit,
        )

    async def get_recent_activity(
        self,
        entity_id: int,
        *,
        visible_project_ids: List[str],
        hours: int = 24,
    ) -> List[Dict]:
        return await self._knowledge_query_reader.get_recent_activity(
            entity_id,
            visible_project_ids=visible_project_ids,
            hours=hours,
        )

    async def find_path(
        self,
        start_entity_id: int,
        end_entity_id: int,
        *,
        visible_project_ids: List[str],
        max_depth: int = 4,
    ) -> List[Dict]:
        return await self._graph_reader.find_path(
            start_entity_id,
            end_entity_id,
            visible_project_ids=visible_project_ids,
            max_depth=max_depth,
        )
