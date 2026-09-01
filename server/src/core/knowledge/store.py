from collections.abc import Callable, Iterable
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from loguru import logger

from common.conf.domain_config import CompiledDomain
from common.schema.artifacts import ArtifactDraft, ArtifactReference, ArtifactRevision
from common.schema.episode.models import Episode, EpisodeCard, EpisodeCheckpoint
from common.schema.ingestion.contracts import (
    EntityWrite,
    ExecutionScope,
    GraphWriteSummary,
    IngestionCommit,
    MessageEntityRef,
    RelationshipWrite,
)
from common.schema.source.references import (
    AssistantMessageWithSources,
    SourceConsulted,
    SourceReference,
    SourceReferenceCandidate,
)
from core.knowledge.conflict_discovery import ConflictPacketBuilder
from core.knowledge.conflict_service import ConflictService
from core.knowledge.conflicts import (
    ConflictDiscoveryPackage,
    ConflictGroup,
    ConflictOrigin,
    ConflictResolutionKind,
    ConflictWriteResult,
)
from core.knowledge.db.embedding_rebuilder import EmbeddingRebuilder
from core.knowledge.db.id_allocator import IdAllocator
from core.knowledge.db.projection_rebuilder import GraphBuilder
from core.knowledge.db.readers.artifact_reader import ArtifactReader
from core.knowledge.db.readers.conflict_discovery_reader import (
    ConflictDiscoveryReader,
)
from core.knowledge.db.readers.conflict_reader import ConflictReader
from core.knowledge.db.readers.entity_reader import EntityReader
from core.knowledge.db.readers.episode_reader import EpisodeReader
from core.knowledge.db.readers.graph_reader import GraphReader
from core.knowledge.db.readers.knowledge_query_reader import KnowledgeQueryReader
from core.knowledge.db.readers.merge_audit_reader import MergeAuditReader
from core.knowledge.db.readers.message_reader import MessageReader
from core.knowledge.db.readers.relationship_observation_reader import (
    RelationshipObservationReader,
)
from core.knowledge.db.readers.source_reference_reader import SourceReferenceReader
from core.knowledge.db.writers.artifact_writer import ArtifactWriter
from core.knowledge.db.writers.conflict_writer import ConflictWriter
from core.knowledge.db.writers.entity_merge_writer import EntityMergeWriter
from core.knowledge.db.writers.entity_reclassification_writer import (
    EntityReclassificationWriter,
    HistoricalReclassificationResult,
)
from core.knowledge.db.writers.episode_writer import EpisodeWriter
from core.knowledge.db.writers.graph_writer import GraphWriter
from core.knowledge.db.writers.human_review_writer import HumanReviewWriter
from core.knowledge.db.writers.merge_audit_writer import MergeAuditWriter
from core.knowledge.db.writers.message_lifecycle_writer import (
    IngestionClaim,
    MessageAcceptance,
    MessageLifecycleWriter,
)
from core.knowledge.db.writers.message_writer import MessageWriter
from core.knowledge.db.writers.relationship_advisory_writer import (
    RelationshipAdvisoryWriter,
)
from core.knowledge.db.writers.relationship_reclassification_writer import (
    HistoricalRelationshipReclassificationResult,
    RelationshipReclassificationWriter,
)
from core.knowledge.db.writers.retention_writer import RetentionWriter
from core.knowledge.db.writers.source_reference_writer import SourceReferenceWriter
from core.knowledge.human_reviews import HumanReview
from core.knowledge.relationship_advisories import (
    AdvisoryThresholds,
    RelationshipAdvisory,
    RelationshipAdvisoryDecision,
)
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
        self._entity_merge_writer = EntityMergeWriter(self._postgres_client)
        self._message_writer = MessageWriter(self._postgres_client)
        self._message_lifecycle_writer = MessageLifecycleWriter(
            self._postgres_client, self._message_writer
        )
        self._human_review_writer = HumanReviewWriter(self._postgres_client)
        self._conflict_writer = ConflictWriter(
            self._postgres_client,
            reviews=self._human_review_writer,
        )
        self._conflict_service = ConflictService(self._conflict_writer)
        self._conflict_discovery_reader = ConflictDiscoveryReader(self._postgres_client)
        self._conflict_reader = ConflictReader(self._postgres_client)
        self._merge_audit_writer = MergeAuditWriter(self._postgres_client)
        self._retention_writer = RetentionWriter(self._postgres_client)
        self._relationship_advisory_writer = RelationshipAdvisoryWriter(
            self._postgres_client,
            reviews=self._human_review_writer,
        )
        self._source_reference_writer = SourceReferenceWriter(self._postgres_client)
        self._artifact_writer = ArtifactWriter(self._postgres_client)
        self._entity_reader = EntityReader(self._postgres_client)
        self._episode_reader = EpisodeReader(self._postgres_client)
        self._graph_reader = GraphReader(self._postgres_client)
        self._message_reader = MessageReader(self._postgres_client)
        self._knowledge_query_reader = KnowledgeQueryReader(self._postgres_client)
        self._merge_audit_reader = MergeAuditReader(self._postgres_client)
        self._source_reference_reader = SourceReferenceReader(self._postgres_client)
        self._artifact_reader = ArtifactReader(self._postgres_client)
        self._relationship_observation_reader = RelationshipObservationReader(
            self._postgres_client
        )
        self._projection_rebuilder = GraphBuilder(self._postgres_client)
        self._embedding_rebuilder = EmbeddingRebuilder(
            self._postgres_client,
            embedding_service,
        )
        logger.info("KnowledgeStore initialized with internal Postgres/AGE backend")

    async def save_message_logs(self, messages: List[Dict]) -> bool:
        return await self._message_writer.save_message_logs(messages)

    async def create_editable_user_message(
        self, message: Dict, *, edit_window_seconds: int
    ) -> MessageAcceptance:
        return await self._message_lifecycle_writer.create_editable_user_message(
            message, edit_window_seconds=edit_window_seconds
        )

    async def edit_user_message(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str | None = None,
        message_id: int,
        content: str,
    ) -> int:
        return await self._message_lifecycle_writer.edit_user_message(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            message_id=message_id,
            content=content,
        )

    async def select_user_message_revision(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str | None = None,
        message_id: int,
        revision: int,
    ) -> str:
        return await self._message_lifecycle_writer.select_user_message_revision(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            message_id=message_id,
            revision=revision,
        )

    async def seal_due_user_messages(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> List[int]:
        return await self._message_lifecycle_writer.seal_due_user_messages(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )

    async def reset_claimed_ingestion(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> List[int]:
        return await self._message_lifecycle_writer.reset_claimed_ingestion(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )

    async def claim_next_ingestion_batch(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        batch_size: int,
    ) -> IngestionClaim | None:
        return await self._message_lifecycle_writer.claim_next_batch(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            batch_size=batch_size,
        )

    async def get_ingestion_queue_health(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> Dict[str, int | None]:
        """Return bounded PostgreSQL queue counters without exposing messages."""

        row = await self._postgres_client.fetch_one(
            """
            SELECT
                count(*) FILTER (WHERE ingestion_state IN ('waiting_for_seal', 'ready')) AS pending_count,
                count(*) FILTER (WHERE ingestion_state = 'claimed') AS claimed_count,
                count(*) FILTER (WHERE ingestion_state = 'failed') AS failed_count,
                min(timestamp_ms) FILTER (WHERE ingestion_state IN ('waiting_for_seal', 'ready')) AS oldest_pending_ms,
                max(timestamp_ms) FILTER (WHERE ingestion_state = 'processed') AS last_processed_ms
            FROM public.messages
            WHERE user_name = %s AND project_id = %s AND session_id = %s
              AND role = 'user'
            """,
            (user_name, project_id, session_id),
        )
        return {
            key: row.get(key) if row else None
            for key in (
                "pending_count",
                "claimed_count",
                "failed_count",
                "oldest_pending_ms",
                "last_processed_ms",
            )
        }

    async def release_ingestion_claim(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        batch_id: str,
    ) -> None:
        await self._message_lifecycle_writer.release_ingestion_claim(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            batch_id=batch_id,
        )

    async def fail_ingestion_claim(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        batch_id: str,
        failure_stage: str,
        failure_code: str,
        error_summary: str,
        retryable: bool,
        max_attempts: int,
    ) -> bool:
        return await self._message_lifecycle_writer.fail_ingestion_claim(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            batch_id=batch_id,
            failure_stage=failure_stage,
            failure_code=failure_code,
            error_summary=error_summary,
            retryable=retryable,
            max_attempts=max_attempts,
        )

    async def retry_failed_ingestion(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        message_ids: List[int],
    ) -> List[int]:
        return await self._message_lifecycle_writer.retry_failed_ingestion(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            message_ids=message_ids,
        )

    async def save_assistant_message_with_source_refs(
        self,
        message: Dict,
        candidates: List[SourceReferenceCandidate],
        *,
        readable_project_ids: List[str],
        artifact: ArtifactDraft | None = None,
    ) -> List[SourceReference]:
        """Persist one assistant message, sources, and artifact atomically."""

        if message.get("role") != "assistant":
            raise ValueError(
                "source references can only be saved for assistant messages"
            )
        if not candidates and artifact is None:
            await self.save_message_logs([message])
            return []

        async with self._postgres_client.transaction() as cur:
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
            return references

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

    async def write_message_source_refs(
        self,
        message_id: int,
        candidates: List[SourceReferenceCandidate],
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        readable_project_ids: List[str],
    ) -> List[SourceReference]:
        return await self._source_reference_writer.write_for_assistant_message(
            message_id,
            candidates,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            readable_project_ids=readable_project_ids,
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

    async def write_batch(
        self,
        entities: List[EntityWrite],
        relationships: List[RelationshipWrite],
        *,
        message_entity_refs: Optional[List[MessageEntityRef]] = None,
        source_message_times=None,
        scope: ExecutionScope,
    ) -> bool:
        return await self._graph_writer.write_batch(
            entities,
            relationships,
            message_entity_refs=message_entity_refs or (),
            source_message_times=source_message_times or (),
            scope=scope,
        )

    async def commit_ingestion(self, commit: IngestionCommit) -> GraphWriteSummary:
        return await self._graph_writer.commit_ingestion(commit)

    async def get_project_episode_source_refs(
        self, episode_id: str, *, user_name: str, project_id: str
    ) -> List[SourceConsulted]:
        return await self._source_reference_reader.get_project_episode_source_refs(
            episode_id, user_name=user_name, project_id=project_id
        )

    async def write_project_episode_window(
        self,
        episodes: List[Episode],
        window_messages: List[Dict],
        *,
        user_name: str,
        project_id: str,
    ) -> bool:
        return await self._episode_writer.write_project_episode_window(
            episodes, window_messages, user_name=user_name, project_id=project_id
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

    async def get_episode_checkpoint(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> EpisodeCheckpoint:
        return await self._episode_reader.get_episode_checkpoint(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )

    async def get_next_episode_window(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        checkpoint: EpisodeCheckpoint,
        message_count: int,
    ) -> List[Dict]:
        return await self._episode_reader.get_next_episode_window(
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
            checkpoint=checkpoint,
            message_count=message_count,
        )

    async def get_next_project_episode_window(
        self, *, user_name: str, project_id: str, message_count: int
    ) -> List[Dict]:
        return await self._episode_reader.get_next_project_episode_window(
            user_name=user_name, project_id=project_id, message_count=message_count
        )

    async def has_ready_project_episode_window(
        self, *, user_name: str, project_id: str, message_count: int
    ) -> bool:
        return await self._episode_reader.has_ready_project_episode_window(
            user_name=user_name,
            project_id=project_id,
            message_count=message_count,
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

    async def get_episode_graph_context(
        self,
        episode_id: str,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> Optional[Dict]:
        return await self._episode_reader.get_episode_graph_context(
            episode_id,
            user_name=user_name,
            project_id=project_id,
            session_id=session_id,
        )

    async def ensure_identity_entity(
        self, user_name: str, aliases: Optional[List[str]] = None
    ) -> Dict:
        return await self._graph_writer.ensure_identity_entity(user_name, aliases)

    async def update_entity_canonical_name(
        self, entity_id: int, canonical_name: str, *, project_id: str
    ) -> None:
        return await self._graph_writer.update_entity_canonical_name(
            entity_id, canonical_name, project_id=project_id
        )

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

    async def merge_entities(
        self,
        primary_id: int,
        secondary_id: int,
        *,
        project_id: str,
        final_topic: Optional[str] = None,
        cur=None,
    ) -> bool:
        return await self._entity_merge_writer.merge_entities(
            primary_id,
            secondary_id,
            project_id=project_id,
            final_topic=final_topic,
            cur=cur,
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

    async def expire_merge_rollback_states(
        self,
        cutoff: datetime,
        *,
        user_name: str,
        project_id: str,
    ) -> int:
        return await self._merge_audit_writer.expire_rollback_states(
            cutoff,
            user_name=user_name,
            project_id=project_id,
        )

    async def purge_expired_operational_records(
        self,
        *,
        user_name: str,
        project_id: str,
        tool_audit_cutoff: datetime,
        merge_history_cutoff: datetime,
    ) -> Dict[str, int]:
        return await self._retention_writer.purge_expired_records(
            user_name=user_name,
            project_id=project_id,
            tool_audit_cutoff=tool_audit_cutoff,
            merge_history_cutoff=merge_history_cutoff,
        )

    async def delete_relationship(
        self,
        entity_a_id: int,
        entity_b_id: int,
        *,
        relationship_type: str,
        project_id: str,
    ) -> bool:
        return await self._entity_merge_writer.delete_relationship(
            entity_a_id,
            entity_b_id,
            relationship_type=relationship_type,
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

    async def get_entity_ids_for_messages(
        self,
        message_ids: List[int],
        *,
        user_name: str,
        session_id: str,
        project_id: str,
    ) -> Dict[int, List[int]]:
        return await self._entity_reader.get_entity_ids_for_messages(
            message_ids,
            user_name=user_name,
            session_id=session_id,
            project_id=project_id,
        )

    async def get_relationship_advisories(
        self,
        *,
        user_name: str,
        project_id: str,
        thresholds: AdvisoryThresholds | None = None,
    ) -> list[RelationshipAdvisory]:
        """Derive actionable unknown-relationship suggestions from evidence."""

        advisories = await self._relationship_observation_reader.get_advisories(
            user_name=user_name,
            project_id=project_id,
            thresholds=thresholds,
        )
        for advisory in advisories:
            await self._relationship_advisory_writer.materialize_pending(
                user_name=user_name,
                project_id=project_id,
                advisory=advisory,
            )
        return advisories

    async def get_open_human_reviews(
        self, *, user_name: str, project_id: str
    ) -> list[HumanReview]:
        return await self._human_review_writer.list_open(
            user_name=user_name,
            project_id=project_id,
        )

    async def build_conflict_discovery_package(
        self,
        *,
        user_name: str,
        project_id: str,
        max_seed_span_days: int,
        max_package_tokens: int,
        token_counter: Callable[[str], int] | None = None,
    ) -> ConflictDiscoveryPackage | None:
        cursor = await self._conflict_discovery_reader.get_cursor(
            user_name=user_name,
            project_id=project_id,
        )
        return await ConflictPacketBuilder(
            self._conflict_discovery_reader,
            token_counter=token_counter,
        ).build(
            cursor,
            max_span_days=max_seed_span_days,
            max_tokens=max_package_tokens,
        )

    async def complete_conflict_discovery(
        self,
        package: ConflictDiscoveryPackage,
        *,
        candidates: Iterable[Any],
    ) -> int:
        """Persist grounded conflict groups and advance the cursor atomically."""

        results = []
        async with self._postgres_client.transaction() as cur:
            for candidate in candidates:
                result = await self._conflict_writer.record_detection(
                    user_name=package.cursor.user_name,
                    project_id=package.cursor.project_id,
                    origin="background_discovery",
                    kind=candidate.kind,
                    rationale=candidate.rationale,
                    confidence=candidate.confidence,
                    evidence_ids=candidate.evidence_ids,
                    metadata={
                        "discovery_packet_tokens": package.estimated_tokens,
                        "packet_compacted": package.compacted,
                    },
                    cur=cur,
                )
                results.append(result)
            await self._conflict_discovery_reader.advance(
                package.cursor,
                last_reviewed_observation_id=package.next_observation_id,
                cur=cur,
            )

        for result in results:
            await self._conflict_service.notify_detection(
                user_name=package.cursor.user_name,
                project_id=package.cursor.project_id,
                origin="background_discovery",
                result=result,
            )
        return sum(int(result.should_notify) for result in results)

    async def record_conflict_detection(
        self,
        *,
        user_name: str,
        project_id: str,
        origin: ConflictOrigin,
        kind: str,
        rationale: str,
        confidence: float | None,
        evidence_ids: List[int],
        metadata: Dict | None = None,
        existing_conflict_id: str | None = None,
    ) -> ConflictWriteResult:
        return await self._conflict_service.record_detection(
            user_name=user_name,
            project_id=project_id,
            origin=origin,
            kind=kind,
            rationale=rationale,
            confidence=confidence,
            evidence_ids=evidence_ids,
            metadata=metadata,
            existing_conflict_id=existing_conflict_id,
        )

    async def get_conflict_group(
        self,
        *,
        conflict_id: str,
        user_name: str,
        project_id: str,
    ) -> Dict | None:
        return await self._conflict_reader.get_detail(
            conflict_id=conflict_id,
            user_name=user_name,
            project_id=project_id,
        )

    async def resolve_conflict_group(
        self,
        *,
        conflict_id: str,
        user_name: str,
        project_id: str,
        resolution_kind: ConflictResolutionKind,
        resolved_by: str,
        resolution_note: str | None = None,
    ) -> ConflictGroup:
        return await self._conflict_service.resolve(
            conflict_id=conflict_id,
            user_name=user_name,
            project_id=project_id,
            resolution_kind=resolution_kind,
            resolved_by=resolved_by,
            resolution_note=resolution_note,
        )

    async def apply_relationship_advisory_action(
        self,
        *,
        user_name: str,
        project_id: str,
        pattern_key: str,
        action: str,
        relationship_type: str | None = None,
        note: str | None = None,
        decided_by: str | None = None,
    ) -> RelationshipAdvisoryDecision:
        """Persist one explicit advisory decision without changing the domain."""

        return await self._relationship_advisory_writer.apply_action(
            user_name=user_name,
            project_id=project_id,
            pattern_key=pattern_key,
            action=action,
            relationship_type=relationship_type,
            note=note,
            decided_by=decided_by,
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

    async def get_neighbor_ids(
        self, entity_id: int, *, visible_project_ids: List[str]
    ) -> Set[int]:
        return await self._graph_reader.get_neighbor_ids(
            entity_id,
            visible_project_ids=visible_project_ids,
        )

    async def get_entities_by_names(
        self, names: List[str], *, visible_project_ids: List[str]
    ) -> List[Dict]:
        return await self._entity_reader.get_entities_by_names(
            names,
            visible_project_ids=visible_project_ids,
        )

    async def get_neighbor_entities(
        self,
        entity_id: int,
        *,
        visible_project_ids: List[str],
        limit: int = 5,
    ) -> List[Dict]:
        return await self._graph_reader.get_neighbor_entities(
            entity_id,
            visible_project_ids=visible_project_ids,
            limit=limit,
        )

    async def get_merge_topic_strength(
        self,
        primary_id: int,
        secondary_id: int,
        project_id: str,
    ) -> Dict:
        return await self._graph_reader.get_merge_topic_strength(
            primary_id,
            secondary_id,
            project_id,
        )

    async def has_direct_edge(
        self, id_a: int, id_b: int, *, visible_project_ids: List[str]
    ) -> bool:
        return await self._graph_reader.has_direct_edge(
            id_a,
            id_b,
            visible_project_ids=visible_project_ids,
        )

    async def search_similar_entities(
        self,
        entity_id: int,
        *,
        visible_project_ids: List[str],
        limit: int = 50,
    ) -> List[Tuple[int, float]]:
        return await self._entity_reader.search_similar_entities(
            entity_id,
            visible_project_ids=visible_project_ids,
            limit=limit,
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

    async def get_graph_stats(
        self, *, visible_project_ids: List[str]
    ) -> Dict[str, int]:
        return await self._graph_reader.get_graph_stats(
            visible_project_ids=visible_project_ids
        )

    async def get_entity_count_by_type(
        self, *, visible_project_ids: List[str]
    ) -> List[Dict]:
        return await self._entity_reader.get_entity_count_by_type(
            visible_project_ids=visible_project_ids
        )

    async def get_entity_count_by_topic(
        self, *, visible_project_ids: List[str]
    ) -> List[Dict]:
        return await self._entity_reader.get_entity_count_by_topic(
            visible_project_ids=visible_project_ids
        )

    async def get_top_connected_entities(
        self, *, visible_project_ids: List[str], limit: int = 10
    ) -> List[Dict]:
        return await self._entity_reader.get_top_connected_entities(
            visible_project_ids=visible_project_ids,
            limit=limit,
        )

    async def get_entity_relationships(
        self, entity_id: int, *, visible_project_ids: List[str]
    ) -> List[Dict]:
        return await self._entity_reader.get_entity_relationships(
            entity_id,
            visible_project_ids=visible_project_ids,
        )

    async def get_recently_active_entities(
        self, *, visible_project_ids: List[str], days: int = 7, limit: int = 10
    ) -> List[Dict]:
        return await self._entity_reader.get_recently_active_entities(
            visible_project_ids=visible_project_ids,
            days=days,
            limit=limit,
        )

    async def get_notable_entities(
        self, *, visible_project_ids: List[str], limit: int = 10
    ) -> List[Dict]:
        return await self._entity_reader.get_notable_entities(
            visible_project_ids=visible_project_ids,
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
