from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

from loguru import logger

from common.schema.contracts import CandidateSuggestion, EngineScope
from common.schema.primitives import FactRecord
from common.scoping import require_scope_value
from infrastructure.postgres_client import PostgresClient
from core.community.community_store import CommunityStore
from core.knowledge.db.id_allocator import IdAllocator
from core.knowledge.db.projection_rebuilder import GraphBuilder
from core.knowledge.db.readers.entity_reader import EntityReader
from core.knowledge.db.readers.fact_reader import FactReader
from core.knowledge.db.readers.graph_reader import GraphReader
from core.knowledge.db.readers.merge_audit_reader import MergeAuditReader
from core.knowledge.db.search_index_rebuilder import SearchIndexer
from core.knowledge.db.tool_queries import ToolQueries
from core.knowledge.db.writers.candidate_suggestion_writer import (
    CandidateSuggestionWriter,
)
from core.knowledge.db.writers.entity_writer import EntityWriter
from core.knowledge.db.writers.fact_audit_writer import FactAuditWriter
from core.knowledge.db.writers.fact_writer import FactWriter
from core.knowledge.db.writers.graph_writer import GraphWriter
from core.knowledge.db.writers.merge_audit_writer import MergeAuditWriter
from core.knowledge.services.embedding_service import EmbeddingService


class KnowledgeStore:
    """
    Application-facing facade over durable knowledge persistence.

    Owns one PostgresClient and composes the focused readers, writers,
    rebuilders, tool queries, and community store that share it. Callers use
    this boundary without depending on the underlying SQL, AGE, or index layout.
    """

    def __init__(self, dsn: str, embedding_service: EmbeddingService):
        self._postgres_client = PostgresClient(dsn=dsn)
        self._id_allocator = IdAllocator(self._postgres_client)
        self._entity_writer = EntityWriter(self._postgres_client)
        self._candidate_suggestion_writer = CandidateSuggestionWriter(
            self._postgres_client
        )
        self._fact_writer = FactWriter(self._postgres_client)
        self._fact_audit_writer = FactAuditWriter(self._postgres_client)
        self._graph_writer = GraphWriter(self._postgres_client)
        self._merge_audit_writer = MergeAuditWriter(self._postgres_client)
        self._entity_reader = EntityReader(self._postgres_client)
        self._fact_reader = FactReader(self._postgres_client)
        self._graph_reader = GraphReader(self._postgres_client)
        self._merge_audit_reader = MergeAuditReader(self._postgres_client)
        self._tools = ToolQueries(self._postgres_client)
        self._projection_rebuilder = GraphBuilder(self._postgres_client)
        self._search_index_rebuilder = SearchIndexer(
            self._postgres_client,
            embedding_service,
        )
        self._community = CommunityStore(self._postgres_client)
        logger.info("KnowledgeStore initialized with internal Postgres/AGE backend")

    async def connect(self):
        await self._postgres_client.connect()

    async def close(self):
        await self._postgres_client.close()

    @property
    def postgres(self) -> PostgresClient:
        return self._postgres_client

    @property
    def community(self) -> CommunityStore:
        return self._community

    async def save_message_logs(self, messages: List[Dict]) -> bool:
        return await self._graph_writer.save_message_logs(messages)

    async def save_candidate_suggestions(
        self,
        scope: EngineScope,
        suggestions: List[CandidateSuggestion],
    ) -> int:
        return await self._candidate_suggestion_writer.save_candidate_suggestions(
            scope, suggestions
        )

    async def allocate_entity_id(self) -> int:
        return await self._id_allocator.allocate_entity_id()

    async def allocate_message_id(self) -> int:
        return await self._id_allocator.allocate_message_id()

    async def write_batch(
        self, entities: List[Dict], relationships: List[Dict]
    ) -> bool:
        return await self._entity_writer.write_batch(entities, relationships)

    async def ensure_identity_entity(
        self, user_name: str, aliases: Optional[List[str]] = None
    ) -> Dict:
        return await self._entity_writer.ensure_identity_entity(user_name, aliases)

    async def create_facts_batch(
        self,
        entity_id: int,
        facts: List[FactRecord],
        *,
        user_name: str,
        project_id: str,
        session_id: Optional[str] = None,
    ) -> int:
        require_scope_value(user_name, "user_name", "create_facts_batch")
        require_scope_value(project_id, "project_id", "create_facts_batch")
        return await self._fact_writer.create_facts_batch(
            entity_id,
            facts,
            user_name=user_name,
            session_id=session_id,
            project_id=project_id,
        )

    async def invalidate_fact(
        self, fact_id: str, invalid_at: datetime, *, project_id: str
    ) -> bool:
        return await self._fact_writer.invalidate_fact(
            fact_id, invalid_at, project_id=project_id
        )

    async def remove_fact_with_audit(self, **kwargs) -> dict:
        return await self._fact_writer.remove_fact_with_audit(**kwargs)

    async def replace_facts_with_audit(self, **kwargs) -> dict:
        return await self._fact_writer.replace_facts_with_audit(**kwargs)

    async def create_applied_fact_change_audit(self, **kwargs) -> None:
        return await self._fact_audit_writer.create_applied_audit(**kwargs)

    async def update_entity_profile(
        self,
        entity_id: int,
        canonical_name: str,
        embedding: List[float],
        last_msg_id: int,
        *,
        project_id: str,
    ):
        return await self._entity_writer.update_entity_profile(
            entity_id, canonical_name, embedding, last_msg_id, project_id=project_id
        )

    async def update_entity_canonical_name(
        self, entity_id: int, canonical_name: str, *, project_id: str
    ) -> None:
        return await self._entity_writer.update_entity_canonical_name(
            entity_id, canonical_name, project_id=project_id
        )

    async def update_entity_embedding(
        self, entity_id: int, embedding: List[float], *, project_id: str
    ):
        return await self._entity_writer.update_entity_embedding(
            entity_id, embedding, project_id=project_id
        )

    async def update_entity_checkpoint(
        self, entity_id: int, last_msg_id: int, *, project_id: str
    ):
        return await self._entity_writer.update_entity_checkpoint(
            entity_id, last_msg_id, project_id=project_id
        )

    async def update_entity_aliases(
        self, alias_updates: Dict[int, List[str]], *, project_id: str
    ) -> None:
        return await self._entity_writer.update_entity_aliases(
            alias_updates, project_id=project_id
        )

    async def create_hierarchy_edge(
        self, parent_id: int, child_id: int, *, project_id: str
    ) -> bool:
        return await self._graph_writer.create_hierarchy_edge(
            parent_id, child_id, project_id=project_id
        )

    async def merge_entities(
        self,
        primary_id: int,
        secondary_id: int,
        *,
        project_id: str,
        final_topic: Optional[str] = None,
    ) -> bool:
        return await self._graph_writer.merge_entities(
            primary_id,
            secondary_id,
            project_id=project_id,
            final_topic=final_topic,
        )

    async def cleanup_null_entities(self, *, project_id: str) -> List[int]:
        return await self._entity_writer.cleanup_null_entities(project_id=project_id)

    async def delete_entity(self, entity_id: int, *, project_id: str) -> bool:
        return await self._entity_writer.delete_entity(entity_id, project_id=project_id)

    async def bulk_delete_entities(
        self, entity_ids: List[int], *, project_id: str
    ) -> List[int]:
        return await self._entity_writer.bulk_delete_entities(
            entity_ids, project_id=project_id
        )

    async def delete_old_invalidated_facts(
        self, cutoff: datetime, *, project_id: str
    ) -> int:
        return await self._fact_writer.delete_old_invalidated_facts(
            cutoff, project_id=project_id
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

    async def delete_relationship(
        self, entity_a_id: int, entity_b_id: int, *, project_id: str
    ) -> bool:
        return await self._graph_writer.delete_relationship(
            entity_a_id, entity_b_id, project_id=project_id
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

    async def rebuild_project_search_indexes(
        self,
        project_id: str,
        user_name: str,
        identity_project_ids: List[str],
    ) -> Dict[str, int]:
        return await self._search_index_rebuilder.rebuild_project_indexes(
            project_id,
            user_name,
            identity_project_ids,
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
    ) -> List[Dict]:
        return await self._graph_reader.get_surrounding_messages(
            message_id,
            user_name=user_name,
            session_id=session_id,
            visible_project_ids=visible_project_ids,
            forward=forward,
            target_total=target_total,
        )

    async def get_facts_for_entity(
        self,
        entity_id: int,
        *,
        visible_project_ids: List[str],
        active_only: bool = True,
    ) -> List[FactRecord]:
        return await self._fact_reader.get_facts_for_entity(
            entity_id,
            visible_project_ids=visible_project_ids,
            active_only=active_only,
        )

    async def search_relevant_facts(
        self,
        entity_id: int,
        query_embedding: List[float],
        *,
        visible_project_ids: List[str],
        limit: int = 5,
    ) -> List[FactRecord]:
        return await self._fact_reader.search_relevant_facts(
            entity_id,
            query_embedding,
            visible_project_ids=visible_project_ids,
            limit=limit,
        )

    async def get_facts_for_entities(
        self,
        entity_ids: List[int],
        *,
        visible_project_ids: List[str],
        active_only: bool = True,
    ) -> Dict[int, List[FactRecord]]:
        return await self._fact_reader.get_facts_for_entities(
            entity_ids,
            visible_project_ids=visible_project_ids,
            active_only=active_only,
        )

    async def get_facts_from_message(
        self,
        msg_id: int,
        *,
        user_name: str,
        session_id: str,
        visible_project_ids: List[str],
    ) -> List[FactRecord]:
        return await self._fact_reader.get_facts_from_message(
            msg_id,
            user_name=user_name,
            session_id=session_id,
            visible_project_ids=visible_project_ids,
        )

    async def validate_existing_ids(
        self, ids: List[int], *, visible_project_ids: List[str]
    ) -> Optional[Set[int]]:
        return await self._entity_reader.validate_existing_ids(
            ids,
            visible_project_ids=visible_project_ids,
        )

    async def get_orphan_entities(
        self,
        protected_id: int = 1,
        orphan_cutoff_ms: int = 0,
        stale_junk_cutoff_ms: int = 0,
        *,
        project_id: str,
    ) -> List[int]:
        return await self._entity_reader.get_orphan_entities(
            protected_id, orphan_cutoff_ms, stale_junk_cutoff_ms, project_id=project_id
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

    async def get_parent_entities(
        self, entity_id: int, *, visible_project_ids: List[str]
    ) -> List[Dict]:
        return await self._graph_reader.get_parent_entities(
            entity_id,
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

    async def get_child_entities(
        self, entity_id: int, *, visible_project_ids: List[str]
    ) -> List[Dict]:
        return await self._graph_reader.get_child_entities(
            entity_id,
            visible_project_ids=visible_project_ids,
        )

    async def get_hierarchy_candidates(
        self,
        project_id: str,
        topic: str,
        parent_type: str,
        child_types: List[str],
        min_weight: int = 2,
    ) -> List[Dict]:
        return await self._graph_reader.get_hierarchy_candidates(
            project_id, topic, parent_type, child_types, min_weight
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

    async def has_hierarchy_edge(
        self, id_a: int, id_b: int, *, visible_project_ids: List[str]
    ) -> bool:
        return await self._graph_reader.has_hierarchy_edge(
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

    async def get_recent_facts(
        self, *, visible_project_ids: List[str], days: int = 7, limit: int = 20
    ) -> List[Dict]:
        return await self._fact_reader.get_recent_facts(
            visible_project_ids=visible_project_ids,
            days=days,
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
        visible_project_ids: List[str],
        msg_limit: int = 5,
        slim: bool = False,
    ) -> Dict:
        return await self._tools.get_hot_topic_context_with_messages(
            hot_topic_names,
            visible_project_ids=visible_project_ids,
            msg_limit=msg_limit,
            slim=slim,
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
        return await self._tools.search_messages_fts(
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
        active_topics: List[str] = None,
        limit: int = 5,
        connections_limit: int = 5,
        evidence_limit: int = 5,
    ) -> List[Dict]:
        return await self._tools.search_entity(
            query,
            visible_project_ids=visible_project_ids,
            active_topics=active_topics,
            limit=limit,
            connections_limit=connections_limit,
            evidence_limit=evidence_limit,
        )

    async def get_related_entities(
        self,
        entity_names: List[str],
        *,
        visible_project_ids: List[str],
        active_topics: List[str] = None,
        limit: int = 50,
    ) -> List[Dict]:
        return await self._tools.get_related_entities(
            entity_names,
            visible_project_ids=visible_project_ids,
            active_topics=active_topics,
            limit=limit,
        )

    async def get_recent_activity(
        self,
        entity_name: str,
        *,
        visible_project_ids: List[str],
        active_topics: List[str] = None,
        hours: int = 24,
    ) -> List[Dict]:
        return await self._tools.get_recent_activity(
            entity_name,
            visible_project_ids=visible_project_ids,
            active_topics=active_topics,
            hours=hours,
        )

    async def find_path_filtered(
        self,
        start_name: str,
        end_name: str,
        *,
        visible_project_ids: List[str],
        active_topics: List[str] = None,
        max_depth: int = 4,
    ) -> Tuple[List[Dict], bool]:
        return await self._tools.find_path_filtered(
            start_name,
            end_name,
            visible_project_ids=visible_project_ids,
            active_topics=active_topics,
            max_depth=max_depth,
        )
