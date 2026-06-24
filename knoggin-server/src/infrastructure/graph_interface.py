from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

from loguru import logger

from common.schema.primitives import FactRecord
from infrastructure.postgres_client import PostgresClient
from knoggin_server.community.community_store import CommunityStore
from knoggin_server.knowledge.db.id_allocator import IdAllocator
from knoggin_server.knowledge.db.projection_rebuilder import ProjectionRebuilder
from knoggin_server.knowledge.db.readers.entity_reader import EntityReader
from knoggin_server.knowledge.db.readers.fact_reader import FactReader
from knoggin_server.knowledge.db.readers.graph_reader import GraphReader
from knoggin_server.knowledge.db.search_index_rebuilder import SearchIndexRebuilder
from knoggin_server.knowledge.db.tool_queries import ToolQueries
from knoggin_server.knowledge.db.writers.entity_writer import EntityWriter
from knoggin_server.knowledge.db.writers.fact_writer import FactWriter
from knoggin_server.knowledge.db.writers.graph_writer import GraphWriter
from knoggin_server.knowledge.services.embedding_service import EmbeddingService


class GraphInterface:
    """
    Facade over the Postgres/AGE persistence layer.

    The implementation is intentionally delegated to focused reader, writer,
    tool-query, and community-store classes. This class keeps call sites simple
    without making each subsystem know the storage layout.
    """

    def __init__(
        self,
        postgres_client: PostgresClient,
        embedding_service: EmbeddingService,
    ):
        self._postgres_client = postgres_client
        self._id_allocator = IdAllocator(self._postgres_client)
        self._entity_writer = EntityWriter(self._postgres_client)
        self._fact_writer = FactWriter(self._postgres_client)
        self._graph_writer = GraphWriter(self._postgres_client)
        self._entity_reader = EntityReader(self._postgres_client)
        self._fact_reader = FactReader(self._postgres_client)
        self._graph_reader = GraphReader(self._postgres_client)
        self._tools = ToolQueries(self._postgres_client)
        self._projection_rebuilder = ProjectionRebuilder(self._postgres_client)
        self._search_index_rebuilder = SearchIndexRebuilder(
            self._postgres_client,
            embedding_service,
        )
        self._community = CommunityStore(self._postgres_client)
        logger.info("GraphClient initialized with shared Postgres/AGE backend")

    @property
    def community(self) -> CommunityStore:
        return self._community

    async def save_message_logs(self, messages: List[Dict]) -> bool:
        return await self._graph_writer.save_message_logs(messages)

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
        user_name: Optional[str] = None,
        session_id: Optional[str] = None,
        project_id: Optional[str] = None,
    ) -> int:
        return await self._fact_writer.create_facts_batch(
            entity_id,
            facts,
            user_name=user_name,
            session_id=session_id,
            project_id=project_id,
        )

    async def invalidate_fact(
        self, fact_id: str, invalid_at: datetime, project_id: Optional[str] = None
    ) -> bool:
        return await self._fact_writer.invalidate_fact(
            fact_id, invalid_at, project_id=project_id
        )

    async def update_entity_profile(
        self,
        entity_id: int,
        canonical_name: str,
        embedding: List[float],
        last_msg_id: int,
        project_id: Optional[str] = None,
    ):
        return await self._entity_writer.update_entity_profile(
            entity_id, canonical_name, embedding, last_msg_id, project_id=project_id
        )

    async def update_entity_canonical_name(
        self, entity_id: int, canonical_name: str, project_id: Optional[str] = None
    ) -> None:
        return await self._entity_writer.update_entity_canonical_name(
            entity_id, canonical_name, project_id=project_id
        )

    async def update_entity_embedding(
        self, entity_id: int, embedding: List[float], project_id: Optional[str] = None
    ):
        return await self._entity_writer.update_entity_embedding(
            entity_id, embedding, project_id=project_id
        )

    async def update_entity_checkpoint(
        self, entity_id: int, last_msg_id: int, project_id: Optional[str] = None
    ):
        return await self._entity_writer.update_entity_checkpoint(
            entity_id, last_msg_id, project_id=project_id
        )

    async def update_entity_aliases(
        self, alias_updates: Dict[int, List[str]], project_id: Optional[str] = None
    ) -> None:
        return await self._entity_writer.update_entity_aliases(
            alias_updates, project_id=project_id
        )

    async def create_hierarchy_edge(
        self, parent_id: int, child_id: int, project_id: Optional[str] = None
    ) -> bool:
        return await self._graph_writer.create_hierarchy_edge(
            parent_id, child_id, project_id=project_id
        )

    async def merge_entities(
        self,
        primary_id: int,
        secondary_id: int,
        project_id: Optional[str] = None,
        final_topic: Optional[str] = None,
    ) -> bool:
        return await self._graph_writer.merge_entities(
            primary_id,
            secondary_id,
            project_id=project_id,
            final_topic=final_topic,
        )

    async def cleanup_null_entities(
        self, project_id: Optional[str] = None
    ) -> List[int]:
        return await self._entity_writer.cleanup_null_entities(project_id=project_id)

    async def delete_entity(
        self, entity_id: int, project_id: Optional[str] = None
    ) -> bool:
        return await self._entity_writer.delete_entity(entity_id, project_id=project_id)

    async def bulk_delete_entities(
        self, entity_ids: List[int], project_id: Optional[str] = None
    ) -> List[int]:
        return await self._entity_writer.bulk_delete_entities(
            entity_ids, project_id=project_id
        )

    async def delete_old_invalidated_facts(
        self, cutoff: datetime, project_id: Optional[str] = None
    ) -> int:
        return await self._fact_writer.delete_old_invalidated_facts(
            cutoff, project_id=project_id
        )



    async def delete_relationship(
        self, entity_a_id: int, entity_b_id: int, project_id: Optional[str] = None
    ) -> bool:
        return await self._graph_writer.delete_relationship(
            entity_a_id, entity_b_id, project_id=project_id
        )

    async def rebuild_project_projection(
        self,
        project_id: str,
        user_name: Optional[str] = None,
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

    async def get_max_entity_id(self) -> int:
        return await self._entity_reader.get_max_entity_id()

    async def get_entity_embedding(self, entity_id: int) -> List[float]:
        return await self._entity_reader.get_entity_embedding(entity_id)

    async def get_message_text(
        self, message_id: int, user_name: str, session_id: str
    ) -> str:
        return await self._graph_reader.get_message_text(
            message_id, user_name, session_id
        )

    async def get_messages_by_ids(
        self,
        ids: List[int],
        user_name: Optional[str] = None,
        session_ids: Optional[List[str]] = None,
    ) -> List[Dict]:
        return await self._graph_reader.get_messages_by_ids(
            ids, user_name=user_name, session_ids=session_ids
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
        forward: int = 3,
        target_total: int = 10,
        user_name: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> List[Dict]:
        return await self._graph_reader.get_surrounding_messages(
            message_id,
            forward,
            target_total,
            user_name=user_name,
            session_id=session_id,
        )

    async def get_facts_for_entity(
        self, entity_id: int, active_only: bool = True
    ) -> List[FactRecord]:
        return await self._fact_reader.get_facts_for_entity(entity_id, active_only)

    async def search_relevant_facts(
        self, entity_id: int, query_embedding: List[float], limit: int = 5
    ) -> List[FactRecord]:
        return await self._fact_reader.search_relevant_facts(
            entity_id, query_embedding, limit
        )

    async def get_facts_for_entities(
        self, entity_ids: List[int], active_only: bool = True
    ) -> Dict[int, List[FactRecord]]:
        return await self._fact_reader.get_facts_for_entities(entity_ids, active_only)

    async def get_facts_from_message(
        self, msg_id: int, user_name: str = None, session_id: str = None
    ) -> List[FactRecord]:
        return await self._fact_reader.get_facts_from_message(
            msg_id, user_name=user_name, session_id=session_id
        )

    async def validate_existing_ids(self, ids: List[int]) -> Optional[Set[int]]:
        return await self._entity_reader.validate_existing_ids(ids)

    async def get_all_entities_for_hydration(self) -> List[Dict]:
        return await self._entity_reader.get_all_entities_for_hydration()

    async def find_alias_collisions(self) -> List[Tuple[int, int]]:
        return await self._entity_reader.find_alias_collisions()

    async def get_orphan_entities(
        self,
        protected_id: int = 1,
        orphan_cutoff_ms: int = 0,
        stale_junk_cutoff_ms: int = 0,
        project_id: Optional[str] = None,
    ) -> List[int]:
        return await self._entity_reader.get_orphan_entities(
            protected_id, orphan_cutoff_ms, stale_junk_cutoff_ms, project_id=project_id
        )

    async def get_neighbor_ids(self, entity_id: int) -> Set[int]:
        return await self._graph_reader.get_neighbor_ids(entity_id)

    async def get_entities_by_names(
        self, names: List[str], visible_project_ids: List[str] = None
    ) -> List[Dict]:
        return await self._entity_reader.get_entities_by_names(
            names, visible_project_ids
        )

    async def get_parent_entities(self, entity_id: int) -> List[Dict]:
        return await self._graph_reader.get_parent_entities(entity_id)

    async def get_neighbor_entities(self, entity_id: int, limit: int = 5) -> List[Dict]:
        return await self._graph_reader.get_neighbor_entities(entity_id, limit)

    async def get_child_entities(self, entity_id: int) -> List[Dict]:
        return await self._graph_reader.get_child_entities(entity_id)

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

    async def has_direct_edge(self, id_a: int, id_b: int) -> bool:
        return await self._graph_reader.has_direct_edge(id_a, id_b)

    async def has_hierarchy_edge(self, id_a: int, id_b: int) -> bool:
        return await self._graph_reader.has_hierarchy_edge(id_a, id_b)

    async def search_similar_entities(
        self, entity_id: int, limit: int = 50, visible_project_ids: List[str] = None
    ) -> List[Tuple[int, float]]:
        return await self._entity_reader.search_similar_entities(
            entity_id, limit, visible_project_ids
        )

    async def search_entities_by_embedding(
        self,
        embedding: List[float],
        limit: int = 10,
        score_threshold: float = 0.8,
        visible_project_ids: List[str] = None,
    ) -> List[Tuple[int, float]]:
        return await self._entity_reader.search_entities_by_embedding(
            embedding, limit, score_threshold, visible_project_ids
        )

    async def list_entities(
        self,
        limit: int = 20,
        offset: int = 0,
        topic: str = None,
        entity_type: str = None,
        search: str = None,
    ) -> Tuple[List[Dict], int]:
        return await self._entity_reader.list_entities(
            limit, offset, topic, entity_type, search
        )

    async def get_entity_by_id(
        self, entity_id: int, visible_project_ids: List[str] = None
    ) -> Optional[Dict]:
        return await self._entity_reader.get_entity_by_id(
            entity_id, visible_project_ids
        )

    async def get_entities_by_ids(self, entity_ids: List[int]) -> List[Dict]:
        return await self._entity_reader.get_entities_by_ids(entity_ids)



    async def get_graph_stats(self) -> Dict[str, int]:
        return await self._graph_reader.get_graph_stats()

    async def get_entity_count_by_type(self) -> List[Dict]:
        return await self._entity_reader.get_entity_count_by_type()

    async def get_entity_count_by_topic(self) -> List[Dict]:
        return await self._entity_reader.get_entity_count_by_topic()

    async def get_top_connected_entities(self, limit: int = 10) -> List[Dict]:
        return await self._entity_reader.get_top_connected_entities(limit)

    async def get_entity_relationships(self, entity_id: int) -> List[Dict]:
        return await self._entity_reader.get_entity_relationships(entity_id)

    async def get_recent_facts(self, days: int = 7, limit: int = 20) -> List[Dict]:
        return await self._fact_reader.get_recent_facts(days, limit)

    async def get_recently_active_entities(
        self, days: int = 7, limit: int = 10
    ) -> List[Dict]:
        return await self._entity_reader.get_recently_active_entities(days, limit)

    async def get_notable_entities(self, limit: int = 10) -> List[Dict]:
        return await self._entity_reader.get_notable_entities(limit)

    async def get_neighbor_ids_batch(
        self, entity_ids: List[int]
    ) -> Dict[int, Set[int]]:
        return await self._graph_reader.get_neighbor_ids_batch(entity_ids)

    async def get_hot_topic_context_with_messages(
        self,
        hot_topic_names: List[str],
        msg_limit: int = 5,
        slim: bool = False,
        visible_project_ids: List[str] = None,
    ) -> Dict:
        return await self._tools.get_hot_topic_context_with_messages(
            hot_topic_names, msg_limit, slim, visible_project_ids
        )

    async def search_messages_fts(
        self,
        query: str,
        limit: int = 50,
        user_name: str = None,
        session_ids: List[str] = None,
        project_ids: List[str] = None,
    ) -> List[Tuple[int, float, str]]:
        return await self._tools.search_messages_fts(
            query,
            limit,
            user_name=user_name,
            session_ids=session_ids,
            project_ids=project_ids,
        )

    async def search_entity(
        self,
        query: str,
        active_topics: List[str] = None,
        limit: int = 5,
        connections_limit: int = 5,
        evidence_limit: int = 5,
        visible_project_ids: List[str] = None,
    ) -> List[Dict]:
        return await self._tools.search_entity(
            query,
            active_topics,
            limit,
            connections_limit,
            evidence_limit,
            visible_project_ids,
        )

    async def get_related_entities(
        self,
        entity_names: List[str],
        active_topics: List[str] = None,
        limit: int = 50,
        visible_project_ids: List[str] = None,
    ) -> List[Dict]:
        return await self._tools.get_related_entities(
            entity_names, active_topics, limit, visible_project_ids
        )

    async def get_recent_activity(
        self,
        entity_name: str,
        active_topics: List[str] = None,
        hours: int = 24,
        visible_project_ids: List[str] = None,
    ) -> List[Dict]:
        return await self._tools.get_recent_activity(
            entity_name, active_topics, hours, visible_project_ids
        )

    async def find_path_filtered(
        self,
        start_name: str,
        end_name: str,
        active_topics: List[str] = None,
        max_depth: int = 4,
        visible_project_ids: List[str] = None,
    ) -> Tuple[List[Dict], bool]:
        return await self._tools.find_path_filtered(
            start_name, end_name, active_topics, max_depth, visible_project_ids
        )
