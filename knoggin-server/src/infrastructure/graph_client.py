from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

from loguru import logger

from common.schema.dtypes import FactRecord
from infrastructure.postgres_client import PostgresClient
from knoggin.community.db.community_store import CommunityStore
from knoggin.knowledge.db.readers.entity_reader import EntityReader
from knoggin.knowledge.db.readers.fact_reader import FactReader
from knoggin.knowledge.db.readers.graph_reader import GraphReader
from knoggin.knowledge.db.tool_queries import ToolQueries
from knoggin.knowledge.db.writers.entity_writer import EntityWriter
from knoggin.knowledge.db.writers.fact_writer import FactWriter
from knoggin.knowledge.db.writers.graph_writer import GraphWriter


class GraphClient:
    def __init__(self, dsn: str):
        self._postgres_client = PostgresClient(dsn=dsn)
        self._entity_writer = EntityWriter(self._postgres_client)
        self._fact_writer = FactWriter(self._postgres_client)
        self._graph_writer = GraphWriter(self._postgres_client)
        self._entity_reader = EntityReader(self._postgres_client)
        self._fact_reader = FactReader(self._postgres_client)
        self._graph_reader = GraphReader(self._postgres_client)
        self._tools = ToolQueries(self._postgres_client)
        self._community = CommunityStore(self._postgres_client)
        logger.info("GraphClient initialized with internal Postgres/AGE backend")

    async def connect(self):
        await self._postgres_client.connect()

    async def close(self):
        await self._postgres_client.close()

    @property
    def community(self) -> CommunityStore:
        return self._community

    async def save_message_logs(self, messages: List[Dict]) -> bool:
        return await self._graph_writer.save_message_logs(messages)

    async def write_batch(
        self, entities: List[Dict], relationships: List[Dict]
    ) -> bool:
        return await self._entity_writer.write_batch(entities, relationships)

    async def create_facts_batch(self, entity_id: int, facts: List[FactRecord]) -> int:
        return await self._fact_writer.create_facts_batch(entity_id, facts)

    async def invalidate_fact(self, fact_id: str, invalid_at: datetime) -> bool:
        return await self._fact_writer.invalidate_fact(fact_id, invalid_at)

    async def update_entity_profile(
        self,
        entity_id: int,
        canonical_name: str,
        embedding: List[float],
        last_msg_id: int,
    ):
        return await self._entity_writer.update_entity_profile(
            entity_id, canonical_name, embedding, last_msg_id
        )

    async def update_entity_canonical_name(
        self, entity_id: int, canonical_name: str
    ) -> None:
        return await self._entity_writer.update_entity_canonical_name(
            entity_id, canonical_name
        )

    async def update_entity_embedding(self, entity_id: int, embedding: List[float]):
        return await self._entity_writer.update_entity_embedding(entity_id, embedding)

    async def update_entity_checkpoint(self, entity_id: int, last_msg_id: int):
        return await self._entity_writer.update_entity_checkpoint(
            entity_id, last_msg_id
        )

    async def update_entity_aliases(self, alias_updates: Dict[int, List[str]]):
        return await self._entity_writer.update_entity_aliases(alias_updates)

    async def create_hierarchy_edge(self, parent_id: int, child_id: int) -> bool:
        return await self._graph_writer.create_hierarchy_edge(parent_id, child_id)

    async def merge_entities(self, primary_id: int, secondary_id: int) -> bool:
        return await self._graph_writer.merge_entities(primary_id, secondary_id)

    async def cleanup_null_entities(self) -> int:
        return await self._entity_writer.cleanup_null_entities()

    async def delete_entity(self, entity_id: int) -> bool:
        return await self._entity_writer.delete_entity(entity_id)

    async def bulk_delete_entities(self, entity_ids: List[int]) -> int:
        return await self._entity_writer.bulk_delete_entities(entity_ids)

    async def delete_old_invalidated_facts(self, cutoff: datetime) -> int:
        return await self._fact_writer.delete_old_invalidated_facts(cutoff)

    async def create_preference(
        self, id: str, content: str, kind: str, session_id: str
    ) -> bool:
        return await self._graph_writer.create_preference(id, content, kind, session_id)

    async def delete_preference(self, pref_id: str) -> bool:
        return await self._graph_writer.delete_preference(pref_id)

    async def delete_relationship(self, entity_a_id: int, entity_b_id: int) -> bool:
        return await self._graph_writer.delete_relationship(entity_a_id, entity_b_id)

    async def get_max_entity_id(self) -> int:
        return await self._entity_reader.get_max_entity_id()

    async def get_entity_embedding(self, entity_id: int) -> List[float]:
        return await self._entity_reader.get_entity_embedding(entity_id)

    async def get_message_text(self, message_id: int) -> str:
        return await self._graph_reader.get_message_text(message_id)

    async def get_messages_by_ids(self, ids: List[int]) -> List[Dict]:
        return await self._graph_reader.get_messages_by_ids(ids)

    async def get_surrounding_messages(
        self, message_id: int, forward: int = 3, target_total: int = 10
    ) -> List[Dict]:
        return await self._graph_reader.get_surrounding_messages(
            message_id, forward, target_total
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

    async def get_facts_from_message(self, msg_id: int) -> List[FactRecord]:
        return await self._fact_reader.get_facts_from_message(msg_id)

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
    ) -> List[int]:
        return await self._entity_reader.get_orphan_entities(
            protected_id, orphan_cutoff_ms, stale_junk_cutoff_ms
        )

    async def get_neighbor_ids(self, entity_id: int) -> Set[int]:
        return await self._graph_reader.get_neighbor_ids(entity_id)

    async def get_entities_by_names(self, names: List[str]) -> List[Dict]:
        return await self._entity_reader.get_entities_by_names(names)

    async def get_parent_entities(self, entity_id: int) -> List[Dict]:
        return await self._graph_reader.get_parent_entities(entity_id)

    async def get_neighbor_entities(self, entity_id: int, limit: int = 5) -> List[Dict]:
        return await self._graph_reader.get_neighbor_entities(entity_id, limit)

    async def get_child_entities(self, entity_id: int) -> List[Dict]:
        return await self._graph_reader.get_child_entities(entity_id)

    async def get_hierarchy_candidates(
        self, topic: str, parent_type: str, child_types: List[str], min_weight: int = 2
    ) -> List[Dict]:
        return await self._graph_reader.get_hierarchy_candidates(
            topic, parent_type, child_types, min_weight
        )

    async def has_direct_edge(self, id_a: int, id_b: int) -> bool:
        return await self._graph_reader.has_direct_edge(id_a, id_b)

    async def has_hierarchy_edge(self, id_a: int, id_b: int) -> bool:
        return await self._graph_reader.has_hierarchy_edge(id_a, id_b)

    async def search_similar_entities(
        self, entity_id: int, limit: int = 50
    ) -> List[Tuple[int, float]]:
        return await self._entity_reader.search_similar_entities(entity_id, limit)

    async def search_entities_by_embedding(
        self, embedding: List[float], limit: int = 10, score_threshold: float = 0.8
    ) -> List[Tuple[int, float]]:
        return await self._entity_reader.search_entities_by_embedding(
            embedding, limit, score_threshold
        )

    async def search_messages_vector(
        self, query_embedding: List[float], limit: int = 50
    ) -> List[Tuple[int, float]]:
        return await self._graph_reader.search_messages_vector(query_embedding, limit)

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

    async def get_entity_by_id(self, entity_id: int) -> Optional[Dict]:
        return await self._entity_reader.get_entity_by_id(entity_id=entity_id)

    async def get_entities_by_ids(self, entity_ids: List[int]) -> List[Dict]:
        return await self._entity_reader.get_entities_by_ids(entity_ids)

    async def list_preferences(self, session_id: str, kind: str = None) -> List[Dict]:
        return await self._graph_reader.list_preferences(session_id, kind)

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
        self, hot_topic_names: List[str], msg_limit: int = 5, slim: bool = False
    ) -> Dict:
        return await self._tools.get_hot_topic_context_with_messages(
            hot_topic_names, msg_limit, slim
        )

    async def search_messages_fts(
        self, query: str, limit: int = 50
    ) -> List[Tuple[int, float]]:
        return await self._tools.search_messages_fts(query, limit)

    async def search_entity(
        self,
        query: str,
        active_topics: List[str] = None,
        limit: int = 5,
        connections_limit: int = 5,
        evidence_limit: int = 5,
    ) -> List[Dict]:
        return await self._tools.search_entity(
            query, active_topics, limit, connections_limit, evidence_limit
        )

    async def get_related_entities(
        self, entity_names: List[str], active_topics: List[str] = None, limit: int = 50
    ) -> List[Dict]:
        return await self._tools.get_related_entities(
            entity_names, active_topics, limit
        )

    async def get_recent_activity(
        self, entity_name: str, active_topics: List[str] = None, hours: int = 24
    ) -> List[Dict]:
        return await self._tools.get_recent_activity(entity_name, active_topics, hours)

    async def find_path_filtered(
        self,
        start_name: str,
        end_name: str,
        active_topics: List[str] = None,
        max_depth: int = 4,
    ) -> Tuple[List[Dict], bool]:
        return await self._tools.find_path_filtered(
            start_name, end_name, active_topics, max_depth
        )
