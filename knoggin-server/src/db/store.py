from datetime import datetime
import asyncio
from loguru import logger
from typing import Dict, List, Optional, Set, Tuple
from neo4j import AsyncGraphDatabase
from dotenv import load_dotenv
import os

from db.query_tools import GraphToolQueries
from db.reader import GraphReader
from db.writer import GraphWriter
from db.community_store import CommunityStore
from common.schema.dtypes import Fact
load_dotenv()

MEMGRAPH_HOST=os.environ.get("MEMGRAPH_HOST", "localhost")
MEMGRAPH_PORT=os.environ.get("MEMGRAPH_PORT", "7687")

class MemGraphStore:
    def __init__(self, uri: str = None):
        if uri is None:
            uri = f"bolt://{MEMGRAPH_HOST}:{MEMGRAPH_PORT}"
        self.driver = AsyncGraphDatabase.driver(uri)
        self._writer = GraphWriter(self.driver)
        self._reader = GraphReader(self.driver)
        self._tools = GraphToolQueries(self.driver)
        self._community = CommunityStore(self.driver)
        logger.info("Graph store initialized (Async)")

    @property
    def community(self) -> CommunityStore:
        return self._community

    async def initialize(self):
        """Async initialization for connectivity and schema."""
        await self._verify_conn()
        await self._setup_schema()

    async def close(self):
        if self.driver:
            await self.driver.close()
    
    async def _verify_conn(self):
        max_retries = 5
        for i in range(max_retries):
            try:
                await self.driver.verify_connectivity()
                return
            except Exception as e:
                if i == max_retries - 1:
                    raise e
                logger.warning(f"Waiting for Memgraph... ({e})")
                await asyncio.sleep(2)

    async def _setup_schema(self):
        """
        Create indices and constraints using Memgraph syntax.
        """
        constraints = [
            "CREATE CONSTRAINT ON (e:Entity) ASSERT e.id IS UNIQUE;",
            "CREATE CONSTRAINT ON (t:Topic) ASSERT t.name IS UNIQUE;",
            "CREATE CONSTRAINT ON (m:Message) ASSERT m.id IS UNIQUE;",
            "CREATE CONSTRAINT ON (f:Fact) ASSERT f.id IS UNIQUE;",
        ]

        indices = [
            "CREATE INDEX ON :Fact(invalid_at);",
            "CREATE INDEX ON :Fact(created_at);",
            "CREATE INDEX ON :Message(timestamp);",
            "CREATE INDEX ON :Entity(canonical_name);",
            "CREATE INDEX ON :Entity(last_mentioned);",
            "CREATE INDEX ON :AAC_Discussion(created_at);",
            "CREATE INDEX ON :AAC_Discussion(status);",
            "CREATE INDEX ON :AAC_Message(timestamp);",
        ]

        vector_indices = [
            """
            CREATE VECTOR INDEX entity_vec ON :Entity(embedding) 
            WITH CONFIG {"dimension": 1024, "capacity": 500000, "metric": "cos"}
            """,
            """
            CREATE VECTOR INDEX fact_vec ON :Fact(embedding) 
            WITH CONFIG {"dimension": 1024, "capacity": 5000000, "metric": "cos"}
            """,
            """
            CREATE VECTOR INDEX message_vec ON :Message(embedding) 
            WITH CONFIG {"dimension": 1024, "capacity": 5000000, "metric": "cos"}
            """
        ]
        
        text_indices = [
            "CREATE TEXT INDEX message_search ON :Message(content)",
            "CREATE TEXT INDEX entity_search ON :Entity(canonical_name, aliases)"
        ]
        
        async with self.driver.session() as session:
            for q in constraints + indices + vector_indices + text_indices:
                try:
                    await session.run(q)
                except Exception as e:
                    logger.debug(f"Schema setup note: {e}")
        
        logger.info("Memgraph schema indices verified.")
    
    # ===== WRITER DELEGATIONS =====

    async def save_message_logs(self, messages: List[Dict]) -> bool:
        return await self._writer.save_message_logs(messages)

    async def write_batch(self, entities: List[Dict], relationships: List[Dict]) -> bool:
        return await self._writer.write_batch(entities, relationships)

    async def create_facts_batch(self, entity_id: int, facts: List[Fact]) -> int:
        return await self._writer.create_facts_batch(entity_id, facts)

    async def invalidate_fact(self, fact_id: str, invalid_at: datetime) -> bool:
        return await self._writer.invalidate_fact(fact_id, invalid_at)

    async def update_entity_profile(self, entity_id: int, canonical_name: str, embedding: List[float], last_msg_id: int):
        return await self._writer.update_entity_profile(entity_id, canonical_name, embedding, last_msg_id)
    
    async def update_entity_canonical_name(self, entity_id: int, canonical_name: str) -> None:
        return await self._writer.update_entity_canonical_name(entity_id, canonical_name)

    async def update_entity_embedding(self, entity_id: int, embedding: List[float]):
        return await self._writer.update_entity_embedding(entity_id, embedding)
    
    async def update_entity_checkpoint(self, entity_id: int, last_msg_id: int):
        return await self._writer.update_entity_checkpoint(entity_id, last_msg_id)
    
    async def update_entity_aliases(self, alias_updates: Dict[int, List[str]]):
        return await self._writer.update_entity_aliases(alias_updates)

    async def create_hierarchy_edge(self, parent_id: int, child_id: int) -> bool:
        return await self._writer.create_hierarchy_edge(parent_id, child_id)

    async def merge_entities(self, primary_id: int, secondary_id: int) -> bool:
        return await self._writer.merge_entities(primary_id, secondary_id)

    async def cleanup_null_entities(self) -> int:
        return await self._writer.cleanup_null_entities()
    
    async def delete_entity(self, entity_id: int) -> bool:
        return await self._writer.delete_entity(entity_id)

    async def bulk_delete_entities(self, entity_ids: List[int]) -> int:
        return await self._writer.bulk_delete_entities(entity_ids)

    async def delete_old_invalidated_facts(self, cutoff: datetime) -> int:
        return await self._writer.delete_old_invalidated_facts(cutoff)
    
    async def create_preference(self, id: str, content: str, kind: str, session_id: str) -> bool:
        return await self._writer.create_preference(id, content, kind, session_id)
    
    async def delete_preference(self, pref_id: str) -> bool:
        return await self._writer.delete_preference(pref_id)
    
    async def delete_relationship(self, entity_a_id: int, entity_b_id: int) -> bool:
        return await self._writer.delete_relationship(entity_a_id, entity_b_id)
    
    # ===== READER DELEGATIONS =====

    async def get_max_entity_id(self) -> int:
        return await self._reader.get_max_entity_id()

    async def get_entity_embedding(self, entity_id: int) -> List[float]:
        return await self._reader.get_entity_embedding(entity_id)

    async def get_message_text(self, message_id: int) -> str:
        return await self._reader.get_message_text(message_id)

    async def get_messages_by_ids(self, ids: List[int]) -> List[Dict]:
        return await self._reader.get_messages_by_ids(ids)

    async def get_surrounding_messages(self, message_id: int, forward: int = 3, target_total: int = 10) -> List[Dict]:
        return await self._reader.get_surrounding_messages(message_id, forward, target_total)

    async def get_facts_for_entity(self, entity_id: int, active_only: bool = True) -> List[Fact]:
        return await self._reader.get_facts_for_entity(entity_id, active_only)

    async def search_relevant_facts(self, entity_id: int, query_embedding: List[float], limit: int = 5) -> List[Fact]:
        return await self._reader.search_relevant_facts(entity_id, query_embedding, limit)

    async def get_facts_for_entities(self, entity_ids: List[int], active_only: bool = True) -> Dict[int, List[Fact]]:
        return await self._reader.get_facts_for_entities(entity_ids, active_only)

    async def get_facts_from_message(self, msg_id: int) -> List[Fact]:
        return await self._reader.get_facts_from_message(msg_id)

    async def validate_existing_ids(self, ids: List[int]) -> Optional[Set[int]]:
        return await self._reader.validate_existing_ids(ids)

    async def get_all_entities_for_hydration(self) -> List[Dict]:
        return await self._reader.get_all_entities_for_hydration()

    async def find_alias_collisions(self) -> List[Tuple[int, int]]:
        return await self._reader.find_alias_collisions()

    async def get_orphan_entities(self, protected_id: int = 1, orphan_cutoff_ms: int = 0, stale_junk_cutoff_ms: int = 0) -> List[int]:
        return await self._reader.get_orphan_entities(protected_id, orphan_cutoff_ms, stale_junk_cutoff_ms)

    async def get_neighbor_ids(self, entity_id: int) -> Set[int]:
        return await self._reader.get_neighbor_ids(entity_id)

    async def get_entities_by_names(self, names: List[str]) -> List[Dict]:
        return await self._reader.get_entities_by_names(names)

    async def get_parent_entities(self, entity_id: int) -> List[Dict]:
        return await self._reader.get_parent_entities(entity_id)

    async def get_neighbor_entities(self, entity_id: int, limit: int = 5) -> List[Dict]:
        return await self._reader.get_neighbor_entities(entity_id, limit)

    async def get_child_entities(self, entity_id: int) -> List[Dict]:
        return await self._reader.get_child_entities(entity_id)
    
    async def get_hierarchy_candidates(self, topic: str, parent_type: str, child_types: List[str], min_weight: int = 2) -> List[Dict]:
        return await self._reader.get_hierarchy_candidates(topic, parent_type, child_types, min_weight)

    async def has_direct_edge(self, id_a: int, id_b: int) -> bool:
        return await self._reader.has_direct_edge(id_a, id_b)

    async def has_hierarchy_edge(self, id_a: int, id_b: int) -> bool:
        return await self._reader.has_hierarchy_edge(id_a, id_b)

    async def search_similar_entities(self, entity_id: int, limit: int = 50) -> List[Tuple[int, float]]:
        return await self._reader.search_similar_entities(entity_id, limit)

    async def search_entities_by_embedding(self, embedding: List[float], limit: int = 10, score_threshold: float = 0.8) -> List[Tuple[int, float]]:
        return await self._reader.search_entities_by_embedding(embedding, limit, score_threshold)

    async def search_messages_vector(self, query_embedding: List[float], limit: int = 50) -> List[Tuple[int, float]]:
        return await self._reader.search_messages_vector(query_embedding, limit)
    
    async def list_entities(self, limit: int = 20, offset: int = 0, topic: str = None, entity_type: str = None, search: str = None) -> Tuple[List[Dict], int]:
        return await self._reader.list_entities(limit, offset, topic, entity_type, search)
    
    async def get_entity_by_id(self, entity_id: int):
        return await self._reader.get_entity_by_id(entity_id=entity_id)
    
    async def get_entities_by_ids(self, entity_ids: List[int]) -> List[Dict]:
        return await self._reader.get_entities_by_ids(entity_ids)
    
    async def list_preferences(self, session_id: str, kind: str = None) -> List[Dict]:
        return await self._reader.list_preferences(session_id, kind)
    
    async def get_graph_stats(self) -> Dict[str, int]:
        return await self._reader.get_graph_stats()
    
    async def get_entity_count_by_type(self) -> List[Dict]:
        return await self._reader.get_entity_count_by_type()
    
    async def get_entity_count_by_topic(self) -> List[Dict]:
        return await self._reader.get_entity_count_by_topic()
    
    async def get_top_connected_entities(self, limit: int = 10) -> List[Dict]:
        return await self._reader.get_top_connected_entities(limit)
    
    async def get_entity_relationships(self, entity_id: int) -> List[Dict]:
        return await self._reader.get_entity_relationships(entity_id)

    async def get_recent_facts(self, days: int = 7, limit: int = 20) -> List[Dict]:
        return await self._reader.get_recent_facts(days, limit)

    async def get_recently_active_entities(self, days: int = 7, limit: int = 10) -> List[Dict]:
        return await self._reader.get_recently_active_entities(days, limit)

    async def get_notable_entities(self, limit: int = 10) -> List[Dict]:
        return await self._reader.get_notable_entities(limit)
    
    async def get_neighbor_ids_batch(self, entity_ids: List[int]) -> Dict[int, Set[int]]:
        return await self._reader.get_neighbor_ids_batch(entity_ids)

    # ===== TOOL QUERY DELEGATIONS =====

    async def get_hot_topic_context_with_messages(self, hot_topic_names: List[str], msg_limit: int = 5, slim: bool = False) -> Dict:
        return await self._tools.get_hot_topic_context_with_messages(hot_topic_names, msg_limit, slim)

    async def search_messages_fts(self, query: str, limit: int = 50) -> List[Tuple[int, float]]:
        return await self._tools.search_messages_fts(query, limit)

    async def search_entity(self, query: str, active_topics: List[str] = None, limit: int = 5, connections_limit: int = 5, evidence_limit: int = 5) -> List[Dict]:
        return await self._tools.search_entity(query, active_topics, limit, connections_limit, evidence_limit)

    async def get_related_entities(self, entity_names: List[str], active_topics: List[str] = None, limit: int = 50) -> List[Dict]:
        return await self._tools.get_related_entities(entity_names, active_topics, limit)

    async def get_recent_activity(self, entity_name: str, active_topics: List[str] = None, hours: int = 24) -> List[Dict]:
        return await self._tools.get_recent_activity(entity_name, active_topics, hours)

    async def find_path_filtered(self, start_name: str, end_name: str, active_topics: List[str] = None, max_depth: int = 4) -> Tuple[List[Dict], bool]:
        return await self._tools._find_path_filtered(start_name, end_name, active_topics, max_depth)
    
    
