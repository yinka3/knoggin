from datetime import datetime
import time
from loguru import logger
from typing import Dict, List, Optional, Set, Tuple
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

from db.query_tools import GraphToolQueries
from db.reader import GraphReader
from db.writer import GraphWriter
from shared.schema.dtypes import Fact
load_dotenv()

MEMGRAPH_USER=os.environ.get("MEMGRAPH_USER")
MEMGRAPH_PASSWORD=os.environ.get("MEMGRAPH_PASSWORD")
MEMGRAPH_HOST=os.environ.get("MEMGRAPH_HOST")
MEMGRAPH_PORT=os.environ.get("MEMGRAPH_PORT")

class MemGraphStore:
    def __init__(self, uri: str = None):
        if uri is None:
            uri = f"bolt://{MEMGRAPH_HOST}:{MEMGRAPH_PORT}"
        self.driver = GraphDatabase.driver(
            uri,
            auth=(MEMGRAPH_USER, MEMGRAPH_PASSWORD)
        )
        self._verify_conn()
        self._setup_schema()
        self._writer = GraphWriter(self.driver)
        self._reader = GraphReader(self.driver)
        self._tools = GraphToolQueries(self.driver)
        logger.info("Graph store initialized")

    def close(self):
        if self.driver:
            self.driver.close()
    
    def _verify_conn(self):
        max_retries = 5
        for i in range(max_retries):
            try:
                self.driver.verify_connectivity()
                return
            except Exception as e:
                if i == max_retries - 1:
                    raise e
                logger.warning(f"Waiting for Memgraph... ({e})")
                time.sleep(2)

    
    def _setup_schema(self):
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
            "CREATE INDEX ON :Entity(last_mentioned);"
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
        
        with self.driver.session() as session:
            for q in constraints + indices + vector_indices + text_indices:
                try:
                    session.run(q)
                except Exception as e:
                    logger.debug(f"Schema setup note: {e}")
        
        logger.info("Memgraph schema indices verified.")
    
    # ===== WRITER DELEGATIONS =====

    def save_message_logs(self, messages: List[Dict]) -> bool:
        return self._writer.save_message_logs(messages)

    def write_batch(self, entities: List[Dict], relationships: List[Dict]) -> bool:
        return self._writer.write_batch(entities, relationships)

    def create_facts_batch(self, entity_id: int, facts: List[Fact]) -> int:
        return self._writer.create_facts_batch(entity_id, facts)

    def invalidate_fact(self, fact_id: str, invalid_at: datetime) -> bool:
        return self._writer.invalidate_fact(fact_id, invalid_at)

    def update_entity_profile(self, entity_id: int, canonical_name: str, embedding: List[float], last_msg_id: int):
        return self._writer.update_entity_profile(entity_id, canonical_name, embedding, last_msg_id)

    def update_entity_embedding(self, entity_id: int, embedding: List[float]):
        return self._writer.update_entity_embedding(entity_id, embedding)
    
    def update_entity_aliases(self, alias_updates: Dict[int, List[str]]):
        return self._writer.update_entity_aliases(alias_updates)

    def create_hierarchy_edge(self, parent_id: int, child_id: int) -> bool:
        return self._writer.create_hierarchy_edge(parent_id, child_id)

    def merge_entities(self, primary_id: int, secondary_id: int) -> bool:
        return self._writer.merge_entities(primary_id, secondary_id)

    def cleanup_null_entities(self) -> int:
        return self._writer.cleanup_null_entities()
    
    def delete_entity(self, entity_id: int) -> bool:
        return self._writer.delete_entity(entity_id)

    def bulk_delete_entities(self, entity_ids: List[int]) -> int:
        return self._writer.bulk_delete_entities(entity_ids)

    def delete_old_invalidated_facts(self, cutoff: datetime) -> int:
        return self._writer.delete_old_invalidated_facts(cutoff)
    
    def create_preference(self, id: str, content: str, kind: str, session_id: str) -> bool:
        return self._writer.create_preference(id, content, kind, session_id)
    
    def delete_preference(self, pref_id: str) -> bool:
        return self._writer.delete_preference(pref_id)
    
    # ===== READER DELEGATIONS =====

    def get_max_entity_id(self) -> int:
        return self._reader.get_max_entity_id()

    def get_entity_embedding(self, entity_id: int) -> List[float]:
        return self._reader.get_entity_embedding(entity_id)

    def get_message_text(self, message_id: int) -> str:
        return self._reader.get_message_text(message_id)

    def get_facts_for_entity(self, entity_id: int, active_only: bool = True) -> List[Fact]:
        return self._reader.get_facts_for_entity(entity_id, active_only)

    def get_facts_for_entities(self, entity_ids: List[int], active_only: bool = True) -> Dict[int, List[Fact]]:
        return self._reader.get_facts_for_entities(entity_ids, active_only)

    def get_facts_from_message(self, msg_id: int) -> List[Fact]:
        return self._reader.get_facts_from_message(msg_id)

    def validate_existing_ids(self, ids: List[int]) -> Optional[Set[int]]:
        return self._reader.validate_existing_ids(ids)

    def get_all_entities_for_hydration(self) -> List[Dict]:
        return self._reader.get_all_entities_for_hydration()

    def find_alias_collisions(self) -> List[Tuple[int, int]]:
        return self._reader.find_alias_collisions()

    def get_orphan_entities(self, protected_id: int = 1, orphan_cutoff_ms: int = 0, stale_junk_cutoff_ms: int = 0) -> List[int]:
        return self._reader.get_orphan_entities(protected_id, orphan_cutoff_ms, stale_junk_cutoff_ms)

    def get_neighbor_ids(self, entity_id: int) -> Set[int]:
        return self._reader.get_neighbor_ids(entity_id)

    def get_entities_by_names(self, names: List[str]) -> List[Dict]:
        return self._reader.get_entities_by_names(names)

    def get_parent_entities(self, entity_id: int) -> List[Dict]:
        return self._reader.get_parent_entities(entity_id)

    def get_neighbor_entities(self, entity_id: int, limit: int = 5) -> List[Dict]:
        return self._reader.get_neighbor_entities(entity_id, limit)

    def get_child_entities(self, entity_id: int) -> List[Dict]:
        return self._reader.get_child_entities(entity_id)
    
    def get_hierarchy_candidates(self, topic: str, parent_type: str, child_types: List[str], min_weight: int = 2) -> List[Dict]:
        return self._reader.get_hierarchy_candidates(topic, parent_type, child_types, min_weight)

    def has_direct_edge(self, id_a: int, id_b: int) -> bool:
        return self._reader.has_direct_edge(id_a, id_b)

    def has_hierarchy_edge(self, id_a: int, id_b: int) -> bool:
        return self._reader.has_hierarchy_edge(id_a, id_b)

    def search_similar_entities(self, entity_id: int, limit: int = 50) -> List[Tuple[int, float]]:
        return self._reader.search_similar_entities(entity_id, limit)

    def search_entities_by_embedding(self, embedding: List[float], limit: int = 10, score_threshold: float = 0.8) -> List[Tuple[int, float]]:
        return self._reader.search_entities_by_embedding(embedding, limit, score_threshold)

    def search_messages_vector(self, query_embedding: List[float], limit: int = 50) -> List[Tuple[int, float]]:
        return self._reader.search_messages_vector(query_embedding, limit)
    
    def list_entities(self, limit: int = 20, offset: int = 0, topic: str = None, entity_type: str = None, search: str = None) -> Tuple[List[Dict], int]:
        return self._reader.list_entities(limit, offset, topic, entity_type, search)
    
    def get_entity_by_id(self, entity_id: int):
        return self._reader.get_entity_by_id(entity_id=entity_id)
    
    def list_preferences(self, session_id: str, kind: str = None) -> List[Dict]:
        return self._reader.list_preferences(session_id, kind)
    
    def get_graph_stats(self) -> Dict[str, int]:
        return self._reader.get_graph_stats()
    
    def get_entity_count_by_type(self) -> List[Dict]:
        return self._reader.get_entity_count_by_type()
    
    def get_entity_count_by_topic(self) -> List[Dict]:
        return self._reader.get_entity_count_by_topic()
    
    def get_top_connected_entities(self, limit: int = 10) -> List[Dict]:
        return self._reader.get_top_connected_entities(limit)

    # ===== TOOL QUERY DELEGATIONS =====

    def get_hot_topic_context_with_messages(self, hot_topic_names: List[str], msg_limit: int = 5, slim: bool = False) -> Dict:
        return self._tools.get_hot_topic_context_with_messages(hot_topic_names, msg_limit, slim)

    def search_messages_fts(self, query: str, limit: int = 50) -> List[Tuple[int, float]]:
        return self._tools.search_messages_fts(query, limit)

    def search_entity(self, query: str, active_topics: List[str] = None, limit: int = 5, connections_limit: int = 5, evidence_limit: int = 5) -> List[Dict]:
        return self._tools.search_entity(query, active_topics, limit, connections_limit, evidence_limit)

    def get_related_entities(self, entity_names: List[str], active_topics: List[str] = None, limit: int = 50) -> List[Dict]:
        return self._tools.get_related_entities(entity_names, active_topics, limit)

    def get_recent_activity(self, entity_name: str, active_topics: List[str] = None, hours: int = 24) -> List[Dict]:
        return self._tools.get_recent_activity(entity_name, active_topics, hours)

    def find_path_filtered(self, start_name: str, end_name: str, active_topics: List[str] = None) -> Tuple[List[Dict], bool]:
        return self._tools._find_path_filtered(start_name, end_name, active_topics)
    
    
