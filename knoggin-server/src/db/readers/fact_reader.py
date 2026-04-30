from datetime import datetime, timezone, timedelta
from loguru import logger
from typing import Dict, List, Optional, Set, Tuple
from neo4j import AsyncDriver
from common.schema.dtypes import FactRecord

class FactReader:
    def __init__(self, driver: AsyncDriver):
        self.driver = driver

    def _hydrate_fact(self, record) -> FactRecord:
        """Convert DB record to FactRecord."""
        return FactRecord.from_db_record(record)


    async def get_facts_for_entity(self, entity_id: int, active_only: bool = True):
        """Get facts from an entity."""
        base = """
        MATCH (e:Entity {id: $entity_id})-[:HAS_FACT]->(f:Fact)
        """
        
        where = "\nWHERE f.invalid_at IS NULL" if active_only else ""
        
        tail = """
        OPTIONAL MATCH (f)-[:EXTRACTED_FROM]->(m:Message)
        RETURN f.id as id, f.source_entity_id as source_entity_id, f.content as content, 
            f.valid_at as valid_at, f.invalid_at as invalid_at, f.confidence as confidence, 
            f.embedding as embedding, m.id as source_msg_id, f.source as source
        ORDER BY f.created_at DESC
        """
        
        query = base + where + tail

        try:
            async with self.driver.session() as session:
                result = await session.run(query, {"entity_id": entity_id, "active_only": active_only})
                records = await result.data()
                return [self._hydrate_fact(record) for record in records]
        except Exception as e:
            logger.error(f"Failed to get facts for entity {entity_id}: {e}")
            return []


    async def get_facts_for_entities(self, entity_ids: List[int], active_only: bool = True) -> Dict[int, List[FactRecord]]:
        """Batch fetch facts for multiple entities. Returns {entity_id: [Fact, ...]}."""
        if not entity_ids:
            return {}
        
        where_clause = "WHERE f.invalid_at IS NULL" if active_only else ""
    
        query = f"""
        MATCH (e:Entity)
        WHERE e.id IN $entity_ids
        CALL {{
            WITH e
            MATCH (e)-[:HAS_FACT]->(f:Fact)
            {where_clause}
            RETURN f
            ORDER BY f.created_at DESC
            LIMIT 5
        }}
        OPTIONAL MATCH (f)-[:EXTRACTED_FROM]->(m:Message)
        RETURN e.id as entity_id, f.id as id, f.source_entity_id as source_entity_id,
            f.content as content, f.valid_at as valid_at, f.invalid_at as invalid_at, 
            f.confidence as confidence, f.embedding as embedding,
            m.id as source_msg_id, f.source as source
        ORDER BY e.id
        """
        
        try:
            async with self.driver.session() as session:
                result = await session.run(query, {"entity_ids": entity_ids})
                records = await result.data()
                
                facts_by_entity: Dict[int, List[FactRecord]] = {eid: [] for eid in entity_ids}
                
                for record in records:
                    eid = record["entity_id"]
                    fact = self._hydrate_fact(record)
                    facts_by_entity[eid].append(fact)
                
                return facts_by_entity
                
        except Exception as e:
            logger.error(f"Failed to batch fetch facts: {e}")
            return {eid: [] for eid in entity_ids}


    async def search_relevant_facts(self, entity_id: int, query_embedding: List[float], limit: int = 5) -> List[FactRecord]:
        """Search facts for a specific entity using native cosine similarity."""
        query = """
        MATCH (e:Entity {id: $entity_id})-[:HAS_FACT]->(f:Fact)
        WHERE f.invalid_at IS NULL AND f.embedding IS NOT NULL
        WITH f, vector.similarity.cosine(f.embedding, $embedding) AS sim
        ORDER BY sim DESC
        LIMIT $limit
        OPTIONAL MATCH (f)-[:EXTRACTED_FROM]->(m:Message)
        RETURN f.id as id, f.source_entity_id as source_entity_id, f.content as content, 
            f.valid_at as valid_at, f.invalid_at as invalid_at, f.confidence as confidence, 
            f.embedding as embedding, m.id as source_msg_id, f.source as source
        """
        try:
            async with self.driver.session() as session:
                result = await session.run(query, {
                    "entity_id": entity_id,
                    "embedding": query_embedding,
                    "limit": limit
                })
                records = await result.data()
                return [self._hydrate_fact(record) for record in records]
        except Exception as e:
            logger.error(f"Failed to search relevant facts for {entity_id}: {e}")
            return []


    async def get_facts_from_message(self, msg_id: int) -> List[FactRecord]:
        """Fetch all facts extracted from a message."""
        query = """
        MATCH (f:Fact)-[:EXTRACTED_FROM]->(m:Message {id: $msg_id})
        RETURN f.id as id, 
            f.source_entity_id as source_entity_id,
            f.content as content, 
            f.valid_at as valid_at,
            f.invalid_at as invalid_at, 
            f.confidence as confidence,
            f.embedding as embedding, 
            $msg_id as source_msg_id,
            f.source as source
        """
        try:
            async with self.driver.session() as session:
                result = await session.run(query, {"msg_id": msg_id})
                records = await result.data()
                return [self._hydrate_fact(record) for record in records]
        except Exception as e:
            logger.error(f"Failed to get facts from message {msg_id}: {e}")
            return []
    

