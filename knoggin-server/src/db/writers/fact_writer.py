from datetime import datetime
from loguru import logger
from typing import Dict, List
from neo4j import AsyncDriver, AsyncManagedTransaction
from common.schema.dtypes import FactRecord

class FactWriter:
    def __init__(self, driver: AsyncDriver):
        self.driver = driver

    async def create_facts_batch(self, entity_id: int, facts: List[FactRecord]) -> int:
        """
        Atomically create multiple facts for an entity.
        Returns number of facts created.
        Raises Exception if ANY fact fails (All-or-Nothing).
        """
        if not facts:
            return 0

        fact_params = []
        for f in facts:
            fact_params.append({
                "id": f.id,
                "content": f.content,
                "valid_at": f.valid_at.isoformat(),
                "invalid_at": f.invalid_at.isoformat() if f.invalid_at else None,
                "confidence": f.confidence,
                "embedding": f.embedding,
                "source_msg_id": f.source_msg_id,
                "source": f.source
            })
        
        async def _execute_batch(tx: AsyncManagedTransaction):
            query = """
            MATCH (e:Entity {id: $entity_id})
            
            UNWIND $batch AS item
            
            CREATE (f:Fact {
                id: item.id,
                source_entity_id: $entity_id,
                content: item.content,
                valid_at: item.valid_at,
                invalid_at: item.invalid_at,
                confidence: item.confidence,
                created_at: timestamp(),
                embedding: item.embedding,
                source: item.source
            })
            CREATE (e)-[:HAS_FACT]->(f)
            
            WITH f, item
            FOREACH (_ IN CASE WHEN item.source_msg_id IS NOT NULL THEN [1] ELSE [] END |
                MERGE (m:Message {id: item.source_msg_id})
                MERGE (f)-[:EXTRACTED_FROM]->(m)
            )
            
            RETURN count(f) as created_count
            """
            
            result = await tx.run(query, {
                "entity_id": entity_id,
                "batch": fact_params
            })
            record = await result.single()
            
            count = record["created_count"] if record else 0
            if count == 0:
                raise Exception(f"Failed to create facts for entity {entity_id} (parent may not exist)")
            return count

        try:
            async with self.driver.session() as session:
                return await session.execute_write(_execute_batch)
        except Exception as e:
            logger.error(f"Batch write failed for entity {entity_id}: {e}")
            raise e
    

    async def invalidate_fact(self, fact_id: str, invalid_at: datetime) -> bool:
        """Mark fact as invalid."""
        query = """
        MATCH (f:Fact {id: $fact_id})
        SET f.invalid_at = $invalid_at
        RETURN f.id as id
        """

        async def _update(tx: AsyncManagedTransaction):
            result = await tx.run(query, {
                "fact_id": fact_id,
                "invalid_at": invalid_at.isoformat()
            })
            record = await result.single()
            return record is not None

        async with self.driver.session() as session:
            return await session.execute_write(_update)

    

    async def delete_old_invalidated_facts(self, cutoff: datetime) -> int:
        """Delete Fact nodes invalidated before cutoff date."""
        query = """
        MATCH (f:Fact)
        WHERE f.invalid_at IS NOT NULL 
        AND f.invalid_at < $cutoff
        DETACH DELETE f
        RETURN count(*) as deleted
        """
        async def _delete(tx: AsyncManagedTransaction):
            result = await tx.run(query, {"cutoff": cutoff.isoformat()})
            record = await result.single()
            return record["deleted"] if record else 0
            
        try:
            async with self.driver.session() as session:
                deleted = await session.execute_write(_delete)
                if deleted > 0:
                    logger.info(f"Deleted {deleted} old invalidated facts")
                return deleted
        except Exception as e:
            logger.error(f"Failed to delete old facts: {e}")
            raise
    

