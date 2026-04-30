from datetime import datetime
from loguru import logger
from typing import Dict, List
from neo4j import AsyncDriver, AsyncManagedTransaction


class EntityWriter:
    def __init__(self, driver: AsyncDriver):
        self.driver = driver

    async def write_batch(self, entities: List[Dict], relationships: List[Dict]):
        entity_params = []
        for e in entities:
            e_clean = e.copy()
            e_clean["aliases"] = e.get("aliases") or []
            entity_params.append(e_clean)

        relationship_params = []
        for r in relationships:
            r_clean = r.copy()
            r_clean["confidence"] = r.get("confidence", 1.0)
            relationship_params.append(r_clean)

        async def _write(tx: AsyncManagedTransaction):
            if entity_params:
                await tx.run("""
                    UNWIND $batch AS data
                    MERGE (e:Entity {id: data.id})
                    ON CREATE SET
                        e.session_id = data.session_id,
                        e.canonical_name = data.canonical_name,
                        e.aliases = data.aliases,
                        e.type = data.type,
                        e.confidence = data.confidence,
                        e.last_updated = timestamp(),
                        e.last_mentioned = timestamp(),
                        e.embedding = data.embedding
                    ON MATCH SET 
                        e.canonical_name = data.canonical_name,
                        e.confidence = data.confidence,
                        e.last_updated = timestamp(),
                        e.last_mentioned = timestamp(),
                        e.embedding = case when data.embedding IS NOT NULL then data.embedding else e.embedding end

                    WITH e, data
                    UNWIND coalesce(e.aliases, []) + data.aliases AS alias
                    WITH e, data, collect(DISTINCT alias) AS unique_aliases
                    SET e.aliases = unique_aliases

                    WITH e, data
                    FOREACH (_ IN CASE WHEN data.topic IS NOT NULL AND data.topic <> "" THEN [1] ELSE [] END |
                        MERGE (t:Topic {name: data.topic})
                        MERGE (e)-[:BELONGS_TO]->(t)
                    )
                """, batch=entity_params)

            if relationship_params:
                result = await tx.run("""
                    UNWIND $batch AS rel
                    MATCH (a:Entity {id: rel.entity_a_id})
                    MATCH (b:Entity {id: rel.entity_b_id})
                    WITH a, b, rel,
                        CASE WHEN a.id < b.id THEN a ELSE b END AS node_a,
                        CASE WHEN a.id < b.id THEN b ELSE a END AS node_b
                    MERGE (node_a)-[r:RELATED_TO]->(node_b)
                    
                    ON CREATE SET 
                        r.weight = 1, 
                        r.confidence = rel.confidence,
                        r.last_seen = timestamp(), 
                        r.message_ids = [rel.message_id],
                        r.context = rel.context
                        
                    ON MATCH SET 
                        r.weight = CASE 
                            WHEN rel.message_id IN coalesce(r.message_ids, []) 
                            THEN r.weight 
                            ELSE r.weight + 1 
                        END,
                        r.confidence = CASE WHEN rel.confidence > r.confidence THEN rel.confidence ELSE r.confidence END,
                        r.last_seen = timestamp(),
                        r.context = CASE WHEN rel.context IS NOT NULL THEN rel.context ELSE r.context END
                    
                    WITH r, rel
                    UNWIND coalesce(r.message_ids, []) + [rel.message_id] AS mid
                    WITH r, collect(DISTINCT mid) AS unique_ids
                    SET r.message_ids = unique_ids
                    RETURN count(DISTINCT r) AS created_count
                """, batch=relationship_params)
                record = await result.single()
                created = record["created_count"] if record else 0
                if created < len(relationship_params):
                    logger.warning(f"write_batch created/updated {created} relationship edges for {len(relationship_params)} inputs. Some entities might be missing.")

        async with self.driver.session() as session:
            await session.execute_write(_write)
        
        return True
    

    async def update_entity_profile(
        self, 
        entity_id: int, 
        canonical_name: str,
        embedding: List[float], 
        last_msg_id: int
    ):
        """
        Update entity metadata and embedding.
        """
        async def _update(tx: AsyncManagedTransaction):
            await tx.run("""
                MATCH (e:Entity {id: $id})
                SET e.canonical_name = $canonical_name,
                    e.embedding = $embedding,
                    e.last_updated = timestamp(),
                    e.last_profiled_msg_id = $last_msg_id
            """, 
            id=entity_id, 
            canonical_name=canonical_name, 
            embedding=embedding,
            last_msg_id=last_msg_id
            )
        
        async with self.driver.session() as session:
            await session.execute_write(_update)
        logger.info(f"Updated entity {entity_id} (checkpoint: msg_{last_msg_id})")
    

    async def update_entity_canonical_name(self, entity_id: int, canonical_name: str) -> None:
        """Update only the canonical name. Does not touch embedding or checkpoint."""
        query = """
        MATCH (e:Entity {id: $id})
        SET e.canonical_name = $canonical_name,
            e.last_updated = timestamp()
        """
        async def _update(tx: AsyncManagedTransaction):
            result = await tx.run(query, {"id": entity_id, "canonical_name": canonical_name})
            await result.consume()

        async with self.driver.session() as session:
            await session.execute_write(_update)
    

    async def update_entity_embedding(self, entity_id: int, embedding: List[float]) -> None:
        """
        Persists a new embedding for an entity.
        """
        query = """
        MATCH (e:Entity {id: $id})
        SET e.embedding = $embedding,
            e.last_updated = timestamp()
        """
        async def _update(tx: AsyncManagedTransaction):
            result = await tx.run(query, {"id": entity_id, "embedding": embedding})
            await result.consume()
            
        async with self.driver.session() as session:
            await session.execute_write(_update)


    async def update_entity_checkpoint(self, entity_id: int, last_msg_id: int) -> None:
        """
        Update ONLY the entity's profiled message checkpoint.
        """
        query = """
        MATCH (e:Entity {id: $id})
        SET e.last_profiled_msg_id = $last_msg_id
        """
        async def _update(tx: AsyncManagedTransaction):
            result = await tx.run(query, {"id": entity_id, "last_msg_id": last_msg_id})
            await result.consume()
            
        async with self.driver.session() as session:
            await session.execute_write(_update)


    async def update_entity_aliases(self, alias_updates: Dict[int, List[str]]):
        """Append new aliases to existing entities."""
        if not alias_updates:
            return
        
        params = [{"id": eid, "new_aliases": aliases} for eid, aliases in alias_updates.items()]
        
        async def _update(tx: AsyncManagedTransaction):
            await tx.run("""
                UNWIND $batch AS data
                MATCH (e:Entity {id: data.id})
                WITH e, data, coalesce(e.aliases, []) AS existing
                UNWIND existing + data.new_aliases AS alias
                WITH e, collect(DISTINCT alias) AS all_aliases
                SET e.aliases = all_aliases, e.last_updated = timestamp()
            """, batch=params)
        
        async with self.driver.session() as session:
            await session.execute_write(_update)
        
        logger.debug(f"Updated aliases for {len(alias_updates)} entities")
    

    async def cleanup_null_entities(self) -> int:
        """Remove entities with null type and their relationships."""
        query = """
        MATCH (e:Entity)
        WHERE e.type IS NULL
        DETACH DELETE e
        RETURN count(e) as deleted
        """
        async def _cleanup(tx: AsyncManagedTransaction):
            result = await tx.run(query)
            record = await result.single()
            return record["deleted"] if record else 0
            
        async with self.driver.session() as session:
            deleted = await session.execute_write(_cleanup)
            if deleted > 0:
                logger.info(f"Cleaned up {deleted} null-type entities")
            return deleted
        
    

    async def delete_entity(self, entity_id: int) -> bool:
        """Delete a single entity, its facts, and all relationships."""
        query = """
        MATCH (e:Entity {id: $id})
        OPTIONAL MATCH (e)-[:HAS_FACT]->(f:Fact)
        DETACH DELETE e, f
        RETURN count(e) as deleted
        """
        async def _delete(tx: AsyncManagedTransaction):
            result = await tx.run(query, {"id": entity_id})
            record = await result.single()
            return record["deleted"] if record else 0
            
        try:
            async with self.driver.session() as session:
                deleted = await session.execute_write(_delete)
                if deleted > 0:
                    logger.info(f"Deleted entity {entity_id} with facts")
                return deleted > 0
        except Exception as e:
            logger.error(f"Failed to delete entity {entity_id}: {e}")
            return False


    async def bulk_delete_entities(self, entity_ids: List[int]) -> int:
        """DETACH DELETE entities by ID list. Returns count deleted."""
        if not entity_ids:
            return 0
        query = """
        MATCH (e:Entity)
        WHERE e.id IN $ids
        OPTIONAL MATCH (e)-[:HAS_FACT]->(f:Fact)
        DETACH DELETE e, f
        RETURN count(DISTINCT e) as deleted
        """
        async def _delete(tx: AsyncManagedTransaction):
            result = await tx.run(query, {"ids": entity_ids})
            record = await result.single()
            return record["deleted"] if record else 0
            
        async with self.driver.session() as session:
            deleted = await session.execute_write(_delete)
            if deleted > 0:
                logger.info(f"Bulk deleted {deleted} orphan entities")
            return deleted
    

