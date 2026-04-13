from datetime import datetime
from loguru import logger
from typing import Dict, List
from neo4j import AsyncDriver, AsyncManagedTransaction

from common.schema.dtypes import Fact


class GraphWriter:
    def __init__(self, driver: AsyncDriver):
        self.driver = driver


    async def create_facts_batch(self, entity_id: int, facts: List[Fact]) -> int:
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
            return 0
    
    async def save_message_logs(self, messages: List[Dict]) -> bool:
        """
        Persist message texts to the graph.
        """
        if not messages:
            return True
        
        query = """
        UNWIND $batch AS msg
        MERGE (m:Message {id: msg.id})
        SET m.content = msg.content,
            m.role = msg.role,
            m.timestamp = msg.timestamp,
            m.embedding = msg.embedding
        """
        
        async def _save(tx: AsyncManagedTransaction):
            result = await tx.run(query, {"batch": messages})
            await result.consume()
            
        try:
            async with self.driver.session() as session:
                await session.execute_write(_save)
                logger.info(f"Saved {len(messages)} message logs to Memgraph.")
                return True
        except Exception as e:
            logger.error(f"Failed to save message logs: {e}")
            return False
        

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
                        e.last_mentioned = timestamp()

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
                await tx.run("""
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
                """, batch=relationship_params)

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
    
    async def merge_entities(self, primary_id: int, secondary_id: int) -> bool:
        """
        Merge secondary entity into primary (single transaction).
        Transfers RELATED_TO and HAS_FACT edges, then deletes secondary.
        """
        if primary_id == secondary_id:
            logger.warning(f"Self-merge rejected: {primary_id}")
            return False
        
        async def _execute_merge(tx: AsyncManagedTransaction):
            # Step 1: Validate both exist
            result = await tx.run("""
                MATCH (p:Entity {id: $primary_id})
                MATCH (s:Entity {id: $secondary_id})
                RETURN p.canonical_name as p_name, 
                    p.aliases as p_aliases,
                    s.canonical_name as s_name, 
                    s.aliases as s_aliases,
                    s.confidence as s_conf,
                    s.last_mentioned as s_last
            """, primary_id=primary_id, secondary_id=secondary_id)
            check = await result.single()
            
            if not check:
                logger.error(f"Merge failed: one or both entities not found ({primary_id}, {secondary_id})")
                return False
            
            # Step 2: Update primary with merged aliases
            combined_aliases = list(set(
                (check["p_aliases"] or []) + 
                (check["s_aliases"] or []) + 
                [check["s_name"]]
            ))
            
            await tx.run("""
                MATCH (p:Entity {id: $primary_id})
                MATCH (s:Entity {id: $secondary_id})
                SET p.aliases = $aliases,
                    p.last_updated = timestamp(),
                    p.confidence = CASE 
                        WHEN coalesce(s.confidence, 0) > coalesce(p.confidence, 0) 
                        THEN s.confidence ELSE p.confidence END,
                    p.last_mentioned = CASE 
                        WHEN coalesce(s.last_mentioned, 0) > coalesce(p.last_mentioned, 0) 
                        THEN s.last_mentioned ELSE p.last_mentioned END
            """, primary_id=primary_id, secondary_id=secondary_id, aliases=combined_aliases)
            
            # Step 2.5: Remove direct relationship between merge targets
            await tx.run("""
                MATCH (p:Entity {id: $primary_id})-[r:RELATED_TO]-(s:Entity {id: $secondary_id})
                DELETE r
            """, primary_id=primary_id, secondary_id=secondary_id)
            
            # Step 3: Transfer RELATED_TO edges + delete old edges
            await tx.run("""
                MATCH (s:Entity {id: $secondary_id})-[r_old:RELATED_TO]-(target:Entity)
                WHERE target.id <> $primary_id
                MATCH (p:Entity {id: $primary_id})
                WITH p, target, r_old,
                    CASE WHEN p.id < target.id THEN p ELSE target END AS node_a,
                    CASE WHEN p.id < target.id THEN target ELSE p END AS node_b
                MERGE (node_a)-[r_new:RELATED_TO]->(node_b)
                ON CREATE SET
                    r_new.weight = r_old.weight,
                    r_new.confidence = r_old.confidence,
                    r_new.message_ids = r_old.message_ids,
                    r_new.last_seen = r_old.last_seen
                ON MATCH SET
                    r_new.weight = r_new.weight + r_old.weight,
                    r_new.confidence = CASE 
                        WHEN r_old.confidence > r_new.confidence 
                        THEN r_old.confidence ELSE r_new.confidence END,
                    r_new.last_seen = CASE 
                        WHEN r_old.last_seen > r_new.last_seen 
                        THEN r_old.last_seen ELSE r_new.last_seen END
                WITH r_new, r_old
                UNWIND coalesce(r_new.message_ids, []) + coalesce(r_old.message_ids, []) AS mid
                WITH r_new, r_old, collect(DISTINCT mid) AS unique_ids
                SET r_new.message_ids = unique_ids
                DELETE r_old
            """, primary_id=primary_id, secondary_id=secondary_id)
            
            # Step 4: Transfer HAS_FACT edges + fix source_entity_id
            await tx.run("""
                MATCH (s:Entity {id: $secondary_id})-[r:HAS_FACT]->(f:Fact)
                MATCH (p:Entity {id: $primary_id})
                DELETE r
                CREATE (p)-[:HAS_FACT]->(f)
                SET f.source_entity_id = $primary_id
            """, primary_id=primary_id, secondary_id=secondary_id)

            # Step 4a: Transfer Topic memberships (BELONGS_TO)
            await tx.run("""
                MATCH (s:Entity {id: $secondary_id})-[r:BELONGS_TO]->(t:Topic)
                MATCH (p:Entity {id: $primary_id})
                MERGE (p)-[:BELONGS_TO]->(t)
                DELETE r
            """, primary_id=primary_id, secondary_id=secondary_id)

            # Step 4b: Transfer Hierarchy Children (PART_OF)
            await tx.run("""
                MATCH (child:Entity)-[r:PART_OF]->(s:Entity {id: $secondary_id})
                MATCH (p:Entity {id: $primary_id})
                MERGE (child)-[:PART_OF]->(p)
                DELETE r
            """, primary_id=primary_id, secondary_id=secondary_id)

            # Step 4c: Transfer Hierarchy Parent (with conflict detection)
            result_4c = await tx.run("""
                MATCH (s:Entity {id: $secondary_id})-[r:PART_OF]->(s_parent:Entity)
                MATCH (p:Entity {id: $primary_id})
                OPTIONAL MATCH (p)-[:PART_OF]->(p_parent:Entity)
                RETURN s_parent.id AS s_parent_id, 
                    s_parent.canonical_name AS s_parent_name,
                    p_parent.id AS p_parent_id,
                    p_parent.canonical_name AS p_parent_name,
                    r AS rel_to_delete
            """, primary_id=primary_id, secondary_id=secondary_id)
            record_4c = await result_4c.single()

            if record_4c and record_4c["s_parent_id"] is not None:
                if record_4c["p_parent_id"] is not None:
                    logger.warning(
                        f"Hierarchy conflict during merge: "
                        f"primary {primary_id} parent='{record_4c['p_parent_name']}', "
                        f"secondary {secondary_id} parent='{record_4c['s_parent_name']}'. "
                        f"Dropping secondary's parent edge."
                    )
                    await tx.run("""
                        MATCH (s:Entity {id: $secondary_id})-[r:PART_OF]->()
                        DELETE r
                    """, secondary_id=secondary_id)
                else:
                    await tx.run("""
                        MATCH (s:Entity {id: $secondary_id})-[r:PART_OF]->(parent:Entity)
                        MATCH (p:Entity {id: $primary_id})
                        MERGE (p)-[:PART_OF]->(parent)
                        DELETE r
                    """, primary_id=primary_id, secondary_id=secondary_id)
            
            # Step 5: Delete secondary entity
            del_result = await tx.run("""
                MATCH (s:Entity {id: $secondary_id})
                DETACH DELETE s
                RETURN count(*) as deleted
            """, secondary_id=secondary_id)
            rec = await del_result.single()
            
            return rec and rec["deleted"] > 0
 
        try:
            async with self.driver.session() as session:
                success = await session.execute_write(_execute_merge)
                if success:
                    logger.info(f"Merged entity {secondary_id} into {primary_id}")
                return success
        except Exception as e:
            logger.error(f"Merge transaction failed: {e}")
            return False
    
    async def create_hierarchy_edge(self, parent_id: int, child_id: int) -> bool:
        """
        Create PART_OF relationship: (child)-[:PART_OF]->(parent)
        
        Returns True if created, False if already exists or failed.
        """
        query = """
        MATCH (child:Entity {id: $child_id})
        MATCH (parent:Entity {id: $parent_id})
        WHERE NOT (child)-[:PART_OF]->(parent)
        CREATE (child)-[:PART_OF {created_at: timestamp()}]->(parent)
        RETURN true as created
        """
        
        async def _create(tx: AsyncManagedTransaction):
            result = await tx.run(query, {
                "child_id": child_id,
                "parent_id": parent_id
            })
            records = await result.data()
            return len(records) > 0

        try:
            async with self.driver.session() as session:
                return await session.execute_write(_create)
            
        except Exception as e:
            logger.error(f"Failed to create hierarchy edge ({child_id})-[:PART_OF]->({parent_id}): {e}")
            return False
    

    async def create_preference(
        self,
        id: str,
        content: str,
        kind: str,  # "preference" or "ick"
        session_id: str
    ) -> bool:
        query = """
        CREATE (p:Preference {
            id: $id,
            content: $content,
            kind: $kind,
            session_id: $session_id,
            created_at: timestamp()
        })
        RETURN p.id AS id
        """
        async def _create(tx: AsyncManagedTransaction):
            result = await tx.run(query, {
                "id": id,
                "content": content,
                "kind": kind,
                "session_id": session_id
            })
            record = await result.single()
            return record is not None

        try:
            async with self.driver.session() as session:
                return await session.execute_write(_create)
        except Exception as e:
            logger.error(f"Failed to create preference: {e}")
            return False


    async def delete_preference(self, pref_id: str) -> bool:
        query = """
        MATCH (p:Preference {id: $id})
        DELETE p
        RETURN count(*) AS deleted
        """
        async def _delete(tx: AsyncManagedTransaction):
            result = await tx.run(query, {"id": pref_id})
            record = await result.single()
            return record and record["deleted"] > 0
            
        try:
            async with self.driver.session() as session:
                return await session.execute_write(_delete)
        except Exception as e:
            logger.error(f"Failed to delete preference: {e}")
            return False
    
    async def delete_relationship(self, entity_a_id: int, entity_b_id: int) -> bool:
        """Delete RELATED_TO edge between two entities."""
        query = """
        MATCH (a:Entity {id: $a_id})-[r:RELATED_TO]-(b:Entity {id: $b_id})
        DELETE r
        RETURN count(r) as deleted
        """
        async def _delete(tx: AsyncManagedTransaction):
            result = await tx.run(query, {"a_id": entity_a_id, "b_id": entity_b_id})
            record = await result.single()
            return record and record["deleted"] > 0
            
        try:
            async with self.driver.session() as session:
                return await session.execute_write(_delete)
        except Exception as e:
            logger.error(f"Failed to delete relationship ({entity_a_id}, {entity_b_id}): {e}")
            return False