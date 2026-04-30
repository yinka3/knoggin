from datetime import datetime
from loguru import logger
from typing import Dict, List
from neo4j import AsyncDriver, AsyncManagedTransaction


class GraphWriter:
    def __init__(self, driver: AsyncDriver):
        self.driver = driver

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
                MERGE (p)-[:HAS_FACT]->(f)
                SET f.source_entity_id = $primary_id
                DELETE r
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
    

