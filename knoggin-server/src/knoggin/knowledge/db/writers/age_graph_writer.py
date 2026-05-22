import json
import time
from typing import Dict, List

from loguru import logger

from infrastructure.postgres_client import PostgresClient


class AgeGraphWriter:
    def __init__(self, client: PostgresClient, graph_name: str = "knoggin_graph"):
        self.client = client
        self.graph_name = graph_name

    def _build_cypher(self, cypher_query: str) -> str:
        return f"SELECT * FROM cypher('{self.graph_name}', $${cypher_query}$$, %s) AS (result agtype)"

    def _current_time_ms(self) -> int:
        return int(time.time() * 1000)

    async def save_message_logs(self, messages: List[Dict]) -> bool:
        if not messages:
            return True

        if not self.client.async_pool:
            raise RuntimeError("PostgresClient async_pool is not initialized")
            
        async with self.client.async_pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    # 1. Write to Graph
                    # Notice we skip saving embedding to graph node to save space
                    cypher = """
                    UNWIND $batch AS msg
                    MERGE (m:Message {id: msg.id})
                    SET m.content = msg.content,
                        m.role = msg.role,
                        m.timestamp = msg.timestamp
                    RETURN count(m)
                    """
                    
                    batch_params = []
                    for msg in messages:
                        batch_params.append({
                            "id": msg["id"],
                            "content": msg["content"],
                            "role": msg["role"],
                            "timestamp": msg.get("timestamp", self._current_time_ms())
                        })
                        
                    await cur.execute(self._build_cypher(cypher), (json.dumps({"batch": batch_params}),))
                    
                    # 2. Write to Hybrid Full Text Search Table
                    # We will use to_tsvector('english', content) inside the INSERT
                    for msg in messages:
                        # Assuming project/session is accessible or default for now
                        await cur.execute(
                            """
                            INSERT INTO message_search (message_id, user_name, session_id, content_tsvector)
                            VALUES (%s, %s, %s, to_tsvector('english', %s))
                            ON CONFLICT (message_id) DO UPDATE SET
                                content_tsvector = EXCLUDED.content_tsvector
                            """,
                            (msg["id"], msg.get("user_name", "default_user"), msg.get("session_id", "default_session"), msg["content"])
                        )

        logger.info(f"Saved {len(messages)} message logs to Postgres/AGE.")
        return True

    async def create_hierarchy_edge(self, parent_id: int, child_id: int) -> bool:
        cypher = """
        MATCH (child:Entity {id: $child_id})
        MATCH (parent:Entity {id: $parent_id})
        WHERE NOT (child)-[:PART_OF]->(parent)
        CREATE (child)-[:PART_OF {created_at: $now}]->(parent)
        RETURN true as created
        """
        
        try:
            res = await self.client.execute_write(
                f"SELECT * FROM cypher('{self.graph_name}', $${cypher}$$, %s) AS (created agtype)", 
                (json.dumps({"child_id": child_id, "parent_id": parent_id, "now": self._current_time_ms()}),)
            )
            # execute_write returns rowcount in our PostgresClient wrapper
            # Wait, execute_write returns rowcount. If we want the actual record, 
            # we should use execute_read since we are returning a value from cypher
            # and want to inspect it. execute_read handles simple writes fine if autocommit=True, 
            # but we use execute_write to guarantee transaction. Let's stick to execute_write and just rely on rowcount.
            # Actually, rowcount of SELECT from cypher will be 1 if it returned `true as created`.
            return res > 0
        except Exception as e:
            logger.error(f"Failed to create hierarchy edge ({child_id})-[:PART_OF]->({parent_id}): {e}")
            return False

    async def delete_relationship(self, entity_a_id: int, entity_b_id: int) -> bool:
        cypher = """
        MATCH (a:Entity {id: $a_id})-[r:RELATED_TO]-(b:Entity {id: $b_id})
        DELETE r
        RETURN count(r)
        """
        try:
            res = await self.client.execute_write(
                f"SELECT * FROM cypher('{self.graph_name}', $${cypher}$$, %s) AS (deleted agtype)",
                (json.dumps({"a_id": entity_a_id, "b_id": entity_b_id}),)
            )
            return res > 0
        except Exception as e:
            logger.error(f"Failed to delete relationship ({entity_a_id}, {entity_b_id}): {e}")
            return False

    async def create_preference(self, id: str, content: str, kind: str, session_id: str) -> bool:
        cypher = """
        CREATE (p:Preference {
            id: $id,
            content: $content,
            kind: $kind,
            session_id: $session_id,
            created_at: $now
        })
        RETURN p.id
        """
        try:
            res = await self.client.execute_write(
                f"SELECT * FROM cypher('{self.graph_name}', $${cypher}$$, %s) AS (id agtype)",
                (json.dumps({
                    "id": id, "content": content, "kind": kind, 
                    "session_id": session_id, "now": self._current_time_ms()
                }),)
            )
            return res > 0
        except Exception as e:
            logger.error(f"Failed to create preference: {e}")
            return False

    async def delete_preference(self, pref_id: str) -> bool:
        cypher = """
        MATCH (p:Preference {id: $id})
        DELETE p
        RETURN count(p)
        """
        try:
            res = await self.client.execute_write(
                f"SELECT * FROM cypher('{self.graph_name}', $${cypher}$$, %s) AS (deleted agtype)",
                (json.dumps({"id": pref_id}),)
            )
            return res > 0
        except Exception as e:
            logger.error(f"Failed to delete preference: {e}")
            return False
            
    async def merge_entities(self, primary_id: int, secondary_id: int) -> bool:
        # Phase 5 Implementation Placeholder
        logger.error("merge_entities is slated for Phase 5 implementation.")
        return False
