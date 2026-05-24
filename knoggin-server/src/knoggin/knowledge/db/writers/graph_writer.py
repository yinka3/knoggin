import json
import time
from typing import Dict, List

from loguru import logger

from infrastructure.db_client import DBClient


class GraphWriter:
    def __init__(self, client: DBClient, graph_name: str = "knoggin_graph"):
        self.client = client
        self.graph_name = graph_name

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
                    # Write to Graph
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
                        batch_params.append(
                            {
                                "id": msg["id"],
                                "content": msg["content"],
                                "role": msg["role"],
                                "timestamp": msg.get(
                                    "timestamp", self._current_time_ms()
                                ),
                            }
                        )

                    await cur.execute(
                        self.client.build_cypher(cypher),
                        (json.dumps({"batch": batch_params}),),
                    )

                    # Write to Hybrid Full Text Search Table
                    for msg in messages:
                        await cur.execute(
                            """
                            INSERT INTO message_search (message_id, user_name, session_id, content_tsvector)
                            VALUES (%s, %s, %s, to_tsvector('english', %s))
                            ON CONFLICT (message_id) DO UPDATE SET
                                content_tsvector = EXCLUDED.content_tsvector
                            """,
                            (
                                msg["id"],
                                msg.get("user_name", "default_user"),
                                msg.get("session_id", "default_session"),
                                msg["content"],
                            ),
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
                self.client.build_cypher(cypher, "created agtype"),
                (
                    json.dumps(
                        {
                            "child_id": child_id,
                            "parent_id": parent_id,
                            "now": self._current_time_ms(),
                        }
                    ),
                ),
            )
            # execute_write returns rowcount, which will be > 0 if the edge was created successfully.
            return res > 0
        except Exception as e:
            logger.error(
                f"Failed to create hierarchy edge ({child_id})-[:PART_OF]->({parent_id}): {e}"
            )
            return False

    async def delete_relationship(self, entity_a_id: int, entity_b_id: int) -> bool:
        cypher = """
        MATCH (a:Entity {id: $a_id})-[r:RELATED_TO]-(b:Entity {id: $b_id})
        DELETE r
        RETURN count(r)
        """
        try:
            res = await self.client.execute_write(
                self.client.build_cypher(cypher, "deleted agtype"),
                (json.dumps({"a_id": entity_a_id, "b_id": entity_b_id}),),
            )
            return res > 0
        except Exception as e:
            logger.error(
                f"Failed to delete relationship ({entity_a_id}, {entity_b_id}): {e}"
            )
            return False

    async def create_preference(
        self, id: str, content: str, kind: str, session_id: str
    ) -> bool:
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
                self.client.build_cypher(cypher, "id agtype"),
                (
                    json.dumps(
                        {
                            "id": id,
                            "content": content,
                            "kind": kind,
                            "session_id": session_id,
                            "now": self._current_time_ms(),
                        }
                    ),
                ),
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
                self.client.build_cypher(cypher, "deleted agtype"),
                (json.dumps({"id": pref_id}),),
            )
            return res > 0
        except Exception as e:
            logger.error(f"Failed to delete preference: {e}")
            return False

    async def merge_entities(self, primary_id: int, secondary_id: int) -> bool:
        if primary_id == secondary_id:
            logger.warning(f"Self-merge rejected: {primary_id}")
            return False

        if not self.client.async_pool:
            raise RuntimeError("PostgresClient async_pool is not initialized")

        try:
            async with self.client.async_pool.connection() as conn:
                async with conn.transaction():
                    async with conn.cursor() as cur:
                        # Validate both exist
                        cypher_validate = """
                        MATCH (p:Entity {id: $primary_id})
                        MATCH (s:Entity {id: $secondary_id})
                        RETURN p.canonical_name as p_name,
                            p.aliases as p_aliases,
                            p.confidence as p_conf,
                            p.last_mentioned as p_last,
                            s.canonical_name as s_name,
                            s.aliases as s_aliases,
                            s.confidence as s_conf,
                            s.last_mentioned as s_last
                        """
                        q_val = self.client.build_cypher(
                            cypher_validate,
                            "p_name agtype, p_aliases agtype, p_conf agtype, p_last agtype, s_name agtype, s_aliases agtype, s_conf agtype, s_last agtype",
                        )
                        await cur.execute(
                            q_val,
                            (
                                json.dumps(
                                    {
                                        "primary_id": primary_id,
                                        "secondary_id": secondary_id,
                                    }
                                ),
                            ),
                        )
                        check = await cur.fetchone()

                        if not check:
                            logger.error(
                                f"Merge failed: one or both entities not found ({primary_id}, {secondary_id})"
                            )
                            return False

                        p_aliases = check["p_aliases"] or []
                        s_aliases = check["s_aliases"] or []
                        s_name_raw = (
                            check["s_name"].strip('"')
                            if isinstance(check["s_name"], str)
                            else check["s_name"]
                        )

                        p_conf = float(check["p_conf"] or 0)
                        s_conf = float(check["s_conf"] or 0)
                        p_last = int(check["p_last"] or 0)
                        s_last = int(check["s_last"] or 0)

                        combined_aliases = list(
                            set(p_aliases + s_aliases + [s_name_raw])
                        )
                        new_conf = s_conf if s_conf > p_conf else p_conf
                        new_last = s_last if s_last > p_last else p_last

                        # Update primary
                        cypher_upd_p = """
                        MATCH (p:Entity {id: $primary_id})
                        SET p.aliases = $aliases,
                            p.last_updated = $now,
                            p.confidence = $conf,
                            p.last_mentioned = $last
                        """
                        await cur.execute(
                            self.client.build_cypher(cypher_upd_p),
                            (
                                json.dumps(
                                    {
                                        "primary_id": primary_id,
                                        "aliases": combined_aliases,
                                        "now": self._current_time_ms(),
                                        "conf": new_conf,
                                        "last": new_last,
                                    }
                                ),
                            ),
                        )

                        # Remove direct relationship
                        cypher_del_direct = """
                        MATCH (p:Entity {id: $primary_id})-[r:RELATED_TO]-(s:Entity {id: $secondary_id})
                        DELETE r
                        """
                        await cur.execute(
                            self.client.build_cypher(cypher_del_direct),
                            (
                                json.dumps(
                                    {
                                        "primary_id": primary_id,
                                        "secondary_id": secondary_id,
                                    }
                                ),
                            ),
                        )

                        # Array Consolidation for RELATED_TO
                        cypher_fetch_edges = """
                        MATCH (e:Entity)-[r:RELATED_TO]-(target:Entity)
                        WHERE e.id IN [$primary_id, $secondary_id]
                        RETURN e.id as source_id, target.id as target_id, r.weight as weight, r.confidence as conf, r.message_ids as msg_ids, r.last_seen as last_seen
                        """
                        q_edges = self.client.build_cypher(
                            cypher_fetch_edges,
                            "source_id agtype, target_id agtype, weight agtype, conf agtype, msg_ids agtype, last_seen agtype",
                        )
                        await cur.execute(
                            q_edges,
                            (
                                json.dumps(
                                    {
                                        "primary_id": primary_id,
                                        "secondary_id": secondary_id,
                                    }
                                ),
                            ),
                        )
                        edge_rows = await cur.fetchall()

                        merged_edges = {}
                        for row in edge_rows:
                            t_id = int(row["target_id"])
                            w = int(row["weight"] or 1)
                            c = float(row["conf"] or 0)
                            ls = int(row["last_seen"] or 0)
                            m_ids = set(row["msg_ids"] or [])

                            if t_id not in merged_edges:
                                merged_edges[t_id] = {
                                    "weight": w,
                                    "conf": c,
                                    "msg_ids": m_ids,
                                    "last_seen": ls,
                                }
                            else:
                                merged_edges[t_id]["weight"] += w
                                if c > merged_edges[t_id]["conf"]:
                                    merged_edges[t_id]["conf"] = c
                                if ls > merged_edges[t_id]["last_seen"]:
                                    merged_edges[t_id]["last_seen"] = ls
                                merged_edges[t_id]["msg_ids"].update(m_ids)

                        if merged_edges:
                            cypher_del_edges = """
                            MATCH (e:Entity)-[r:RELATED_TO]-(target:Entity)
                            WHERE e.id IN [$primary_id, $secondary_id]
                            DELETE r
                            """
                            await cur.execute(
                                self.client.build_cypher(cypher_del_edges),
                                (
                                    json.dumps(
                                        {
                                            "primary_id": primary_id,
                                            "secondary_id": secondary_id,
                                        }
                                    ),
                                ),
                            )

                            edges_batch = []
                            for t_id, props in merged_edges.items():
                                edges_batch.append(
                                    {
                                        "target_id": t_id,
                                        "weight": props["weight"],
                                        "conf": props["conf"],
                                        "msg_ids": list(props["msg_ids"]),
                                        "last_seen": props["last_seen"],
                                    }
                                )

                            cypher_write_edges = """
                            UNWIND $batch AS edge
                            MATCH (p:Entity {id: $primary_id})
                            MATCH (t:Entity {id: edge.target_id})
                            WITH p, t, edge,
                                CASE WHEN p.id < t.id THEN p ELSE t END AS node_a,
                                CASE WHEN p.id < t.id THEN t ELSE p END AS node_b
                            CREATE (node_a)-[r:RELATED_TO {
                                weight: edge.weight,
                                confidence: edge.conf,
                                message_ids: edge.msg_ids,
                                last_seen: edge.last_seen
                            }]->(node_b)
                            """
                            await cur.execute(
                                self.client.build_cypher(cypher_write_edges),
                                (
                                    json.dumps(
                                        {"primary_id": primary_id, "batch": edges_batch}
                                    ),
                                ),
                            )

                        # Transfer HAS_FACT, BELONGS_TO, PART_OF (children)
                        cypher_transfer_facts = """
                        MATCH (s:Entity {id: $secondary_id})-[r:HAS_FACT]->(f:Fact)
                        MATCH (p:Entity {id: $primary_id})
                        MERGE (p)-[:HAS_FACT]->(f)
                        SET f.source_entity_id = $primary_id
                        DELETE r
                        """
                        await cur.execute(
                            self.client.build_cypher(cypher_transfer_facts),
                            (
                                json.dumps(
                                    {
                                        "primary_id": primary_id,
                                        "secondary_id": secondary_id,
                                    }
                                ),
                            ),
                        )

                        cypher_transfer_topics = """
                        MATCH (s:Entity {id: $secondary_id})-[r:BELONGS_TO]->(t:Topic)
                        MATCH (p:Entity {id: $primary_id})
                        MERGE (p)-[:BELONGS_TO]->(t)
                        DELETE r
                        """
                        await cur.execute(
                            self.client.build_cypher(cypher_transfer_topics),
                            (
                                json.dumps(
                                    {
                                        "primary_id": primary_id,
                                        "secondary_id": secondary_id,
                                    }
                                ),
                            ),
                        )

                        cypher_transfer_children = """
                        MATCH (child:Entity)-[r:PART_OF]->(s:Entity {id: $secondary_id})
                        MATCH (p:Entity {id: $primary_id})
                        MERGE (child)-[:PART_OF]->(p)
                        DELETE r
                        """
                        await cur.execute(
                            self.client.build_cypher(cypher_transfer_children),
                            (
                                json.dumps(
                                    {
                                        "primary_id": primary_id,
                                        "secondary_id": secondary_id,
                                    }
                                ),
                            ),
                        )

                        # Parent transfer with conflict detection
                        cypher_parents = """
                        MATCH (s:Entity {id: $secondary_id})-[r:PART_OF]->(s_parent:Entity)
                        MATCH (p:Entity {id: $primary_id})
                        OPTIONAL MATCH (p)-[:PART_OF]->(p_parent:Entity)
                        RETURN s_parent.id AS s_parent_id, p_parent.id AS p_parent_id
                        """
                        await cur.execute(
                            self.client.build_cypher(
                                cypher_parents, "s_parent_id agtype, p_parent_id agtype"
                            ),
                            (
                                json.dumps(
                                    {
                                        "primary_id": primary_id,
                                        "secondary_id": secondary_id,
                                    }
                                ),
                            ),
                        )
                        record_4c = await cur.fetchone()

                        if record_4c and record_4c["s_parent_id"] is not None:
                            if record_4c["p_parent_id"] is not None:
                                logger.warning(
                                    f"Hierarchy conflict during merge: primary {primary_id} and secondary {secondary_id} both have parents. Dropping secondary's parent edge."
                                )
                                cypher_del_s_parent = "MATCH (s:Entity {id: $secondary_id})-[r:PART_OF]->() DELETE r"
                                await cur.execute(
                                    self.client.build_cypher(cypher_del_s_parent),
                                    (json.dumps({"secondary_id": secondary_id}),),
                                )
                            else:
                                cypher_trans_parent = """
                                MATCH (s:Entity {id: $secondary_id})-[r:PART_OF]->(parent:Entity)
                                MATCH (p:Entity {id: $primary_id})
                                MERGE (p)-[:PART_OF]->(parent)
                                DELETE r
                                """
                                await cur.execute(
                                    self.client.build_cypher(cypher_trans_parent),
                                    (
                                        json.dumps(
                                            {
                                                "primary_id": primary_id,
                                                "secondary_id": secondary_id,
                                            }
                                        ),
                                    ),
                                )

                        # Delete Secondary from Graph
                        cypher_del_s = (
                            "MATCH (s:Entity {id: $secondary_id}) DETACH DELETE s"
                        )
                        await cur.execute(
                            self.client.build_cypher(cypher_del_s),
                            (json.dumps({"secondary_id": secondary_id}),),
                        )

                        # Dual-Write Cleanup
                        await cur.execute(
                            "DELETE FROM entity_search WHERE entity_id = %s",
                            (secondary_id,),
                        )
                        await cur.execute(
                            "UPDATE fact_search SET entity_id = %s WHERE entity_id = %s",
                            (primary_id, secondary_id),
                        )

                        logger.info(f"Merged entity {secondary_id} into {primary_id}")
                        return True
        except Exception as e:
            logger.error(f"Merge transaction failed: {e}")
            return False
