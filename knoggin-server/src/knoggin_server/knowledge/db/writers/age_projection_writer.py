import json
from typing import Dict, List, Optional

from common.scoping import IDENTITY_ENTITY_ID
from common.utils.time_utils import get_now_ms
from infrastructure.postgres_client import PostgresClient


class AgeProjectionWriter:
    """Writes derived traversal state into Apache AGE."""

    def __init__(self, client: PostgresClient, graph_name: str = "knoggin_graph"):
        self.client = client
        self.graph_name = graph_name

    def _current_time_ms(self) -> int:
        return get_now_ms()

    def _build_cypher(
        self,
        cypher_query: str,
        return_types: str = "result agtype",
    ) -> str:
        return self.client.build_cypher(
            cypher_query,
            return_types,
            graph_name=self.graph_name,
        )

    async def project_messages(self, cur, messages: List[Dict]) -> None:
        cypher = """
        UNWIND $batch AS msg
        MERGE (m:Message {
            user_name: msg.user_name,
            session_id: msg.session_id,
            id: msg.id
        })
        SET m.content = msg.content,
            m.role = msg.role,
            m.timestamp = msg.timestamp,
            m.project_id = msg.project_id
        RETURN count(m)
        """
        await cur.execute(
            self._build_cypher(cypher),
            (json.dumps({"batch": messages}),),
        )

    async def project_entities(self, cur, entities: List[Dict]) -> None:
        cypher = """
        UNWIND $batch AS data
        MERGE (e:Entity {id: data.id})
        SET e.user_name = data.user_name,
            e.session_id = data.session_id,
            e.project_id = data.project_id,
            e.canonical_name = data.canonical_name,
            e.type = coalesce(e.type, data.type),
            e.confidence = data.confidence,
            e.last_updated = coalesce(data.last_updated, data.now),
            e.last_mentioned = coalesce(data.last_mentioned, data.now),
            e.last_profiled_msg_id = data.last_profiled_msg_id

        WITH e, data,
            coalesce(e.aliases, []) + coalesce(data.aliases, []) AS all_aliases
        WITH e,
            CASE WHEN size(all_aliases) = 0
                 THEN [null]
                 ELSE all_aliases
            END AS safe_aliases
        UNWIND safe_aliases AS alias
        WITH e, collect(DISTINCT alias) AS merged_aliases
        WITH e, [x IN merged_aliases WHERE x IS NOT NULL] AS final_aliases
        SET e.aliases = final_aliases

        RETURN e.id
        """
        await cur.execute(
            self._build_cypher(cypher),
            (json.dumps({"batch": entities}),),
        )

    async def clear_project_projection(self, cur, project_id: str) -> None:
        """Clear project-scoped AGE projection before rebuilding it from SQL."""
        cypher = """
        MATCH (n)
        WHERE (
            n:Entity OR n:Message OR n:Fact
        )
          AND n.project_id = $project_id
          AND coalesce(n.id, -1) <> $identity_entity_id
        DETACH DELETE n
        RETURN count(n)
        """
        await cur.execute(
            self._build_cypher(cypher),
            (
                json.dumps(
                    {
                        "project_id": project_id,
                        "identity_entity_id": IDENTITY_ENTITY_ID,
                    }
                ),
            ),
        )

        orphan_topic_cypher = """
        MATCH (t:Topic)
        WHERE NOT ()-[:BELONGS_TO]->(t)
        DELETE t
        RETURN count(t)
        """
        await cur.execute(
            self._build_cypher(orphan_topic_cypher),
            (json.dumps({}),),
        )

    async def project_entity_topics(self, cur, topics: List[Dict]) -> None:
        if not topics:
            return

        cypher = """
        UNWIND $batch AS data
        MATCH (e:Entity {id: data.id})
        MERGE (t:Topic {name: data.topic})
        MERGE (e)-[:BELONGS_TO]->(t)
        RETURN count(e)
        """
        await cur.execute(
            self._build_cypher(cypher),
            (json.dumps({"batch": topics}),),
        )

    async def project_relationships(self, cur, relationships: List[Dict]) -> None:
        if not relationships:
            return

        cypher = """
        UNWIND $batch AS rel
        MATCH (a:Entity {id: rel.entity_a_id})
        MATCH (b:Entity {id: rel.entity_b_id})
        WHERE (a.project_id = rel.project_id OR a.id = $identity_entity_id)
          AND (b.project_id = rel.project_id OR b.id = $identity_entity_id)
        MERGE (a)-[r:RELATED_TO]->(b)
        SET r.weight = coalesce(r.weight, 0) + 1,
            r.confidence = CASE
                WHEN r.confidence IS NULL THEN rel.confidence
                WHEN rel.confidence > r.confidence THEN rel.confidence
                ELSE r.confidence
            END,
            r.last_seen = rel.now,
            r.message_ids = CASE
                WHEN r.message_ids IS NULL THEN [rel.evidence_ref]
                WHEN rel.evidence_ref IN coalesce(r.message_ids, [])
                    THEN r.message_ids
                ELSE coalesce(r.message_ids, []) + [rel.evidence_ref]
            END,
            r.context = CASE
                WHEN r.context IS NULL THEN rel.context
                WHEN rel.context IS NOT NULL THEN rel.context
                ELSE r.context
            END
        RETURN count(r)
        """
        await cur.execute(
            self._build_cypher(cypher),
            (
                json.dumps(
                    {
                        "batch": relationships,
                        "identity_entity_id": IDENTITY_ENTITY_ID,
                    }
                ),
            ),
        )

    async def replace_relationships_for_entities(
        self,
        cur,
        project_id: str,
        entity_ids: List[int],
        relationships: List[Dict],
    ) -> None:
        """Replace affected relationship projection with canonical SQL state."""
        if not entity_ids:
            return

        delete_cypher = """
        MATCH (e:Entity)-[r:RELATED_TO]-(target:Entity)
        WHERE e.id IN $entity_ids
          AND (e.project_id = $project_id OR e.id = $identity_entity_id)
          AND (target.project_id = $project_id OR target.id = $identity_entity_id)
        WITH DISTINCT r
        DELETE r
        RETURN count(r)
        """
        await cur.execute(
            self._build_cypher(delete_cypher),
            (
                json.dumps(
                    {
                        "project_id": project_id,
                        "entity_ids": entity_ids,
                        "identity_entity_id": IDENTITY_ENTITY_ID,
                    }
                ),
            ),
        )

        if not relationships:
            return

        write_cypher = """
        UNWIND $batch AS rel
        MATCH (a:Entity {id: rel.entity_a_id})
        MATCH (b:Entity {id: rel.entity_b_id})
        WHERE (a.project_id = rel.project_id OR a.id = $identity_entity_id)
          AND (b.project_id = rel.project_id OR b.id = $identity_entity_id)
        MERGE (a)-[r:RELATED_TO]->(b)
        SET r.weight = rel.weight,
            r.confidence = rel.confidence,
            r.last_seen = rel.last_seen,
            r.message_ids = rel.message_ids,
            r.context = rel.context
        RETURN count(r)
        """
        await cur.execute(
            self._build_cypher(write_cypher),
            (
                json.dumps(
                    {
                        "batch": relationships,
                        "identity_entity_id": IDENTITY_ENTITY_ID,
                    }
                ),
            ),
        )

    async def transfer_merged_entity_dependencies(
        self,
        cur,
        primary_id: int,
        secondary_id: int,
    ) -> None:
        cypher_transfer_facts = """
        MATCH (s:Entity {id: $secondary_id})-[r:HAS_FACT]->(f:Fact)
        MATCH (p:Entity {id: $primary_id})
        MERGE (p)-[:HAS_FACT]->(f)
        SET f.source_entity_id = $primary_id
        DELETE r
        RETURN count(f)
        """
        await cur.execute(
            self._build_cypher(cypher_transfer_facts),
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
        RETURN count(t)
        """
        await cur.execute(
            self._build_cypher(cypher_transfer_topics),
            (
                json.dumps(
                    {
                        "primary_id": primary_id,
                        "secondary_id": secondary_id,
                    }
                ),
            ),
        )

    async def update_merged_entity(
        self,
        cur,
        primary_id: int,
        project_id: str,
        aliases: List[str],
        confidence: float,
        last_mentioned_ms: int,
        now_ms: int,
    ) -> None:
        cypher = """
        MATCH (p:Entity {id: $primary_id})
        WHERE p.project_id = $project_id
        SET p.aliases = $aliases,
            p.last_updated = $now,
            p.confidence = $confidence,
            p.last_mentioned = $last_mentioned
        RETURN p.id
        """
        await cur.execute(
            self._build_cypher(cypher),
            (
                json.dumps(
                    {
                        "primary_id": primary_id,
                        "project_id": project_id,
                        "aliases": aliases,
                        "confidence": confidence,
                        "last_mentioned": last_mentioned_ms,
                        "now": now_ms,
                    }
                ),
            ),
        )

    async def replace_hierarchy_edges_for_entities(
        self,
        cur,
        project_id: str,
        entity_ids: List[int],
        hierarchy_edges: List[Dict],
    ) -> None:
        if not entity_ids:
            return

        delete_cypher = """
        MATCH (child:Entity)-[r:PART_OF]->(parent:Entity)
        WHERE (child.id IN $entity_ids OR parent.id IN $entity_ids)
          AND child.project_id = $project_id
          AND parent.project_id = $project_id
        WITH DISTINCT r
        DELETE r
        RETURN count(r)
        """
        await cur.execute(
            self._build_cypher(delete_cypher),
            (
                json.dumps(
                    {
                        "project_id": project_id,
                        "entity_ids": entity_ids,
                    }
                ),
            ),
        )

        if not hierarchy_edges:
            return

        write_cypher = """
        UNWIND $batch AS edge
        MATCH (child:Entity {id: edge.child_id})
        MATCH (parent:Entity {id: edge.parent_id})
        WHERE child.project_id = edge.project_id
          AND parent.project_id = edge.project_id
        MERGE (child)-[r:PART_OF]->(parent)
        SET r.created_at = edge.created_at
        RETURN count(r)
        """
        await cur.execute(
            self._build_cypher(write_cypher),
            (json.dumps({"batch": hierarchy_edges}),),
        )

    async def delete_entity_projection(
        self,
        cur,
        entity_id: int,
        project_id: str,
    ) -> None:
        cypher = """
        MATCH (e:Entity {id: $entity_id})
        WHERE e.project_id = $project_id
        DETACH DELETE e
        RETURN count(e)
        """
        await cur.execute(
            self._build_cypher(cypher),
            (
                json.dumps(
                    {
                        "entity_id": entity_id,
                        "project_id": project_id,
                    }
                ),
            ),
        )

    async def project_facts(
        self,
        cur,
        entity_id: int,
        facts: List[Dict],
        user_name: str,
        session_id: Optional[str],
        project_id: str,
    ) -> int:
        if not facts:
            return 0

        cypher = """
        MATCH (e:Entity {id: $entity_id})
        WHERE e.project_id = $project_id
        UNWIND $batch AS item
        MERGE (f:Fact {id: item.id})
        SET f.source_entity_id = $entity_id,
            f.content = item.content,
            f.valid_at = item.valid_at,
            f.invalid_at = item.invalid_at,
            f.confidence = item.confidence,
            f.user_name = $user_name,
            f.project_id = $project_id,
            f.source_msg_id = item.source_msg_id,
            f.source_user_name = item.source_user_name,
            f.source_session_id = item.source_session_id,
            f.source = item.source
        MERGE (e)-[:HAS_FACT]->(f)
        WITH f, item
        RETURN count(f) AS projected_count
        """
        await cur.execute(
            self._build_cypher(cypher, "projected_count agtype"),
            (
                json.dumps(
                    {
                        "entity_id": entity_id,
                        "batch": facts,
                        "user_name": user_name,
                        "session_id": session_id,
                        "project_id": project_id,
                    }
                ),
            ),
        )
        record = await cur.fetchone()
        return int(record["projected_count"]) if record else 0

    async def project_fact_message_links(self, cur, facts: List[Dict]) -> None:
        msg_params = [fact for fact in facts if fact["source_msg_id"] is not None]
        if not msg_params:
            return

        cypher = """
        UNWIND $batch AS item
        MATCH (f:Fact {id: item.id})
        MATCH (m:Message {
            user_name: item.source_user_name,
            session_id: item.source_session_id,
            id: item.source_msg_id
        })
        MERGE (f)-[:EXTRACTED_FROM]->(m)
        RETURN count(f)
        """
        await cur.execute(
            self._build_cypher(cypher),
            (json.dumps({"batch": msg_params}),),
        )

    async def invalidate_fact(
        self,
        cur,
        fact_id: str,
        invalid_at: str,
        project_id: str,
    ) -> None:
        cypher = """
        MATCH (f:Fact {id: $fact_id})
        WHERE f.project_id = $project_id
        SET f.invalid_at = $invalid_at
        RETURN f.id
        """
        await cur.execute(
            self._build_cypher(cypher, "id agtype"),
            (
                json.dumps(
                    {
                        "fact_id": fact_id,
                        "invalid_at": invalid_at,
                        "project_id": project_id,
                    }
                ),
            ),
        )

    async def delete_facts(self, cur, fact_ids: List[str], project_id: str) -> None:
        if not fact_ids:
            return

        cypher = """
        MATCH (f:Fact)
        WHERE f.id IN $fact_ids
          AND f.project_id = $project_id
        DETACH DELETE f
        RETURN count(f)
        """
        await cur.execute(
            self._build_cypher(cypher, "deleted agtype"),
            (
                json.dumps(
                    {
                        "fact_ids": fact_ids,
                        "project_id": project_id,
                    }
                ),
            ),
        )

    async def create_hierarchy_edge(
        self,
        parent_id: int,
        child_id: int,
        project_id: str,
        now_ms: Optional[int] = None,
    ) -> bool:
        cypher = """
        MATCH (child:Entity {id: $child_id})
        MATCH (parent:Entity {id: $parent_id})
        WHERE child.project_id = $project_id
          AND parent.project_id = $project_id
          AND NOT (child)-[:PART_OF]->(parent)
        CREATE (child)-[:PART_OF {created_at: $now}]->(parent)
        RETURN true as created
        """
        res = await self.client.execute_write(
            self._build_cypher(cypher, "created agtype"),
            (
                json.dumps(
                    {
                        "child_id": child_id,
                        "parent_id": parent_id,
                        "project_id": project_id,
                        "now": (
                            now_ms if now_ms is not None else self._current_time_ms()
                        ),
                    }
                ),
            ),
        )
        return res > 0

    async def create_hierarchy_edge_with_cursor(
        self,
        cur,
        parent_id: int,
        child_id: int,
        project_id: str,
        now_ms: int,
    ) -> bool:
        cypher = """
        MATCH (child:Entity {id: $child_id})
        MATCH (parent:Entity {id: $parent_id})
        WHERE child.project_id = $project_id
          AND parent.project_id = $project_id
          AND NOT (child)-[:PART_OF]->(parent)
        CREATE (child)-[:PART_OF {created_at: $now}]->(parent)
        RETURN true as created
        """
        await cur.execute(
            self._build_cypher(cypher, "created agtype"),
            (
                json.dumps(
                    {
                        "child_id": child_id,
                        "parent_id": parent_id,
                        "project_id": project_id,
                        "now": now_ms,
                    }
                ),
            ),
        )
        record = await cur.fetchone()
        return bool(record and record["created"])

    async def delete_relationship(
        self,
        entity_a_id: int,
        entity_b_id: int,
        project_id: str,
    ) -> bool:
        cypher = """
        MATCH (a:Entity {id: $a_id})-[r:RELATED_TO]-(b:Entity {id: $b_id})
        WHERE (a.project_id = $project_id OR a.id = $identity_entity_id)
          AND (b.project_id = $project_id OR b.id = $identity_entity_id)
        DELETE r
        RETURN count(r)
        """
        res = await self.client.execute_write(
            self._build_cypher(cypher, "deleted agtype"),
            (
                json.dumps(
                    {
                        "a_id": entity_a_id,
                        "b_id": entity_b_id,
                        "project_id": project_id,
                        "identity_entity_id": IDENTITY_ENTITY_ID,
                    }
                ),
            ),
        )
        return res > 0

    async def delete_relationship_with_cursor(
        self,
        cur,
        entity_a_id: int,
        entity_b_id: int,
        project_id: str,
    ) -> bool:
        cypher = """
        MATCH (a:Entity {id: $a_id})-[r:RELATED_TO]-(b:Entity {id: $b_id})
        WHERE (a.project_id = $project_id OR a.id = $identity_entity_id)
          AND (b.project_id = $project_id OR b.id = $identity_entity_id)
        DELETE r
        RETURN count(r) AS deleted
        """
        await cur.execute(
            self._build_cypher(cypher, "deleted agtype"),
            (
                json.dumps(
                    {
                        "a_id": entity_a_id,
                        "b_id": entity_b_id,
                        "project_id": project_id,
                        "identity_entity_id": IDENTITY_ENTITY_ID,
                    }
                ),
            ),
        )
        record = await cur.fetchone()
        return bool(record and int(record["deleted"]) > 0)
