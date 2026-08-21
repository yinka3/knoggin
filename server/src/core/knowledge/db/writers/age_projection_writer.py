import json
from typing import Dict, List

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

    async def project_entities(self, cur, entities: List[Dict]) -> None:
        cypher = """
        UNWIND $batch AS data
        MERGE (e:Entity {id: data.id})
        SET e.user_name = data.user_name,
            e.project_id = data.project_id,
            e.canonical_name = data.canonical_name,
            e.type = data.type,
            e.topic = data.topic,
            e.last_mentioned = data.now

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

    async def project_identity(self, cur, identity: Dict) -> None:
        """Project the canonical identity node with exact reserved properties."""
        cypher = """
        MERGE (e:Entity {id: $id})
        SET e.user_name = $user_name,
            e.project_id = $project_id,
            e.canonical_name = $canonical_name,
            e.aliases = $aliases,
            e.type = $type,
            e.topic = $topic,
            e.last_mentioned = $now
        RETURN e.id
        """
        await cur.execute(
            self._build_cypher(cypher),
            (json.dumps(identity),),
        )

    async def clear_project_projection(self, cur, project_id: str) -> None:
        """Clear project-scoped AGE projection before rebuilding it from SQL."""
        params = (
            json.dumps(
                {
                    "project_id": project_id,
                    "identity_entity_id": IDENTITY_ENTITY_ID,
                }
            ),
        )
        cypher = """
        MATCH (n:Entity)
        WHERE n.project_id = $project_id
          AND coalesce(n.id, -1) <> $identity_entity_id
        DETACH DELETE n
        RETURN count(n)
        """
        await cur.execute(self._build_cypher(cypher), params)

    async def project_entity_domain(self, cur, entities: List[Dict]) -> None:
        """Update type and timestamp properties for explicit reclassification."""

        if not entities:
            return

        batch = []
        for entity in entities:
            entity_id = int(entity["id"])
            project_id = str(entity["project_id"])
            entity_type = str(entity["type"]).strip()
            if not entity_type or not project_id:
                raise ValueError("Reclassified entity projection fields are required")
            batch.append(
                {
                    "id": entity_id,
                    "project_id": project_id,
                    "type": entity_type,
                }
            )

        cypher = """
        UNWIND $batch AS data
        MATCH (e:Entity {id: data.id})
        WHERE e.project_id = data.project_id
        SET e.type = data.type
        RETURN count(e)
        """
        await cur.execute(
            self._build_cypher(cypher),
            (json.dumps({"batch": batch}),),
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
        MERGE (a)-[r:RELATED_TO {relationship_id: rel.relationship_id}]->(b)
        SET r.project_id = rel.project_id,
            r.relationship_type = rel.relationship_type,
            r.symmetric = rel.symmetric
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
          AND r.project_id = $project_id
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
        MERGE (a)-[r:RELATED_TO {relationship_id: rel.relationship_id}]->(b)
        SET r.project_id = rel.project_id,
            r.relationship_type = rel.relationship_type,
            r.symmetric = rel.symmetric
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

    async def update_merged_entity(
        self,
        cur,
        primary_id: int,
        project_id: str,
        aliases: List[str],
        last_mentioned_ms: int,
    ) -> None:
        cypher = """
        MATCH (p:Entity {id: $primary_id})
        WHERE p.project_id = $project_id
        SET p.aliases = $aliases,
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
                        "last_mentioned": last_mentioned_ms,
                    }
                ),
            ),
        )

    async def delete_entity_projection(
        self,
        cur,
        entity_id: int,
        project_id: str,
    ) -> None:
        await self.delete_entities_projection(cur, [entity_id], project_id)

    async def delete_entities_projection(
        self,
        cur,
        entity_ids: List[int],
        project_id: str,
    ) -> None:
        if not entity_ids:
            return

        params = {
            "entity_ids": entity_ids,
            "project_id": project_id,
        }
        entity_cypher = """
        MATCH (e:Entity)
        WHERE e.id IN $entity_ids
          AND e.project_id = $project_id
        DETACH DELETE e
        RETURN count(DISTINCT e)
        """
        await cur.execute(
            self._build_cypher(entity_cypher),
            (json.dumps(params),),
        )

    async def delete_relationship(
        self,
        cur,
        relationship_id: str,
        project_id: str,
    ) -> bool:
        cypher = """
        MATCH ()-[r:RELATED_TO {relationship_id: $relationship_id}]-()
        WHERE r.project_id = $project_id
        DELETE r
        RETURN count(r) AS deleted
        """
        await cur.execute(
            self._build_cypher(cypher, "deleted agtype"),
            (
                json.dumps(
                    {
                        "relationship_id": relationship_id,
                        "project_id": project_id,
                    }
                ),
            ),
        )
        record = await cur.fetchone()
        return bool(record and int(record["deleted"]) > 0)
