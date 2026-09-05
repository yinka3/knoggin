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
            e.canonical_name = data.canonical_name

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
        """Project the canonical user-global identity node."""
        cypher = """
        MERGE (e:Entity {id: $id})
        SET e.user_name = $user_name,
            e.canonical_name = $canonical_name,
            e.aliases = $aliases
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
        MATCH ()-[r:RELATED_TO]->()
        WHERE r.project_id = $project_id
        DELETE r
        RETURN count(r)
        """
        await cur.execute(self._build_cypher(cypher), params)

    async def project_entity_domain(self, cur, entities: List[Dict]) -> None:
        """Contexts are canonical SQL state, not properties of global AGE nodes."""

        # Keep the method as an explicit lifecycle seam for callers.  AGE
        # projects global identities and project-scoped relationships only.
        return None

    async def project_relationships(self, cur, relationships: List[Dict]) -> None:
        if not relationships:
            return

        cypher = """
        UNWIND $batch AS rel
        MATCH (a:Entity {id: rel.entity_a_id})
        MATCH (b:Entity {id: rel.entity_b_id})
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
                    {"batch": relationships}
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
        aliases: List[str],
    ) -> None:
        cypher = """
        MATCH (p:Entity {id: $primary_id})
        SET p.aliases = $aliases
        RETURN p.id
        """
        await cur.execute(
            self._build_cypher(cypher),
            (
                json.dumps(
                    {
                        "primary_id": primary_id,
                        "aliases": aliases,
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
        }
        entity_cypher = """
        MATCH (e:Entity)
        WHERE e.id IN $entity_ids
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
