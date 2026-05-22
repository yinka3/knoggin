import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple

from loguru import logger

from infrastructure.postgres_client import PostgresClient


class AgeEntityReader:
    def __init__(self, client: PostgresClient, graph_name: str = "knoggin_graph"):
        self.client = client
        self.graph_name = graph_name

    def _build_cypher(self, cypher_query: str, return_types: str) -> str:
        """
        Helper to wrap a Cypher query for AGE.
        `return_types` should be like "id agtype, name agtype"
        """
        # We always pass params as the third argument (%s) to the cypher function
        return f"SELECT * FROM cypher('{self.graph_name}', $${cypher_query}$$, %s) AS ({return_types})"

    def _parse_agtype(self, val):
        """Basic helper to unwrap agtype returned by psycopg (often just standard dicts/lists/scalars if configured, but safe to handle)."""
        return val

    async def get_max_entity_id(self) -> int:
        """Returns the highest entity ID currently in the DB."""
        # Querying the relational table is faster than the graph
        query = "SELECT COALESCE(MAX(entity_id), 0) as max_id FROM entity_search"
        try:
            result = await self.client.execute_read(query)
            return result[0]["max_id"] if result else 0
        except Exception as e:
            logger.error(f"Failed to get max entity ID: {e}")
            raise

    async def get_entity_embedding(self, entity_id: int) -> List[float]:
        query = "SELECT embedding FROM entity_search WHERE entity_id = %s"
        try:
            result = await self.client.execute_read(query, (entity_id,))
            if result and result[0]["embedding"]:
                # pgvector returns a list or ndarray
                emb = result[0]["embedding"]
                if hasattr(emb, "tolist"):
                    return emb.tolist()
                return list(emb)
            return []
        except Exception as e:
            logger.error(f"Failed to get embedding for entity {entity_id}: {e}")
            return []

    async def list_entities(
        self,
        limit: int = 20,
        offset: int = 0,
        topic: Optional[str] = None,
        entity_type: Optional[str] = None,
        search: Optional[str] = None,
    ) -> Tuple[List[Dict], int]:
        """Paginated entity listing with optional filters."""
        where_clauses = []
        params = {"limit": limit, "offset": offset}

        if entity_type:
            where_clauses.append("e.type = $entity_type")
            params["entity_type"] = entity_type

        if search:
            # AGE string functions: toLower
            where_clauses.append("toLower(e.canonical_name) CONTAINS toLower($search)")
            params["search"] = search

        if topic:
            where_clauses.append("t.name = $topic")
            params["topic"] = topic

        topic_match = (
            "MATCH (e)-[:BELONGS_TO]->(t:Topic)"
            if topic
            else "OPTIONAL MATCH (e)-[:BELONGS_TO]->(t:Topic)"
        )
        where_str = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        # Query 1: Count
        count_cypher = f"""
        MATCH (e:Entity)
        {topic_match}
        {where_str}
        RETURN count(e)
        """
        count_query = self._build_cypher(count_cypher, "total agtype")

        # Query 2: Data
        data_cypher = f"""
        MATCH (e:Entity)
        {topic_match}
        {where_str}
        WITH e, t
        OPTIONAL MATCH (e)-[:HAS_FACT]->(f) WHERE f.invalid_at IS NULL
        WITH e, t, collect(f.content)[0..2] AS fact_snippets
        RETURN e.id,
            e.session_id,
            e.canonical_name,
            e.type,
            t.name,
            e.last_mentioned / 1000,
            fact_snippets
        ORDER BY e.last_mentioned DESC
        SKIP $offset
        LIMIT $limit
        """
        data_query = self._build_cypher(
            data_cypher,
            "id agtype, session_id agtype, canonical_name agtype, type agtype, topic agtype, last_mentioned agtype, fact_snippets agtype",
        )

        try:
            params_json = json.dumps(params)
            count_res = await self.client.execute_read(count_query, (params_json,))
            total = int(count_res[0]["total"]) if count_res and count_res[0]["total"] else 0

            if total == 0:
                return [], 0

            entities_res = await self.client.execute_read(data_query, (params_json,))
            entities = []
            for row in entities_res:
                # Basic string join for snippets since AGE doesn't support complex reduce easily
                snippets = row["fact_snippets"] or []
                summary = ". ".join(filter(None, snippets)) if snippets else None

                entities.append({
                    "id": int(row["id"]),
                    "session_id": row["session_id"],
                    "canonical_name": row["canonical_name"],
                    "type": row["type"],
                    "topic": row["topic"],
                    "last_mentioned": float(row["last_mentioned"] or 0),
                    "summary": summary,
                })
            return entities, total
        except Exception as e:
            logger.error(f"Failed to list entities: {e}")
            return [], 0

    async def get_entity_by_id(self, entity_id: int) -> Optional[Dict]:
        cypher = """
        MATCH (e:Entity {id: $entity_id})
        OPTIONAL MATCH (e)-[:BELONGS_TO]->(t:Topic)
        RETURN e.id,
            e.session_id,
            e.canonical_name,
            e.aliases,
            e.type,
            t.name,
            e.last_mentioned / 1000,
            e.last_updated / 1000,
            e.last_profiled_msg_id
        """
        query = self._build_cypher(
            cypher,
            "id agtype, session_id agtype, canonical_name agtype, aliases agtype, type agtype, topic agtype, last_mentioned agtype, last_updated agtype, last_profiled_msg_id agtype",
        )
        try:
            res = await self.client.execute_read(query, (json.dumps({"entity_id": entity_id}),))
            if not res:
                return None
            row = res[0]
            return {
                "id": int(row["id"]) if row["id"] else None,
                "session_id": row["session_id"],
                "canonical_name": row["canonical_name"],
                "aliases": row["aliases"] or [],
                "type": row["type"],
                "topic": row["topic"],
                "last_mentioned": float(row["last_mentioned"] or 0),
                "last_updated": float(row["last_updated"] or 0),
                "last_profiled_msg_id": row["last_profiled_msg_id"],
            }
        except Exception as e:
            logger.error(f"Failed to get entity {entity_id}: {e}")
            return None

    async def get_entities_by_ids(self, entity_ids: List[int]) -> List[Dict]:
        if not entity_ids:
            return []
            
        cypher = """
        MATCH (e:Entity)
        WHERE e.id IN $entity_ids
        OPTIONAL MATCH (e)-[:BELONGS_TO]->(t:Topic)
        RETURN e.id,
            e.session_id,
            e.canonical_name,
            e.aliases,
            e.type,
            t.name,
            e.last_mentioned / 1000,
            e.last_updated / 1000,
            e.last_profiled_msg_id
        """
        query = self._build_cypher(
            cypher,
            "id agtype, session_id agtype, canonical_name agtype, aliases agtype, type agtype, topic agtype, last_mentioned agtype, last_updated agtype, last_profiled_msg_id agtype",
        )
        
        # We also need embeddings from the relational table for full compatibility
        emb_query = "SELECT entity_id, embedding FROM entity_search WHERE entity_id = ANY(%s)"
        
        try:
            res = await self.client.execute_read(query, (json.dumps({"entity_ids": entity_ids}),))
            
            # Fetch embeddings
            emb_res = await self.client.execute_read(emb_query, (entity_ids,))
            embeddings_map = {row["entity_id"]: row["embedding"] for row in emb_res}
            
            entities = []
            for row in res:
                eid = int(row["id"])
                entities.append({
                    "id": eid,
                    "session_id": row["session_id"],
                    "canonical_name": row["canonical_name"],
                    "aliases": row["aliases"] or [],
                    "type": row["type"],
                    "topic": row["topic"],
                    "last_mentioned": float(row["last_mentioned"] or 0),
                    "last_updated": float(row["last_updated"] or 0),
                    "last_profiled_msg_id": row["last_profiled_msg_id"],
                    "embedding": embeddings_map.get(eid, [])
                })
            return entities
        except Exception as e:
            logger.error(f"Failed to fetch entities by ids: {e}")
            return []

    async def find_alias_collisions(self) -> List[Tuple[int, int]]:
        cypher = """
        MATCH (e:Entity)
        UNWIND (coalesce(e.aliases, []) + coalesce(e.canonical_name, [])) AS name
        WITH toLower(name) AS lower_name, collect(e.id) AS ids
        WHERE size(ids) > 1
        UNWIND ids AS id_a
        UNWIND ids AS id_b
        WITH id_a, id_b WHERE id_a < id_b
        RETURN DISTINCT id_a, id_b
        """
        query = self._build_cypher(cypher, "id_a agtype, id_b agtype")
        try:
            res = await self.client.execute_read(query, ("{}",))
            return [(int(r["id_a"]), int(r["id_b"])) for r in res]
        except Exception as e:
            logger.error(f"Failed to find alias collisions: {e}")
            return []

    async def get_entities_by_names(self, names: List[str]) -> List[Dict]:
        lower_names = [n.lower() for n in names]
        cypher = """
        MATCH (e:Entity)
        WHERE toLower(e.canonical_name) IN $names
            OR any(alias IN e.aliases WHERE toLower(alias) IN $names)
        OPTIONAL MATCH (e)-[:HAS_FACT]->(f) WHERE f.invalid_at IS NULL
        RETURN e.id, e.canonical_name, e.type, e.aliases, collect(f.content) as facts
        """
        query = self._build_cypher(
            cypher,
            "id agtype, canonical_name agtype, type agtype, aliases agtype, facts agtype"
        )
        try:
            res = await self.client.execute_read(query, (json.dumps({"names": lower_names}),))
            return [
                {
                    "id": int(row["id"]),
                    "canonical_name": row["canonical_name"],
                    "type": row["type"],
                    "aliases": row["aliases"] or [],
                    "facts": row["facts"] or [],
                }
                for row in res
            ]
        except Exception as e:
            logger.error(f"Failed to get entities by names: {e}")
            return []

    async def search_similar_entities(self, entity_id: int, limit: int = 50) -> List[Tuple[int, float]]:
        """Find similar entities using Postgres pgvector (replaces Memgraph vector index)."""
        # 1. Get the source vector
        emb = await self.get_entity_embedding(entity_id)
        if not emb:
            return []
            
        # 2. Search using pgvector cosine distance `<=>`
        # `<=>` returns distance, so we do 1 - distance for similarity
        query = """
        SELECT entity_id, 1 - (embedding <=> %s::vector) AS similarity
        FROM entity_search
        WHERE entity_id != %s
        ORDER BY embedding <=> %s::vector
        LIMIT %s
        """
        try:
            res = await self.client.execute_read(query, (emb, entity_id, emb, limit))
            return [(r["entity_id"], r["similarity"]) for r in res]
        except Exception as e:
            logger.error(f"Failed to search similar entities for {entity_id}: {e}")
            return []

    async def search_entities_by_embedding(
        self, embedding: List[float], limit: int = 10, score_threshold: float = 0.8
    ) -> List[Tuple[int, float]]:
        query = """
        SELECT entity_id, 1 - (embedding <=> %s::vector) AS similarity
        FROM entity_search
        WHERE 1 - (embedding <=> %s::vector) >= %s
        ORDER BY embedding <=> %s::vector
        LIMIT %s
        """
        try:
            res = await self.client.execute_read(query, (embedding, embedding, score_threshold, embedding, limit))
            return [(r["entity_id"], r["similarity"]) for r in res]
        except Exception as e:
            logger.error(f"Entity vector search failed: {e}")
            return []

    async def validate_existing_ids(self, ids: List[int]) -> Optional[Set[int]]:
        cypher = """
        MATCH (e:Entity)
        WHERE e.id IN $ids
        RETURN e.id
        """
        query = self._build_cypher(cypher, "id agtype")
        try:
            res = await self.client.execute_read(query, (json.dumps({"ids": ids}),))
            return {int(r["id"]) for r in res}
        except Exception as e:
            logger.error(f"Liveness check failed: {e}")
            return None
