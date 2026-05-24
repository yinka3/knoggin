import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple

from loguru import logger

from infrastructure.db_client import DBClient


class EntityReader:
    def __init__(self, client: DBClient, graph_name: str = "knoggin_graph"):
        self.client = client
        self.graph_name = graph_name

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
        count_query = self.client.build_cypher(count_cypher, "total agtype")

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
        data_query = self.client.build_cypher(
            data_cypher,
            "id agtype, session_id agtype, canonical_name agtype, type agtype, topic agtype, last_mentioned agtype, fact_snippets agtype",
        )

        try:
            params_json = json.dumps(params)
            count_res = await self.client.execute_read(count_query, (params_json,))
            total = (
                int(count_res[0]["total"]) if count_res and count_res[0]["total"] else 0
            )

            if total == 0:
                return [], 0

            entities_res = await self.client.execute_read(data_query, (params_json,))
            entities = []
            for row in entities_res:
                # Basic string join for snippets since AGE doesn't support complex reduce easily
                snippets = row["fact_snippets"] or []
                summary = ". ".join(filter(None, snippets)) if snippets else None

                entities.append(
                    {
                        "id": int(row["id"]),
                        "session_id": row["session_id"],
                        "canonical_name": row["canonical_name"],
                        "type": row["type"],
                        "topic": row["topic"],
                        "last_mentioned": float(row["last_mentioned"] or 0),
                        "summary": summary,
                    }
                )
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
        query = self.client.build_cypher(
            cypher,
            "id agtype, session_id agtype, canonical_name agtype, aliases agtype, type agtype, topic agtype, last_mentioned agtype, last_updated agtype, last_profiled_msg_id agtype",
        )
        try:
            res = await self.client.execute_read(
                query, (json.dumps({"entity_id": entity_id}),)
            )
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
        query = self.client.build_cypher(
            cypher,
            "id agtype, session_id agtype, canonical_name agtype, aliases agtype, type agtype, topic agtype, last_mentioned agtype, last_updated agtype, last_profiled_msg_id agtype",
        )

        # We also need embeddings from the relational table for full compatibility
        emb_query = (
            "SELECT entity_id, embedding FROM entity_search WHERE entity_id = ANY(%s)"
        )

        try:
            res = await self.client.execute_read(
                query, (json.dumps({"entity_ids": entity_ids}),)
            )

            # Fetch embeddings
            emb_res = await self.client.execute_read(emb_query, (entity_ids,))
            embeddings_map = {row["entity_id"]: row["embedding"] for row in emb_res}

            entities = []
            for row in res:
                eid = int(row["id"])
                entities.append(
                    {
                        "id": eid,
                        "session_id": row["session_id"],
                        "canonical_name": row["canonical_name"],
                        "aliases": row["aliases"] or [],
                        "type": row["type"],
                        "topic": row["topic"],
                        "last_mentioned": float(row["last_mentioned"] or 0),
                        "last_updated": float(row["last_updated"] or 0),
                        "last_profiled_msg_id": row["last_profiled_msg_id"],
                        "embedding": embeddings_map.get(eid, []),
                    }
                )
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
        query = self.client.build_cypher(cypher, "id_a agtype, id_b agtype")
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
        query = self.client.build_cypher(
            cypher,
            "id agtype, canonical_name agtype, type agtype, aliases agtype, facts agtype",
        )
        try:
            res = await self.client.execute_read(
                query, (json.dumps({"names": lower_names}),)
            )
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

    async def search_similar_entities(
        self, entity_id: int, limit: int = 50
    ) -> List[Tuple[int, float]]:
        """Find similar entities using Postgres pgvector (replaces GraphClient vector index)."""
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
            res = await self.client.execute_read(
                query, (embedding, embedding, score_threshold, embedding, limit)
            )
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
        query = self.client.build_cypher(cypher, "id agtype")
        try:
            res = await self.client.execute_read(query, (json.dumps({"ids": ids}),))
            return {int(r["id"]) for r in res}
        except Exception as e:
            logger.error(f"Liveness check failed: {e}")
            return None

    async def get_all_entities_for_hydration(self) -> list[dict]:
        cypher = """
        MATCH (e:Entity)
        WHERE e.id IS NOT NULL
        OPTIONAL MATCH (e)-[:BELONGS_TO]->(t:Topic)
        RETURN e.id, e.canonical_name, e.aliases, e.type, t.name as topic, e.session_id
        """
        query = self.client.build_cypher(
            cypher,
            "id agtype, canonical_name agtype, aliases agtype, type agtype, topic agtype, session_id agtype",
        )
        emb_query = "SELECT entity_id, embedding FROM entity_search"
        try:
            res = await self.client.execute_read(query, ("{}",))
            emb_res = await self.client.execute_read(emb_query)
            embeddings_map = {
                row["entity_id"]: (
                    row["embedding"].tolist()
                    if hasattr(row["embedding"], "tolist")
                    else list(row["embedding"])
                )
                for row in emb_res
            }

            entities = []
            for row in res:
                eid = int(row["id"])
                entities.append(
                    {
                        "id": eid,
                        "canonical_name": row["canonical_name"],
                        "aliases": row["aliases"] or [],
                        "type": row["type"],
                        "topic": row["topic"],
                        "session_id": row["session_id"],
                        "embedding": embeddings_map.get(eid, []),
                    }
                )
            return entities
        except Exception as e:
            logger.error(f"Failed to hydrate entities: {e}")
            return []

    async def get_orphan_entities(
        self,
        protected_id: int = 1,
        orphan_cutoff_ms: int = 0,
        stale_junk_cutoff_ms: int = 0,
    ) -> List[int]:
        cypher = """
        MATCH (e:Entity)
        WHERE e.id <> $protected_id
        AND NOT EXISTS { MATCH (e)-[:HAS_FACT]->(f_active:Fact) WHERE f_active.invalid_at IS NULL }
        OPTIONAL MATCH (e)-[r:RELATED_TO]-(neighbor)
        WITH e, collect(neighbor.id) as neighbors
        WHERE (size(neighbors) = 0 AND e.last_mentioned < $orphan_cutoff)
           OR (size(neighbors) = 1 AND neighbors[0] = $protected_id AND e.last_mentioned < $stale_cutoff)
        RETURN e.id
        """
        query = self.client.build_cypher(cypher, "id agtype")
        try:
            res = await self.client.execute_read(
                query,
                (
                    json.dumps(
                        {
                            "protected_id": protected_id,
                            "orphan_cutoff": orphan_cutoff_ms,
                            "stale_cutoff": stale_junk_cutoff_ms,
                        }
                    ),
                ),
            )
            return [int(r["id"]) for r in res]
        except Exception as e:
            logger.error(f"Failed to fetch orphans: {e}")
            return []

    async def get_entity_count_by_type(self) -> List[Dict]:
        cypher = """
        MATCH (e:Entity)
        WHERE e.type IS NOT NULL
        RETURN e.type, count(e) as count
        ORDER BY count DESC
        """
        query = self.client.build_cypher(cypher, "type agtype, count agtype")
        try:
            res = await self.client.execute_read(query, ("{}",))
            return [
                {
                    "type": r["type"].strip('"')
                    if isinstance(r["type"], str)
                    else r["type"],
                    "count": int(r["count"]),
                }
                for r in res
            ]
        except Exception as e:
            logger.error(f"Failed to get entity count by type: {e}")
            return []

    async def get_entity_count_by_topic(self) -> List[Dict]:
        cypher = """
        MATCH (e:Entity)-[:BELONGS_TO]->(t:Topic)
        RETURN t.name as topic, count(e) as count
        ORDER BY count DESC
        """
        query = self.client.build_cypher(cypher, "topic agtype, count agtype")
        try:
            res = await self.client.execute_read(query, ("{}",))
            return [
                {
                    "topic": r["topic"].strip('"')
                    if isinstance(r["topic"], str)
                    else r["topic"],
                    "count": int(r["count"]),
                }
                for r in res
            ]
        except Exception as e:
            logger.error(f"Failed to get entity count by topic: {e}")
            return []

    async def get_top_connected_entities(self, limit: int = 10) -> List[Dict]:
        cypher = """
        MATCH (e:Entity)-[r:RELATED_TO]-()
        WITH e, count(r) AS connections
        ORDER BY connections DESC
        LIMIT $limit
        RETURN e.canonical_name as name, e.type as type, connections
        """
        query = self.client.build_cypher(
            cypher, "name agtype, type agtype, connections agtype"
        )
        try:
            res = await self.client.execute_read(query, (json.dumps({"limit": limit}),))
            return [
                {
                    "name": r["name"].strip('"')
                    if isinstance(r["name"], str)
                    else r["name"],
                    "type": r["type"].strip('"')
                    if isinstance(r["type"], str)
                    else r["type"],
                    "connections": int(r["connections"]),
                }
                for r in res
            ]
        except Exception as e:
            logger.error(f"Failed to get top connected entities: {e}")
            return []

    async def get_entity_relationships(self, entity_id: int) -> List[Dict]:
        cypher = """
        MATCH (e:Entity {id: $entity_id})-[r:RELATED_TO]-(neighbor:Entity)
        RETURN neighbor.id as neighbor_id,
            neighbor.canonical_name as neighbor_name,
            r.weight as weight,
            r.message_ids as message_ids,
            r.context as context,
            r.confidence as confidence
        """
        query = self.client.build_cypher(
            cypher,
            "neighbor_id agtype, neighbor_name agtype, weight agtype, message_ids agtype, context agtype, confidence agtype",
        )
        try:
            res = await self.client.execute_read(
                query, (json.dumps({"entity_id": entity_id}),)
            )
            return [
                {
                    "neighbor_id": int(r["neighbor_id"]),
                    "neighbor_name": r["neighbor_name"].strip('"')
                    if isinstance(r["neighbor_name"], str)
                    else r["neighbor_name"],
                    "weight": float(r["weight"] or 1.0),
                    "message_ids": r["message_ids"] or [],
                    "context": r["context"].strip('"')
                    if isinstance(r["context"], str)
                    else r["context"],
                    "confidence": float(r["confidence"] or 1.0),
                }
                for r in res
            ]
        except Exception as e:
            logger.error(f"Failed to get relationships for entity {entity_id}: {e}")
            return []

    async def get_recently_active_entities(
        self, days: int = 7, limit: int = 10
    ) -> List[Dict]:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        cypher = """
        MATCH (e:Entity)-[:HAS_FACT]->(f:Fact)
        WHERE f.valid_at > $cutoff
        AND f.invalid_at IS NULL
        WITH e, count(f) as recent_facts, max(f.valid_at) as last_activity
        OPTIONAL MATCH (e)-[:BELONGS_TO]->(t:Topic)
        RETURN e.id as id, e.canonical_name as name, e.type as type, t.name as topic, recent_facts, last_activity
        ORDER BY recent_facts DESC, last_activity DESC
        LIMIT $limit
        """
        query = self.client.build_cypher(
            cypher,
            "id agtype, name agtype, type agtype, topic agtype, recent_facts agtype, last_activity agtype",
        )
        try:
            res = await self.client.execute_read(
                query, (json.dumps({"cutoff": cutoff, "limit": limit}),)
            )
            return [
                {
                    "id": int(r["id"]),
                    "name": r["name"].strip('"')
                    if isinstance(r["name"], str)
                    else r["name"],
                    "type": r["type"].strip('"')
                    if isinstance(r["type"], str)
                    else r["type"],
                    "topic": r["topic"].strip('"')
                    if isinstance(r["topic"], str)
                    else r["topic"],
                    "recent_facts": int(r["recent_facts"]),
                    "last_activity": r["last_activity"].strip('"')
                    if isinstance(r["last_activity"], str)
                    else r["last_activity"],
                }
                for r in res
            ]
        except Exception as e:
            logger.error(f"Failed to get recently active entities: {e}")
            return []

    async def get_notable_entities(self, limit: int = 10) -> List[Dict]:
        cypher = """
        MATCH (e:Entity)
        WHERE e.canonical_name IS NOT NULL
        OPTIONAL MATCH (e)-[r]-()
        WITH e, count(DISTINCT r) as connection_count
        OPTIONAL MATCH (e)-[:HAS_FACT]->(f:Fact)
        WHERE f.invalid_at IS NULL
        WITH e, connection_count, count(f) as fact_count
        OPTIONAL MATCH (e)-[:BELONGS_TO]->(t:Topic)
        RETURN e.id as id, e.canonical_name as name, e.type as type, t.name as topic, connection_count, fact_count
        ORDER BY connection_count DESC
        LIMIT $limit
        """
        query = self.client.build_cypher(
            cypher,
            "id agtype, name agtype, type agtype, topic agtype, connection_count agtype, fact_count agtype",
        )
        try:
            res = await self.client.execute_read(query, (json.dumps({"limit": limit}),))
            return [
                {
                    "id": int(r["id"]),
                    "name": r["name"].strip('"')
                    if isinstance(r["name"], str)
                    else r["name"],
                    "type": r["type"].strip('"')
                    if isinstance(r["type"], str)
                    else r["type"],
                    "topic": r["topic"].strip('"')
                    if isinstance(r["topic"], str)
                    else r["topic"],
                    "connection_count": int(r["connection_count"]),
                    "fact_count": int(r["fact_count"]),
                }
                for r in res
            ]
        except Exception as e:
            logger.error(f"Failed to get notable entities: {e}")
            return []
