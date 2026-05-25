import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List

from loguru import logger

from common.schema.dtypes import FactRecord
from infrastructure.postgres_client import PostgresClient


class FactReader:
    def __init__(self, client: PostgresClient, graph_name: str = "knoggin_graph"):
        self.client = client
        self.graph_name = graph_name

    def _hydrate_fact(self, record, embedding: List[float] = None) -> FactRecord:
        """Convert DB record to FactRecord."""
        # Handle datetime parsing from ISO strings returned by AGE
        valid_at_str = record["valid_at"]
        invalid_at_str = record.get("invalid_at")

        # Strip quotes if AGE returned them as JSON strings
        if isinstance(valid_at_str, str) and valid_at_str.startswith('"'):
            valid_at_str = valid_at_str.strip('"')
        if isinstance(invalid_at_str, str) and invalid_at_str.startswith('"'):
            invalid_at_str = invalid_at_str.strip('"')

        valid_at = datetime.fromisoformat(valid_at_str)
        invalid_at = datetime.fromisoformat(invalid_at_str) if invalid_at_str else None

        # Clean string fields
        fact_id = (
            record["id"].strip('"')
            if isinstance(record["id"], str)
            else str(record["id"])
        )
        content = (
            record["content"].strip('"')
            if isinstance(record["content"], str)
            else record["content"]
        )
        source = (
            record["source"].strip('"')
            if record.get("source") and isinstance(record["source"], str)
            else record.get("source")
        )
        source_user_name = self._clean_string(record.get("source_user_name"))
        source_session_id = self._clean_string(record.get("source_session_id"))
        source_msg_id = self._clean_string(record.get("source_msg_id"))

        return FactRecord(
            id=fact_id,
            source_entity_id=int(record["source_entity_id"]),
            content=content,
            valid_at=valid_at,
            invalid_at=invalid_at,
            confidence=float(record["confidence"]),
            embedding=embedding,
            source_msg_id=int(source_msg_id) if source_msg_id is not None else None,
            source_user_name=source_user_name,
            source_session_id=source_session_id,
            source=source,
        )

    @staticmethod
    def _clean_string(value):
        if isinstance(value, str):
            return value.strip('"')
        return value

    async def get_facts_for_entity(self, entity_id: int, active_only: bool = True):
        where = "WHERE f.invalid_at IS NULL" if active_only else ""
        cypher = f"""
        MATCH (e:Entity {{id: $entity_id}})-[:HAS_FACT]->(f:Fact)
        {where}
        OPTIONAL MATCH (f)-[:EXTRACTED_FROM]->(m:Message)
        RETURN f.id, f.source_entity_id, f.content, f.valid_at, f.invalid_at, f.confidence,
            f.source, m.id as source_msg_id, m.user_name as source_user_name,
            m.session_id as source_session_id, f.created_at
        ORDER BY f.created_at DESC
        """

        query = self.client.build_cypher(
            cypher,
            "id agtype, source_entity_id agtype, content agtype, valid_at agtype, invalid_at agtype, confidence agtype, source agtype, source_msg_id agtype, source_user_name agtype, source_session_id agtype, created_at agtype",
        )

        try:
            res = await self.client.execute_read(
                query, (json.dumps({"entity_id": entity_id}),)
            )
            return [self._hydrate_fact(row) for row in res]
        except Exception as e:
            logger.error(f"Failed to get facts for entity {entity_id}: {e}")
            return []

    async def get_facts_for_entities(
        self, entity_ids: List[int], active_only: bool = True
    ) -> Dict[int, List[FactRecord]]:
        if not entity_ids:
            return {}

        where = "AND f.invalid_at IS NULL" if active_only else ""

        # We fetch all matching facts and truncate to 5 per entity in Python,
        # avoiding complex AGE/Postgres subquery bridging.
        cypher = f"""
        MATCH (e:Entity)-[:HAS_FACT]->(f:Fact)
        WHERE e.id IN $entity_ids {where}
        OPTIONAL MATCH (f)-[:EXTRACTED_FROM]->(m:Message)
        RETURN e.id as entity_id, f.id as id, f.source_entity_id as source_entity_id,
            f.content as content, f.valid_at as valid_at, f.invalid_at as invalid_at,
            f.confidence as confidence, f.source as source, m.id as source_msg_id,
            m.user_name as source_user_name, m.session_id as source_session_id,
            f.created_at
        ORDER BY f.created_at DESC
        """

        query = self.client.build_cypher(
            cypher,
            "entity_id agtype, id agtype, source_entity_id agtype, content agtype, valid_at agtype, invalid_at agtype, confidence agtype, source agtype, source_msg_id agtype, source_user_name agtype, source_session_id agtype, created_at agtype",
        )

        try:
            res = await self.client.execute_read(
                query, (json.dumps({"entity_ids": entity_ids}),)
            )
            facts_by_entity: Dict[int, List[FactRecord]] = {
                eid: [] for eid in entity_ids
            }

            for row in res:
                eid = int(row["entity_id"])
                if len(facts_by_entity[eid]) < 5:
                    facts_by_entity[eid].append(self._hydrate_fact(row))

            return facts_by_entity
        except Exception as e:
            logger.error(f"Failed to batch fetch facts: {e}")
            return {eid: [] for eid in entity_ids}

    async def search_relevant_facts(
        self, entity_id: int, query_embedding: List[float], limit: int = 5
    ) -> List[FactRecord]:
        """Search facts using native pgvector cosine similarity."""
        # 1. Search vector table for top N fact_ids
        search_query = """
        SELECT fact_id, embedding
        FROM fact_search
        WHERE entity_id = %s AND invalid_at IS NULL
        ORDER BY embedding <=> %s::vector
        LIMIT %s
        """
        try:
            search_res = await self.client.execute_read(
                search_query, (entity_id, query_embedding, limit)
            )
            if not search_res:
                return []

            fact_ids = [row["fact_id"] for row in search_res]
            embeddings_map = {row["fact_id"]: row["embedding"] for row in search_res}

            # 2. Fetch those specific facts from graph
            cypher = """
            MATCH (f:Fact)
            WHERE f.id IN $fact_ids
            OPTIONAL MATCH (f)-[:EXTRACTED_FROM]->(m:Message)
            RETURN f.id, f.source_entity_id, f.content, f.valid_at, f.invalid_at,
                f.confidence, f.source, m.id as source_msg_id,
                m.user_name as source_user_name, m.session_id as source_session_id
            """
            query = self.client.build_cypher(
                cypher,
                "id agtype, source_entity_id agtype, content agtype, valid_at agtype, invalid_at agtype, confidence agtype, source agtype, source_msg_id agtype, source_user_name agtype, source_session_id agtype",
            )

            graph_res = await self.client.execute_read(
                query, (json.dumps({"fact_ids": fact_ids}),)
            )

            results = []
            for row in graph_res:
                fid = (
                    row["id"].strip('"')
                    if isinstance(row["id"], str)
                    else str(row["id"])
                )
                emb = embeddings_map.get(fid)
                if emb and hasattr(emb, "tolist"):
                    emb = emb.tolist()
                elif emb:
                    emb = list(emb)

                results.append(self._hydrate_fact(row, embedding=emb))

            return results

        except Exception as e:
            logger.error(f"Failed to search relevant facts for {entity_id}: {e}")
            return []

    async def get_facts_from_message(
        self, msg_id: int, user_name: str = None, session_id: str = None
    ) -> List[FactRecord]:
        if not user_name or not session_id:
            logger.warning("Refusing unsafe fact source lookup without user/session scope")
            return []

        cypher = """
        MATCH (f:Fact)-[:EXTRACTED_FROM]->(m:Message {
            user_name: $user_name,
            session_id: $session_id,
            id: $msg_id
        })
        RETURN f.id, f.source_entity_id, f.content, f.valid_at, f.invalid_at,
            f.confidence, f.source, m.id as source_msg_id,
            m.user_name as source_user_name, m.session_id as source_session_id
        """
        query = self.client.build_cypher(
            cypher,
            "id agtype, source_entity_id agtype, content agtype, valid_at agtype, invalid_at agtype, confidence agtype, source agtype, source_msg_id agtype, source_user_name agtype, source_session_id agtype",
        )
        try:
            res = await self.client.execute_read(
                query,
                (
                    json.dumps(
                        {
                            "msg_id": msg_id,
                            "user_name": user_name,
                            "session_id": session_id,
                        }
                    ),
                ),
            )
            return [self._hydrate_fact(row) for row in res]
        except Exception as e:
            logger.error(f"Failed to get facts from message {msg_id}: {e}")
            return []

    async def get_recent_facts(self, days: int = 7, limit: int = 20) -> List[Dict]:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        cypher = """
        MATCH (e:Entity)-[:HAS_FACT]->(f:Fact)
        WHERE f.valid_at > $cutoff
        AND f.invalid_at IS NULL
        RETURN f.id, f.content, f.valid_at as created_at, e.canonical_name as entity_name, e.type as entity_type
        ORDER BY f.valid_at DESC
        LIMIT $limit
        """
        query = self.client.build_cypher(
            cypher,
            "id agtype, content agtype, created_at agtype, entity_name agtype, entity_type agtype",
        )
        try:
            res = await self.client.execute_read(
                query, (json.dumps({"cutoff": cutoff, "limit": limit}),)
            )
            return [
                {
                    "id": row["id"].strip('"')
                    if isinstance(row["id"], str)
                    else str(row["id"]),
                    "content": row["content"].strip('"')
                    if isinstance(row["content"], str)
                    else row["content"],
                    "created_at": row["created_at"].strip('"')
                    if isinstance(row["created_at"], str)
                    else row["created_at"],
                    "entity_name": row["entity_name"].strip('"')
                    if isinstance(row["entity_name"], str)
                    else row["entity_name"],
                    "entity_type": row["entity_type"].strip('"')
                    if isinstance(row["entity_type"], str)
                    else row["entity_type"],
                }
                for row in res
            ]
        except Exception as e:
            logger.error(f"Failed to get recent facts: {e}")
            return []
