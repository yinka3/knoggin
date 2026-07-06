import json
from datetime import datetime, timedelta
from typing import Dict, List

from loguru import logger

from common.schema.primitives import FactRecord
from common.scoping import require_scope_value, require_visible_project_ids
from common.utils.time_utils import get_now
from infrastructure.postgres_client import PostgresClient


class FactReader:
    def __init__(self, client: PostgresClient, graph_name: str = "knoggin_graph"):
        self.client = client
        self.graph_name = graph_name

    def _parse_vector(self, val) -> List[float]:
        if val is None:
            return []
        if hasattr(val, "tolist"):
            return [float(x) for x in val.tolist()]
        if isinstance(val, str):
            raw = val.strip()
            if not raw:
                return []
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                parsed = raw.strip("[]").split(",")
            return [float(x) for x in parsed if str(x).strip()]
        return [float(x) for x in val]

    def _hydrate_fact(self, record, embedding: List[float] = None) -> FactRecord:
        """Convert DB record to FactRecord."""
        valid_at = self._parse_datetime(record["valid_at"])
        invalid_at_value = record.get("invalid_at")
        invalid_at = (
            self._parse_datetime(invalid_at_value) if invalid_at_value else None
        )

        fact_id = str(self._clean_string(record["id"]))
        content = self._clean_string(record["content"])
        source = self._clean_string(record.get("source")) or "user"
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
            embedding=embedding or [],
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

    @classmethod
    def _parse_datetime(cls, value):
        value = cls._clean_string(value)
        if isinstance(value, datetime):
            return value
        return datetime.fromisoformat(value)

    async def get_facts_for_entity(
        self,
        entity_id: int,
        *,
        visible_project_ids: List[str],
        active_only: bool = True,
    ):
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_facts_for_entity",
        )
        active_sql = "AND invalid_at IS NULL" if active_only else ""
        query = f"""
        SELECT
            fact_id AS id,
            entity_id AS source_entity_id,
            content,
            valid_at,
            invalid_at,
            confidence,
            source,
            source_msg_id,
            source_user_name,
            source_session_id
        FROM facts
        WHERE entity_id = %s
          AND project_id = ANY(%s)
        {active_sql}
        ORDER BY valid_at DESC, fact_id
        """

        try:
            res = await self.client.fetch_all(
                query,
                (entity_id, visible_project_ids),
            )
            return [self._hydrate_fact(row) for row in res]
        except Exception as e:
            logger.error(f"Failed to get facts for entity {entity_id}: {e}")
            return []

    async def get_facts_for_entities(
        self,
        entity_ids: List[int],
        *,
        visible_project_ids: List[str],
        active_only: bool = True,
    ) -> Dict[int, List[FactRecord]]:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_facts_for_entities",
        )
        if not entity_ids:
            return {}

        active_sql = "AND invalid_at IS NULL" if active_only else ""
        query = f"""
        SELECT *
        FROM (
            SELECT
                entity_id,
                fact_id AS id,
                entity_id AS source_entity_id,
                content,
                valid_at,
                invalid_at,
                confidence,
                source,
                source_msg_id,
                source_user_name,
                source_session_id,
                row_number() OVER (
                    PARTITION BY entity_id
                    ORDER BY valid_at DESC, fact_id
                ) AS rank
            FROM facts
            WHERE entity_id = ANY(%s)
              AND project_id = ANY(%s)
            {active_sql}
        ) ranked
        WHERE rank <= 5
        ORDER BY entity_id, rank
        """

        try:
            res = await self.client.fetch_all(
                query,
                (entity_ids, visible_project_ids),
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
        self,
        entity_id: int,
        query_embedding: List[float],
        *,
        visible_project_ids: List[str],
        limit: int = 5,
    ) -> List[FactRecord]:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "search_relevant_facts",
        )
        """Search facts using native pgvector cosine similarity."""
        # 1. Search vector table for top N fact_ids
        search_query = """
        SELECT fact_id, embedding
        FROM fact_search
        WHERE entity_id = %s
          AND project_id = ANY(%s)
          AND invalid_at IS NULL
        ORDER BY embedding <=> %s::vector
        LIMIT %s
        """
        try:
            search_res = await self.client.fetch_all(
                search_query,
                (entity_id, visible_project_ids, query_embedding, limit),
            )
            if not search_res:
                return []

            fact_ids = [row["fact_id"] for row in search_res]
            embeddings_map = {
                row["fact_id"]: self._parse_vector(row["embedding"])
                for row in search_res
            }

            facts_query = """
            SELECT
                fact_id AS id,
                entity_id AS source_entity_id,
                content,
                valid_at,
                invalid_at,
                confidence,
                source,
                source_msg_id,
                source_user_name,
                source_session_id
            FROM facts
            WHERE fact_id = ANY(%s)
              AND project_id = ANY(%s)
            """
            fact_res = await self.client.fetch_all(
                facts_query,
                (fact_ids, visible_project_ids),
            )
            facts_by_id = {
                str(self._clean_string(row["id"])): row
                for row in fact_res
            }

            results = []
            for fid in fact_ids:
                row = facts_by_id.get(fid)
                if not row:
                    continue
                results.append(
                    self._hydrate_fact(
                        row,
                        embedding=embeddings_map.get(fid, []),
                    )
                )

            return results

        except Exception as e:
            logger.error(f"Failed to search relevant facts for {entity_id}: {e}")
            return []

    async def get_facts_from_message(
        self,
        msg_id: int,
        *,
        user_name: str,
        session_id: str,
        visible_project_ids: List[str],
    ) -> List[FactRecord]:
        user_name = require_scope_value(
            user_name,
            "user_name",
            "get_facts_from_message",
        )
        session_id = require_scope_value(
            session_id,
            "session_id",
            "get_facts_from_message",
        )
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_facts_from_message",
        )

        query = """
        SELECT
            fact_id AS id,
            entity_id AS source_entity_id,
            content,
            valid_at,
            invalid_at,
            confidence,
            source,
            source_msg_id,
            source_user_name,
            source_session_id
        FROM facts
        WHERE source_msg_id = %s
          AND source_user_name = %s
          AND source_session_id = %s
          AND project_id = ANY(%s)
        ORDER BY valid_at DESC, fact_id
        """
        try:
            res = await self.client.fetch_all(
                query,
                (msg_id, user_name, session_id, visible_project_ids),
            )
            return [self._hydrate_fact(row) for row in res]
        except Exception as e:
            logger.error(f"Failed to get facts from message {msg_id}: {e}")
            return []

    async def get_recent_facts(
        self,
        *,
        visible_project_ids: List[str],
        days: int = 7,
        limit: int = 20,
    ) -> List[Dict]:
        visible_project_ids = require_visible_project_ids(
            visible_project_ids,
            "get_recent_facts",
        )
        cutoff = get_now() - timedelta(days=days)
        query = """
        SELECT
            f.fact_id AS id,
            f.content,
            f.valid_at AS created_at,
            e.canonical_name AS entity_name,
            e.type AS entity_type
        FROM facts f
        JOIN entities e ON e.entity_id = f.entity_id
        WHERE f.valid_at > %s
          AND f.invalid_at IS NULL
          AND f.project_id = ANY(%s)
        ORDER BY f.valid_at DESC
        LIMIT %s
        """
        try:
            res = await self.client.fetch_all(
                query,
                (cutoff, visible_project_ids, limit),
            )
            return [
                {
                    "id": str(self._clean_string(row["id"])),
                    "content": self._clean_string(row["content"]),
                    "created_at": (
                        self._parse_datetime(row["created_at"]).isoformat()
                    ),
                    "entity_name": self._clean_string(row["entity_name"]),
                    "entity_type": self._clean_string(row["entity_type"]),
                }
                for row in res
            ]
        except Exception as e:
            logger.error(f"Failed to get recent facts: {e}")
            return []
