import json
from typing import Any, Optional

from common.scoping import require_scope_value
from infrastructure.postgres_client import PostgresClient


class FactAuditReader:
    """Reads scoped fact-change audit history."""

    _JSON_FIELDS = (
        "source_msg_ids",
        "invalidated_fact_ids",
        "invalidated_fact_snapshots",
        "created_fact_ids",
        "metadata",
    )

    def __init__(self, client: PostgresClient):
        self.client = client

    @classmethod
    def _hydrate(cls, record: dict[str, Any]) -> dict[str, Any]:
        result = dict(record)
        for field in cls._JSON_FIELDS:
            value = result.get(field)
            if isinstance(value, str):
                try:
                    result[field] = json.loads(value)
                except json.JSONDecodeError:
                    result[field] = [] if field != "metadata" else {}
            elif value is None:
                result[field] = [] if field != "metadata" else {}
        return result

    @staticmethod
    def _pagination(limit: int, offset: int, operation: str) -> tuple[int, int]:
        if not isinstance(limit, int) or limit <= 0:
            raise ValueError(f"{operation} requires positive limit")
        if not isinstance(offset, int) or offset < 0:
            raise ValueError(f"{operation} requires non-negative offset")
        return limit, offset

    async def get_fact_change_audit(
        self,
        fact_change_id: str,
        *,
        user_name: str,
        project_id: str,
    ) -> Optional[dict[str, Any]]:
        operation = "get_fact_change_audit"
        fact_change_id = require_scope_value(
            fact_change_id,
            "fact_change_id",
            operation,
        )
        user_name = require_scope_value(user_name, "user_name", operation)
        project_id = require_scope_value(project_id, "project_id", operation)
        rows = await self.client.fetch_all(
            """
            SELECT *
            FROM fact_change_audits
            WHERE fact_change_id = %s
              AND user_name = %s
              AND project_id = %s
            """,
            (fact_change_id, user_name, project_id),
        )
        return self._hydrate(rows[0]) if rows else None

    async def list_fact_change_audits_for_entity(
        self,
        *,
        user_name: str,
        project_id: str,
        entity_id: int,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        operation = "list_fact_change_audits_for_entity"
        user_name = require_scope_value(user_name, "user_name", operation)
        project_id = require_scope_value(project_id, "project_id", operation)
        if not isinstance(entity_id, int) or entity_id <= 0:
            raise ValueError(f"{operation} requires positive entity_id")
        limit, offset = self._pagination(limit, offset, operation)
        rows = await self.client.fetch_all(
            """
            SELECT *
            FROM fact_change_audits
            WHERE user_name = %s
              AND project_id = %s
              AND entity_id = %s
            ORDER BY created_at DESC, fact_change_id DESC
            LIMIT %s OFFSET %s
            """,
            (user_name, project_id, entity_id, limit, offset),
        )
        return [self._hydrate(row) for row in rows]

    async def list_fact_change_audits_for_project(
        self,
        *,
        user_name: str,
        project_id: str,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        operation = "list_fact_change_audits_for_project"
        user_name = require_scope_value(user_name, "user_name", operation)
        project_id = require_scope_value(project_id, "project_id", operation)
        limit, offset = self._pagination(limit, offset, operation)
        rows = await self.client.fetch_all(
            """
            SELECT *
            FROM fact_change_audits
            WHERE user_name = %s
              AND project_id = %s
            ORDER BY created_at DESC, fact_change_id DESC
            LIMIT %s OFFSET %s
            """,
            (user_name, project_id, limit, offset),
        )
        return [self._hydrate(row) for row in rows]
