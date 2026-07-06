import json
from typing import Any

from infrastructure.postgres_client import PostgresClient


class FactAuditWriter:
    """Writes durable fact-change audit state."""

    def __init__(self, client: PostgresClient):
        self.client = client

    @staticmethod
    def _insert_params(
        *,
        fact_change_id: str,
        user_name: str,
        project_id: str,
        entity_id: int,
        actor: str,
        change_type: str,
        reason: str | None,
        session_id: str | None,
        source_msg_ids: list[int] | None,
        invalidated_fact_ids: list[str] | None,
        invalidated_fact_snapshots: list[dict[str, Any]] | None,
        created_fact_ids: list[str] | None,
        replacement_content: str | None,
        metadata: dict[str, Any] | None,
    ) -> tuple:
        return (
            fact_change_id,
            user_name,
            project_id,
            entity_id,
            session_id,
            actor,
            change_type,
            reason,
            json.dumps(source_msg_ids or []),
            json.dumps(invalidated_fact_ids or []),
            json.dumps(invalidated_fact_snapshots or [], default=str),
            json.dumps(created_fact_ids or []),
            replacement_content,
            json.dumps(metadata or {}, default=str),
        )

    @staticmethod
    def _insert_sql() -> str:
        return """
        INSERT INTO fact_change_audits (
            fact_change_id,
            user_name,
            project_id,
            entity_id,
            session_id,
            actor,
            change_type,
            reason,
            source_msg_ids,
            invalidated_fact_ids,
            invalidated_fact_snapshots,
            created_fact_ids,
            replacement_content,
            metadata,
            status
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s::jsonb,
            'applying'
        )
        """

    async def create_applying_audit_with_cursor(
        self,
        cur,
        *,
        fact_change_id: str,
        user_name: str,
        project_id: str,
        entity_id: int,
        actor: str,
        change_type: str,
        reason: str | None = None,
        session_id: str | None = None,
        source_msg_ids: list[int] | None = None,
        invalidated_fact_ids: list[str] | None = None,
        invalidated_fact_snapshots: list[dict[str, Any]] | None = None,
        created_fact_ids: list[str] | None = None,
        replacement_content: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        await cur.execute(
            self._insert_sql(),
            self._insert_params(
                fact_change_id=fact_change_id,
                user_name=user_name,
                project_id=project_id,
                entity_id=entity_id,
                actor=actor,
                change_type=change_type,
                reason=reason,
                session_id=session_id,
                source_msg_ids=source_msg_ids,
                invalidated_fact_ids=invalidated_fact_ids,
                invalidated_fact_snapshots=invalidated_fact_snapshots,
                created_fact_ids=created_fact_ids,
                replacement_content=replacement_content,
                metadata=metadata,
            ),
        )

    async def create_applying_audit(
        self,
        *,
        fact_change_id: str,
        user_name: str,
        project_id: str,
        entity_id: int,
        actor: str,
        change_type: str,
        reason: str | None = None,
        session_id: str | None = None,
        source_msg_ids: list[int] | None = None,
        invalidated_fact_ids: list[str] | None = None,
        invalidated_fact_snapshots: list[dict[str, Any]] | None = None,
        created_fact_ids: list[str] | None = None,
        replacement_content: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        await self.client.execute(
            self._insert_sql(),
            self._insert_params(
                fact_change_id=fact_change_id,
                user_name=user_name,
                project_id=project_id,
                entity_id=entity_id,
                actor=actor,
                change_type=change_type,
                reason=reason,
                session_id=session_id,
                source_msg_ids=source_msg_ids,
                invalidated_fact_ids=invalidated_fact_ids,
                invalidated_fact_snapshots=invalidated_fact_snapshots,
                created_fact_ids=created_fact_ids,
                replacement_content=replacement_content,
                metadata=metadata,
            ),
        )

    async def create_applied_audit(
        self,
        *,
        fact_change_id: str,
        user_name: str,
        project_id: str,
        entity_id: int,
        actor: str,
        change_type: str,
        reason: str | None = None,
        session_id: str | None = None,
        source_msg_ids: list[int] | None = None,
        invalidated_fact_ids: list[str] | None = None,
        invalidated_fact_snapshots: list[dict[str, Any]] | None = None,
        created_fact_ids: list[str] | None = None,
        replacement_content: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        await self.create_applying_audit(
            fact_change_id=fact_change_id,
            user_name=user_name,
            project_id=project_id,
            entity_id=entity_id,
            actor=actor,
            change_type=change_type,
            reason=reason,
            session_id=session_id,
            source_msg_ids=source_msg_ids,
            invalidated_fact_ids=invalidated_fact_ids,
            invalidated_fact_snapshots=invalidated_fact_snapshots,
            created_fact_ids=created_fact_ids,
            replacement_content=replacement_content,
            metadata=metadata,
        )
        await self.mark_applied(
            fact_change_id,
            invalidated_fact_ids=invalidated_fact_ids,
            created_fact_ids=created_fact_ids,
        )

    @staticmethod
    def _mark_applied_sql() -> str:
        return """
        UPDATE fact_change_audits
        SET status = 'applied',
            failure_reason = NULL,
            invalidated_fact_ids = COALESCE(
                %s::jsonb,
                invalidated_fact_ids
            ),
            created_fact_ids = COALESCE(%s::jsonb, created_fact_ids)
        WHERE fact_change_id = %s
        """

    @staticmethod
    def _mark_applied_params(
        fact_change_id: str,
        *,
        invalidated_fact_ids: list[str] | None,
        created_fact_ids: list[str] | None,
    ) -> tuple:
        return (
            json.dumps(invalidated_fact_ids)
            if invalidated_fact_ids is not None
            else None,
            json.dumps(created_fact_ids) if created_fact_ids is not None else None,
            fact_change_id,
        )

    async def mark_applied_with_cursor(
        self,
        cur,
        fact_change_id: str,
        *,
        invalidated_fact_ids: list[str] | None = None,
        created_fact_ids: list[str] | None = None,
    ) -> None:
        await cur.execute(
            self._mark_applied_sql(),
            self._mark_applied_params(
                fact_change_id,
                invalidated_fact_ids=invalidated_fact_ids,
                created_fact_ids=created_fact_ids,
            ),
        )

    async def mark_applied(
        self,
        fact_change_id: str,
        *,
        invalidated_fact_ids: list[str] | None = None,
        created_fact_ids: list[str] | None = None,
    ) -> None:
        await self.client.execute(
            self._mark_applied_sql(),
            self._mark_applied_params(
                fact_change_id,
                invalidated_fact_ids=invalidated_fact_ids,
                created_fact_ids=created_fact_ids,
            ),
        )

    async def mark_failed(self, fact_change_id: str, reason: str) -> None:
        await self.client.execute(
            """
            UPDATE fact_change_audits
            SET status = 'failed',
                failure_reason = %s
            WHERE fact_change_id = %s
            """,
            (reason, fact_change_id),
        )
