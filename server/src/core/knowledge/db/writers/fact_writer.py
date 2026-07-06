import json
from datetime import datetime
from typing import Any, List, Optional

from loguru import logger

from common.schema.primitives import FactRecord
from common.scoping import require_scope_value
from common.utils.time_utils import get_now
from infrastructure.postgres_client import PostgresClient
from core.knowledge.db.writers.age_projection_writer import (
    AgeProjectionWriter,
)
from core.knowledge.db.writers.fact_audit_writer import FactAuditWriter


class FactWriter:
    def __init__(self, client: PostgresClient, graph_name: str = "knoggin_graph"):
        self.client = client
        self.graph_name = graph_name
        self.projection = AgeProjectionWriter(client, graph_name=graph_name)
        self.audit_writer = FactAuditWriter(client)

    @staticmethod
    def _clean_string(value):
        if isinstance(value, str):
            return value.strip('"')
        return value

    @staticmethod
    def _fact_params(
        fact: FactRecord,
        *,
        user_name: str,
        session_id: Optional[str],
    ) -> dict[str, Any]:
        source_user_name = fact.source_user_name or user_name
        source_session_id = fact.source_session_id or session_id
        if fact.source_msg_id is not None and (
            not source_user_name or not source_session_id
        ):
            raise ValueError(
                "Cannot link facts to source messages without user/session scope"
            )

        return {
            "id": fact.id,
            "content": fact.content,
            "valid_at": fact.valid_at.isoformat(),
            "invalid_at": fact.invalid_at.isoformat() if fact.invalid_at else None,
            "confidence": fact.confidence,
            "source_msg_id": fact.source_msg_id,
            "source_user_name": source_user_name,
            "source_session_id": source_session_id,
            "source": fact.source,
        }

    @classmethod
    def _fact_snapshot(cls, record: dict) -> dict[str, Any]:
        return {
            "fact_id": str(cls._clean_string(record["fact_id"])),
            "entity_id": int(record["entity_id"]),
            "user_name": cls._clean_string(record["user_name"]),
            "project_id": cls._clean_string(record["project_id"]),
            "content": cls._clean_string(record["content"]),
            "valid_at": record.get("valid_at"),
            "invalid_at": record.get("invalid_at"),
            "confidence": float(record.get("confidence", 1.0)),
            "source_msg_id": record.get("source_msg_id"),
            "source_user_name": cls._clean_string(record.get("source_user_name")),
            "source_session_id": cls._clean_string(record.get("source_session_id")),
            "source": cls._clean_string(record.get("source")),
        }

    async def _insert_fact_with_cursor(
        self,
        cur,
        *,
        entity_id: int,
        fact: FactRecord,
        user_name: str,
        project_id: str,
        session_id: Optional[str] = None,
    ) -> dict[str, Any]:
        item = self._fact_params(fact, user_name=user_name, session_id=session_id)
        await cur.execute(
            """
            INSERT INTO facts (
                fact_id,
                entity_id,
                user_name,
                project_id,
                content,
                valid_at,
                invalid_at,
                confidence,
                source_msg_id,
                source_user_name,
                source_session_id,
                source
            )
            SELECT
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            WHERE EXISTS (
                SELECT 1
                FROM entities
                WHERE entity_id = %s
                  AND project_id = %s
            )
            ON CONFLICT (fact_id) DO UPDATE SET
                entity_id = EXCLUDED.entity_id,
                user_name = EXCLUDED.user_name,
                project_id = EXCLUDED.project_id,
                content = EXCLUDED.content,
                valid_at = EXCLUDED.valid_at,
                invalid_at = EXCLUDED.invalid_at,
                confidence = EXCLUDED.confidence,
                source_msg_id = EXCLUDED.source_msg_id,
                source_user_name = EXCLUDED.source_user_name,
                source_session_id = EXCLUDED.source_session_id,
                source = EXCLUDED.source
            RETURNING fact_id
            """,
            (
                item["id"],
                entity_id,
                user_name,
                project_id,
                item["content"],
                item["valid_at"],
                item["invalid_at"],
                item["confidence"],
                item["source_msg_id"],
                item["source_user_name"],
                item["source_session_id"],
                item["source"],
                entity_id,
                project_id,
            ),
        )
        record = await cur.fetchone()
        if not record:
            raise ValueError(
                f"Failed to create fact for entity {entity_id} under project scope"
            )

        projected_count = await self.projection.project_facts(
            cur,
            entity_id,
            [item],
            user_name,
            session_id,
            project_id,
        )
        if projected_count == 0:
            raise Exception(f"Failed to project fact for entity {entity_id}")
        await self.projection.project_fact_message_links(cur, [item])

        if fact.embedding:
            await cur.execute(
                """
                INSERT INTO fact_search (
                    fact_id,
                    entity_id,
                    user_name,
                    project_id,
                    embedding,
                    invalid_at
                )
                VALUES (%s, %s, %s, %s, %s::vector, %s)
                ON CONFLICT (fact_id) DO UPDATE SET
                    entity_id = EXCLUDED.entity_id,
                    user_name = EXCLUDED.user_name,
                    project_id = EXCLUDED.project_id,
                    invalid_at = EXCLUDED.invalid_at,
                    embedding = COALESCE(
                        EXCLUDED.embedding,
                        fact_search.embedding
                    )
                """,
                (
                    fact.id,
                    entity_id,
                    user_name,
                    project_id,
                    json.dumps(fact.embedding),
                    fact.invalid_at,
                ),
            )
        return item

    async def _invalidate_fact_ids_with_cursor(
        self,
        cur,
        fact_ids: list[str],
        invalid_at: datetime,
        *,
        project_id: str,
    ) -> None:
        if not fact_ids:
            return

        await cur.execute(
            """
            UPDATE facts
            SET invalid_at = %s
            WHERE fact_id = ANY(%s)
              AND project_id = %s
            """,
            (invalid_at, fact_ids, project_id),
        )
        for fact_id in fact_ids:
            await self.projection.invalidate_fact(
                cur,
                fact_id,
                invalid_at.isoformat(),
                project_id,
            )
        await cur.execute(
            """
            UPDATE fact_search
            SET invalid_at = %s
            WHERE fact_id = ANY(%s)
              AND project_id = %s
            """,
            (invalid_at, fact_ids, project_id),
        )

    async def create_facts_batch(
        self,
        entity_id: int,
        facts: List[FactRecord],
        *,
        user_name: str,
        project_id: str,
        session_id: Optional[str] = None,
    ) -> int:
        user_name = require_scope_value(
            user_name, "user_name", "create_facts_batch"
        )
        project_id = require_scope_value(
            project_id, "project_id", "create_facts_batch"
        )
        if not facts:
            return 0

        fact_params = [
            self._fact_params(f, user_name=user_name, session_id=session_id)
            for f in facts
        ]

        async with self.client.transaction() as cur:
            count = 0
            for item in fact_params:
                await cur.execute(
                    """
                    INSERT INTO facts (
                        fact_id,
                        entity_id,
                        user_name,
                        project_id,
                        content,
                        valid_at,
                        invalid_at,
                        confidence,
                        source_msg_id,
                        source_user_name,
                        source_session_id,
                        source
                    )
                    SELECT
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    WHERE EXISTS (
                        SELECT 1
                        FROM entities
                        WHERE entity_id = %s
                          AND project_id = %s
                    )
                    ON CONFLICT (fact_id) DO UPDATE SET
                        entity_id = EXCLUDED.entity_id,
                        user_name = EXCLUDED.user_name,
                        project_id = EXCLUDED.project_id,
                        content = EXCLUDED.content,
                        valid_at = EXCLUDED.valid_at,
                        invalid_at = EXCLUDED.invalid_at,
                        confidence = EXCLUDED.confidence,
                        source_msg_id = EXCLUDED.source_msg_id,
                        source_user_name = EXCLUDED.source_user_name,
                        source_session_id = EXCLUDED.source_session_id,
                        source = EXCLUDED.source
                    RETURNING fact_id
                    """,
                    (
                        item["id"],
                        entity_id,
                        user_name,
                        project_id,
                        item["content"],
                        item["valid_at"],
                        item["invalid_at"],
                        item["confidence"],
                        item["source_msg_id"],
                        item["source_user_name"],
                        item["source_session_id"],
                        item["source"],
                        entity_id,
                        project_id,
                    ),
                )
                record = await cur.fetchone()
                if record:
                    count += 1

            if count == 0:
                raise Exception(
                    "Failed to create facts for entity "
                    f"{entity_id} (parent may not exist)"
                )

            projected_count = await self.projection.project_facts(
                cur,
                entity_id,
                fact_params,
                user_name,
                session_id,
                project_id,
            )
            if projected_count == 0:
                raise Exception(
                    f"Failed to project facts for entity {entity_id}"
                )
            await self.projection.project_fact_message_links(cur, fact_params)

            # Write to Postgres fact_search table (Vectors)
            for f in facts:
                if f.embedding:
                    await cur.execute(
                        """
                        INSERT INTO fact_search (
                            fact_id,
                            entity_id,
                            user_name,
                            project_id,
                            embedding,
                            invalid_at
                        )
                        VALUES (%s, %s, %s, %s, %s::vector, %s)
                        ON CONFLICT (fact_id) DO UPDATE SET
                            entity_id = EXCLUDED.entity_id,
                            user_name = EXCLUDED.user_name,
                            project_id = EXCLUDED.project_id,
                            invalid_at = EXCLUDED.invalid_at,
                            embedding = COALESCE(
                                EXCLUDED.embedding,
                                fact_search.embedding
                            )
                        """,
                        (
                            f.id,
                            entity_id,
                            user_name,
                            project_id,
                            json.dumps(f.embedding),
                            f.invalid_at,
                        ),
            )

        return count

    async def invalidate_fact(
        self, fact_id: str, invalid_at: datetime, *, project_id: str
    ) -> bool:
        project_id = require_scope_value(
            project_id, "project_id", "invalidate_fact"
        )
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                UPDATE facts
                SET invalid_at = %s
                WHERE fact_id = %s
                  AND project_id = %s
                RETURNING fact_id
                """,
                (invalid_at, fact_id, project_id),
            )
            record = await cur.fetchone()

            if record:
                await self.projection.invalidate_fact(
                    cur,
                    fact_id,
                    invalid_at.isoformat(),
                    project_id,
                )
                await cur.execute(
                    """
                    UPDATE fact_search
                    SET invalid_at = %s
                    WHERE fact_id = %s
                      AND project_id = %s
                    """,
                    (invalid_at, fact_id, project_id),
                )
                return True
        return False

    async def remove_fact_with_audit(
        self,
        *,
        fact_change_id: str,
        user_name: str,
        project_id: str,
        entity_id: int,
        fact_id: str,
        actor: str,
        change_type: str,
        reason: str,
        session_id: Optional[str] = None,
    ) -> dict:
        fact_change_id = require_scope_value(
            fact_change_id, "fact_change_id", "remove_fact_with_audit"
        )
        user_name = require_scope_value(
            user_name, "user_name", "remove_fact_with_audit"
        )
        project_id = require_scope_value(
            project_id, "project_id", "remove_fact_with_audit"
        )
        actor = require_scope_value(actor, "actor", "remove_fact_with_audit")
        change_type = require_scope_value(
            change_type, "change_type", "remove_fact_with_audit"
        )
        fact_id = require_scope_value(fact_id, "fact_id", "remove_fact_with_audit")
        if entity_id <= 0:
            raise ValueError("remove_fact_with_audit requires positive entity_id")

        invalid_at = get_now()
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                SELECT *
                FROM facts
                WHERE user_name = %s
                  AND project_id = %s
                  AND entity_id = %s
                  AND fact_id = %s
                  AND invalid_at IS NULL
                FOR UPDATE
                """,
                (user_name, project_id, entity_id, fact_id),
            )
            record = await cur.fetchone()
            if not record:
                raise ValueError("No active scoped fact found for removal")

            snapshot = self._fact_snapshot(record)
            source_msg_ids = (
                [int(snapshot["source_msg_id"])]
                if snapshot.get("source_msg_id") is not None
                else []
            )
            await self.audit_writer.create_applying_audit_with_cursor(
                cur,
                fact_change_id=fact_change_id,
                user_name=user_name,
                project_id=project_id,
                entity_id=entity_id,
                actor=actor,
                change_type=change_type,
                reason=reason,
                session_id=session_id,
                source_msg_ids=source_msg_ids,
                invalidated_fact_ids=[fact_id],
                invalidated_fact_snapshots=[snapshot],
                created_fact_ids=[],
                replacement_content=None,
            )
            await self._invalidate_fact_ids_with_cursor(
                cur, [fact_id], invalid_at, project_id=project_id
            )
            await self.audit_writer.mark_applied_with_cursor(
                cur,
                fact_change_id,
                invalidated_fact_ids=[fact_id],
                created_fact_ids=[],
            )

        return {
            "fact_change_id": fact_change_id,
            "entity_id": entity_id,
            "invalidated_fact_ids": [fact_id],
            "created_fact_ids": [],
        }

    async def replace_facts_with_audit(
        self,
        *,
        fact_change_id: str,
        user_name: str,
        project_id: str,
        entity_id: int,
        fact_ids: list[str],
        actor: str,
        change_type: str,
        reason: str,
        replacement_fact: Optional[FactRecord] = None,
        replacement_content: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> dict:
        fact_change_id = require_scope_value(
            fact_change_id, "fact_change_id", "replace_facts_with_audit"
        )
        user_name = require_scope_value(
            user_name, "user_name", "replace_facts_with_audit"
        )
        project_id = require_scope_value(
            project_id, "project_id", "replace_facts_with_audit"
        )
        actor = require_scope_value(actor, "actor", "replace_facts_with_audit")
        change_type = require_scope_value(
            change_type, "change_type", "replace_facts_with_audit"
        )
        if entity_id <= 0:
            raise ValueError("replace_facts_with_audit requires positive entity_id")
        if not fact_ids:
            raise ValueError("replace_facts_with_audit requires fact_ids")

        scoped_fact_ids = [
            require_scope_value(fid, "fact_id", "replace_facts_with_audit")
            for fid in fact_ids
        ]
        if len(set(scoped_fact_ids)) != len(scoped_fact_ids):
            raise ValueError("replace_facts_with_audit rejects duplicate fact_ids")

        invalid_at = get_now()
        created_fact_ids = [replacement_fact.id] if replacement_fact else []
        final_replacement_content = (
            replacement_content
            if replacement_content is not None
            else replacement_fact.content if replacement_fact else None
        )

        async with self.client.transaction() as cur:
            await cur.execute(
                """
                SELECT *
                FROM facts
                WHERE user_name = %s
                  AND project_id = %s
                  AND entity_id = %s
                  AND fact_id = ANY(%s)
                  AND invalid_at IS NULL
                FOR UPDATE
                """,
                (user_name, project_id, entity_id, scoped_fact_ids),
            )
            records = await cur.fetchall()
            found_ids = {str(self._clean_string(row["fact_id"])) for row in records}
            missing_ids = [fid for fid in scoped_fact_ids if fid not in found_ids]
            if missing_ids:
                raise ValueError(
                    "Missing active scoped facts for replacement: "
                    + ", ".join(missing_ids)
                )

            snapshots = [self._fact_snapshot(row) for row in records]
            source_msg_ids = [
                int(snapshot["source_msg_id"])
                for snapshot in snapshots
                if snapshot.get("source_msg_id") is not None
            ]
            await self.audit_writer.create_applying_audit_with_cursor(
                cur,
                fact_change_id=fact_change_id,
                user_name=user_name,
                project_id=project_id,
                entity_id=entity_id,
                actor=actor,
                change_type=change_type,
                reason=reason,
                session_id=session_id,
                source_msg_ids=source_msg_ids,
                invalidated_fact_ids=scoped_fact_ids,
                invalidated_fact_snapshots=snapshots,
                created_fact_ids=created_fact_ids,
                replacement_content=final_replacement_content,
            )
            if replacement_fact:
                await self._insert_fact_with_cursor(
                    cur,
                    entity_id=entity_id,
                    fact=replacement_fact,
                    user_name=user_name,
                    project_id=project_id,
                    session_id=session_id,
                )
            await self._invalidate_fact_ids_with_cursor(
                cur, scoped_fact_ids, invalid_at, project_id=project_id
            )
            await self.audit_writer.mark_applied_with_cursor(
                cur,
                fact_change_id,
                invalidated_fact_ids=scoped_fact_ids,
                created_fact_ids=created_fact_ids,
            )

        return {
            "fact_change_id": fact_change_id,
            "entity_id": entity_id,
            "invalidated_fact_ids": scoped_fact_ids,
            "created_fact_ids": created_fact_ids,
        }

    async def delete_old_invalidated_facts(
        self, cutoff: datetime, *, project_id: str
    ) -> int:
        project_id = require_scope_value(
            project_id,
            "project_id",
            "delete_old_invalidated_facts",
        )
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                DELETE FROM facts
                WHERE invalid_at IS NOT NULL
                  AND invalid_at < %s
                  AND project_id = %s
                RETURNING fact_id
                """,
                (cutoff, project_id),
            )
            records = await cur.fetchall()
            deleted_ids = [
                str(self._clean_string(r["fact_id"]))
                for r in records
            ]

            if deleted_ids:
                await self.projection.delete_facts(
                    cur,
                    deleted_ids,
                    project_id,
                )
                logger.info(f"Deleted {len(deleted_ids)} old invalidated facts")

        return len(deleted_ids) if records else 0
