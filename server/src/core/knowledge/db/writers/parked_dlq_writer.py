"""Durable records for DLQ work that requires human attention."""

from __future__ import annotations

import json
from typing import Any

from common.scoping import require_scope_value
from core.knowledge.db.writers.human_review_writer import HumanReviewWriter


class ParkedDLQWriter:
    """Persists only parked DLQ work; Redis remains the operational queue."""

    def __init__(self, client, reviews: HumanReviewWriter | None = None) -> None:
        self.client = client
        self.reviews = reviews or HumanReviewWriter(client)

    async def park(
        self,
        *,
        dlq_id: str,
        user_name: str,
        project_id: str,
        entry: dict[str, Any],
    ) -> None:
        dlq_id = require_scope_value(dlq_id, "dlq_id", "park_dlq_item")
        user_name = require_scope_value(user_name, "user_name", "park_dlq_item")
        project_id = require_scope_value(project_id, "project_id", "park_dlq_item")
        payload = dict(entry)
        payload["dlq_id"] = dlq_id
        payload.setdefault("user_name", user_name)
        payload.setdefault("project_id", project_id)
        session_id = payload.get("session_id")
        if session_id is not None:
            session_id = str(session_id)
        stage = str(payload.get("stage") or "unknown")
        attempt = int(payload.get("attempt") or 0)
        error_message = str(payload.get("error") or "") or None
        async with self.client.transaction() as cur:
            await cur.execute(
                """
            INSERT INTO public.parked_dlq_items (
                dlq_id, user_name, project_id, session_id, stage, attempt,
                error_message, payload, status, parked_at, requeued_at,
                completed_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, 'parked', now(),
                    NULL, NULL, now())
            ON CONFLICT (dlq_id) DO UPDATE SET
                user_name = EXCLUDED.user_name,
                project_id = EXCLUDED.project_id,
                session_id = EXCLUDED.session_id,
                stage = EXCLUDED.stage,
                attempt = EXCLUDED.attempt,
                error_message = EXCLUDED.error_message,
                payload = EXCLUDED.payload,
                status = 'parked',
                parked_at = now(),
                requeued_at = NULL,
                completed_at = NULL,
                updated_at = now()
                """,
                (
                    dlq_id,
                    user_name,
                    project_id,
                    session_id,
                    stage,
                    attempt,
                    error_message,
                    json.dumps(payload, sort_keys=True, default=str),
                ),
            )
            await self.reviews.open(
                user_name=user_name,
                project_id=project_id,
                kind="parked_dlq",
                subject_type="parked_dlq_item",
                subject_id=dlq_id,
                priority="high",
                title=f"Parked ingestion work: {stage}",
                summary=error_message,
                metadata={
                    "session_id": session_id,
                    "stage": stage,
                    "attempt": attempt,
                },
                cur=cur,
            )

    async def get_parked(
        self, *, dlq_id: str, user_name: str, project_id: str
    ) -> dict[str, Any] | None:
        rows = await self.client.fetch_all(
            """
            SELECT payload
            FROM public.parked_dlq_items
            WHERE dlq_id = %s
              AND user_name = %s
              AND project_id = %s
              AND status = 'parked'
            """,
            (dlq_id, user_name, project_id),
        )
        if not rows:
            return None
        payload = rows[0]["payload"]
        return json.loads(payload) if isinstance(payload, str) else dict(payload)

    async def list_requeued(self, *, user_name: str, project_id: str) -> list[dict[str, Any]]:
        rows = await self.client.fetch_all(
            """
            SELECT payload FROM public.parked_dlq_items
            WHERE user_name = %s AND project_id = %s AND status = 'requeued'
            ORDER BY requeued_at
            """,
            (user_name, project_id),
        )
        return [
            json.loads(row["payload"]) if isinstance(row["payload"], str) else dict(row["payload"])
            for row in rows
        ]

    async def mark_requeued(
        self, *, dlq_id: str, user_name: str, project_id: str
    ) -> bool:
        return await self._transition(
            dlq_id=dlq_id,
            user_name=user_name,
            project_id=project_id,
            from_status="parked",
            to_status="requeued",
        )

    async def mark_completed_if_requeued(
        self, *, dlq_id: str, user_name: str, project_id: str
    ) -> bool:
        return await self._transition(
            dlq_id=dlq_id,
            user_name=user_name,
            project_id=project_id,
            from_status="requeued",
            to_status="completed",
        )

    async def _transition(
        self,
        *,
        dlq_id: str,
        user_name: str,
        project_id: str,
        from_status: str,
        to_status: str,
    ) -> bool:
        async with self.client.transaction() as cur:
            await cur.execute(
                """
            UPDATE public.parked_dlq_items
            SET status = %s,
                requeued_at = CASE WHEN %s = 'requeued' THEN now() ELSE requeued_at END,
                completed_at = CASE WHEN %s = 'completed' THEN now() ELSE completed_at END,
                updated_at = now()
            WHERE dlq_id = %s
              AND user_name = %s
              AND project_id = %s
              AND status = %s
            RETURNING dlq_id
                """,
                (
                    to_status,
                    to_status,
                    to_status,
                    dlq_id,
                    user_name,
                    project_id,
                    from_status,
                ),
            )
            transitioned = await cur.fetchone()
            if transitioned and to_status == "requeued":
                await self.reviews.resolve(
                    user_name=user_name,
                    project_id=project_id,
                    kind="parked_dlq",
                    subject_id=dlq_id,
                    cur=cur,
                )
        return bool(transitioned)
