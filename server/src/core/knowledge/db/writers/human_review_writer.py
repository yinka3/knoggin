"""Unified inbox records that point to workflow-owned review subjects."""

from __future__ import annotations

import json
import uuid
from typing import Any

from common.scoping import require_scope_value
from core.knowledge.human_reviews import HumanReview


class HumanReviewWriter:
    """Open and resolve inbox entries without owning workflow decisions."""

    _NAMESPACE = uuid.UUID("0d7dc0f7-d5e7-4eb0-902a-9967a21121da")

    def __init__(self, client) -> None:
        self.client = client

    async def open(
        self,
        *,
        user_name: str,
        project_id: str,
        kind: str,
        subject_type: str,
        subject_id: str,
        title: str,
        summary: str | None = None,
        priority: str = "normal",
        metadata: dict[str, Any] | None = None,
        cur=None,
    ) -> str:
        user_name = require_scope_value(user_name, "user_name", "open_human_review")
        project_id = require_scope_value(
            project_id, "project_id", "open_human_review"
        )
        kind = require_scope_value(kind, "kind", "open_human_review")
        subject_type = require_scope_value(
            subject_type, "subject_type", "open_human_review"
        )
        subject_id = require_scope_value(
            subject_id, "subject_id", "open_human_review"
        )
        if priority not in {"low", "normal", "high"}:
            raise ValueError("Human review priority must be low, normal, or high")
        review_id = self._review_id(user_name, project_id, kind, subject_id)
        params = (
            review_id,
            user_name,
            project_id,
            kind,
            subject_type,
            subject_id,
            priority,
            title,
            summary,
            json.dumps(metadata or {}, sort_keys=True, default=str),
        )
        query = """
            INSERT INTO public.human_reviews (
                review_id, user_name, project_id, kind, subject_type, subject_id,
                status, priority, title, summary, metadata, created_at, updated_at,
                resolved_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, 'open', %s, %s, %s, %s::jsonb,
                    now(), now(), NULL)
            ON CONFLICT (user_name, project_id, kind, subject_id) DO UPDATE SET
                subject_type = EXCLUDED.subject_type,
                status = 'open',
                priority = EXCLUDED.priority,
                title = EXCLUDED.title,
                summary = EXCLUDED.summary,
                metadata = EXCLUDED.metadata,
                updated_at = now(),
                resolved_at = NULL
        """
        await self._execute(cur, query, params)
        return review_id

    async def resolve(
        self,
        *,
        user_name: str,
        project_id: str,
        kind: str,
        subject_id: str,
        cur=None,
    ) -> None:
        await self._execute(
            cur,
            """
            UPDATE public.human_reviews
            SET status = 'resolved', resolved_at = now(), updated_at = now()
            WHERE user_name = %s
              AND project_id = %s
              AND kind = %s
              AND subject_id = %s
              AND status = 'open'
            """,
            (user_name, project_id, kind, subject_id),
        )

    async def list_open(
        self, *, user_name: str, project_id: str
    ) -> list[HumanReview]:
        rows = await self.client.fetch_all(
            """
            SELECT review_id, kind, subject_type, subject_id, priority, title,
                   summary, metadata, created_at, updated_at
            FROM public.human_reviews
            WHERE user_name = %s AND project_id = %s AND status = 'open'
            ORDER BY
                CASE priority WHEN 'high' THEN 0 WHEN 'normal' THEN 1 ELSE 2 END,
                created_at DESC
            """,
            (user_name, project_id),
        )
        return [
            HumanReview(
                review_id=row["review_id"],
                user_name=user_name,
                project_id=project_id,
                kind=row["kind"],
                subject_type=row["subject_type"],
                subject_id=row["subject_id"],
                status="open",
                priority=row["priority"],
                title=row["title"],
                summary=row.get("summary"),
                metadata=self._metadata(row.get("metadata")),
            )
            for row in rows
        ]

    @staticmethod
    def _review_id(user_name: str, project_id: str, kind: str, subject_id: str) -> str:
        return str(
            uuid.uuid5(
                HumanReviewWriter._NAMESPACE,
                f"{user_name}:{project_id}:{kind}:{subject_id}",
            )
        )

    @staticmethod
    def _metadata(value: Any) -> dict[str, Any]:
        if isinstance(value, str):
            value = json.loads(value)
        return dict(value or {})

    async def _execute(self, cur, query: str, params: tuple) -> None:
        if cur is None:
            await self.client.execute(query, params)
        else:
            await cur.execute(query, params)
