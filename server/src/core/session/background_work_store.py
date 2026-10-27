"""Durable records of background work, so a restart does not lose it."""

from __future__ import annotations

import json
from typing import Any, Literal

BackgroundKind = Literal["research_job", "folder_notice"]


class BackgroundWorkStore:
    """One row per research job or folder notice. The owning service keeps
    its live state in memory and writes it here at each change."""

    def __init__(self, postgres, user_name: str) -> None:
        self._postgres = postgres
        self._user_name = user_name

    async def save(
        self, kind: BackgroundKind, work_id: str, *, project_id: str,
        session_id: str, state: str, payload: dict[str, Any],
    ) -> None:
        await self._postgres.execute(
            """INSERT INTO public.background_work
                   (work_id, user_name, kind, project_id, session_id, state, payload)
               VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
               ON CONFLICT (work_id) DO UPDATE
               SET state = EXCLUDED.state, payload = EXCLUDED.payload, updated_at = now()""",
            (work_id, self._user_name, kind, project_id, session_id, state,
             json.dumps(payload, default=str)),
        )

    async def delete(self, work_id: str) -> None:
        await self._postgres.execute(
            "DELETE FROM public.background_work WHERE user_name = %s AND work_id = %s",
            (self._user_name, work_id),
        )

    async def load(self, kind: BackgroundKind, *, limit: int = 500) -> list[dict[str, Any]]:
        """The newest rows of one kind, oldest first."""

        rows = await self._postgres.fetch_all(
            """SELECT work_id, project_id, session_id, state, payload
               FROM public.background_work
               WHERE user_name = %s AND kind = %s
               ORDER BY created_at DESC LIMIT %s""",
            (self._user_name, kind, limit),
        )
        return list(reversed(rows))
