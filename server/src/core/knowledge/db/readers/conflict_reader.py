"""Scoped reads for user-led conflict review workflows."""

from __future__ import annotations

import json
from typing import Any

from common.scoping import require_scope_value


class ConflictReader:
    """Loads a conflict group with its immutable evidence snapshots."""

    def __init__(self, client) -> None:
        self.client = client

    async def get_detail(
        self,
        *,
        conflict_id: str,
        user_name: str,
        project_id: str,
    ) -> dict[str, Any] | None:
        conflict_id = require_scope_value(conflict_id, "conflict_id", "get_conflict")
        user_name = require_scope_value(user_name, "user_name", "get_conflict")
        project_id = require_scope_value(project_id, "project_id", "get_conflict")
        group = await self.client.fetch_one(
            """
            SELECT conflict_id, user_name, project_id, status, origin, kind,
                   rationale, confidence, evidence_signature, resolution_kind,
                   resolution_note, resolved_by, resolved_at, metadata,
                   created_at, updated_at, last_detected_at
            FROM public.conflict_groups
            WHERE conflict_id = %s
              AND user_name = %s
              AND project_id = %s
            """,
            (conflict_id, user_name, project_id),
        )
        if group is None:
            return None
        evidence = await self.client.fetch_all(
            """
            SELECT evidence_ref_id, observation_id, observation_snapshot, added_at
            FROM public.conflict_evidence_refs
            WHERE conflict_id = %s
            ORDER BY added_at, evidence_ref_id
            """,
            (conflict_id,),
        )
        detail = dict(group)
        for field in ("metadata",):
            if isinstance(detail.get(field), str):
                detail[field] = json.loads(detail[field])
        detail["evidence"] = []
        for row in evidence:
            item = dict(row)
            if isinstance(item.get("observation_snapshot"), str):
                item["observation_snapshot"] = json.loads(item["observation_snapshot"])
            detail["evidence"].append(item)
        return detail
