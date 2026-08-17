"""Durable conflict-group workflow over relationship-observation evidence."""

from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any, Iterable

from common.scoping import require_scope_value
from core.knowledge.conflicts import (
    ConflictGroup,
    ConflictOrigin,
    ConflictResolutionKind,
    ConflictWriteResult,
)
from core.knowledge.db.writers.human_review_writer import HumanReviewWriter


class ConflictWriter:
    """Creates, extends, and resolves conflict groups without mutating evidence."""

    def __init__(self, client, reviews: HumanReviewWriter | None = None) -> None:
        self.client = client
        self.reviews = reviews or HumanReviewWriter(client)

    async def record_detection(
        self,
        *,
        user_name: str,
        project_id: str,
        origin: ConflictOrigin,
        kind: str,
        rationale: str,
        confidence: float | None,
        evidence_ids: Iterable[int],
        metadata: dict[str, Any] | None = None,
        existing_conflict_id: str | None = None,
    ) -> ConflictWriteResult:
        user_name = require_scope_value(user_name, "user_name", "record_conflict")
        project_id = require_scope_value(
            project_id, "project_id", "record_conflict"
        )
        rationale = require_scope_value(rationale, "rationale", "record_conflict")
        if origin not in {
            "background_discovery",
            "agent_discovery",
            "user_created",
        }:
            raise ValueError("Unknown conflict origin")
        if kind not in {
            "possible_contradiction",
            "temporal_ambiguity",
            "possible_state_change",
            "identity_or_entity_ambiguity",
        }:
            raise ValueError("Unknown conflict kind")
        if confidence is not None and not 0.0 <= confidence <= 1.0:
            raise ValueError("Conflict confidence must be between zero and one")
        ids = sorted({int(value) for value in evidence_ids})
        if len(ids) < 2 or any(value <= 0 for value in ids):
            raise ValueError("A conflict requires at least two observation IDs")
        if existing_conflict_id is not None:
            existing_conflict_id = require_scope_value(
                existing_conflict_id, "existing_conflict_id", "record_conflict"
            )

        async with self.client.transaction() as cur:
            evidence = await self._load_evidence(
                cur,
                user_name=user_name,
                project_id=project_id,
                evidence_ids=ids,
            )
            if len(evidence) != len(ids):
                raise ValueError(
                    "Conflict evidence must be relationship observations in this project"
                )
            if existing_conflict_id is not None:
                return await self._extend_open_group(
                    cur,
                    conflict_id=existing_conflict_id,
                    user_name=user_name,
                    project_id=project_id,
                    origin=origin,
                    kind=kind,
                    rationale=rationale,
                    confidence=confidence,
                    evidence=evidence,
                    metadata=metadata,
                )

            containing_groups = await self._find_contained_open_groups(
                cur,
                user_name=user_name,
                project_id=project_id,
                evidence_ids=ids,
            )
            if len(containing_groups) == 1:
                return await self._extend_open_group(
                    cur,
                    conflict_id=containing_groups[0]["conflict_id"],
                    user_name=user_name,
                    project_id=project_id,
                    origin=origin,
                    kind=kind,
                    rationale=rationale,
                    confidence=confidence,
                    evidence=evidence,
                    metadata=metadata,
                )

            signature = self.evidence_signature(ids)
            await cur.execute(
                """
                INSERT INTO public.conflict_groups (
                    conflict_id, user_name, project_id, status, origin, kind,
                    rationale, confidence, evidence_signature, metadata
                )
                VALUES (%s, %s, %s, 'open', %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (user_name, project_id, evidence_signature) DO NOTHING
                RETURNING *
                """,
                (
                    str(uuid.uuid4()),
                    user_name,
                    project_id,
                    origin,
                    kind,
                    rationale,
                    confidence,
                    signature,
                    json.dumps(metadata or {}, sort_keys=True, default=str),
                ),
            )
            row = await cur.fetchone()
            if row is None:
                await cur.execute(
                    """
                    SELECT *
                    FROM public.conflict_groups
                    WHERE user_name = %s
                      AND project_id = %s
                      AND evidence_signature = %s
                    FOR UPDATE
                    """,
                    (user_name, project_id, signature),
                )
                row = await cur.fetchone()
                if row is None:
                    raise RuntimeError("Conflict group disappeared during deduplication")
                await cur.execute(
                    """
                    UPDATE public.conflict_groups
                    SET last_detected_at = now(), updated_at = now()
                    WHERE conflict_id = %s
                    """,
                    (row["conflict_id"],),
                )
                return ConflictWriteResult(group=self._group(row), created=False)

            added = await self._add_evidence(cur, row["conflict_id"], evidence)
            await self.reviews.open(
                user_name=user_name,
                project_id=project_id,
                kind="conflict_group",
                subject_type="conflict_group",
                subject_id=row["conflict_id"],
                priority="high",
                title="Possible conflicting relationship evidence",
                summary=rationale,
                metadata={
                    "origin": origin,
                    "kind": kind,
                    "evidence_ids": ids,
                },
                cur=cur,
            )
            return ConflictWriteResult(
                group=self._group(row),
                created=True,
                evidence_added=added,
            )

    async def resolve(
        self,
        *,
        conflict_id: str,
        user_name: str,
        project_id: str,
        resolution_kind: ConflictResolutionKind,
        resolved_by: str,
        resolution_note: str | None = None,
    ) -> ConflictGroup:
        conflict_id = require_scope_value(conflict_id, "conflict_id", "resolve_conflict")
        user_name = require_scope_value(user_name, "user_name", "resolve_conflict")
        project_id = require_scope_value(project_id, "project_id", "resolve_conflict")
        resolved_by = require_scope_value(
            resolved_by, "resolved_by", "resolve_conflict"
        )
        if resolution_kind not in {
            "confirmed_conflict",
            "normal_temporal_change",
            "not_a_conflict",
            "insufficient_evidence",
            "custom",
        }:
            raise ValueError("Unknown conflict resolution kind")
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                UPDATE public.conflict_groups
                SET status = 'resolved',
                    resolution_kind = %s,
                    resolution_note = %s,
                    resolved_by = %s,
                    resolved_at = now(),
                    updated_at = now()
                WHERE conflict_id = %s
                  AND user_name = %s
                  AND project_id = %s
                RETURNING *
                """,
                (
                    resolution_kind,
                    resolution_note,
                    resolved_by,
                    conflict_id,
                    user_name,
                    project_id,
                ),
            )
            row = await cur.fetchone()
            if row is None:
                raise ValueError("Unknown conflict group in this project")
            await self.reviews.resolve(
                user_name=user_name,
                project_id=project_id,
                kind="conflict_group",
                subject_id=conflict_id,
                cur=cur,
            )
            return self._group(row)

    @staticmethod
    def evidence_signature(evidence_ids: Iterable[int]) -> str:
        joined = ",".join(str(value) for value in sorted(set(evidence_ids)))
        return hashlib.sha256(joined.encode("utf-8")).hexdigest()

    async def _extend_open_group(
        self,
        cur,
        *,
        conflict_id: str,
        user_name: str,
        project_id: str,
        origin: ConflictOrigin,
        kind: str,
        rationale: str,
        confidence: float | None,
        evidence: list[dict[str, Any]],
        metadata: dict[str, Any] | None,
    ) -> ConflictWriteResult:
        await cur.execute(
            """
            SELECT *
            FROM public.conflict_groups
            WHERE conflict_id = %s
              AND user_name = %s
              AND project_id = %s
              AND status = 'open'
            FOR UPDATE
            """,
            (conflict_id, user_name, project_id),
        )
        row = await cur.fetchone()
        if row is None:
            raise ValueError("Only an open conflict group can receive new evidence")
        added = await self._add_evidence(cur, conflict_id, evidence)
        if added:
            evidence_ids = await self._live_evidence_ids(cur, conflict_id)
            await cur.execute(
                """
                UPDATE public.conflict_groups
                SET kind = %s,
                    rationale = %s,
                    confidence = %s,
                    evidence_signature = %s,
                    metadata = metadata || %s::jsonb,
                    last_detected_at = now(),
                    updated_at = now()
                WHERE conflict_id = %s
                RETURNING *
                """,
                (
                    kind,
                    rationale,
                    confidence,
                    self.evidence_signature(evidence_ids),
                    json.dumps(
                        {
                            **(metadata or {}),
                            "last_detection_origin": origin,
                        },
                        sort_keys=True,
                        default=str,
                    ),
                    conflict_id,
                ),
            )
            row = await cur.fetchone()
            await self.reviews.open(
                user_name=user_name,
                project_id=project_id,
                kind="conflict_group",
                subject_type="conflict_group",
                subject_id=conflict_id,
                priority="high",
                title="Possible conflicting relationship evidence",
                summary=rationale,
                metadata={
                    "origin": origin,
                    "kind": kind,
                    "evidence_ids": evidence_ids,
                },
                cur=cur,
            )
        return ConflictWriteResult(
            group=self._group(row),
            created=False,
            evidence_added=added,
        )

    async def _find_contained_open_groups(
        self,
        cur,
        *,
        user_name: str,
        project_id: str,
        evidence_ids: list[int],
    ) -> list[dict[str, Any]]:
        """Return up to two groups whose complete live evidence is submitted.

        One shared observation is too weak to merge independently discovered
        groups. A unique group whose evidence set is wholly contained in the
        new report is safe to extend; two candidates remain separate for human
        review instead of being merged by inference.
        """
        await cur.execute(
            """
            SELECT conflict.*
            FROM public.conflict_groups conflict
            WHERE conflict.user_name = %s
              AND conflict.project_id = %s
              AND conflict.status = 'open'
              AND EXISTS (
                  SELECT 1
                  FROM public.conflict_evidence_refs ref
                  WHERE ref.conflict_id = conflict.conflict_id
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM public.conflict_evidence_refs ref
                  WHERE ref.conflict_id = conflict.conflict_id
                    AND (
                        ref.observation_id IS NULL
                        OR ref.observation_id <> ALL(%s)
                    )
              )
            ORDER BY conflict.updated_at DESC
            LIMIT 2
            FOR UPDATE
            """,
            (user_name, project_id, evidence_ids),
        )
        return await cur.fetchall()

    async def _live_evidence_ids(self, cur, conflict_id: str) -> list[int]:
        """Return the current durable evidence set in stable signature order."""

        await cur.execute(
            """
            SELECT observation_id
            FROM public.conflict_evidence_refs
            WHERE conflict_id = %s
              AND observation_id IS NOT NULL
            ORDER BY observation_id
            """,
            (conflict_id,),
        )
        return [int(row["observation_id"]) for row in await cur.fetchall()]

    async def _load_evidence(
        self,
        cur,
        *,
        user_name: str,
        project_id: str,
        evidence_ids: list[int],
    ) -> list[dict[str, Any]]:
        await cur.execute(
            """
            SELECT
                observation.observation_id,
                observation.relationship_id,
                observation.message_id,
                observation.session_id,
                observation.source_entity_id,
                source.canonical_name AS source_entity_name,
                observation.target_entity_id,
                target.canonical_name AS target_entity_name,
                observation.observed_relationship_label,
                observation.canonical_relationship_type,
                observation.domain_status,
                observation.confidence,
                observation.context,
                observation.observed_at_ms
            FROM public.relationship_observations observation
            JOIN public.entities source ON source.entity_id = observation.source_entity_id
            JOIN public.entities target ON target.entity_id = observation.target_entity_id
            WHERE observation.user_name = %s
              AND observation.project_id = %s
              AND observation.observation_id = ANY(%s)
            ORDER BY observation.observed_at_ms, observation.observation_id
            """,
            (user_name, project_id, evidence_ids),
        )
        return await cur.fetchall()

    async def _add_evidence(
        self, cur, conflict_id: str, evidence: list[dict[str, Any]]
    ) -> int:
        added = 0
        for row in evidence:
            snapshot = self._snapshot(row)
            await cur.execute(
                """
                INSERT INTO public.conflict_evidence_refs (
                    conflict_id, observation_id, observation_snapshot
                )
                VALUES (%s, %s, %s::jsonb)
                ON CONFLICT (conflict_id, observation_id) DO NOTHING
                """,
                (
                    conflict_id,
                    row["observation_id"],
                    json.dumps(snapshot, sort_keys=True, default=str),
                ),
            )
            added += max(cur.rowcount, 0)
        return added

    @staticmethod
    def _snapshot(row: dict[str, Any]) -> dict[str, Any]:
        return {
            key: row.get(key)
            for key in (
                "observation_id",
                "relationship_id",
                "message_id",
                "session_id",
                "source_entity_id",
                "source_entity_name",
                "target_entity_id",
                "target_entity_name",
                "observed_relationship_label",
                "canonical_relationship_type",
                "domain_status",
                "confidence",
                "context",
                "observed_at_ms",
            )
        }

    @staticmethod
    def _group(row: dict[str, Any]) -> ConflictGroup:
        metadata = row.get("metadata") or {}
        if isinstance(metadata, str):
            metadata = json.loads(metadata)
        return ConflictGroup(
            conflict_id=row["conflict_id"],
            user_name=row["user_name"],
            project_id=row["project_id"],
            status=row["status"],
            origin=row["origin"],
            kind=row["kind"],
            rationale=row["rationale"],
            confidence=row.get("confidence"),
            evidence_signature=row["evidence_signature"],
            resolution_kind=row.get("resolution_kind"),
            resolution_note=row.get("resolution_note"),
            metadata=dict(metadata),
        )
