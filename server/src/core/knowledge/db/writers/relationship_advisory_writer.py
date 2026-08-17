"""Durable state transitions for relationship advisories."""

from __future__ import annotations

from common.scoping import require_scope_value
from core.knowledge.relationship_advisories import (
    RelationshipAdvisory,
    RelationshipAdvisoryDecision,
    apply_advisory_action,
)
from core.knowledge.db.writers.human_review_writer import HumanReviewWriter


class RelationshipAdvisoryWriter:
    """Persist current advisory state and an append-only decision audit."""

    def __init__(self, client, reviews: HumanReviewWriter | None = None):
        self.client = client
        self.reviews = reviews or HumanReviewWriter(client)

    async def materialize_pending(
        self,
        *,
        user_name: str,
        project_id: str,
        advisory: RelationshipAdvisory,
    ) -> None:
        """Make an evidence-backed advisory a durable review subject once."""

        if advisory.disposition != "pending":
            return
        async with self.client.transaction() as cur:
            await cur.execute(
                """
                INSERT INTO relationship_advisories (
                    user_name, project_id, pattern_key, disposition
                )
                VALUES (%s, %s, %s, 'pending')
                ON CONFLICT (user_name, project_id, pattern_key) DO NOTHING
                """,
                (user_name, project_id, advisory.pattern_key),
            )
            await self.reviews.open(
                user_name=user_name,
                project_id=project_id,
                kind="relationship_advisory",
                subject_type="relationship_advisory",
                subject_id=advisory.pattern_key,
                priority="normal",
                title=f"Relationship advisory: {advisory.observed_label}",
                summary=(
                    f"{advisory.occurrence_count} observations between "
                    f"{advisory.source_type or 'unknown'} and "
                    f"{advisory.target_type or 'unknown'} entities."
                ),
                metadata={
                    "observed_label": advisory.observed_label,
                    "source_type": advisory.source_type,
                    "target_type": advisory.target_type,
                    "occurrence_count": advisory.occurrence_count,
                    "message_ids": list(advisory.message_ids),
                },
                cur=cur,
            )

    async def apply_action(
        self,
        *,
        user_name: str,
        project_id: str,
        pattern_key: str,
        action: str,
        relationship_type: str | None = None,
        note: str | None = None,
        decided_by: str | None = None,
    ) -> RelationshipAdvisoryDecision:
        user_name = require_scope_value(
            user_name,
            "user_name",
            "apply_relationship_advisory_action",
        )
        project_id = require_scope_value(
            project_id,
            "project_id",
            "apply_relationship_advisory_action",
        )
        pattern_key = require_scope_value(
            pattern_key,
            "pattern_key",
            "apply_relationship_advisory_action",
        )
        if decided_by is not None:
            decided_by = require_scope_value(
                decided_by,
                "decided_by",
                "apply_relationship_advisory_action",
            )

        async with self.client.transaction() as cur:
            await cur.execute(
                """
                SELECT
                    pattern_key,
                    disposition,
                    proposed_relationship_type,
                    last_action,
                    decision_note,
                    decided_by,
                    revision
                FROM relationship_advisories
                WHERE user_name = %s
                  AND project_id = %s
                  AND pattern_key = %s
                FOR UPDATE
                """,
                (user_name, project_id, pattern_key),
            )
            row = await cur.fetchone()
            current = None
            if row is not None:
                current = RelationshipAdvisoryDecision(
                    pattern_key=row["pattern_key"],
                    disposition=row.get("disposition", "pending"),
                    proposed_relationship_type=row.get("proposed_relationship_type"),
                    last_action=row.get("last_action"),
                    decision_note=row.get("decision_note"),
                    decided_by=row.get("decided_by"),
                    revision=int(row.get("revision", 0)),
                )

            decision = apply_advisory_action(
                current,
                pattern_key=pattern_key,
                action=action,
                relationship_type=relationship_type,
                note=note,
                decided_by=decided_by,
            )
            await cur.execute(
                """
                INSERT INTO relationship_advisories (
                    user_name,
                    project_id,
                    pattern_key,
                    disposition,
                    proposed_relationship_type,
                    last_action,
                    decision_note,
                    decided_by,
                    decision_at,
                    revision
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now(), %s)
                ON CONFLICT (user_name, project_id, pattern_key)
                DO UPDATE SET
                    disposition = EXCLUDED.disposition,
                    proposed_relationship_type =
                        EXCLUDED.proposed_relationship_type,
                    last_action = EXCLUDED.last_action,
                    decision_note = EXCLUDED.decision_note,
                    decided_by = EXCLUDED.decided_by,
                    decision_at = EXCLUDED.decision_at,
                    revision = EXCLUDED.revision,
                    updated_at = now()
                """,
                (
                    user_name,
                    project_id,
                    pattern_key,
                    decision.disposition,
                    decision.proposed_relationship_type,
                    decision.last_action,
                    decision.decision_note,
                    decision.decided_by,
                    decision.revision,
                ),
            )
            await cur.execute(
                """
                INSERT INTO relationship_advisory_decisions (
                    user_name,
                    project_id,
                    pattern_key,
                    action,
                    proposed_relationship_type,
                    decision_note,
                    decided_by,
                    revision
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    user_name,
                    project_id,
                    pattern_key,
                    decision.last_action,
                    decision.proposed_relationship_type,
                    decision.decision_note,
                    decision.decided_by,
                    decision.revision,
                ),
            )
            if decision.disposition == "pending":
                await self.reviews.open(
                    user_name=user_name,
                    project_id=project_id,
                    kind="relationship_advisory",
                    subject_type="relationship_advisory",
                    subject_id=pattern_key,
                    priority="normal",
                    title=f"Relationship advisory: {pattern_key}",
                    summary=None,
                    cur=cur,
                )
            else:
                await self.reviews.resolve(
                    user_name=user_name,
                    project_id=project_id,
                    kind="relationship_advisory",
                    subject_id=pattern_key,
                    cur=cur,
                )
        return decision
