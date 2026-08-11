"""Reads relationship evidence and derives unknown-pattern advisories."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from common.scoping import require_scope_value
from core.knowledge.relationship_advisories import (
    AdvisoryThresholds,
    RelationshipAdvisory,
    RelationshipAdvisoryDecision,
    build_relationship_advisories,
)
from infrastructure.postgres_client import PostgresClient


class RelationshipObservationReader:
    def __init__(self, client: PostgresClient):
        self.client = client

    async def get_unrecognized_observations(
        self,
        *,
        user_name: str,
        project_id: str,
    ) -> list[dict[str, Any]]:
        user_name = require_scope_value(
            user_name,
            "user_name",
            "get_unrecognized_relationship_observations",
        )
        project_id = require_scope_value(
            project_id,
            "project_id",
            "get_unrecognized_relationship_observations",
        )
        return await self.client.fetch_all(
            """
            SELECT
                observation_id,
                relationship_id,
                user_name,
                project_id,
                session_id,
                message_id,
                source_entity_id,
                target_entity_id,
                source_type,
                target_type,
                observed_relationship_label,
                canonical_relationship_type,
                domain_status,
                confidence,
                context,
                observed_at_ms
            FROM relationship_observations
            WHERE user_name = %s
              AND project_id = %s
              AND domain_status = 'unrecognized'
            ORDER BY observed_at_ms, observation_id
            """,
            (user_name, project_id),
        )

    async def get_advisories(
        self,
        *,
        user_name: str,
        project_id: str,
        thresholds: AdvisoryThresholds | None = None,
    ) -> list[RelationshipAdvisory]:
        observations = await self.get_unrecognized_observations(
            user_name=user_name,
            project_id=project_id,
        )
        advisories = build_relationship_advisories(
            observations,
            thresholds=thresholds,
        )
        decisions = await self.get_advisory_decisions(
            user_name=user_name,
            project_id=project_id,
        )
        return [
            replace(
                advisory,
                disposition=decision.disposition,
                proposed_relationship_type=decision.proposed_relationship_type,
                decision_note=decision.decision_note,
                last_action=decision.last_action,
                decision_revision=decision.revision,
            )
            if (decision := decisions.get(advisory.pattern_key)) is not None
            else advisory
            for advisory in advisories
        ]

    async def get_advisory_decisions(
        self,
        *,
        user_name: str,
        project_id: str,
    ) -> dict[str, RelationshipAdvisoryDecision]:
        user_name = require_scope_value(
            user_name,
            "user_name",
            "get_relationship_advisory_decisions",
        )
        project_id = require_scope_value(
            project_id,
            "project_id",
            "get_relationship_advisory_decisions",
        )
        rows = await self.client.fetch_all(
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
            """,
            (user_name, project_id),
        )
        return {
            row["pattern_key"]: RelationshipAdvisoryDecision(
                pattern_key=row["pattern_key"],
                disposition=row.get("disposition", "pending"),
                proposed_relationship_type=row.get("proposed_relationship_type"),
                last_action=row.get("last_action"),
                decision_note=row.get("decision_note"),
                decided_by=row.get("decided_by"),
                revision=int(row.get("revision", 0)),
            )
            for row in rows
        }
