"""Reads relationship evidence and derives unknown-pattern advisories."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from common.scoping import require_scope_value
from core.knowledge.db.writers.maintenance_review_writer import MaintenanceReviewWriter
from core.knowledge.maintenance_reviews import RelationshipAdvisoryPlan
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
                observed_relationship_label,
                interpretation_source,
                source_context.entity_type AS source_type,
                target_context.entity_type AS target_type,
                context,
                observed_at_ms
            FROM relationship_observations
            LEFT JOIN project_entity_contexts source_context
              ON source_context.project_id = relationship_observations.project_id
             AND source_context.entity_id = relationship_observations.source_entity_id
            LEFT JOIN project_entity_contexts target_context
              ON target_context.project_id = relationship_observations.project_id
             AND target_context.entity_id = relationship_observations.target_entity_id
            WHERE relationship_observations.user_name = %s
              AND relationship_observations.project_id = %s
              AND relationship_observations.interpretation_source = 'observed'
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
        reviews = await MaintenanceReviewWriter(self.client).list(
            user_name=user_name,
            project_id=project_id,
        )
        decisions: dict[str, RelationshipAdvisoryDecision] = {}
        for review in reviews:
            if review.kind != "relationship_advisory":
                continue
            plan = review.proposed_plan
            if not isinstance(plan, RelationshipAdvisoryPlan):
                continue
            disposition = "pending"
            if review.status == "applied":
                disposition = "accepted"
            elif review.status == "dismissed":
                disposition = "suppressed" if plan.action == "suppress" else "dismissed"
            previous = decisions.get(plan.pattern_key)
            stored_revision = review.expected_state.get("revision")
            revision = (
                int(stored_revision)
                if isinstance(stored_revision, int)
                and not isinstance(stored_revision, bool)
                and stored_revision >= 0
                else (previous.revision + 1 if previous else 1)
            )
            decisions[plan.pattern_key] = RelationshipAdvisoryDecision(
                pattern_key=plan.pattern_key,
                disposition=disposition,
                proposed_relationship_type=plan.proposed_relationship_type,
                last_action=plan.action,
                decision_note=plan.note,
                revision=revision,
            )
        return decisions
