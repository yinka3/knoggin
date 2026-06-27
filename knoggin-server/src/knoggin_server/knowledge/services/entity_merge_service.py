from __future__ import annotations

import hashlib
import json
import re
import secrets
import uuid
from typing import Any, Dict, List, Optional

from common.scoping import IDENTITY_ENTITY_ID
from infrastructure.postgres_client import PostgresClient
from infrastructure.redis_client import RedisKeys


class EntityMergeService:
    """Policy and audit boundary around destructive entity merges."""

    _EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
    _PHONE_RE = re.compile(r"(?<!\d)(?:\+?1[\s.-]?)?(?:\(\d{3}\)|\d{3})[\s.-]\d{3}[\s.-]\d{4}(?!\d)")

    def __init__(self, postgres: PostgresClient, knowledge_store, redis=None):
        self.postgres = postgres
        self.knowledge_store = knowledge_store
        self.redis = redis

    async def propose(
        self,
        *,
        user_name: str,
        project_id: str,
        primary_id: int,
        duplicate_id: int,
        evidence_fact_ids: List[str],
        reasoning: str,
        model_confidence: Optional[float] = None,
    ) -> Dict[str, Any]:
        checks: Dict[str, Any] = {}
        rejection = self._basic_rejection(primary_id, duplicate_id, reasoning)
        if rejection:
            return self._rejected(rejection, checks)

        evidence_ids = list(dict.fromkeys(str(item).strip() for item in evidence_fact_ids if str(item).strip()))
        if not evidence_ids:
            return self._rejected("At least one evidence fact ID is required.", checks)

        snapshot = await self._snapshot(project_id, primary_id, duplicate_id)
        entities = snapshot["entities"]
        if len(entities) != 2:
            return self._rejected(
                "Both entities must still exist in the current project.",
                {"entities_exist_in_project": False},
            )
        checks["entities_exist_in_project"] = True
        checks["identity_entity_protected"] = True

        entity_by_id = {int(entity["entity_id"]): entity for entity in entities}
        primary = entity_by_id[primary_id]
        duplicate = entity_by_id[duplicate_id]
        primary_type = self._normalize(primary.get("type"))
        duplicate_type = self._normalize(duplicate.get("type"))
        type_compatible = not primary_type or not duplicate_type or primary_type == duplicate_type
        checks["entity_types_compatible"] = type_compatible
        if not type_compatible:
            return self._rejected(
                f"Entity types conflict: {primary.get('type')} vs {duplicate.get('type')}.",
                checks,
            )

        snapshot_fact_ids = {str(fact["fact_id"]) for fact in snapshot["facts"]}
        missing_evidence = [fact_id for fact_id in evidence_ids if fact_id not in snapshot_fact_ids]
        checks["evidence_belongs_to_candidate"] = not missing_evidence
        if missing_evidence:
            return self._rejected(
                "Evidence facts must belong to one of the proposed entities in the current project.",
                {**checks, "missing_evidence_fact_ids": missing_evidence},
            )

        identifier_conflicts = self._stable_identifier_conflicts(snapshot)
        checks["stable_identifiers_compatible"] = not identifier_conflicts
        if identifier_conflicts:
            return self._rejected(
                "Stable identifiers conflict; these appear to be different entities.",
                {**checks, "stable_identifier_conflicts": identifier_conflicts},
            )

        # Free-form facts and timelines cannot be proven compatible by generic
        # string rules. Keep this explicit and force human review.
        checks["important_facts_and_timelines"] = "manual_review_required"  # Human must review nuanced fact/timeline compatibility.
        checks["model_confidence_is_advisory"] = True  # Model confidence is evidence, not authorization.
        checks["automatic_execution_enabled"] = False  # Proposals cannot execute destructive merges.

        state_hash = self._state_hash(snapshot)
        proposal_id = str(uuid.uuid4())
        confirmation_token = secrets.token_urlsafe(32)
        token_hash = self._token_hash(confirmation_token)
        await self.postgres.execute(
            """
            INSERT INTO entity_merge_proposals (
                proposal_id,
                user_name,
                project_id,
                primary_entity_id,
                duplicate_entity_id,
                evidence_fact_ids,
                reasoning,
                model_confidence,
                reviewed_state_hash,
                reviewed_state,
                policy_checks,
                confirmation_token_hash
            )
            VALUES (
                %s, %s, %s, %s, %s, %s::jsonb, %s, %s,
                %s, %s::jsonb, %s::jsonb, %s
            )
            """,
            (
                proposal_id,
                user_name,
                project_id,
                primary_id,
                duplicate_id,
                json.dumps(evidence_ids),
                reasoning.strip(),
                model_confidence,
                state_hash,
                json.dumps(snapshot, default=str),
                json.dumps(checks),
                token_hash,
            ),
        )
        return {
            "policy_result": "confirmation_required",
            "proposal_id": proposal_id,
            "confirmation_token": confirmation_token,
            "primary_id": primary_id,
            "duplicate_id": duplicate_id,
            "policy_checks": checks,
            "message": "The proposal passed deterministic checks but requires explicit user confirmation.",
        }

    async def confirm(
        self,
        *,
        proposal_id: str,
        confirmation_token: str,
        confirmed_by: str,
    ) -> Dict[str, Any]:
        """User/admin entry point. This method is intentionally not an agent tool."""
        rows = await self.postgres.fetch_all(
            """
            SELECT *
            FROM entity_merge_proposals
            WHERE proposal_id = %s
            """,
            (proposal_id,),
        )
        if not rows:
            return {"policy_result": "rejected", "reason": "Unknown merge proposal."}

        proposal = rows[0]
        if proposal["status"] != "confirmation_required":
            return {
                "policy_result": "rejected",
                "reason": f"Proposal is already {proposal['status']}.",
            }
        if not secrets.compare_digest(
            proposal["confirmation_token_hash"],
            self._token_hash(confirmation_token),
        ):
            return {"policy_result": "rejected", "reason": "Invalid confirmation token."}
        if confirmed_by != proposal["user_name"]:
            return {
                "policy_result": "rejected",
                "reason": "The confirmation actor is not authorized for this proposal.",
            }

        primary_id = int(proposal["primary_entity_id"])
        duplicate_id = int(proposal["duplicate_entity_id"])
        project_id = proposal["project_id"]
        before_state = await self._snapshot(project_id, primary_id, duplicate_id)
        if self._state_hash(before_state) != proposal["reviewed_state_hash"]:
            await self._set_failure(
                proposal_id,
                "rejected",
                "The entities changed after review; create a new proposal.",
            )
            return {
                "policy_result": "rejected",
                "reason": "The merge proposal is stale because the candidate state changed.",
            }

        claimed = await self.postgres.execute(
            """
            UPDATE entity_merge_proposals
            SET status = 'executing',
                confirmed_at = NOW(),
                confirmed_by = %s
            WHERE proposal_id = %s
              AND status = 'confirmation_required'
            """,
            (confirmed_by, proposal_id),
        )
        if claimed != 1:
            return {
                "policy_result": "rejected",
                "reason": "The proposal was already claimed or changed.",
            }

        audit_id = str(uuid.uuid4())
        evidence_ids = proposal["evidence_fact_ids"]
        if isinstance(evidence_ids, str):
            evidence_ids = json.loads(evidence_ids)
        await self.postgres.execute(
            """
            INSERT INTO entity_merge_audits (
                audit_id,
                proposal_id,
                user_name,
                project_id,
                primary_entity_id,
                duplicate_entity_id,
                evidence_fact_ids,
                reasoning,
                confirmed_by,
                before_state
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s::jsonb
            )
            """,
            (
                audit_id,
                proposal_id,
                proposal["user_name"],
                project_id,
                primary_id,
                duplicate_id,
                json.dumps(evidence_ids),
                proposal["reasoning"],
                confirmed_by,
                json.dumps(before_state, default=str),
            ),
        )

        merged = await self.knowledge_store.merge_entities(
            primary_id,
            duplicate_id,
            project_id=project_id,
        )
        if not merged:
            await self.postgres.execute(
                """
                UPDATE entity_merge_audits
                SET status = 'failed',
                    failure_reason = %s
                WHERE audit_id = %s
                """,
                ("The canonical merge transaction rejected or failed.", audit_id),
            )
            await self._set_failure(
                proposal_id,
                "failed",
                "The canonical merge transaction rejected or failed.",
            )
            return {
                "policy_result": "rejected",
                "reason": "The canonical merge transaction failed.",
            }

        after_state = await self._snapshot(project_id, primary_id, duplicate_id)
        await self.postgres.execute(
            """
            UPDATE entity_merge_audits
            SET status = 'executed',
                after_state = %s::jsonb
            WHERE audit_id = %s
            """,
            (
                json.dumps(after_state, default=str),
                audit_id,
            ),
        )
        await self.postgres.execute(
            """
            UPDATE entity_merge_proposals
            SET status = 'executed',
                executed_at = NOW()
            WHERE proposal_id = %s
            """,
            (proposal_id,),
        )
        if self.redis:
            merge_key = RedisKeys.merge_queue(proposal["user_name"], project_id)
            dirty_key = RedisKeys.dirty_entities(proposal["user_name"], project_id)
            await self.redis.srem(merge_key, str(primary_id), str(duplicate_id))
            await self.redis.sadd(dirty_key, str(primary_id))
        return {
            "policy_result": "executed",
            "proposal_id": proposal_id,
            "primary_id": primary_id,
            "duplicate_id": duplicate_id,
        }

    async def _snapshot(
        self, project_id: str, primary_id: int, duplicate_id: int
    ) -> Dict[str, Any]:
        ids = (primary_id, duplicate_id)
        entities = await self.postgres.fetch_all(
            """
            SELECT
                e.entity_id,
                e.user_name,
                e.project_id,
                e.session_id,
                e.canonical_name,
                e.type,
                e.topic,
                e.confidence,
                e.last_mentioned_ms,
                e.last_updated_ms,
                e.last_profiled_msg_id,
                COALESCE(
                    array_agg(DISTINCT a.alias ORDER BY a.alias)
                    FILTER (WHERE a.alias IS NOT NULL),
                    ARRAY[]::text[]
                ) AS aliases
            FROM entities e
            LEFT JOIN entity_aliases a ON a.entity_id = e.entity_id
            WHERE e.project_id = %s
              AND e.entity_id = ANY(%s)
            GROUP BY e.entity_id
            ORDER BY e.entity_id
            """,
            (project_id, list(ids)),
        )
        facts = await self.postgres.fetch_all(
            """
            SELECT *
            FROM facts
            WHERE project_id = %s
              AND entity_id = ANY(%s)
            ORDER BY fact_id
            """,
            (project_id, list(ids)),
        )
        relationships = await self.postgres.fetch_all(
            """
            SELECT
                r.*,
                COALESCE(
                    json_agg(
                        json_build_object(
                            'user_name', ref.user_name,
                            'session_id', ref.session_id,
                            'message_id', ref.message_id
                        )
                        ORDER BY
                            ref.user_name,
                            ref.session_id,
                            ref.message_id
                    ) FILTER (WHERE ref.relationship_id IS NOT NULL),
                    '[]'
                ) AS evidence_refs
            FROM relationships r
            LEFT JOIN relationship_evidence_refs ref
              ON ref.relationship_id = r.relationship_id
            WHERE r.project_id = %s
              AND (r.entity_a_id = ANY(%s) OR r.entity_b_id = ANY(%s))
            GROUP BY r.relationship_id
            ORDER BY r.relationship_id
            """,
            (project_id, list(ids), list(ids)),
        )
        hierarchy = await self.postgres.fetch_all(
            """
            SELECT *
            FROM hierarchy_edges
            WHERE project_id = %s
              AND (parent_id = ANY(%s) OR child_id = ANY(%s))
            ORDER BY parent_id, child_id
            """,
            (project_id, list(ids), list(ids)),
        )
        return {
            "entities": entities,
            "facts": facts,
            "relationships": relationships,
            "hierarchy": hierarchy,
        }

    def _stable_identifier_conflicts(self, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        values: Dict[int, Dict[str, set[str]]] = {}
        facts_by_entity: Dict[int, List[str]] = {}
        for fact in snapshot["facts"]:
            if fact.get("invalid_at") is None:
                facts_by_entity.setdefault(int(fact["entity_id"]), []).append(fact["content"])
        for entity in snapshot["entities"]:
            entity_id = int(entity["entity_id"])
            text = " ".join(
                [
                    entity.get("canonical_name") or "",
                    *list(entity.get("aliases") or []),
                    *facts_by_entity.get(entity_id, []),
                ]
            )
            values[entity_id] = {
                "email": {match.lower() for match in self._EMAIL_RE.findall(text)},
                "phone": {re.sub(r"\D", "", match) for match in self._PHONE_RE.findall(text)},
            }

        conflicts: Dict[str, Any] = {}
        if len(values) != 2:
            return conflicts
        first, second = values.values()
        for kind in ("email", "phone"):
            if first[kind] and second[kind] and first[kind].isdisjoint(second[kind]):
                conflicts[kind] = [sorted(first[kind]), sorted(second[kind])]
        return conflicts

    async def _set_failure(self, proposal_id: str, status: str, reason: str) -> None:
        await self.postgres.execute(
            """
            UPDATE entity_merge_proposals
            SET status = %s,
                failure_reason = %s
            WHERE proposal_id = %s
            """,
            (status, reason, proposal_id),
        )

    @staticmethod
    def _basic_rejection(primary_id: int, duplicate_id: int, reasoning: str) -> Optional[str]:
        if primary_id == duplicate_id:
            return "An entity cannot be merged into itself."
        if IDENTITY_ENTITY_ID in (primary_id, duplicate_id):
            return "The protected identity entity cannot be merged."
        if not reasoning or not reasoning.strip():
            return "Merge reasoning is required."
        return None

    @staticmethod
    def _normalize(value: Any) -> str:
        return str(value or "").strip().casefold()

    @staticmethod
    def _token_hash(token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    @staticmethod
    def _state_hash(snapshot: Dict[str, Any]) -> str:
        payload = json.dumps(snapshot, sort_keys=True, default=str, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @staticmethod
    def _rejected(reason: str, checks: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "policy_result": "rejected",
            "reason": reason,
            "policy_checks": checks,
        }
