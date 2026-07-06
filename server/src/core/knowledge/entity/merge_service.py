from __future__ import annotations

import hashlib
import json
import re
import secrets
import uuid
from typing import Any, Dict, List, Optional

from common.scoping import IDENTITY_ENTITY_ID
from common.utils.events import emit
from common.utils.time_utils import get_now, parse_iso_time
from infrastructure.postgres_client import PostgresClient
from infrastructure.redis_client import RedisKeys
from core.knowledge.db.readers.merge_audit_reader import MergeAuditReader
from core.knowledge.db.writers.merge_audit_writer import MergeAuditWriter

MERGE_ROLLBACK_RETENTION_HOURS = 5


class EntityMergeService:
    """Policy and audit boundary around destructive entity merges."""

    _EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
    _PHONE_RE = re.compile(
        r"(?<!\d)(?:\+?1[\s.-]?)?(?:\(\d{3}\)|\d{3})"
        r"[\s.-]\d{3}[\s.-]\d{4}(?!\d)"
    )

    def __init__(self, postgres: PostgresClient, knowledge_store, redis=None):
        self.postgres = postgres
        self.knowledge_store = knowledge_store
        self.redis = redis
        self.merge_audit_reader = MergeAuditReader(postgres)
        self.merge_audit_writer = MergeAuditWriter(postgres)

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

        evidence_ids = list(
            dict.fromkeys(
                str(item).strip()
                for item in evidence_fact_ids
                if str(item).strip()
            )
        )
        if not evidence_ids:
            return self._rejected("At least one evidence fact ID is required.", checks)

        snapshot = await self._snapshot(user_name, project_id, primary_id, duplicate_id)
        entities = snapshot["entities"]
        if len(entities) != 2:
            return self._rejected(
                "Both entities must still exist in the current project.",
                {
                    "entities_exist_in_project": False,
                    "entities_visible_in_authorized_scope": False,
                },
            )
        checks["entities_exist_in_project"] = True
        scoped = self._entities_visible_in_scope(snapshot, user_name, project_id)
        checks["entities_visible_in_authorized_scope"] = scoped
        if not scoped:
            return self._rejected(
                "Both entities must be visible in the authorized user/project scope.",
                checks,
            )
        checks["identity_entity_protected"] = True

        entity_by_id = {int(entity["entity_id"]): entity for entity in entities}
        primary = entity_by_id[primary_id]
        duplicate = entity_by_id[duplicate_id]
        primary_type = self._normalize(primary.get("type"))
        duplicate_type = self._normalize(duplicate.get("type"))
        type_compatible = (
            not primary_type
            or not duplicate_type
            or primary_type == duplicate_type
        )
        checks["entity_types_compatible"] = type_compatible
        if not type_compatible:
            return self._rejected(
                "Entity types conflict: "
                f"{primary.get('type')} vs {duplicate.get('type')}.",
                checks,
            )

        snapshot_fact_ids = {str(fact["fact_id"]) for fact in snapshot["facts"]}
        missing_evidence = [
            fact_id for fact_id in evidence_ids if fact_id not in snapshot_fact_ids
        ]
        checks["evidence_belongs_to_candidate"] = not missing_evidence
        if missing_evidence:
            return self._rejected(
                "Evidence facts must belong to one of the proposed entities in "
                "the current project.",
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
        checks["important_facts_and_timelines"] = "confirmation_required"
        checks["model_confidence_is_advisory"] = True

        state_hash = self._state_hash(snapshot)
        proposal_id = str(uuid.uuid4())
        confirmation_token = secrets.token_urlsafe(32)
        token_hash = self._token_hash(confirmation_token)
        await self.merge_audit_writer.create_proposal(
            proposal_id=proposal_id,
            user_name=user_name,
            project_id=project_id,
            primary_id=primary_id,
            duplicate_id=duplicate_id,
            evidence_ids=evidence_ids,
            reasoning=reasoning,
            model_confidence=model_confidence,
            reviewed_state_hash=state_hash,
            reviewed_state=snapshot,
            policy_checks=checks,
            confirmation_token_hash=token_hash,
        )
        return {
            "policy_result": "confirmation_required",
            "proposal_id": proposal_id,
            "confirmation_token": confirmation_token,
            "primary_id": primary_id,
            "duplicate_id": duplicate_id,
            "policy_checks": checks,
            "message": (
                "The proposal passed deterministic checks but requires explicit "
                "user confirmation."
            ),
        }

    async def confirm(
        self,
        *,
        proposal_id: str,
        confirmation_token: str,
        confirmed_by: str,
    ) -> Dict[str, Any]:
        """User/admin entry point. This method is intentionally not an agent tool."""
        proposal = await self.merge_audit_reader.get_proposal(proposal_id)
        if not proposal:
            return {"policy_result": "rejected", "reason": "Unknown merge proposal."}

        if proposal["status"] != "confirmation_required":
            return {
                "policy_result": "rejected",
                "reason": f"Proposal is already {proposal['status']}.",
            }
        if not secrets.compare_digest(
            proposal["confirmation_token_hash"],
            self._token_hash(confirmation_token),
        ):
            return {
                "policy_result": "rejected",
                "reason": "Invalid confirmation token.",
            }
        if confirmed_by != proposal["user_name"]:
            return {
                "policy_result": "rejected",
                "reason": "The confirmation actor is not authorized for this proposal.",
            }

        primary_id = int(proposal["primary_entity_id"])
        duplicate_id = int(proposal["duplicate_entity_id"])
        project_id = proposal["project_id"]
        before_state = await self._snapshot(
            proposal["user_name"],
            project_id,
            primary_id,
            duplicate_id,
        )
        if self._state_hash(before_state) != proposal["reviewed_state_hash"]:
            await self.merge_audit_writer.set_proposal_failure(
                proposal_id,
                "rejected",
                "The entities changed after review; create a new proposal.",
            )
            return {
                "policy_result": "rejected",
                "reason": (
                    "The merge proposal is stale because the candidate state "
                    "changed."
                ),
            }

        claimed = await self.merge_audit_writer.claim_proposal_for_execution(
            proposal_id,
            confirmed_by,
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
        await self.merge_audit_writer.create_audit(
            audit_id=audit_id,
            proposal=proposal,
            evidence_ids=evidence_ids,
            before_state=before_state,
            confirmed_by=confirmed_by,
        )

        merged = await self.knowledge_store.merge_entities(
            primary_id,
            duplicate_id,
            project_id=project_id,
        )
        if not merged:
            await self.merge_audit_writer.mark_audit_failed(
                audit_id,
                "The canonical merge transaction rejected or failed.",
            )
            await self.merge_audit_writer.set_proposal_failure(
                proposal_id,
                "failed",
                "The canonical merge transaction rejected or failed.",
            )
            return {
                "policy_result": "rejected",
                "reason": "The canonical merge transaction failed.",
            }

        after_state = await self._snapshot(
            proposal["user_name"],
            project_id,
            primary_id,
            duplicate_id,
        )
        await self.merge_audit_writer.mark_audit_executed(
            audit_id=audit_id,
            after_state=after_state,
            rollback_retention_hours=MERGE_ROLLBACK_RETENTION_HOURS,
        )
        await self.merge_audit_writer.mark_proposal_executed(proposal_id)
        if self.redis:
            merge_key = RedisKeys.merge_queue(proposal["user_name"], project_id)
            dirty_key = RedisKeys.dirty_entities(proposal["user_name"], project_id)
            await self.redis.srem(merge_key, str(primary_id), str(duplicate_id))
            await emit(
                project_id,
                "job",
                "merge_queue_removed",
                {
                    "user_name": proposal["user_name"],
                    "project_id": project_id,
                    "merge_key": merge_key,
                    "entity_ids": [primary_id, duplicate_id],
                    "cleared_count": 2,
                    "reason": "merge_executed",
                    "proposal_id": proposal_id,
                    "primary_id": primary_id,
                    "duplicate_id": duplicate_id,
                },
            )
            await self.redis.sadd(dirty_key, str(primary_id))
            await emit(
                project_id,
                "job",
                "dirty_entities_marked",
                {
                    "user_name": proposal["user_name"],
                    "project_id": project_id,
                    "dirty_key": dirty_key,
                    "entity_ids": [primary_id],
                    "marked_count": 1,
                    "reason": "merge_executed",
                },
            )
        return {
            "policy_result": "executed",
            "proposal_id": proposal_id,
            "primary_id": primary_id,
            "duplicate_id": duplicate_id,
        }

    async def rollback(self, audit_id: str, actor: str) -> Dict[str, Any]:
        """Admin/service-only entry point. This method is not an agent tool."""
        audit = await self.merge_audit_reader.get_audit(audit_id)
        if not audit:
            return {"policy_result": "rejected", "reason": "Unknown merge audit."}

        reason = self._rollback_preflight_rejection(audit)
        if reason:
            return {"policy_result": "rejected", "reason": reason}

        before_state = self._json_value(audit["before_state"])
        after_state = self._json_value(audit["after_state"])
        user_name = audit["user_name"]
        project_id = audit["project_id"]
        primary_id = int(audit["primary_entity_id"])
        duplicate_id = int(audit["duplicate_entity_id"])

        current_state = await self._snapshot(
            user_name,
            project_id,
            primary_id,
            duplicate_id,
        )
        duplicate_reused = any(
            int(entity["entity_id"]) == duplicate_id
            for entity in current_state["entities"]
        )
        if duplicate_reused:
            reason = (
                "Duplicate entity ID has been reused; rollback requires manual repair."
            )
            await self.merge_audit_writer.mark_rollback_failure(audit_id, reason)
            return {"policy_result": "rejected", "reason": reason}

        if self._state_hash(current_state) != self._state_hash(after_state):
            reason = (
                "Current merge state differs from the audited post-merge state; "
                "rollback requires manual repair."
            )
            await self.merge_audit_writer.mark_rollback_failure(audit_id, reason)
            return {"policy_result": "rejected", "reason": reason}

        await self.merge_audit_writer.restore_before_state(
            before_state,
            project_id=project_id,
            primary_id=primary_id,
            duplicate_id=duplicate_id,
            audit_id=audit_id,
            actor=actor,
        )

        projection_result = None
        projection_error = None
        rebuild_projection = getattr(
            self.knowledge_store,
            "rebuild_project_projection",
            None,
        )
        if rebuild_projection is not None:
            try:
                projection_result = await rebuild_projection(project_id, user_name)
            except Exception as exc:
                projection_error = str(exc)

        result = {
            "policy_result": "rolled_back",
            "audit_id": audit_id,
            "primary_id": primary_id,
            "duplicate_id": duplicate_id,
            "search_rebuild_required": True,
        }
        if projection_result is not None:
            result["projection_rebuild"] = projection_result
        if projection_error is not None:
            result["projection_rebuild_error"] = projection_error
        return result

    async def _snapshot(
        self,
        user_name: str,
        project_id: str,
        primary_id: int,
        duplicate_id: int,
    ) -> Dict[str, Any]:
        return await self.merge_audit_reader.snapshot(
            user_name,
            project_id,
            primary_id,
            duplicate_id,
        )

    @staticmethod
    def _entities_visible_in_scope(
        snapshot: Dict[str, Any],
        user_name: str,
        project_id: str,
    ) -> bool:
        for entity in snapshot["entities"]:
            if entity.get("user_name") != user_name:
                return False
            if entity.get("project_id") != project_id:
                return False
        for fact in snapshot["facts"]:
            if fact.get("user_name") != user_name:
                return False
            if fact.get("project_id") != project_id:
                return False
        for relationship in snapshot["relationships"]:
            if relationship.get("user_name") != user_name:
                return False
            if relationship.get("project_id") != project_id:
                return False
        for edge in snapshot["hierarchy"]:
            if edge.get("project_id") != project_id:
                return False
        return True

    def _stable_identifier_conflicts(self, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        values: Dict[int, Dict[str, set[str]]] = {}
        facts_by_entity: Dict[int, List[str]] = {}
        for fact in snapshot["facts"]:
            if fact.get("invalid_at") is None:
                facts_by_entity.setdefault(int(fact["entity_id"]), []).append(
                    fact["content"]
                )
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
                "phone": {
                    re.sub(r"\D", "", match)
                    for match in self._PHONE_RE.findall(text)
                },
            }

        conflicts: Dict[str, Any] = {}
        if len(values) != 2:
            return conflicts
        first, second = values.values()
        for kind in ("email", "phone"):
            if first[kind] and second[kind] and first[kind].isdisjoint(second[kind]):
                conflicts[kind] = [sorted(first[kind]), sorted(second[kind])]
        return conflicts

    @staticmethod
    def _rollback_preflight_rejection(audit: Dict[str, Any]) -> Optional[str]:
        if audit["status"] != "executed":
            return "Only executed merge audits can be rolled back."
        rollback_status = audit.get("rollback_status") or "unavailable"
        if rollback_status == "rolled_back":
            return "Merge audit has already been rolled back."
        if rollback_status == "expired":
            return "Merge rollback state has expired."
        if rollback_status == "failed":
            return "Merge rollback previously failed and requires manual repair."
        if rollback_status != "available":
            return "Merge rollback state is not available."
        if not audit.get("before_state") or not audit.get("after_state"):
            return "Merge rollback state is missing."
        expires_at = audit.get("rollback_expires_at")
        parsed_expiry = (
            parse_iso_time(expires_at) if isinstance(expires_at, str) else expires_at
        )
        if parsed_expiry is not None and parsed_expiry <= get_now():
            return "Merge rollback window has expired."
        return None

    @staticmethod
    def _json_value(value: Any) -> Any:
        if isinstance(value, str):
            return json.loads(value)
        return value

    @staticmethod
    def _basic_rejection(
        primary_id: int,
        duplicate_id: int,
        reasoning: str,
    ) -> Optional[str]:
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
        payload = json.dumps(
            snapshot,
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @staticmethod
    def _rejected(reason: str, checks: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "policy_result": "rejected",
            "reason": reason,
            "policy_checks": checks,
        }
