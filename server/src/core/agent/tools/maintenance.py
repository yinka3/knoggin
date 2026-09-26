from typing import Dict, List, Optional

from common.schema.evidence import EvidencePointer


class MaintenanceTools:
    """Agent tools for maintaining the knowledge graph."""

    def _require_entity_maintenance_service(self):
        service = self.entity_maintenance_service
        if service is None:
            raise RuntimeError("Entity maintenance is unavailable in this tool context")
        return service

    def _require_project_maintenance_service(self):
        service = self.project_maintenance_service
        if service is None:
            raise RuntimeError("Project maintenance is unavailable in this tool context")
        return service

    async def inspect_duplicate_entities(self) -> Dict:
        """Return bounded duplicate-entity candidates for maintenance review."""

        service = self._require_entity_maintenance_service()
        candidates = await service.discover_duplicate_candidates()

        if not candidates:
            return {
                "message": (
                    "Scanned entities and found no high-confidence duplicate "
                    "candidates."
                )
            }

        candidates.sort(
            key=lambda candidate: candidate["message_ref_count"], reverse=True
        )
        return {
            "message": f"Found {len(candidates)} potential duplicates.",
            "suggestions": [
                self._format_global_merge_candidate(candidate)
                for candidate in candidates[:5]
            ],
        }

    @staticmethod
    def _format_global_merge_candidate(candidate: Dict) -> Dict:
        return {
            "primary_id": candidate["entity_a_id"],
            "primary_name": candidate["name_a"],
            "secondary_id": candidate["entity_b_id"],
            "secondary_name": candidate["name_b"],
            "project_ids": candidate["project_ids"],
            "message_ref_count": candidate["message_ref_count"],
            "reasons": [candidate["signal"]],
            "survivor_required": True,
        }

    async def propose_entity_merge(
        self,
        primary_id: int,
        duplicate_id: int,
        reasoning: str,
        evidence_message_ids: Optional[List[int]] = None,
        evidence_episode_ids: Optional[List[str]] = None,
    ) -> Dict:
        """Submit a grounded merge proposal without granting destructive access."""
        service = self._require_entity_maintenance_service()
        evidence_refs = [
            *(
                EvidencePointer(kind="message", identifier=str(message_id))
                for message_id in (evidence_message_ids or [])
            ),
            *(
                EvidencePointer(kind="episode", identifier=str(episode_id))
                for episode_id in (evidence_episode_ids or [])
            ),
        ]
        review = await service.propose(
            user_name=self.user_name,
            survivor_entity_id=primary_id,
            retired_entity_id=duplicate_id,
            evidence_refs=evidence_refs,
            reasoning=reasoning,
        )
        return {
            "policy_result": "confirmation_required",
            "review_id": review.review_id,
            "plan": review.proposed_plan.model_dump(mode="json"),
            "expected_state": review.expected_state,
            "message": "Global merge review created; explicit user confirmation is required.",
        }

    async def report_relationship_conflict(
        self,
        evidence_observation_ids: List[int],
        kind: str,
        reasoning: str,
        confidence: float,
    ) -> Dict:
        """Create a reviewable conflict group from retrieved relationship evidence.

        This preserves the cited observations exactly as they were. It neither
        changes relationship evidence nor decides which observation is current.
        """
        result = await self._require_project_maintenance_service().record_conflict_detection(
            self.project_id,
            origin="agent_discovery",
            kind=kind,
            rationale=reasoning,
            confidence=confidence,
            evidence_ids=evidence_observation_ids,
            metadata={"reported_by": "agent"},
        )
        return {
            "review_id": result.group.conflict_id,
            "created": result.created,
            "evidence_added": result.evidence_added,
            "status": result.group.status,
            "message": (
                "Recorded a possible conflict as a typed maintenance review. "
                "The relationship evidence was not changed."
            ),
        }
