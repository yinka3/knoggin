from typing import Dict, List, Optional

from loguru import logger

from common.schema.evidence import EvidencePointer
from core.knowledge.conflict_service import ConflictService
from core.knowledge.db.writers.conflict_writer import ConflictWriter


class MaintenanceTools:
    """Agent tools for maintaining the knowledge graph."""

    def _require_entity_maintenance_service(self):
        service = self.entity_maintenance_service
        if service is None:
            raise RuntimeError("Entity maintenance is unavailable in this tool context")
        return service

    async def check_graph_health(self) -> Dict:
        """
        Check if there are any duplicate entities in the graph that need merging.
        Returns a list of potential duplicates for the agent to review.
        """
        try:
            service = self._require_entity_maintenance_service()
            candidates = (
                await service.discover_duplicate_candidates()
            )

            if not candidates:
                return {
                    "message": (
                        "Graph is healthy. Scanned entities but found no "
                        "high-confidence duplicates."
                    )
                }

            # Return top 5 to avoid overwhelming the agent.
            candidates.sort(key=lambda candidate: candidate["message_ref_count"], reverse=True)
            return {
                "message": f"Found {len(candidates)} potential duplicates.",
                "suggestions": [
                    self._format_global_merge_candidate(candidate)
                    for candidate in candidates[:5]
                ],
            }

        except Exception as e:
            logger.error(f"Error checking graph health: {e}")
            return {"error": str(e)}

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
        try:
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
        except Exception as e:
            logger.error(f"Error proposing entity merge: {e}")
            return {"error": str(e)}

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
        try:
            result = await ConflictService(
                ConflictWriter(self.postgres)
            ).record_detection(
                user_name=self.user_name,
                project_id=self.project_id,
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
        except Exception as exc:
            logger.error("Error reporting relationship conflict: {}", exc)
            return {"error": str(exc)}
