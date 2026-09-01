from typing import Dict, List, Optional

from loguru import logger

from core.knowledge.entity.maintenance import EntityMaintenance
from core.knowledge.entity.merge_service import EntityMergeService


class MaintenanceTools:
    """Agent tools for maintaining the knowledge graph."""

    async def check_graph_health(self) -> Dict:
        """
        Check if there are any duplicate entities in the graph that need merging.
        Returns a list of potential duplicates for the agent to review.
        """
        try:
            candidates = await EntityMaintenance(
                self.entities
            ).discover_duplicate_candidates()

            if not candidates:
                return {
                    "message": (
                        "Graph is healthy. Scanned entities but found no "
                        "high-confidence duplicates."
                    )
                }

            # Return top 5 to avoid overwhelming the agent.
            candidates.sort(
                key=self._merge_candidate_rank_key,
                reverse=True,
            )
            return {
                "message": f"Found {len(candidates)} potential duplicates.",
                "suggestions": [
                    self._format_merge_candidate(candidate)
                    for candidate in candidates[:5]
                ],
            }

        except Exception as e:
            logger.error(f"Error checking graph health: {e}")
            return {"error": str(e)}

    @staticmethod
    def _merge_candidate_rank_key(candidate: Dict) -> tuple:
        evidence_support = str(candidate.get("evidence_support") or "").casefold()
        return (
            1 if evidence_support == "entailment" else 0,
            candidate.get("fuzz_score") or 0,
            candidate.get("cosine_score") or 0,
            candidate.get("shared_neighbor_count") or 0,
        )

    @staticmethod
    def _format_merge_candidate(candidate: Dict) -> Dict:
        evidence = []
        for side, key in (("primary", "evidence_a"), ("secondary", "evidence_b")):
            for item in candidate.get(key, []) or []:
                reference = {
                    "side": side,
                    "kind": item.get("kind"),
                    "text": item.get("text"),
                }
                for identifier in ("message_id", "episode_id", "session_id"):
                    if item.get(identifier) is not None:
                        reference[identifier] = item[identifier]
                evidence.append(reference)

        formatted = {
            "primary_id": candidate.get("primary_id"),
            "primary_name": candidate.get("primary_name"),
            "primary_type": candidate.get("primary_type"),
            "secondary_id": candidate.get("secondary_id"),
            "secondary_name": candidate.get("secondary_name"),
            "secondary_type": candidate.get("secondary_type"),
            "topic_a": candidate.get("topic_a"),
            "topic_b": candidate.get("topic_b"),
            "fuzz_score": candidate.get("fuzz_score", 0),
            "shared_neighbor_count": candidate.get("shared_neighbor_count", 0),
            "reasons": list(candidate.get("reasons", [])),
            "evidence": evidence,
        }
        if "cosine_score" in candidate:
            formatted["cosine_score"] = candidate.get("cosine_score")
        if "evidence_support" in candidate:
            formatted["evidence_support"] = candidate.get("evidence_support")
        if "evidence_support_pairs" in candidate:
            formatted["evidence_support_pairs"] = list(
                candidate.get("evidence_support_pairs") or []
            )
        return formatted

    async def propose_entity_merge(
        self,
        primary_id: int,
        duplicate_id: int,
        reasoning: str,
        evidence_message_ids: Optional[List[int]] = None,
        evidence_episode_ids: Optional[List[str]] = None,
        confidence: Optional[float] = None,
    ) -> Dict:
        """Submit a grounded merge proposal without granting destructive access."""
        try:
            service = EntityMergeService(
                self.postgres,
                self.knowledge_store,
            )
            return await service.propose(
                user_name=self.user_name,
                project_id=self.project_id,
                primary_id=primary_id,
                duplicate_id=duplicate_id,
                evidence_message_ids=evidence_message_ids or [],
                evidence_episode_ids=evidence_episode_ids or [],
                reasoning=reasoning,
                model_confidence=confidence,
            )
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
            result = await self.knowledge_store.record_conflict_detection(
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
                # Kept as a response label for callers that have not yet
                # migrated their UI; the value is the MaintenanceReview ID.
                "conflict_id": result.group.conflict_id,
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
