from typing import Dict, List, Optional

from loguru import logger

from infrastructure.redis_client import RedisKeys
from knoggin_server.knowledge.entity.merge_service import EntityMergeService


class MaintenanceTools:
    """Agent tools for maintaining the knowledge graph."""

    async def check_graph_health(self) -> Dict:
        """
        Check if there are any duplicate entities in the graph that need merging.
        Returns a list of potential duplicates for the agent to review.
        """
        try:
            dirty_ids = None
            if self.redis:
                merge_key = RedisKeys.merge_queue(self.user_name, self.project_id)
                dirty_raw = await self.redis.srandmember(merge_key, 50)
                dirty_ids = {int(eid) for eid in dirty_raw} if dirty_raw else None

            candidates = await self.entities.detect_merge_entity_candidates(
                dirty_ids=dirty_ids
            )

            if not candidates:
                return {
                    "message": (
                        "Graph is healthy. Scanned entities but found no "
                        "high-confidence duplicates."
                    )
                }

            # Return top 5 to avoid overwhelming the agent
            candidates.sort(key=lambda x: x.get("fuzz_score", 0), reverse=True)
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
    def _format_merge_candidate(candidate: Dict) -> Dict:
        facts = []
        for side, key in (("primary", "facts_a"), ("secondary", "facts_b")):
            for fact in candidate.get(key, []) or []:
                fact_id = getattr(fact, "id", None)
                content = getattr(fact, "content", None)
                if isinstance(fact, dict):
                    fact_id = fact.get("id") or fact.get("fact_id")
                    content = fact.get("content")
                facts.append(
                    {
                        "side": side,
                        "fact_id": fact_id,
                        "content": content,
                    }
                )

        return {
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
            "evidence_facts": facts,
        }

    async def propose_entity_merge(
        self,
        primary_id: int,
        duplicate_id: int,
        evidence_fact_ids: List[str],
        reasoning: str,
        confidence: Optional[float] = None,
    ) -> Dict:
        """Submit a grounded merge proposal without granting destructive access."""
        try:
            service = EntityMergeService(
                self.postgres,
                self.knowledge_store,
                redis=self.redis,
            )
            return await service.propose(
                user_name=self.user_name,
                project_id=self.project_id,
                primary_id=primary_id,
                duplicate_id=duplicate_id,
                evidence_fact_ids=evidence_fact_ids,
                reasoning=reasoning,
                model_confidence=confidence,
            )
        except Exception as e:
            logger.error(f"Error proposing entity merge: {e}")
            return {"error": str(e)}
