from typing import Dict, List, Optional
from loguru import logger

from common.utils.data_utils import cosine_similarity
from infrastructure.redis_client import RedisKeys
from knoggin_server.knowledge.services.entity_merge_service import EntityMergeService

class MaintenanceTools:
    """Agent tools for maintaining the knowledge graph."""

    async def check_graph_health(self) -> Dict:
        """
        Check if there are any duplicate entities in the graph that need merging.
        Returns a list of potential duplicates for the agent to review.
        """
        try:
            merge_key = RedisKeys.merge_queue(self.user_name, self.project_id)
            if not self.redis:
                return {"message": "Redis client not available"}

            dirty_raw = await self.redis.srandmember(merge_key, 50)
            if not dirty_raw:
                return {"message": "Graph is healthy. No duplicate candidates found at this time."}

            dirty_ids = {int(eid) for eid in dirty_raw}
            candidates = []

            for eid in dirty_ids:
                similar = await self.graph_client.search_similar_entities(
                    eid,
                    limit=3,
                    visible_project_ids=[self.project_id],
                )
                if not similar:
                    continue

                for sim_id, score in similar:
                    if sim_id == eid or sim_id in [c["primary_id"] for c in candidates] or sim_id in [c["secondary_id"] for c in candidates]:
                        continue
                        
                    if score >= 0.65: # Show all potential matches to the agent
                        primary = min(eid, sim_id)
                        secondary = max(eid, sim_id)
                        
                        # Get canonical names
                        profile_p = await self.entities.get_profile(primary)
                        profile_s = await self.entities.get_profile(secondary)
                        
                        name_p = profile_p.get("canonical_name", str(primary)) if profile_p else str(primary)
                        name_s = profile_s.get("canonical_name", str(secondary)) if profile_s else str(secondary)
                        
                        candidates.append({
                            "primary_id": primary,
                            "primary_name": name_p,
                            "secondary_id": secondary,
                            "secondary_name": name_s,
                            "similarity_score": round(score, 3)
                        })

            if not candidates:
                return {"message": "Graph is healthy. Scanned entities but found no high-confidence duplicates."}

            # Return top 5 to avoid overwhelming the agent
            candidates.sort(key=lambda x: x["similarity_score"], reverse=True)
            return {
                "message": f"Found {len(candidates)} potential duplicates.",
                "suggestions": candidates[:5]
            }

        except Exception as e:
            logger.error(f"Error checking graph health: {e}")
            return {"error": str(e)}

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
                self.graph_client,
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
