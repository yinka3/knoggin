from typing import Dict, List, Optional

from loguru import logger

from infrastructure.redis_client import RedisKeys


class TopicTools:
    """Project-scoped tools for proposing guarded topic configuration changes."""

    async def update_topics(
        self,
        add_topics: Optional[List[Dict]] = None,
        deactivate_topics: Optional[List[str]] = None,
        reasoning: str = "",
    ) -> Dict:
        if self.topic_config is None:
            return {"error": "No project topic configuration is active"}

        previous = self.topic_config.snapshot()
        try:
            changes = self.topic_config.apply_agent_update(
                add_topics=add_topics,
                deactivate_topics=deactivate_topics,
            )
            await self.topic_config.save(
                self.postgres,
                self.user_name,
                self.project_id,
            )
            self.active_topics = self.topic_config.active_topics
            if self.topic_refresh_callback:
                self.topic_refresh_callback()

            counter_key = RedisKeys.project_heartbeat_counter(
                self.user_name,
                self.project_id,
            )
            await self.redis.set(counter_key, 0)
            return {
                "success": True,
                **changes,
                "reasoning": reasoning.strip(),
                "active_topics": self.topic_config.active_topics,
            }
        except ValueError as exc:
            self.topic_config.replace(previous)
            return {"error": str(exc)}
        except Exception as exc:
            self.topic_config.replace(previous)
            logger.error(f"Failed to update project topics: {exc}")
            return {"error": f"Failed to persist topic update: {exc}"}
