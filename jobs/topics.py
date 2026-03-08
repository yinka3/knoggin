import json
import asyncio
import re
from loguru import logger
from jobs.base import BaseJob, JobContext, JobResult
from shared.services.llm import LLMService
from shared.config.topics_config import TopicConfig
from main.prompts import get_topic_evolution_prompt
from shared.utils.events import emit
from shared.infra.redis import RedisKeys


class TopicConfigJob(BaseJob):
    """
    Heartbeat job that re-evaluates topic config every N messages.
    Uses recent conversation to evolve topics — add new ones,
    deactivate stale ones, adjust labels.
    """

    def __init__(
        self, 
        llm: LLMService, 
        topic_config: TopicConfig,
        update_callback,
        interval_msgs: int = 40,
        conversation_window: int = 50
    ):
        self.llm = llm
        self.topic_config = topic_config
        self.update_callback = update_callback
        self.interval_msgs = interval_msgs
        self.conversation_window = conversation_window

    @property
    def name(self) -> str:
        return "topic_config"

    async def should_run(self, ctx: JobContext) -> bool:
        count_key = RedisKeys.heartbeat_counter(ctx.user_name, ctx.session_id)
        count = await ctx.redis.get(count_key)
        if int(count or 0) < self.interval_msgs:
            return False
        
        buffer_key = RedisKeys.buffer(ctx.user_name, ctx.session_id)
        buffer_len = await ctx.redis.llen(buffer_key)
        if buffer_len > 0:
            return False
        
        return True

    async def execute(self, ctx: JobContext) -> JobResult:
        count_key = RedisKeys.heartbeat_counter(ctx.user_name, ctx.session_id)
        
        sorted_key = RedisKeys.recent_conversation(ctx.user_name, ctx.session_id)
        conv_key = RedisKeys.conversation(ctx.user_name, ctx.session_id)
        
        turn_ids = await ctx.redis.zrevrange(sorted_key, 0, self.conversation_window - 1)
        if not turn_ids:
            await ctx.redis.set(count_key, 0)
            return JobResult(success=True, summary="No conversation to evaluate")
        
        turn_ids = list(reversed(turn_ids))
        turn_data = await ctx.redis.hmget(conv_key, *turn_ids)
        
        lines = []
        for turn_id, raw in zip(turn_ids, turn_data):
            if not raw:
                continue
            parsed = json.loads(raw)
            role = "USER" if parsed["role"] == "user" else "AGENT"
            lines.append(f"[{role}]: {parsed['content']}")
        
        conversation_text = "\n".join(lines)
        
        if not conversation_text.strip():
            await ctx.redis.set(count_key, 0)
            return JobResult(success=True, summary="Empty conversation")
        
        current_config = json.dumps(self.topic_config.raw, indent=2)
        
        user_content = (
            f"## Current Config\n{current_config}\n\n"
            f"## Recent Conversation\n{conversation_text}"
        )
        
        system = get_topic_evolution_prompt()
        
        await emit(ctx.session_id, "job", "llm_call", {
            "stage": "topic_evolution",
            "prompt": user_content
        }, verbose_only=True)
        
        response = await self.llm.call_llm(system, user_content, model=self.llm.merge_model)
        
        if not response:
            logger.warning("Topic evolution LLM returned None")
            return JobResult(success=False, summary="LLM failed")
        
        try:
            clean = response.strip()
            if clean.startswith("```"):
                clean = clean.split("\n", 1)[1].rsplit("```", 1)[0]
            new_config = json.loads(clean)
        except json.JSONDecodeError as e:
            logger.warning(f"Topic evolution returned invalid JSON: {e}")
            return JobResult(success=False, summary=f"Invalid JSON: {e}")
        
        new_config = self.sanitize_topic_evolution(self.topic_config.raw, new_config)

        if new_config is None:
            await ctx.redis.set(count_key, 0)
            return JobResult(success=False, summary="Rejected: destructive changes")

        old_topics = set(self.topic_config.raw.keys())
        new_topics = set(new_config.keys())
        added = new_topics - old_topics
        
        old_active = {t for t, c in self.topic_config.raw.items() if c.get("active", True)}
        new_active = {t for t, c in new_config.items() if c.get("active", True)}
        deactivated = old_active - new_active
        
        if not added and not deactivated and new_config == self.topic_config.raw:
            await ctx.redis.set(count_key, 0)
            return JobResult(success=True, summary="No changes needed")
        
        await self.update_callback(new_config)
        await ctx.redis.set(count_key, 0)
        
        summary_parts = []
        if added:
            summary_parts.append(f"added: {', '.join(added)}")
        if deactivated:
            summary_parts.append(f"deactivated: {', '.join(deactivated)}")
        
        summary = f"Topics updated — {'; '.join(summary_parts)}" if summary_parts else "Topics adjusted"
        
        logger.info(f"[HEARTBEAT] {summary}")
        await emit(ctx.session_id, "job", "topic_config_evolved", {
            "added": list(added),
            "deactivated": list(deactivated),
            "total_topics": len(new_config)
        })
        
        return JobResult(success=True, summary=summary)
    
    @staticmethod
    def sanitize_topic_evolution(old_config: dict, new_config: dict) -> dict | None:
        """
        Deterministic guard between LLM output and config application.
        Returns sanitized config or None if change is too destructive.
        """
        sanitized = {}

        for topic_name, old_cfg in old_config.items():
            if topic_name not in new_config:
                logger.warning(f"[TOPIC GUARD] LLM removed '{topic_name}', restoring as-is")
                sanitized[topic_name] = old_cfg
            else:
                sanitized[topic_name] = new_config[topic_name]

        for topic_name, new_cfg in new_config.items():
            if topic_name not in sanitized:
                sanitized[topic_name] = new_cfg

        for protected in ("General", "Identity"):
            if protected in old_config:
                sanitized[protected] = old_config[protected]

        for topic_name in old_config:
            if topic_name in sanitized:
                sanitized[topic_name]["hierarchy"] = old_config[topic_name].get("hierarchy", {})

        old_active = {t for t, c in old_config.items() if c.get("active", True)}
        new_active = {t for t, c in sanitized.items() if c.get("active", True)}
        deactivated = old_active - new_active

        if old_active and len(deactivated) > len(old_active) // 2:
            logger.warning(
                f"[TOPIC GUARD] Rejected: bulk deactivation "
                f"({len(deactivated)}/{len(old_active)} active topics). "
                f"Attempted: {deactivated}"
            )
            return None

        new_topic_names = [t for t in sanitized if t not in old_config]
        if len(new_topic_names) > 3:
            logger.warning(f"[TOPIC GUARD] Capping new topics from {len(new_topic_names)} to 3")
            for excess in new_topic_names[3:]:
                del sanitized[excess]

        for topic_name, cfg in sanitized.items():
            raw_labels = cfg.get("labels", [])
            clean = []
            for label in raw_labels:
                if not isinstance(label, str):
                    continue
                label = label.strip().lower()
                if not label or len(label) > 30:
                    continue
                if not re.match(r'^[a-z][a-z0-9 _-]*$', label):
                    continue
                clean.append(label)
            cfg["labels"] = clean

        for topic_name in list(new_topic_names):
            if topic_name not in sanitized:
                continue
            cfg = sanitized[topic_name]
            if not isinstance(cfg.get("labels"), list):
                cfg["labels"] = []
            if not isinstance(cfg.get("aliases"), list):
                cfg["aliases"] = []
            if not isinstance(cfg.get("hierarchy"), dict):
                cfg["hierarchy"] = {}
            if "active" not in cfg:
                cfg["active"] = True

        return sanitized