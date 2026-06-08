import copy
import json
from typing import Dict, List, Optional

import redis.asyncio as aioredis
from loguru import logger

from common.schema.settings import TopicSchema
from common.utils.json_utils import safe_json_loads
from infrastructure.redis_client import RedisKeys


def build_label_block(topics_config: Dict[str, TopicSchema]) -> str:
    """Formats topics config into prompt-friendly label list for VP-01."""
    lines = []
    for topic, config in topics_config.items():
        if topic == "Identity":
            continue
        if config.active is False:
            continue
        labels = config.labels
        if labels:
            lines.append(f"Topic: {topic}")
            lines.append(f"  Labels: {', '.join(labels)}")
            lines.append("")
    return "\n".join(lines)


def build_topic_alias_lookup(topics_config: Dict[str, TopicSchema]) -> Dict[str, str]:
    """Builds reverse lookup: alias/variant → canonical topic name."""
    lookup = {}
    for topic_name, config in topics_config.items():
        lookup[topic_name.lower()] = topic_name
        for alias in config.aliases:
            lookup[alias.lower()] = topic_name
    return lookup


def get_active_topic_names(topics_config: Dict[str, TopicSchema]) -> List[str]:
    """Returns list of topic names where active=True."""
    return [
        topic_name
        for topic_name, config in topics_config.items()
        if config.active
    ]


class TopicConfig:
    """
    Centralized topic configuration with lazy-computed derived values.
    Single source of truth for label blocks, aliases, hierarchy, and active topics.
    """

    DEFAULT_CONFIG: Dict[str, TopicSchema] = {
        "General": TopicSchema(active=True, hot=False, labels=[], hierarchy={}, aliases=[])
    }

    def __init__(self, config: Dict[str, TopicSchema]):
        self._config = config
        self._alias_lookup: Optional[Dict[str, str]] = None
        self._label_block: Optional[str] = None
        self._hierarchy: Optional[Dict[str, dict]] = None
        self._active_topics: Optional[List[str]] = None
        self._hot_topics: Optional[List[str]] = None

    @classmethod
    async def load(
        cls, redis_client: aioredis.Redis, user_name: str, project_id: str
    ) -> "TopicConfig":
        """Load config from Redis."""
        raw = await redis_client.hget(
            RedisKeys.project_topic_config(user_name), project_id
        )
        if raw:
            try:
                raw_dict = safe_json_loads(raw)
                if not raw_dict:
                    config = copy.deepcopy(cls.DEFAULT_CONFIG)
                else:
                    config = {k: TopicSchema(**v) for k, v in raw_dict.items()}
            except Exception as e:
                logger.error(f"Failed to decode topic config from Redis: {e}")
                config = copy.deepcopy(cls.DEFAULT_CONFIG)
        else:
            config = copy.deepcopy(cls.DEFAULT_CONFIG)
        return cls(config)

    async def save(self, redis_client: aioredis.Redis, user_name: str, project_id: str):
        """Persist config to Redis."""
        dumped = {k: v.model_dump() for k, v in self._config.items()}
        await redis_client.hset(
            RedisKeys.project_topic_config(user_name), project_id, json.dumps(dumped)
        )
        logger.debug(f"TopicConfig saved for project {project_id}")

    def _clear_cache(self):
        """Clear all cached derived values."""
        self._alias_lookup = None
        self._label_block = None
        self._hierarchy = None
        self._active_topics = None
        self._hot_topics = None

    @property
    def raw(self) -> Dict[str, TopicSchema]:
        """Raw config models."""
        return self._config

    @property
    def alias_lookup(self) -> Dict[str, str]:
        """Lazy-built alias → canonical topic mapping."""
        if self._alias_lookup is None:
            self._alias_lookup = build_topic_alias_lookup(self._config)
        return self._alias_lookup

    @property
    def label_block(self) -> str:
        """Lazy-built prompt block for VP-01."""
        if self._label_block is None:
            self._label_block = build_label_block(self._config)
        return self._label_block

    @property
    def hierarchy(self) -> Dict[str, dict]:
        """Lazy-built topic → hierarchy mapping."""
        if self._hierarchy is None:
            self._hierarchy = {
                topic: cfg.hierarchy for topic, cfg in self._config.items()
            }
        return self._hierarchy

    @property
    def active_topics(self) -> List[str]:
        """Lazy-built list of active topic names."""
        if self._active_topics is None:
            self._active_topics = get_active_topic_names(self._config)
        return self._active_topics

    @property
    def hot_topics(self) -> List[str]:
        """Lazy-built list of hot topic names (active + hot=True)."""
        if self._hot_topics is None:
            active = set(self.active_topics)
            self._hot_topics = [
                name
                for name, cfg in self._config.items()
                if cfg.hot and name in active
            ]
        return self._hot_topics

    def normalize_topic(self, topic: str) -> Optional[str]:
        """Normalize extracted topic to canonical name."""
        if not topic:
            return None
        canonical = self.alias_lookup.get(topic.lower())
        if canonical:
            return canonical
        # Fallback to General only if it's active
        if self.is_active("General"):
            return "General"
        return None

    def get_labels_for_topic(self, topic: str) -> List[str]:
        """Get allowed labels for a specific topic."""
        config = self._config.get(topic)
        if config:
            return config.labels
        return []

    def is_active(self, topic: str) -> bool:
        """Check if a topic is currently active."""
        config = self._config.get(topic)
        if config:
            return config.active
        return False

    def update(self, new_config: dict):
        """
        Update config and invalidate cache.
        Logs warnings for label modifications.
        """
        for topic_name, topic_cfg_dict in new_config.items():
            if isinstance(topic_cfg_dict, dict):
                topic_cfg = TopicSchema(**topic_cfg_dict)
            else:
                topic_cfg = topic_cfg_dict

            if topic_name in self._config:
                old_labels = set(self._config[topic_name].labels)
                new_labels = set(topic_cfg.labels)
                if old_labels != new_labels:
                    logger.warning(
                        f"Labels modified for '{topic_name}': {old_labels} → {new_labels}"
                    )
            else:
                logger.info(f"Adding new topic via update: {topic_name}")

            self._config[topic_name] = topic_cfg

        self._clear_cache()
        logger.info(f"TopicConfig updated: {list(new_config.keys())}")

    def add_topic(self, topic_name: str, config: TopicSchema):
        """Add a new topic. Safe mid-session."""
        if topic_name in self._config:
            logger.warning(
                f"Topic '{topic_name}' already exists. Use update() instead."
            )
            return

        self._config[topic_name] = config
        self._clear_cache()
        logger.info(f"Topic added: {topic_name}")

    def remove_topic(self, name: str):
        """Remove a topic and clear derived caches."""
        if name in self._config:
            del self._config[name]
            self._clear_cache()

    def toggle_active(self, topic_name: str, active: bool):
        """Toggle topic active state."""
        if topic_name not in self._config:
            logger.warning(f"Topic '{topic_name}' not found.")
            return

        self._config[topic_name].active = active
        self._clear_cache()
        logger.info(f"Topic '{topic_name}' active={active}")

    def validate_hot_topics(self, hot_topics: List[str]) -> List[str]:
        """Filter hot topics to only include active ones."""
        if not hot_topics:
            return []

        active = set(self.active_topics)
        valid = []
        invalid = []

        for topic in hot_topics:
            canonical = self.normalize_topic(topic)
            if canonical and canonical in active:
                if canonical not in valid:
                    valid.append(canonical)
            else:
                invalid.append(topic)

        if invalid:
            logger.warning(
                f"Hot topics filtered out (not active or unknown): {invalid}"
            )

        return valid
