import json
from typing import Dict, List, Optional, Tuple
from loguru import logger
import redis.asyncio as aioredis

from shared.redisclient import RedisKeys


def build_label_block(topics_config: dict) -> str:
    """Formats topics config into prompt-friendly label list for VP-01."""
    lines = []
    for topic, config in topics_config.items():
        labels = config.get("labels", [])
        if labels:
            lines.append(f"Topic: {topic}")
            lines.append(f"  Labels: {', '.join(labels)}")
            lines.append("")
    return "\n".join(lines)

def build_label_alias_lookup(topics_config: dict) -> Dict[str, List[Tuple[str, str]]]:
    """Builds reverse lookup: label alias → [(canonical_label, topic), ...]"""
    lookup = {}
    for topic_name, config in topics_config.items():
        for alias, canonical in config.get("label_aliases", {}).items():
            alias_lower = alias.lower()
            if alias_lower not in lookup:
                lookup[alias_lower] = []
            lookup[alias_lower].append((canonical, topic_name))
    return lookup


def build_topic_alias_lookup(topics_config: dict) -> Dict[str, str]:
    """Builds reverse lookup: alias/variant → canonical topic name."""
    lookup = {}
    for topic_name, config in topics_config.items():
        lookup[topic_name.lower()] = topic_name
        for alias in config.get("aliases", []):
            lookup[alias.lower()] = topic_name
    return lookup


def get_active_topic_names(topics_config: dict) -> List[str]:
    """Returns list of topic names where active=True."""
    return [
        topic_name 
        for topic_name, config in topics_config.items() 
        if config.get("active", True)
    ]


class TopicConfig:
    """
    Centralized topic configuration with lazy-computed derived values.
    Single source of truth for label blocks, aliases, hierarchy, and active topics.
    """
    
    DEFAULT_CONFIG = {
        "General": {
            "active": True, 
            "labels": [],
            "hierarchy": {}, 
            "aliases": [],
            "label_aliases": {},
        }
    }
    
    def __init__(self, config: dict):
        self._config = config
        self._alias_lookup: Optional[Dict[str, str]] = None
        self._label_block: Optional[str] = None
        self._hierarchy: Optional[Dict[str, dict]] = None
        self._active_topics: Optional[List[str]] = None
        self._label_alias_lookup: Optional[Dict[str, List[Tuple[str, str]]]] = None
    
    @classmethod
    async def load(
        cls, 
        redis_client: aioredis.Redis, 
        user_name: str, 
        session_id: str
    ) -> "TopicConfig":
        """Load config from Redis."""
        raw = await redis_client.hget(RedisKeys.session_config(user_name), session_id)
        if raw:
            config = json.loads(raw)
        else:
            config = cls.DEFAULT_CONFIG.copy()
        return cls(config)
    
    async def save(
        self, 
        redis_client: aioredis.Redis, 
        user_name: str, 
        session_id: str
    ):
        """Persist config to Redis."""
        await redis_client.hset(
            RedisKeys.session_config(user_name),
            session_id, 
            json.dumps(self._config)
        )
        logger.debug(f"TopicConfig saved for session {session_id}")
    
    @property
    def label_alias_lookup(self) -> Dict[str, List[Tuple[str, str]]]:
        """Lazy-built label alias → [(canonical, topic), ...] mapping."""
        if self._label_alias_lookup is None:
            self._label_alias_lookup = build_label_alias_lookup(self._config)
        return self._label_alias_lookup
    
    def _clear_cache(self):
        """Clear all cached derived values."""
        self._alias_lookup = None
        self._label_block = None
        self._hierarchy = None
        self._active_topics = None
        self._label_alias_lookup = None
    
    @property
    def raw(self) -> dict:
        """Raw config dict."""
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
                topic: cfg.get("hierarchy", {})
                for topic, cfg in self._config.items()
            }
        return self._hierarchy
    
    @property
    def active_topics(self) -> List[str]:
        """Lazy-built list of active topic names."""
        if self._active_topics is None:
            self._active_topics = get_active_topic_names(self._config)
        return self._active_topics
    
    def normalize_topic(self, topic: str) -> Optional[str]:
        """Normalize extracted topic to canonical name."""
        if not topic:
            return None
        return self.alias_lookup.get(topic.lower(), "General")
    
    def get_labels_for_topic(self, topic: str) -> List[str]:
        """Get allowed labels for a specific topic."""
        config = self._config.get(topic, {})
        return config.get("labels", [])
    
    def is_active(self, topic: str) -> bool:
        """Check if a topic is currently active."""
        config = self._config.get(topic, {})
        return config.get("active", True)
    
    def update(self, new_config: dict):
        """
        Update config and invalidate cache.
        Logs warnings for label modifications.
        """
        for topic_name in self._config:
            if topic_name in new_config:
                old_labels = set(self._config[topic_name].get("labels", []))
                new_labels = set(new_config[topic_name].get("labels", []))
                if old_labels != new_labels:
                    logger.warning(
                        f"Labels modified for '{topic_name}': {old_labels} → {new_labels}"
                    )
        
        self._config = new_config
        self._clear_cache()
        logger.info(f"TopicConfig updated: {list(new_config.keys())}")
    
    def add_topic(self, topic_name: str, config: dict):
        """Add a new topic. Safe mid-session."""
        if topic_name in self._config:
            logger.warning(f"Topic '{topic_name}' already exists. Use update() instead.")
            return
        
        self._config[topic_name] = config
        self._clear_cache()
        logger.info(f"Topic added: {topic_name}")
    
    def toggle_active(self, topic_name: str, active: bool):
        """Toggle topic active state."""
        if topic_name not in self._config:
            logger.warning(f"Topic '{topic_name}' not found.")
            return
        
        self._config[topic_name]["active"] = active
        self._active_topics = None  # only invalidate active_topics cache
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
            logger.warning(f"Hot topics filtered out (not active or unknown): {invalid}")
        
        return valid