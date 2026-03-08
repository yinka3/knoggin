
import re
from typing import Dict, List, Optional
from loguru import logger

from shared.config.topics_config import TopicConfig
from shared.services.topic_gen import generate_topics, DEFAULT_TOPICS


# Label validation — same regex as TopicConfigJob.sanitize_topic_evolution
LABEL_PATTERN = re.compile(r'^[a-z][a-z0-9 _-]*$')
MAX_LABEL_LEN = 30


def _validate_label(label: str) -> Optional[str]:
    """Validate and normalize a label. Returns cleaned label or None if invalid."""
    label = label.strip().lower()
    if not label or len(label) > MAX_LABEL_LEN:
        return None
    if not LABEL_PATTERN.match(label):
        return None
    return label


class TopicBuilder:
    """Fluent builder for topic configurations. Always includes General and Identity."""

    def __init__(self):
        self._topics: Dict[str, dict] = {}

    def topic(
        self,
        name: str,
        labels: List[str] = None,
        aliases: List[str] = None,
        hierarchy: Dict = None,
        active: bool = True,
        hot: bool = False,
    ) -> "TopicBuilder":
        """Add a topic to the configuration. Returns self for chaining."""
        if name in ("General", "Identity"):
            logger.warning(f"'{name}' is system-managed and will be auto-included. Skipping.")
            return self

        clean_labels = []
        for raw in (labels or []):
            cleaned = _validate_label(raw)
            if cleaned:
                clean_labels.append(cleaned)
            else:
                logger.warning(f"Invalid label '{raw}' for topic '{name}' — skipped")

        self._topics[name] = {
            "active": active,
            "labels": clean_labels,
            "aliases": aliases or [],
            "hierarchy": hierarchy or {},
            "hot": hot,
        }
        return self

    def build(self) -> dict:
        """Build the final topic config dict. Always includes General and Identity."""
        return {**DEFAULT_TOPICS, **self._topics}


class TopicManager:
    """Runtime topic configuration manager. Changes propagate immediately."""

    def __init__(self, topic_config: TopicConfig):
        self._config = topic_config

    @classmethod
    def from_builder(cls, builder: TopicBuilder) -> "TopicManager":
        """Create a TopicManager from a TopicBuilder."""
        return cls(TopicConfig(builder.build()))

    @property
    def raw(self) -> dict:
        return self._config.raw

    @property
    def active_topics(self) -> List[str]:
        return self._config.active_topics

    @property
    def hot_topics(self) -> List[str]:
        return self._config.hot_topics

    def list_topics(self) -> List[Dict]:
        return [
            {
                "name": name,
                "active": cfg.get("active", True),
                "hot": cfg.get("hot", False),
                "labels": cfg.get("labels", []),
                "aliases": cfg.get("aliases", []),
                "hierarchy": cfg.get("hierarchy", {}),
            }
            for name, cfg in self._config.raw.items()
        ]

    def get_topic(self, name: str) -> Optional[Dict]:
        cfg = self._config.raw.get(name)
        if not cfg:
            return None
        return {
            "name": name,
            "active": cfg.get("active", True),
            "hot": cfg.get("hot", False),
            "labels": cfg.get("labels", []),
            "aliases": cfg.get("aliases", []),
            "hierarchy": cfg.get("hierarchy", {}),
        }

    def add_topic(
        self,
        name: str,
        labels: List[str] = None,
        aliases: List[str] = None,
        hierarchy: Dict = None,
        active: bool = True,
        hot: bool = False,
    ) -> bool:
        if name in ("General", "Identity"):
            logger.warning(f"'{name}' is system-managed. Skipping.")
            return False

        if name in self._config.raw:
            logger.warning(f"Topic '{name}' already exists.")
            return False

        # Validate labels
        clean_labels = [l for raw in (labels or []) if (l := _validate_label(raw))]

        self._config.add_topic(name, {
            "active": active,
            "labels": clean_labels,
            "aliases": aliases or [],
            "hierarchy": hierarchy or {},
            "hot": hot,
        })
        return True

    def remove_topic(self, name: str) -> bool:
        """Remove a topic. General and Identity cannot be removed."""
        if name in ("General", "Identity"):
            logger.warning(f"Cannot remove system-managed topic '{name}'.")
            return False

        if name not in self._config.raw:
            return False

        self._config.remove_topic(name)
        logger.info(f"Topic '{name}' removed")
        return True

    def set_active(self, name: str, active: bool = True) -> bool:
        if name not in self._config.raw:
            return False
        self._config.toggle_active(name, active)
        return True

    def set_hot(self, name: str, hot: bool = True) -> bool:
        if name not in self._config.raw:
            return False
        self._config.raw[name]["hot"] = hot
        self._config._hot_topics = None
        logger.info(f"Topic '{name}' hot={hot}")
        return True

    def update_labels(self, name: str, add: List[str] = None, remove: List[str] = None) -> bool:
        if name not in self._config.raw:
            return False

        current = list(self._config.raw[name].get("labels", []))

        if remove:
            to_remove = {_validate_label(l) for l in remove} - {None}
            current = [l for l in current if l not in to_remove]

        if add:
            existing = set(current)
            for raw in add:
                cleaned = _validate_label(raw)
                if cleaned and cleaned not in existing:
                    current.append(cleaned)
                    existing.add(cleaned)

        self._config.raw[name]["labels"] = current
        self._config._clear_cache()
        logger.info(f"Labels updated for '{name}'")
        return True

    def add_aliases(self, name: str, aliases: List[str]) -> bool:
        if name not in self._config.raw:
            return False

        existing = set(self._config.raw[name].get("aliases", []))
        new_aliases = [a for a in aliases if a not in existing]

        if new_aliases:
            self._config.raw[name]["aliases"] = list(existing) + new_aliases
            self._config._clear_cache()
            logger.info(f"Added aliases to '{name}': {new_aliases}")
        return True

    def set_hierarchy(self, name: str, hierarchy: Dict) -> bool:
        if name not in self._config.raw:
            return False
        self._config.raw[name]["hierarchy"] = hierarchy
        self._config._clear_cache()
        logger.info(f"Hierarchy updated for '{name}'")
        return True

    @staticmethod
    async def generate(client, description: str, max_topics: int = 6) -> dict:
        return await generate_topics(client.llm, description, max_topics)