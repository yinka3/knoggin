import copy
import json
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Dict, List, Optional

import yaml
from loguru import logger

from common.schema.settings import TopicSchema
from common.utils.json_utils import safe_json_loads

_TOPICS_SEED_FILE = Path(__file__).parent.parent / "templates" / "topics.yaml"
_TOPIC_NAME = re.compile(r"^[A-Z][A-Za-z0-9 _-]{1,39}$")
_LABEL = re.compile(r"^[a-z][a-z0-9 _-]{0,29}$")
_ALIAS = re.compile(r"^[a-z0-9][a-z0-9 _-]{0,39}$")
_OBSOLETE_TOPIC_SCHEMA_KEYS = frozenset({"hierarchy"})
PROTECTED_TOPICS = frozenset({"Identity"})
MAX_NEW_TOPICS_PER_UPDATE = 3
MAX_LABELS_PER_TOPIC = 20
MAX_ALIASES_PER_TOPIC = 20


def load_topic_seed() -> Dict[str, TopicSchema]:
    """Load immutable project defaults from packaged topics.yaml."""
    if not _TOPICS_SEED_FILE.is_file():
        raise FileNotFoundError(f"Topic seed file not found: {_TOPICS_SEED_FILE}")
    raw = yaml.safe_load(_TOPICS_SEED_FILE.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError("topics.yaml must contain a mapping of topic names")
    config = _validate_topic_mapping(raw)
    missing = PROTECTED_TOPICS - set(config)
    if missing:
        raise ValueError(f"topics.yaml is missing protected topics: {sorted(missing)}")
    config["Identity"].active = True
    return config


def _validate_topic_name(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("Topic names must be strings")
    name = " ".join(value.split())
    if not _TOPIC_NAME.fullmatch(name):
        raise ValueError(f"Invalid topic name: {value!r}")
    return name


def _validate_topic_mapping(
    config: Mapping[str, TopicSchema | dict],
) -> Dict[str, TopicSchema]:
    """Validate one complete topic configuration, including cross-topic rules."""

    if not isinstance(config, Mapping):
        raise ValueError("Topic configuration must be a mapping")

    validated: Dict[str, TopicSchema] = {}
    topic_keys = set()
    for raw_name, raw_schema in config.items():
        name = _validate_topic_name(raw_name)
        key = name.casefold()
        if key in topic_keys:
            raise ValueError(f"Duplicate topic name after normalization: {name}")
        topic_keys.add(key)
        schema = TopicSchema.model_validate(_migrate_topic_schema(raw_schema))
        schema.labels = _clean_unique_strings(
            schema.labels,
            field="labels",
            limit=MAX_LABELS_PER_TOPIC,
            pattern=_LABEL,
        )
        schema.aliases = _clean_unique_strings(
            schema.aliases,
            field="aliases",
            limit=MAX_ALIASES_PER_TOPIC,
            pattern=_ALIAS,
        )
        validated[name] = schema

    alias_owners = {}
    for name, schema in validated.items():
        for alias in schema.aliases:
            alias_key = alias.casefold()
            if alias_key in topic_keys:
                raise ValueError(
                    f"Topic alias collides with a canonical topic name: {alias}"
                )
            if owner := alias_owners.get(alias_key):
                raise ValueError(
                    "Topic alias collides with another topic alias: "
                    f"{alias} ({owner} and {name})"
                )
            alias_owners[alias_key] = name
    return validated


def _migrate_topic_schema(raw_schema: TopicSchema | dict) -> TopicSchema | dict:
    """Drop recognized obsolete topic-config fields before strict validation.

    Hierarchy is stored as `PART_OF` edges in the knowledge graph.  It was
    previously duplicated as an unused topic-config object, so persisted
    project configurations may still contain the obsolete key.
    """

    if isinstance(raw_schema, TopicSchema):
        return raw_schema
    if not isinstance(raw_schema, Mapping):
        return raw_schema
    migrated = dict(raw_schema)
    for key in _OBSOLETE_TOPIC_SCHEMA_KEYS:
        migrated.pop(key, None)
    return migrated


def build_label_block(topics_config: Dict[str, TopicSchema]) -> str:
    """Format active topic labels for entity extraction prompts."""
    lines = []
    for topic, config in topics_config.items():
        if topic == "Identity" or not config.active or not config.labels:
            continue
        lines.append(f"Topic: {topic}")
        lines.append(f"  Labels: {', '.join(config.labels)}")
        lines.append("")
    return "\n".join(lines)


def build_topic_alias_lookup(
    topics_config: Dict[str, TopicSchema],
) -> Dict[str, str]:
    lookup = {}
    for topic_name, config in topics_config.items():
        lookup[topic_name.lower()] = topic_name
        for alias in config.aliases:
            lookup[alias.lower()] = topic_name
    return lookup


def get_active_topic_names(
    topics_config: Dict[str, TopicSchema],
) -> List[str]:
    return [name for name, config in topics_config.items() if config.active]


def _clean_unique_strings(
    values,
    *,
    field: str,
    limit: int,
    pattern: Optional[re.Pattern] = None,
) -> List[str]:
    cleaned = []
    seen = set()
    for raw in values or []:
        if not isinstance(raw, str):
            raise ValueError(f"{field} values must be strings")
        value = raw.strip()
        if not value:
            continue
        normalized = value.lower()
        if pattern and not pattern.fullmatch(normalized):
            raise ValueError(f"Invalid {field} value: {raw!r}")
        if normalized not in seen:
            seen.add(normalized)
            cleaned.append(normalized)
        if len(cleaned) > limit:
            raise ValueError(f"{field} may contain at most {limit} values")
    return cleaned


class TopicConfig:
    """Project-scoped topic configuration backed by Postgres."""

    def __init__(self, config: Dict[str, TopicSchema]):
        self._config = _validate_topic_mapping(copy.deepcopy(config))
        self._alias_lookup: Optional[Dict[str, str]] = None
        self._label_block: Optional[str] = None
        self._active_topics: Optional[List[str]] = None
        self._hot_topics: Optional[List[str]] = None
        self._enforce_protected_topics()

    @classmethod
    def seed(cls) -> "TopicConfig":
        return cls(load_topic_seed())

    @classmethod
    async def load(
        cls,
        pg_client,
        user_name: str,
        project_id: str,
    ) -> "TopicConfig":
        rows = await pg_client.fetch_all(
            """
            SELECT topic_config
            FROM public.projects
            WHERE user_name = %(user_name)s AND project_id = %(project_id)s
            """,
            {"user_name": user_name, "project_id": project_id},
        )
        if not rows:
            raise ValueError(f"Project not found while loading topics: {project_id}")

        raw = rows[0].get("topic_config")
        if isinstance(raw, str):
            raw = safe_json_loads(raw, {})
        if raw is None or raw == {} or raw == "":
            raise ValueError(
                f"Project {project_id} has no persisted topic projection; "
                "create it from an active DomainConfig"
            )
        try:
            return cls(raw)
        except Exception as exc:
            raise ValueError(
                f"Invalid persisted topic config for project {project_id}: {exc}"
            ) from exc

    async def save(self, pg_client, user_name: str, project_id: str) -> None:
        dumped = {
            name: config.model_dump(mode="json")
            for name, config in self._config.items()
        }
        updated = await pg_client.execute(
            """
            UPDATE public.projects
            SET topic_config = %(config)s, updated_at = now()
            WHERE user_name = %(user_name)s AND project_id = %(project_id)s
            """,
            {
                "config": json.dumps(dumped),
                "user_name": user_name,
                "project_id": project_id,
            },
        )
        if updated != 1:
            raise ValueError(f"Project not found while saving topics: {project_id}")
        logger.debug(f"TopicConfig saved for project {project_id}")

    def snapshot(self) -> Dict[str, TopicSchema]:
        return copy.deepcopy(self._config)

    def replace(self, config: Dict[str, TopicSchema]) -> None:
        self._config = _validate_topic_mapping(copy.deepcopy(config))
        self._enforce_protected_topics()
        self._clear_cache()

    def apply_agent_update(
        self,
        *,
        add_topics: Optional[List[dict]] = None,
        deactivate_topics: Optional[List[str]] = None,
    ) -> Dict[str, List[str]]:
        additions = add_topics or []
        active_before = set(self.active_topics)
        if len(additions) > MAX_NEW_TOPICS_PER_UPDATE:
            raise ValueError(
                f"At most {MAX_NEW_TOPICS_PER_UPDATE} topics may be added per update"
            )

        added = []
        for proposal in additions:
            if not isinstance(proposal, dict):
                raise ValueError("Each topic proposal must be an object")
            name = str(proposal.get("name", "")).strip()
            if not _TOPIC_NAME.fullmatch(name):
                raise ValueError(f"Invalid topic name: {name!r}")
            if name in self._config:
                raise ValueError(f"Topic already exists: {name}")
            if name.lower() in self.alias_lookup:
                raise ValueError(f"Topic name collides with an existing alias: {name}")

            labels = _clean_unique_strings(
                proposal.get("labels"),
                field="labels",
                limit=MAX_LABELS_PER_TOPIC,
                pattern=_LABEL,
            )
            aliases = _clean_unique_strings(
                proposal.get("aliases"),
                field="aliases",
                limit=MAX_ALIASES_PER_TOPIC,
                pattern=_ALIAS,
            )
            if name.lower() in aliases:
                aliases.remove(name.lower())
            collisions = [alias for alias in aliases if alias in self.alias_lookup]
            if collisions:
                raise ValueError(
                    f"Topic aliases collide with existing topics: {collisions}"
                )

            self._config[name] = TopicSchema(
                active=True,
                hot=False,
                labels=labels,
                aliases=aliases,
            )
            self._clear_cache()
            added.append(name)

        requested_deactivations = list(
            dict.fromkeys(
                str(name).strip()
                for name in (deactivate_topics or [])
                if str(name).strip()
            )
        )
        protected = sorted(PROTECTED_TOPICS.intersection(requested_deactivations))
        if protected:
            raise ValueError(f"Protected topics cannot be deactivated: {protected}")
        unknown = [name for name in requested_deactivations if name not in self._config]
        if unknown:
            raise ValueError(f"Unknown topics cannot be deactivated: {unknown}")

        actual_deactivations = [
            name for name in requested_deactivations if self._config[name].active
        ]
        if active_before and len(actual_deactivations) > len(active_before) // 2:
            raise ValueError("Bulk topic deactivation was rejected")

        for name in actual_deactivations:
            self._config[name].active = False

        self._enforce_protected_topics()
        self._config = _validate_topic_mapping(self._config)
        self._clear_cache()
        return {"added": added, "deactivated": actual_deactivations}

    def _enforce_protected_topics(self) -> None:
        seed = load_topic_seed()
        for name in PROTECTED_TOPICS:
            if name not in self._config:
                self._config[name] = copy.deepcopy(seed[name])
            self._config[name].active = True

    def _clear_cache(self) -> None:
        self._alias_lookup = None
        self._label_block = None
        self._active_topics = None
        self._hot_topics = None

    @property
    def raw(self) -> Dict[str, TopicSchema]:
        return copy.deepcopy(self._config)

    @property
    def alias_lookup(self) -> Dict[str, str]:
        if self._alias_lookup is None:
            self._alias_lookup = build_topic_alias_lookup(self._config)
        return dict(self._alias_lookup)

    @property
    def label_block(self) -> str:
        if self._label_block is None:
            self._label_block = build_label_block(self._config)
        return self._label_block

    @property
    def active_topics(self) -> List[str]:
        if self._active_topics is None:
            self._active_topics = get_active_topic_names(self._config)
        return list(self._active_topics)

    @property
    def hot_topics(self) -> List[str]:
        if self._hot_topics is None:
            active = set(self.active_topics)
            self._hot_topics = [
                name
                for name, config in self._config.items()
                if config.hot and name in active
            ]
        return list(self._hot_topics)

    def normalize_topic(self, topic: str) -> Optional[str]:
        if not topic:
            return None
        canonical = self.alias_lookup.get(topic.lower())
        if canonical:
            return canonical
        return None

    def get_labels_for_topic(self, topic: str) -> List[str]:
        config = self._config.get(topic)
        return list(config.labels) if config else []

    def is_active(self, topic: str) -> bool:
        config = self._config.get(topic)
        return bool(config and config.active)

    def validate_hot_topics(self, hot_topics: List[str]) -> List[str]:
        if not hot_topics:
            return []
        active = set(self.active_topics)
        valid = []
        for topic in hot_topics:
            canonical = (
                self.alias_lookup.get(topic.strip().lower())
                if isinstance(topic, str) and topic.strip()
                else None
            )
            if canonical and canonical in active and canonical not in valid:
                valid.append(canonical)
        return valid
