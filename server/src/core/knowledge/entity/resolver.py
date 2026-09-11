from __future__ import annotations

import asyncio
import re
import threading
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from cachetools import LRUCache, cached
from loguru import logger
from rapidfuzz import fuzz, process
from wordfreq import word_frequency

from common.schema.ingestion.contracts import (
    ContextBlockEntityAssociation,
    ContextBlockMention,
    EntityWrite,
    ProjectEntityClassification,
    ResolvedContextBlockMention,
    ValidationIssue,
)
from common.schema.settings import EntityResolutionSettings
from common.scoping import (
    IDENTITY_ENTITY_ID,
    require_scope_value,
    require_visible_project_ids,
)
from common.utils.events import emit_sync
from core.ingestion.policy import IngestionPolicy
from core.knowledge.entity.embedding import (
    build_entity_embedding_text,
)
from core.knowledge.entity.index import EntityIndex
from core.knowledge.entity.profile import EntityProfile
from core.knowledge.services.embedding_service import EmbeddingService

if TYPE_CHECKING:
    from core.knowledge.store import KnowledgeStore


@dataclass
class EntityCandidate:
    entity_id: int
    score: float = 0.0
    signals: set[str] = field(default_factory=set)
    exact_score: Optional[float] = None
    fuzzy_score: Optional[float] = None
    vector_score: Optional[float] = None

    def __iter__(self):
        yield self.entity_id
        yield self.score

    def __getitem__(self, index: int):
        return (self.entity_id, self.score)[index]

    def __eq__(self, other):
        if isinstance(other, tuple):
            return (self.entity_id, self.score) == other
        if isinstance(other, EntityCandidate):
            return (
                self.entity_id == other.entity_id
                and self.score == other.score
                and self.signals == other.signals
                and self.exact_score == other.exact_score
                and self.fuzzy_score == other.fuzzy_score
                and self.vector_score == other.vector_score
            )
        return False

    def add_signal(self, signal: str, score: float) -> None:
        self.signals.add(signal)
        self.score = max(self.score, score)
        if signal == "exact":
            self.exact_score = max(self.exact_score or 0.0, score)
        elif signal == "fuzzy":
            self.fuzzy_score = max(self.fuzzy_score or 0.0, score)
        elif signal == "vector":
            self.vector_score = max(self.vector_score or 0.0, score)

    @property
    def has_direct_name_evidence(self) -> bool:
        return "exact" in self.signals and "ambiguous_alias" not in self.signals


class EntityResolver:
    def __init__(
        self,
        knowledge_store: "KnowledgeStore",
        embedding_service: EmbeddingService,
        project_id: str,
        readable_project_ids: List[str],
        fuzzy_substring_threshold: int = 75,
        fuzzy_non_substring_threshold: int = 91,
        generic_token_freq: int = 10,
        candidate_fuzzy_threshold: int = 85,
        candidate_vector_threshold: float = 0.85,
    ):

        self.knowledge_store = knowledge_store
        self.project_id = require_scope_value(
            project_id,
            "project_id",
            "EntityResolver",
        )
        self.readable_project_ids = require_visible_project_ids(
            readable_project_ids,
            "EntityResolver",
        )
        self.embedding_service = embedding_service
        self._index = EntityIndex()
        self._alias_version = 0
        self._lock = threading.RLock()
        self._resolution_lock = asyncio.Lock()

        self.candidate_fuzzy_threshold = candidate_fuzzy_threshold
        self.candidate_vector_threshold = candidate_vector_threshold
        self.fuzzy_substring_threshold = fuzzy_substring_threshold
        self.fuzzy_non_substring_threshold = fuzzy_non_substring_threshold
        self.generic_token_freq = generic_token_freq

    @property
    def resolution_lock(self) -> asyncio.Lock:
        return self._resolution_lock

    def get_alias_version(self) -> int:
        with self._lock:
            return self._alias_version

    def _bump_alias_version(self) -> None:
        self._alias_version += 1

    def update_settings(self, config: EntityResolutionSettings):
        """Update resolution thresholds on the fly."""
        self.fuzzy_substring_threshold = config.fuzzy_substring_threshold
        self.fuzzy_non_substring_threshold = config.fuzzy_non_substring_threshold
        self.generic_token_freq = config.generic_token_freq
        self.candidate_fuzzy_threshold = config.candidate_fuzzy_threshold
        self.candidate_vector_threshold = config.candidate_vector_threshold

        logger.info(
            "EntityResolver settings updated: "
            f"sub={self.fuzzy_substring_threshold}, "
            f"non-sub={self.fuzzy_non_substring_threshold}, "
            f"freq={self.generic_token_freq}"
        )

    @staticmethod
    def _candidate_search_key(name: str) -> str:
        """Normalize a surface for reusable durable/cached candidate searches."""

        return name.strip().casefold()

    async def resolve_context_block_mentions(
        self,
        mentions: list[ContextBlockMention],
        *,
        block_text_by_id: dict[object, str],
        policy: IngestionPolicy,
        parent_work_record=None,
        allocate_entity_id,
        issues: Optional[List[ValidationIssue]] = None,
    ) -> Dict[str, Any]:
        """Resolve typed Context-block mentions without manufacturing message refs.

        Context-first callers use this boundary: block associations are returned
        independently, and literal message evidence is deliberately left to the
        result assembler.
        """

        if any(not isinstance(mention, ContextBlockMention) for mention in mentions):
            raise TypeError("Context entity resolution requires ContextBlockMention values")
        if not isinstance(block_text_by_id, dict) or any(
            not isinstance(text, str) for text in block_text_by_id.values()
        ):
            raise TypeError("Context entity resolution requires block text by ID")

        async with self.resolution_lock:
            entity_ids: List[int] = []
            new_ids: set[int] = set()
            alias_ids: set[int] = set()
            alias_updates: Dict[int, List[str]] = {}
            pending_entity_writes: Dict[int, EntityWrite] = {}
            pending_ids_by_surface: Dict[str, List[int]] = {}
            pending_support_by_id: Dict[int, str] = {}
            project_classifications: Dict[int, ProjectEntityClassification] = {}
            resolved_mentions: list[ResolvedContextBlockMention] = []
            associations: list[ContextBlockEntityAssociation] = []

            candidate_entries = await self.candidate_entries_for_context_block_mentions(
                mentions,
                policy=policy,
                parent_work_record=parent_work_record,
            )

            for index, mention in enumerate(mentions):
                entry = candidate_entries[index]
                if entry is None:
                    continue
                surface_key = self._candidate_search_key(mention.name)
                entity_id: int | None = None
                selected_profile: EntityProfile | None = None
                support_text = "\n".join(
                    block_text_by_id.get(block_id, "") for block_id in mention.block_ids
                )

                if entry[0] == "candidates":
                    for candidate in entry[1]:
                        candidate_id = candidate.entity_id
                        profile = await self.get_profile(candidate_id)
                        compatibility = (
                            self.schema_compatibility(
                                mention.entity_type,
                                mention.topic,
                                profile,
                                policy,
                            )
                            if profile
                            else "missing_profile"
                        )
                        if (
                            candidate.score < policy.resolution_threshold
                            or profile is None
                            or not self.is_profile_visible(profile)
                        ):
                            continue
                        if self.should_accept_candidate(
                            mention.name,
                            mention.entity_type,
                            mention.topic,
                            support_text,
                            profile,
                            candidate_id,
                            policy=policy,
                            compatibility=compatibility,
                            candidate=candidate,
                        ):
                            classification = self._classification_for_resolution(
                                candidate_id,
                                mention,
                                profile,
                            )
                            if self._classification_conflicts(
                                project_classifications,
                                classification,
                            ):
                                continue
                            entity_id = candidate_id
                            selected_profile = profile
                            new_aliases = self._new_aliases_for_selected_entity(
                                candidate_id,
                                [mention.name.strip()],
                            )
                            if new_aliases:
                                alias_ids.add(candidate_id)
                                alias_updates.setdefault(candidate_id, []).extend(
                                    new_aliases
                                )
                            break

                if entity_id is None:
                    for pending_id in pending_ids_by_surface.get(surface_key, []):
                        pending_write = pending_entity_writes[pending_id]
                        if self._should_reuse_pending_entity(
                            mention,
                            support_text,
                            pending_write,
                            pending_support_by_id[pending_id],
                            policy,
                        ):
                            entity_id = pending_id
                            break
                    if entity_id is None:
                        entity_id = await allocate_entity_id()
                        pending_entity_writes[
                            entity_id
                        ] = await self.prepare_pending_entity(
                            entity_id,
                            mention.name.strip(),
                            [mention.name.strip()],
                            mention.entity_type,
                            mention.topic,
                        )
                        new_ids.add(entity_id)
                        pending_ids_by_surface.setdefault(surface_key, []).append(
                            entity_id
                        )
                        pending_support_by_id[entity_id] = support_text

                classification = self._classification_for_resolution(
                    entity_id,
                    mention,
                    selected_profile,
                )
                if self._classification_conflicts(
                    project_classifications,
                    classification,
                ):
                    raise ValueError(
                        "Context entity resolution produced conflicting project classifications"
                    )
                if classification is not None:
                    project_classifications[entity_id] = classification

                if entity_id not in entity_ids:
                    entity_ids.append(entity_id)
                resolved_mentions.append(
                    ResolvedContextBlockMention(mention=mention, entity_id=entity_id)
                )
                associations.extend(
                    ContextBlockEntityAssociation(
                        block_id=block_id,
                        entity_id=entity_id,
                        mention_text=mention.name,
                    )
                    for block_id in mention.block_ids
                )

            unique_associations = tuple(
                {
                    (association.block_id, association.entity_id, association.mention_text.casefold()): association
                    for association in associations
                }.values()
            )
            return {
                "entity_ids": tuple(entity_ids),
                "new_entity_ids": frozenset(new_ids),
                "alias_updated_ids": frozenset(alias_ids),
                "alias_updates": {
                    entity_id: tuple(dict.fromkeys(aliases))
                    for entity_id, aliases in alias_updates.items()
                },
                "pending_entity_writes": pending_entity_writes,
                "project_classifications": project_classifications,
                "resolved_mentions": tuple(resolved_mentions),
                "block_entity_associations": unique_associations,
            }

    def _classification_for_resolution(
        self,
        entity_id: int,
        mention: ContextBlockMention,
        profile: EntityProfile | None,
    ) -> ProjectEntityClassification | None:
        """Stage the type storage must use in this resolver's target project."""

        if entity_id == IDENTITY_ENTITY_ID:
            return None
        if profile is not None and profile.is_classified_in(self.project_id):
            return ProjectEntityClassification(
                entity_id=entity_id,
                entity_type=profile.entity_type,
                topic=profile.topic,
                membership="existing",
            )
        return ProjectEntityClassification(
            entity_id=entity_id,
            entity_type=mention.entity_type,
            topic=mention.topic,
            membership="missing",
        )

    @staticmethod
    def _classification_conflicts(
        staged: Dict[int, ProjectEntityClassification],
        classification: ProjectEntityClassification | None,
    ) -> bool:
        if classification is None:
            return False
        previous = staged.get(classification.entity_id)
        return previous is not None and previous != classification

    async def candidate_entries_for_context_block_mentions(
        self,
        mentions: list[ContextBlockMention],
        *,
        policy: IngestionPolicy,
        parent_work_record=None,
    ) -> list[Optional[Tuple[str, Any]]]:
        """Build candidate searches for Context-block mention identity decisions."""

        names_by_key: Dict[str, str] = {}
        for mention in mentions:
            if mention.name:
                names_by_key.setdefault(
                    self._candidate_search_key(mention.name), mention.name
                )
        unique_names = list(names_by_key.values())
        embedding_map = {}
        if unique_names:
            if getattr(self.embedding_service, "supports_model_work_records", False):
                embeddings = await self.embedding_service.encode(
                    unique_names,
                    parent_work_record=parent_work_record,
                )
            else:
                embeddings = await self.embedding_service.encode(unique_names)
            embedding_map = {
                self._candidate_search_key(name): embedding
                for name, embedding in zip(unique_names, embeddings)
            }

        entries: list[Optional[Tuple[str, Any]]] = []
        seen: Dict[str, Tuple[str, Any]] = {}
        for mention in mentions:
            search_key = self._candidate_search_key(mention.name)
            if search_key not in seen:
                candidates = await self.get_candidate_ids(
                    mention.name,
                    precomputed_embedding=embedding_map.get(search_key),
                    candidate_fuzzy_threshold=policy.candidate_fuzzy_threshold,
                    candidate_vector_threshold=policy.candidate_vector_threshold,
                    strict=True,
                )
                seen[search_key] = (
                    ("candidates", candidates) if candidates else ("new", None)
                )
            entries.append(seen[search_key])
        return entries

    def _should_reuse_pending_entity(
        self,
        mention: ContextBlockMention,
        support_text: str,
        pending_write: EntityWrite,
        pending_support_text: str,
        policy: IngestionPolicy,
    ) -> bool:
        """Reuse a private pending ID only when the frozen evidence is clear."""

        if self._candidate_search_key(mention.name) != self._candidate_search_key(
            pending_write.canonical_name
        ):
            return False
        profile = EntityProfile.registered(
            canonical_name=pending_write.canonical_name,
            entity_type=pending_write.entity_type,
            topic=pending_write.topic,
            project_id=self.project_id,
            embedding=None,
        )
        if (
            self.schema_compatibility(
                mention.entity_type,
                mention.topic,
                profile,
                policy,
            )
            != "compatible"
        ):
            return False

        normalized_support = " ".join(support_text.split()).casefold()
        pending_normalized_support = " ".join(pending_support_text.split()).casefold()
        return (
            bool(normalized_support)
            and normalized_support == pending_normalized_support
        )

    def is_profile_visible(self, profile: EntityProfile) -> bool:
        return profile.project_id in set(self.readable_project_ids)

    def should_accept_candidate(
        self,
        name: str,
        mention_type: str,
        mention_topic: str,
        message_text: str,
        profile: EntityProfile,
        candidate_id: int,
        *,
        policy: IngestionPolicy,
        compatibility: Optional[str] = None,
        candidate: EntityCandidate | None = None,
    ) -> bool:
        """Apply all conservative reuse policy for one existing entity candidate."""

        compatibility = compatibility or self.schema_compatibility(
            mention_type, mention_topic, profile, policy
        )
        if compatibility == "incompatible" or (
            candidate is not None and "ambiguous_alias" in candidate.signals
        ):
            return False

        evidence = self._name_evidence_level(
            name,
            mention_type,
            message_text,
            profile,
            candidate_id,
            policy=policy,
            compatibility=compatibility,
            candidate=candidate,
        )
        if evidence == "strong":
            return True
        if evidence == "medium":
            return self._has_positive_entity_context(
                name, mention_type, message_text, profile, compatibility, policy
            )
        return compatibility == "compatible" and self._has_contextual_support(
            name, message_text, profile, compatibility, candidate_id, policy
        )

    def schema_compatibility(
        self,
        mention_type: str,
        mention_topic: str,
        profile: EntityProfile,
        policy: IngestionPolicy,
    ) -> str:
        if not profile.is_classified_in(self.project_id):
            # A visible foreign project establishes that an identity exists, but
            # its vocabulary cannot classify this project's occurrence.
            return "neutral"
        mention_configured_type = policy.domain.canonical_entity_type(
            mention_type
        ) or policy.domain.resolve_entity_type(mention_type)
        profile_configured_type = policy.domain.canonical_entity_type(
            profile.entity_type or ""
        ) or policy.domain.resolve_entity_type(profile.entity_type or "")
        mention_type_lower = (
            (mention_configured_type or mention_type or "").strip().lower()
        )
        profile_type_lower = (
            (profile_configured_type or profile.entity_type or "").strip().lower()
        )
        if mention_type_lower and mention_type_lower == profile_type_lower:
            return "compatible"
        if mention_configured_type and profile_configured_type:
            return "incompatible"

        mention_topic_normalized = self._normalize_resolution_topic(
            mention_topic, policy
        )
        profile_topic_normalized = self._normalize_resolution_topic(
            profile.topic or "", policy
        )
        if (
            mention_topic_normalized
            and profile_topic_normalized
            and mention_topic_normalized == profile_topic_normalized
            and mention_topic_normalized.casefold() != "general"
        ):
            return "compatible"

        mention_label_topics = self._label_topics(mention_type_lower, policy)
        profile_label_topics = self._label_topics(profile_type_lower, policy)
        if mention_label_topics and profile_label_topics:
            return (
                "compatible"
                if mention_label_topics & profile_label_topics
                else "incompatible"
            )
        return "neutral"

    def _name_evidence_level(
        self,
        name: str,
        mention_type: str,
        message_text: str,
        profile: EntityProfile,
        candidate_id: int,
        *,
        policy: IngestionPolicy,
        compatibility: str,
        candidate: EntityCandidate | None,
    ) -> str:
        mention = name.strip().casefold()
        if not mention:
            return "none"
        if candidate is not None and "ambiguous_alias" in candidate.signals:
            return "weak"
        owners = self.get_entity_ids_for_name(mention)
        if owners and candidate_id not in owners:
            return "none"
        if len(owners) > 1:
            return "weak"

        aliases = {
            alias.strip().casefold()
            for alias in self.get_mentions_for_id(candidate_id)
            if alias and alias.strip()
        }
        exact_name = mention == (profile.canonical_name or "").strip().casefold() or (
            mention in aliases
        )
        if self._is_acronym_alias(name, profile.canonical_name or "", list(aliases)):
            return "strong"
        if not exact_name:
            return "weak" if candidate is not None else "none"
        if self._is_common_word_mention(
            name, policy
        ) and not self._has_positive_entity_context(
            name, mention_type, message_text, profile, compatibility, policy
        ):
            return "weak"
        if (
            candidate is not None
            and len(candidate.signals & {"exact", "fuzzy", "vector"}) > 1
        ):
            return "strong"
        if len(self._word_tokens(name)) > 1:
            return "strong"
        return "medium" if compatibility == "compatible" else "weak"

    def _label_topics(self, label: str, policy: IngestionPolicy) -> set[str]:
        entity_type = policy.domain.canonical_entity_type(label) or (
            policy.domain.resolve_entity_type(label)
        )
        topic = policy.domain.topic_for_entity_type(entity_type)
        return {topic} if topic is not None else set()

    @staticmethod
    def _normalize_resolution_topic(
        topic: str, policy: IngestionPolicy
    ) -> Optional[str]:
        return policy.domain.normalize_topic(topic.strip()) if topic else None

    def _has_positive_entity_context(
        self,
        name: str,
        mention_type: str,
        message_text: str,
        profile: EntityProfile,
        compatibility: str,
        policy: IngestionPolicy,
    ) -> bool:
        if compatibility != "compatible":
            return False
        mention_type_lower = (mention_type or "").strip().casefold()
        profile_type_lower = (profile.entity_type or "").strip().casefold()
        type_matches = bool(
            mention_type_lower and mention_type_lower == profile_type_lower
        )
        label_topic_overlap = bool(
            self._label_topics(mention_type_lower, policy)
            & self._label_topics(profile_type_lower, policy)
        )
        return (type_matches or label_topic_overlap) and self._has_rich_context(
            name, message_text, policy
        )

    def _has_contextual_support(
        self,
        name: str,
        message_text: str,
        profile: EntityProfile,
        compatibility: str,
        candidate_id: int,
        policy: IngestionPolicy,
    ) -> bool:
        if self._is_acronym_alias(
            name, profile.canonical_name or "", self.get_mentions_for_id(candidate_id)
        ):
            return True
        return compatibility == "compatible" and self._has_rich_context(
            name, message_text, policy
        )

    def _has_rich_context(
        self, name: str, message_text: str, policy: IngestionPolicy
    ) -> bool:
        name_tokens = set(self._word_tokens(name))
        context_tokens = [
            token
            for token in self._word_tokens(message_text)
            if token not in name_tokens
        ]
        content_tokens = [
            token
            for token in context_tokens
            if token not in policy.sparse_context_verbs and len(token) > 2
        ]
        return len(content_tokens) >= 3

    def _is_common_word_mention(self, name: str, policy: IngestionPolicy) -> bool:
        tokens = self._word_tokens(name)
        return (
            len(tokens) == 1
            and len(tokens[0]) > 2
            and word_frequency(tokens[0], "en")
            >= policy.common_word_frequency_threshold
        )

    @staticmethod
    def _word_tokens(text: str) -> List[str]:
        return re.findall(r"[a-z0-9]+", (text or "").lower())

    def _is_acronym_alias(
        self, name: str, canonical_name: str, aliases: List[str]
    ) -> bool:
        mention = name.strip().lower()
        if not mention or len(mention) < 2 or not mention.isalnum():
            return False
        for known_name in [canonical_name, *aliases]:
            initials = "".join(token[0] for token in self._word_tokens(known_name))
            if initials and mention == initials:
                return True
        return False

    def _cache_record_for_project(self, entity: dict) -> dict:
        """Select this resolver's local classification from a durable entity row."""

        contexts = entity.get("contexts")
        if contexts:
            context = next(
                (
                    item
                    for item in contexts
                    if item.get("project_id") == self.project_id
                ),
                contexts[0],
            )
            entity = {
                **entity,
                "project_id": context.get("project_id"),
                "type": context.get("entity_type"),
                "topic": context.get("topic"),
            }
        return entity

    def _populate_cache(self, entity: dict) -> EntityProfile:
        """Hydrate internal indexes from a KnowledgeStore entity record."""

        entity = self._cache_record_for_project(entity)
        with self._lock:
            profile, aliases_changed = self._index.populate(entity)
            if aliases_changed:
                self._bump_alias_version()
        return profile

    async def publish_committed_entity_ids(
        self, affected_entity_ids: Iterable[int]
    ) -> None:
        """Refresh this resolver from the rows durably committed for one window.

        The caller supplies only IDs read from the committed Context revision.
        This deliberately never accepts a pending semantic build: the commit
        writer may resume at its checkpoint without validating such a rebuild.
        """

        normalized_ids = set()
        for entity_id in affected_entity_ids:
            if (
                not isinstance(entity_id, int)
                or isinstance(entity_id, bool)
                or entity_id <= 0
            ):
                raise ValueError(
                    "Committed entity publication requires positive integer IDs"
                )
            normalized_ids.add(entity_id)
        entity_ids = tuple(sorted(normalized_ids))
        if not entity_ids:
            return

        durable_rows = await self.knowledge_store.get_entities_by_ids(
            list(entity_ids),
            visible_project_ids=self.readable_project_ids,
        )
        rows_by_id = {
            int(entity["id"]): entity
            for entity in durable_rows
            if int(entity["id"]) in set(entity_ids)
        }

        names = {
            str(name).strip()
            for entity in rows_by_id.values()
            for name in [entity.get("canonical_name"), *(entity.get("aliases") or [])]
            if name and str(name).strip()
        }
        colliding_rows = (
            await self.knowledge_store.get_entities_by_names(
                sorted(names), visible_project_ids=self.readable_project_ids
            )
            if names
            else []
        )
        for entity in colliding_rows:
            rows_by_id[int(entity["id"])] = entity

        requested_ids = set(entity_ids)
        with self._lock:
            aliases_changed = False
            _, removed_aliases = self._index.remove(
                sorted(requested_ids - set(rows_by_id))
            )
            aliases_changed = aliases_changed or removed_aliases
            for entity_id in sorted(rows_by_id):
                _, refreshed_aliases = self._index.refresh(
                    self._cache_record_for_project(rows_by_id[entity_id])
                )
                aliases_changed = aliases_changed or refreshed_aliases
            if aliases_changed:
                self._bump_alias_version()

    async def get_id(self, name: str) -> Optional[int]:
        if not name:
            return None

        with self._lock:
            stored_id = self._index.get_entity_id_for_name(name)
            if stored_id is not None:
                return stored_id
        found = await self.knowledge_store.get_entities_by_names(
            [name], visible_project_ids=self.readable_project_ids
        )
        if found:
            for entity in found:
                self._populate_cache(entity)
            with self._lock:
                return self._index.get_entity_id_for_name(name)
        return None

    async def get_profile(self, entity_id: int) -> Optional[EntityProfile]:
        with self._lock:
            profile = self._index.get_profile(entity_id)
            if profile:
                return profile

        # Cache miss: fetch from knowledge_store
        entity = await self.knowledge_store.get_entity_by_id(
            entity_id, visible_project_ids=self.readable_project_ids
        )
        if entity:
            return self._populate_cache(entity)
        return None

    def get_cached_profile(self, entity_id: int) -> Optional[EntityProfile]:
        """Return a cached profile without hydrating from storage."""
        with self._lock:
            return self._index.get_profile(entity_id)

    def has_cached_entity(self, entity_id: int) -> bool:
        """Return whether an entity is currently present in the local cache."""
        with self._lock:
            return self._index.has_entity(entity_id)

    def iter_cached_entity_ids(self) -> List[int]:
        """Return entity IDs currently present in the local cache."""
        with self._lock:
            return self._index.iter_profile_ids()

    def get_profiles(self) -> Dict[int, EntityProfile]:
        with self._lock:
            return self._index.get_profiles()

    def get_mentions_for_id(self, entity_id: int) -> List[str]:
        with self._lock:
            return self._index.get_mentions(entity_id)

    def get_known_aliases(self) -> Dict[str, int]:
        with self._lock:
            return self._index.get_aliases()

    def get_ids_for_name(self, name: str) -> set[int]:
        return self.get_entity_ids_for_name(name)

    def get_entity_ids_for_name(self, name: str) -> set[int]:
        with self._lock:
            return self._index.get_entity_ids_for_name(name)

    async def get_embedding_for_id(self, entity_id: int) -> List[float]:
        """Retrieve embedding from graph by ID."""
        with self._lock:
            profile = self._index.get_profile(entity_id)
            if profile and profile.embedding:
                return profile.embedding
        return await self.knowledge_store.get_entity_embedding(
            entity_id,
            visible_project_ids=self.readable_project_ids,
        )

    async def compute_batch_embeddings(self, texts: List[str]) -> List[List[float]]:
        """
        Compute embeddings for a batch of texts (used by Processor).
        """
        if not texts:
            return []

        return await self.embedding_service.encode(texts)

    async def get_neighbor_ids_batch(
        self, candidate_ids: List[int]
    ) -> Dict[int, set[int]]:
        """Fetch neighbors for a batch of candidates."""
        return await self.knowledge_store.get_neighbor_ids_batch(
            candidate_ids,
            visible_project_ids=self.readable_project_ids,
        )

    def _new_aliases_for_selected_entity(
        self, entity_id: int, mentions: List[str]
    ) -> List[str]:
        """Return collision-free aliases for the candidate ID already selected."""

        with self._lock:
            if not self._index.has_entity(entity_id):
                return []
            return [
                mention
                for mention in mentions
                if mention
                and mention.strip()
                and not self._index.get_entity_ids_for_name(mention)
            ]

    def commit_new_aliases(self, entity_id: int, aliases: List[str]):
        """Explicitly commit aliases after Graph validation."""
        if not aliases:
            return

        with self._lock:
            if not self._index.has_entity(entity_id):
                return
            safe_aliases = []
            for mention in aliases:
                owners = self._index.get_entity_ids_for_name(mention)
                if owners and owners != {entity_id}:
                    logger.warning(
                        f"Alias collision: '{mention}' belongs to {sorted(owners)}, "
                        f"skipping for {entity_id}"
                    )
                    continue
                safe_aliases.append(mention)
            aliases_changed = self._index.commit_aliases(entity_id, safe_aliases)
            if aliases_changed:
                self._bump_alias_version()

    @cached(cache=LRUCache(maxsize=5))
    def _build_generic_tokens(self, alias_version: int) -> set:
        """Tokens appearing in N+ distinct entities are generic."""
        token_to_entities = defaultdict(set)

        with self._lock:
            profiles_snapshot = self._index.get_profiles()
            aliases_snapshot = {
                eid: self._index.get_mentions(eid) for eid in profiles_snapshot
            }

        for ent_id, profile in profiles_snapshot.items():
            canonical = profile.canonical_lower
            for token in canonical.split():
                token_to_entities[token].add(ent_id)

            for alias in aliases_snapshot.get(ent_id, []):
                for token in alias.lower().split():
                    token_to_entities[token].add(ent_id)

        return {
            token
            for token, ent_ids in token_to_entities.items()
            if len(ent_ids) >= self.generic_token_freq
        }

    async def get_candidate_ids(
        self,
        mention: str,
        precomputed_embedding: List[float] = None,
        *,
        candidate_fuzzy_threshold: int | None = None,
        candidate_vector_threshold: float | None = None,
        strict: bool = False,
    ) -> List[EntityCandidate]:
        mention_lower = self._candidate_search_key(mention) if mention else ""
        if not mention_lower:
            return []

        candidates: Dict[int, EntityCandidate] = {}
        fuzzy_threshold = (
            self.candidate_fuzzy_threshold
            if candidate_fuzzy_threshold is None
            else candidate_fuzzy_threshold
        )
        vector_threshold = (
            self.candidate_vector_threshold
            if candidate_vector_threshold is None
            else candidate_vector_threshold
        )

        # The name index is a cache, not a complete owner set. Always consult
        # durable scoped state before treating an exact name or alias as direct
        # identity evidence.
        durable_exact_ids: set[int] = set()
        try:
            durable_exact_rows = await self.knowledge_store.get_entities_by_names(
                [mention_lower],
                visible_project_ids=self.readable_project_ids,
            )
            for entity in durable_exact_rows:
                entity_id = int(entity["id"])
                self._populate_cache(entity)
                durable_exact_ids.add(entity_id)
        except Exception as exc:
            if strict:
                raise
            logger.warning("Durable exact candidate lookup failed: {}", exc)

        exact_is_ambiguous = len(durable_exact_ids) > 1
        for entity_id in durable_exact_ids:
            candidate = candidates.setdefault(entity_id, EntityCandidate(entity_id))
            candidate.add_signal("exact", 1.0)
            if exact_is_ambiguous:
                candidate.add_signal("ambiguous_alias", 1.0)

        with self._lock:
            choices = self._index.iter_aliases()
            scorer = fuzz.ratio if len(mention_lower) < 4 else fuzz.WRatio
            results = process.extract(
                mention_lower,
                choices,
                limit=50,
                score_cutoff=fuzzy_threshold,
                scorer=scorer,
            )

            for alias, fuzz_score, _ in results:
                # Exact identity evidence comes only from the durable lookup.
                # A stale cached alias must not come back as fuzzy score 1.0.
                if alias == mention_lower:
                    continue
                owner_ids = self._index.get_entity_ids_for_name(alias)
                if owner_ids:
                    normalized = fuzz_score / 100.0
                    alias_is_ambiguous = len(owner_ids) > 1
                    for entity_id in owner_ids:
                        candidate = candidates.setdefault(
                            entity_id,
                            EntityCandidate(entity_id),
                        )
                        candidate.add_signal("fuzzy", normalized)
                        if alias_is_ambiguous:
                            candidate.add_signal("ambiguous_alias", normalized)

        vector = precomputed_embedding
        if vector is None:
            try:
                vector = await self.embedding_service.encode_single(mention)
            except Exception as exc:
                if strict:
                    raise
                logger.warning("Encoding failed: {}", exc)
                vector = None

        vector_results = []
        if vector:
            try:
                vector_results = (
                    await self.knowledge_store.search_entities_by_embedding(
                        vector,
                        limit=5,
                        score_threshold=vector_threshold,
                        visible_project_ids=self.readable_project_ids,
                    )
                )
            except Exception as exc:
                if strict:
                    raise
                logger.warning("Vector search failed, using fuzzy only: {}", exc)
                vector_results = []
        for entity_id, vector_score in vector_results:
            if entity_id:
                candidates.setdefault(
                    entity_id,
                    EntityCandidate(entity_id),
                ).add_signal("vector", vector_score)

        # Exact rows were just read from durable active/scoped state. Every
        # other cache or vector candidate must be revalidated before it can be
        # accepted, since a warmed resolver may retain a retired identity.
        non_exact_ids = sorted(set(candidates) - durable_exact_ids)
        verified_ids = set(durable_exact_ids)
        if non_exact_ids:
            try:
                hydrated = await self.knowledge_store.get_entities_by_ids(
                    non_exact_ids,
                    visible_project_ids=self.readable_project_ids,
                )
                for entity in hydrated:
                    entity_id = int(entity["id"])
                    self._populate_cache(entity)
                    verified_ids.add(entity_id)
            except Exception as exc:
                if strict:
                    raise
                logger.warning("Candidate hydration failed: {}", exc)

        return sorted(
            (
                candidate
                for entity_id, candidate in candidates.items()
                if entity_id in verified_ids
            ),
            key=lambda candidate: (-candidate.score, candidate.entity_id),
        )

    async def prepare_pending_entity(
        self,
        entity_id: int,
        canonical_name: str,
        aliases: List[str],
        entity_type: str,
        topic: str,
    ) -> EntityWrite:
        """Build a new entity write without exposing it through shared indexes."""

        embedding = await self.embedding_service.encode_single(
            build_entity_embedding_text(canonical_name, entity_type)
        )
        return EntityWrite(
            entity_id=entity_id,
            is_new=True,
            canonical_name=canonical_name,
            entity_type=entity_type,
            topic=topic,
            embedding=tuple(embedding) if embedding is not None else None,
            aliases=tuple(alias for alias in aliases if alias and alias.strip()),
        )

    def remove_entities(self, entity_ids: List[int]) -> int:
        """Remove entities from entities indexes. Call after KnowledgeStore deletion."""
        if not entity_ids:
            return 0

        removed = 0
        with self._lock:
            removed, aliases_changed = self._index.remove(entity_ids)
            if aliases_changed:
                self._bump_alias_version()

        if removed > 0:
            logger.info(f"Removed {removed} entities from entities")
            emit_sync(
                self.project_id,
                "entities",
                "entities_removed",
                {"requested": len(entity_ids), "removed": removed},
            )
        return removed
