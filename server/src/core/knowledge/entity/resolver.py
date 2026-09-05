from __future__ import annotations

import asyncio
import re
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Tuple

from cachetools import LRUCache, cached
from loguru import logger
from rapidfuzz import fuzz, process
from wordfreq import word_frequency

from common.schema.ingestion.contracts import (
    ContextBlockEntityAssociation,
    ContextBlockMention,
    EntityWrite,
    ResolvedContextBlockMention,
    ValidationIssue,
)
from common.schema.settings import EntityResolutionSettings
from common.scoping import require_scope_value, require_visible_project_ids
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

    def mention_dedupe_key(
        self,
        name: str,
        mention_type: str,
        topic: str,
        policy: IngestionPolicy,
    ) -> str:
        """Return the conservative in-batch identity key for one mention."""

        del mention_type, topic, policy
        return name.strip().casefold()

    async def candidate_entries_for_mentions(
        self,
        mentions: List[Tuple[int, str, str, str]],
        *,
        policy: IngestionPolicy,
        parent_work_record=None,
    ) -> List[Optional[Tuple[str, Any]]]:
        """Build reusable candidate searches under one batch policy snapshot."""

        unique_names = list({name for _, name, _, _ in mentions if name})
        embedding_map = {}
        if unique_names:
            if getattr(
                self.embedding_service,
                "supports_model_work_records",
                False,
            ):
                embeddings = await self.embedding_service.encode(
                    unique_names,
                    parent_work_record=parent_work_record,
                )
            else:
                embeddings = await self.embedding_service.encode(unique_names)
            embedding_map = dict(zip(unique_names, embeddings))

        entries: List[Optional[Tuple[str, object]]] = []
        seen_by_dedupe_key: Dict[Tuple[str, str, str], Tuple[str, object]] = {}
        for _, name, mention_type, topic in mentions:
            if not name:
                entries.append(None)
                continue
            dedupe_key = self.mention_dedupe_key(name, mention_type, topic, policy)
            if dedupe_key not in seen_by_dedupe_key:
                candidates = await self.get_candidate_ids(
                    name,
                    precomputed_embedding=embedding_map.get(name),
                    candidate_fuzzy_threshold=policy.candidate_fuzzy_threshold,
                    candidate_vector_threshold=policy.candidate_vector_threshold,
                    strict=True,
                )
                seen_by_dedupe_key[dedupe_key] = (
                    ("candidates", candidates) if candidates else ("new", None)
                )
            entries.append(seen_by_dedupe_key[dedupe_key])
        return entries

    async def resolve_mentions(
        self,
        mentions: List[Tuple[int, str, str, str]],
        *,
        messages: Iterable[Any],
        policy: IngestionPolicy,
        parent_work_record=None,
        allocate_entity_id,
        issues: Optional[List[ValidationIssue]] = None,
    ) -> Dict[str, Any]:
        """Resolve mentions and prepare the durable entity changes for a batch.

        The resolver owns the identity decision: candidate acceptance, in-batch
        deduplication, alias staging, and pending entity allocation.  The caller
        only applies the returned aggregate state after this decision completes.
        """

        async with self.resolution_lock:
            msg_text_map = {
                message["id"]: message["message"] for message in messages
            }
            entity_ids: List[int] = []
            new_ids: set[int] = set()
            alias_ids: set[int] = set()
            entity_msg_map: Dict[int, List[int]] = {}
            created_in_batch: Dict[Tuple[str, str, str], int] = {}
            alias_updates: Dict[int, List[str]] = {}
            pending_entity_writes: Dict[int, EntityWrite] = {}

            mention_candidates = await self.candidate_entries_for_mentions(
                mentions,
                policy=policy,
                parent_work_record=parent_work_record,
            )

            for index, (msg_id, name, mention_type, topic) in enumerate(mentions):
                if not name:
                    continue

                entry = mention_candidates[index]
                if entry is None:
                    continue

                dedupe_key = self.mention_dedupe_key(
                    name, mention_type, topic, policy
                )
                entity_id = None

                if entry[0] == "candidates":
                    message_text = msg_text_map.get(msg_id, "")
                    for candidate in entry[1]:
                        candidate_id = candidate.entity_id
                        profile = await self.get_profile(candidate_id)
                        compatibility = (
                            self.schema_compatibility(
                                mention_type,
                                topic,
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
                            name,
                            mention_type,
                            topic,
                            message_text,
                            profile,
                            candidate_id,
                            policy=policy,
                            compatibility=compatibility,
                            candidate=candidate,
                        ):
                            entity_id = candidate_id
                            existing_id, aliases_added, new_aliases = (
                                self.validate_existing(
                                    profile.canonical_name,
                                    [name.strip()],
                                )
                            )
                            if existing_id and aliases_added:
                                alias_ids.add(existing_id)
                                alias_updates.setdefault(existing_id, []).extend(
                                    new_aliases
                                )
                            break

                if entity_id is None:
                    if dedupe_key in created_in_batch:
                        entity_id = created_in_batch[dedupe_key]
                    else:
                        entity_id = await allocate_entity_id()
                        pending_entity_writes[
                            entity_id
                        ] = await self.prepare_pending_entity(
                            entity_id,
                            name.strip(),
                            [name.strip()],
                            mention_type,
                            topic,
                        )
                        new_ids.add(entity_id)
                        created_in_batch[dedupe_key] = entity_id

                if entity_id is not None:
                    if entity_id not in entity_msg_map:
                        entity_msg_map[entity_id] = []
                        entity_ids.append(entity_id)
                    entity_msg_map[entity_id].append(msg_id)

            return {
                "entity_ids": entity_ids,
                "new_entity_ids": new_ids,
                "alias_updated_ids": alias_ids,
                "entity_message_map": entity_msg_map,
                "alias_updates": alias_updates,
                "pending_entity_writes": pending_entity_writes,
            }

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

        The old message-local resolver remains only for the still-authoritative
        legacy pipeline until Batch 8.  Context-first callers use this boundary:
        block associations are returned independently, and literal message
        evidence is deliberately left to the result assembler.
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
            created_in_build: Dict[Tuple[str, str, str], int] = {}
            alias_updates: Dict[int, List[str]] = {}
            pending_entity_writes: Dict[int, EntityWrite] = {}
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
                dedupe_key = self.mention_dedupe_key(
                    mention.name,
                    mention.entity_type,
                    mention.topic,
                    policy,
                )
                entity_id: int | None = None
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
                            entity_id = candidate_id
                            existing_id, aliases_added, new_aliases = (
                                self.validate_existing(
                                    profile.canonical_name,
                                    [mention.name.strip()],
                                )
                            )
                            if existing_id and aliases_added:
                                alias_ids.add(existing_id)
                                alias_updates.setdefault(existing_id, []).extend(
                                    new_aliases
                                )
                            break

                if entity_id is None:
                    if dedupe_key in created_in_build:
                        entity_id = created_in_build[dedupe_key]
                    else:
                        entity_id = await allocate_entity_id()
                        pending_entity_writes[entity_id] = await self.prepare_pending_entity(
                            entity_id,
                            mention.name.strip(),
                            [mention.name.strip()],
                            mention.entity_type,
                            mention.topic,
                        )
                        new_ids.add(entity_id)
                        created_in_build[dedupe_key] = entity_id

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
                "resolved_mentions": tuple(resolved_mentions),
                "block_entity_associations": unique_associations,
            }

    async def candidate_entries_for_context_block_mentions(
        self,
        mentions: list[ContextBlockMention],
        *,
        policy: IngestionPolicy,
        parent_work_record=None,
    ) -> list[Optional[Tuple[str, Any]]]:
        """Build candidate searches for Context-block mention identity decisions."""

        unique_names = list({mention.name for mention in mentions if mention.name})
        embedding_map = {}
        if unique_names:
            if getattr(self.embedding_service, "supports_model_work_records", False):
                embeddings = await self.embedding_service.encode(
                    unique_names,
                    parent_work_record=parent_work_record,
                )
            else:
                embeddings = await self.embedding_service.encode(unique_names)
            embedding_map = dict(zip(unique_names, embeddings))

        entries: list[Optional[Tuple[str, Any]]] = []
        seen: Dict[Tuple[str, str, str], Tuple[str, Any]] = {}
        for mention in mentions:
            dedupe_key = self.mention_dedupe_key(
                mention.name,
                mention.entity_type,
                mention.topic,
                policy,
            )
            if dedupe_key not in seen:
                candidates = await self.get_candidate_ids(
                    mention.name,
                    precomputed_embedding=embedding_map.get(mention.name),
                    candidate_fuzzy_threshold=policy.candidate_fuzzy_threshold,
                    candidate_vector_threshold=policy.candidate_vector_threshold,
                    strict=True,
                )
                seen[dedupe_key] = (
                    ("candidates", candidates) if candidates else ("new", None)
                )
            entries.append(seen[dedupe_key])
        return entries

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
        mention_type_lower = (
            (
                policy.domain.canonical_entity_type(mention_type)
                or policy.domain.resolve_entity_type(mention_type)
                or mention_type
            )
            .strip()
            .lower()
        )
        profile_type_lower = (
            (
                policy.domain.canonical_entity_type(profile.entity_type or "")
                or policy.domain.resolve_entity_type(profile.entity_type or "")
                or profile.entity_type
                or ""
            )
            .strip()
            .lower()
        )
        if mention_type_lower and mention_type_lower == profile_type_lower:
            return "compatible"

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

    def _populate_cache(self, entity: dict) -> EntityProfile:
        """Hydrate internal indexes from a KnowledgeStore entity record."""
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
        with self._lock:
            profile, aliases_changed = self._index.populate(entity)
            if aliases_changed:
                self._bump_alias_version()
        return profile

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

    def validate_existing(
        self, canonical_name: str, mentions: List[str]
    ) -> Tuple[Optional[int], bool, List[str]]:
        """
        Check if canonical_name exists. If yes, register mention aliases and return ID.
        If no, return None (caller handles demotion).

        Returns:
            Tuple of (entity_id, aliases_added, new_aliases_list)
        """
        if not canonical_name:
            return None, False, []

        with self._lock:
            entity_id = self._index.get_entity_id_for_name(canonical_name)
            logger.debug(f"validate_existing: '{canonical_name}' -> id={entity_id}")
            if entity_id is None:
                return None, False, []

            new_aliases = []
            for mention in mentions:
                owners = self._index.get_entity_ids_for_name(mention)
                if not owners:
                    new_aliases.append(mention)

            return entity_id, len(new_aliases) > 0, new_aliases

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

        if not mention:
            return []

        candidates: Dict[int, EntityCandidate] = {}
        mention_lower = mention.strip().casefold()
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

        with self._lock:
            exact_ids = self._index.get_entity_ids_for_name(mention_lower)
            exact_is_ambiguous = len(exact_ids) > 1
            for exact_id in exact_ids:
                candidate = candidates.setdefault(exact_id, EntityCandidate(exact_id))
                candidate.add_signal("exact", 1.0)
                if exact_is_ambiguous:
                    candidate.add_signal("ambiguous_alias", 1.0)

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
                owner_ids = self._index.get_entity_ids_for_name(alias)
                if owner_ids:
                    normalized = fuzz_score / 100.0
                    alias_is_ambiguous = len(owner_ids) > 1
                    for eid in owner_ids:
                        candidate = candidates.get(eid)
                        if (
                            normalized == 1.0
                            and candidate
                            and "exact" in candidate.signals
                        ):
                            continue
                        candidate = candidates.setdefault(eid, EntityCandidate(eid))
                        candidate.add_signal("fuzzy", normalized)
                        if alias_is_ambiguous:
                            candidate.add_signal("ambiguous_alias", normalized)

        vector = precomputed_embedding
        if vector is None:
            try:
                vector = await self.embedding_service.encode_single(mention)
            except Exception as e:
                if strict:
                    raise
                logger.warning(f"Encoding failed: {e}")
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
            except Exception as e:
                if strict:
                    raise
                logger.warning(f"Vector search failed, using fuzzy only: {e}")
                vector_results = []
        for eid, vec_score in vector_results:
            if eid:
                candidates.setdefault(eid, EntityCandidate(eid)).add_signal(
                    "vector", vec_score
                )

        # The durable vector index can contain a visible identity that has not
        # yet been loaded into this process. Hydrate those rows before applying
        # the cache membership filter; otherwise vector-only matches disappear.
        with self._lock:
            missing_ids = [
                entity_id
                for entity_id in candidates
                if not self._index.has_entity(entity_id)
            ]
        if missing_ids:
            try:
                hydrated = await self.knowledge_store.get_entities_by_ids(
                    missing_ids,
                    visible_project_ids=self.readable_project_ids,
                )
                for entity in hydrated:
                    self._populate_cache(entity)
            except Exception as exc:
                if strict:
                    raise
                logger.warning("Candidate hydration failed: {}", exc)

        with self._lock:
            valid_candidates = [
                candidate
                for eid, candidate in candidates.items()
                if self._index.has_entity(eid)
            ]
        return sorted(
            valid_candidates,
            key=lambda candidate: candidate.score,
            reverse=True,
        )

    async def register_entity(
        self,
        entity_id: int,
        canonical_name: str,
        mentions: List[str],
        entity_type: str,
        topic: str,
        session_id: str = None,
        project_id: str = None,
    ) -> List[float]:
        """
        Register new entity: update all indexes and return embedding.
        """

        project_id = project_id or self.project_id
        # Identity vectors are shared across every project context.  A context
        # type must not alter the vector persisted for the same identity.
        text_to_embed = build_entity_embedding_text(canonical_name, None)
        embedding = await self.embedding_service.encode_single(text_to_embed)

        with self._lock:
            logger.info(
                f"Adding entity {entity_id}-{canonical_name} to entities indexes."
            )

            profile = EntityProfile.registered(
                canonical_name=canonical_name,
                entity_type=entity_type,
                topic=topic,
                project_id=project_id,
                embedding=embedding,
            )

            safe_mentions = []
            for mention in mentions:
                owners = self._index.get_entity_ids_for_name(mention)
                if owners and owners != {entity_id}:
                    logger.warning(
                        f"Alias collision: '{mention}' belongs to {sorted(owners)}, "
                        f"skipping for {entity_id}"
                    )
                    continue
                safe_mentions.append(mention)

            aliases_changed = self._index.register(
                entity_id,
                profile,
                canonical_name,
                safe_mentions,
            )
            if aliases_changed:
                self._bump_alias_version()

        return embedding

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

    def apply_committed_entity_writes(
        self, entity_writes: List[EntityWrite] | tuple[EntityWrite, ...]
    ) -> None:
        """Expose newly durable entity rows to the local resolver cache.

        Callers must invoke this only after the encompassing database transaction
        succeeds. Pending entities deliberately remain invisible to other batches.
        """

        with self._lock:
            aliases_changed = False
            for write in entity_writes:
                if not write.is_new:
                    continue
                profile = EntityProfile.registered(
                    canonical_name=write.canonical_name,
                    entity_type=write.entity_type,
                    topic=write.topic,
                    project_id=self.project_id,
                    embedding=list(write.embedding) if write.embedding else None,
                )
                aliases_changed = (
                    self._index.register(
                        write.entity_id,
                        profile,
                        write.canonical_name,
                        list(write.aliases),
                    )
                    or aliases_changed
                )
            if aliases_changed:
                self._bump_alias_version()

    async def compute_embedding(
        self,
        entity_id: int,
        resolution_text: str,
        precomputed: Optional[List[float]] = None,
    ) -> List[float]:
        with self._lock:
            profile = self._index.get_profile(entity_id)
            if not profile:
                logger.warning(f"Cannot update profile for unknown entity {entity_id}")
                return []

        embedding = precomputed
        if embedding is None:
            embedding = await self.embedding_service.encode_single(resolution_text)

        with self._lock:
            profile = self._index.get_profile(entity_id)
            if not profile:
                logger.warning(f"Cannot update profile for unknown entity {entity_id}")
                return []

            logger.info(
                f"Updating embedding for entity {entity_id}-{profile.canonical_name}"
            )
            self._index.update_embedding(entity_id, embedding)

        return embedding

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
