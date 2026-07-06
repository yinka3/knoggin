from __future__ import annotations

import asyncio
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from cachetools import LRUCache, cached
from loguru import logger
from rapidfuzz import fuzz, process

from common.schema.primitives import FactRecord
from common.schema.settings import EntityResolutionSettings
from common.scoping import require_scope_value, require_visible_project_ids
from common.utils.core_utils import is_substring_match
from common.utils.data_utils import cosine_similarity
from common.utils.events import emit_sync
from knoggin_server.knowledge.entity.embedding import (
    build_entity_embedding_text,
)
from knoggin_server.knowledge.entity.index import EntityIndex
from knoggin_server.knowledge.entity.profile import EntityProfile
from knoggin_server.knowledge.services.embedding_service import EmbeddingService

if TYPE_CHECKING:
    from infrastructure.knowledge_store import KnowledgeStore


VECTOR_MERGE_SIM_THRESHOLD = 0.90
VECTOR_MERGE_SPARSE_FACT_SIM_THRESHOLD = 0.97
MERGE_FACT_NLI_PAIR_LIMIT = 8


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
        hierarchy_config: Optional[dict] = None,
        fuzzy_substring_threshold: int = 75,
        fuzzy_non_substring_threshold: int = 91,
        generic_token_freq: int = 10,
        candidate_fuzzy_threshold: int = 85,
        candidate_vector_threshold: float = 0.85,
    ):

        self.knowledge_store = knowledge_store
        self.hierarchy_config = hierarchy_config or {}
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

    def _populate_cache(self, entity: dict) -> EntityProfile:
        """Hydrate internal indexes from a KnowledgeStore entity record."""
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

    async def search_relevant_facts(
        self, entity_id: int, embedding: List[float], limit: int = 5
    ) -> List[FactRecord]:
        """Search relevant facts for a specific entity."""
        return await self.knowledge_store.search_relevant_facts(
            entity_id,
            embedding,
            visible_project_ids=self.readable_project_ids,
            limit=limit,
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
        self, mention: str, precomputed_embedding: List[float] = None
    ) -> List[EntityCandidate]:

        if not mention:
            return []

        candidates: Dict[int, EntityCandidate] = {}
        mention_lower = mention.strip().casefold()

        with self._lock:
            exact_ids = self._index.get_entity_ids_for_name(mention_lower)
            exact_is_ambiguous = len(exact_ids) > 1
            for exact_id in exact_ids:
                candidate = candidates.setdefault(
                    exact_id, EntityCandidate(exact_id)
                )
                candidate.add_signal("exact", 1.0)
                if exact_is_ambiguous:
                    candidate.add_signal("ambiguous_alias", 1.0)

            choices = self._index.iter_aliases()
            scorer = fuzz.ratio if len(mention_lower) < 4 else fuzz.WRatio
            results = process.extract(
                mention_lower,
                choices,
                limit=50,
                score_cutoff=self.candidate_fuzzy_threshold,
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
                logger.warning(f"Encoding failed: {e}")
                vector = None

        vector_results = []
        if vector:
            try:
                vector_results = (
                    await self.knowledge_store.search_entities_by_embedding(
                        vector,
                        limit=5,
                        score_threshold=self.candidate_vector_threshold,
                        visible_project_ids=self.readable_project_ids,
                    )
                )
            except Exception as e:
                logger.warning(f"Vector search failed, using fuzzy only: {e}")
                vector_results = []
        for eid, vec_score in vector_results:
            if eid:
                candidates.setdefault(eid, EntityCandidate(eid)).add_signal(
                    "vector", vec_score
                )

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
        text_to_embed = build_entity_embedding_text(canonical_name, entity_type)
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
                session_id=session_id,
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

    def merge_into(
        self,
        primary_id: int,
        secondary_id: int,
        primary_profile_updates: dict = None,
    ):
        """Transfer secondary aliases to primary and remove secondary indexes."""
        with self._lock:
            aliases_transferred = self._index.merge_into(
                primary_id,
                secondary_id,
                primary_profile_updates,
            )
            if aliases_transferred:
                self._bump_alias_version()

            logger.info(
                f"Merged entity {secondary_id} into {primary_id}, "
                f"transferred {aliases_transferred} aliases"
            )
            emit_sync(
                self.project_id,
                "entities",
                "entity_merged",
                {
                    "primary_id": primary_id,
                    "secondary_id": secondary_id,
                    "aliases_transferred": aliases_transferred,
                },
            )

    async def find_alias_collisions_targeted(
        self, target_ids: set
    ) -> List[Tuple[int, int]]:
        """Check alias collisions only involving the given entity IDs."""
        collisions = []

        # Hydrate targets first
        profiles = {}
        for eid in target_ids:
            p = await self.get_profile(eid)
            if p:
                profiles[eid] = p

        for eid, profile in profiles.items():
            names = list(self.get_mentions_for_id(eid))
            names.append(profile.canonical_lower)

            for name in names:
                with self._lock:
                    mapped_ids = self._index.get_entity_ids_for_name(name)
                for mapped_id in mapped_ids - {eid}:
                    pair = tuple(sorted((eid, mapped_id)))
                    if pair not in collisions:
                        collisions.append(pair)

        return collisions

    async def resolve_entity_name(self, entity: str) -> Optional[str]:
        """Resolve user input to canonical entity name via exact or fuzzy match."""
        candidates = await self.get_candidate_ids(entity)

        if not candidates:
            return None

        for candidate in candidates:
            if "exact" in candidate.signals and "ambiguous_alias" in candidate.signals:
                continue
            profile = await self.get_profile(candidate.entity_id)
            if profile:
                return profile.canonical_name
        return None

    async def detect_merge_entity_candidates(self, dirty_ids: set = None) -> list:
        """Detect potential entity merges using vector search + fuzzy matching."""
        # With lazy loading, scan memory or the specifically passed IDs.
        # If dirty_ids is None, we scan the current memory cache.
        with self._lock:
            scan_targets = dirty_ids if dirty_ids else self._index.iter_profile_ids()

        if not scan_targets:
            logger.debug("Merge detection skipped: No entities to check.")
            return []

        logger.info(
            "Merge detection started. "
            f"Scanning {len(scan_targets)} entities against graph."
        )

        generic_tokens = self._build_generic_tokens(self.get_alias_version())
        candidate_pairs = await self._collect_candidate_pairs(
            scan_targets, generic_tokens
        )

        if not candidate_pairs:
            return []

        entity_ids = set()
        for id_a, id_b in candidate_pairs.keys():
            entity_ids.add(id_a)
            entity_ids.add(id_b)

        facts_by_entity = await self.knowledge_store.get_facts_for_entities(
            sorted(entity_ids),
            visible_project_ids=self.readable_project_ids,
            active_only=True,
        )

        candidates = []
        for (id_a, id_b), candidate_meta in candidate_pairs.items():
            result = await self._classify_pair(
                id_a,
                id_b,
                candidate_meta,
                facts_by_entity,
            )
            if result:
                result["reasons"] = list(candidate_meta["reasons"])
                candidates.append(result)

        logger.info(f"Detection complete: {len(candidates)} candidates found")
        return candidates

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

    async def _collect_candidate_pairs(
        self, target_ids: list, generic_tokens: set
    ) -> Dict[Tuple[int, int], dict]:
        """
        Vector search + fuzzy filter.
        Returns {(id_a, id_b): {"fuzz_score": score, "reasons": [...]}}
        """
        seen_pairs = {}

        for primary_id in target_ids:
            primary_profile = await self.get_profile(primary_id)
            if not primary_profile:
                continue

            primary_name = primary_profile.canonical_name

            neighbors = await self.knowledge_store.search_similar_entities(
                primary_id,
                visible_project_ids=self.readable_project_ids,
                limit=50,
            )

            for neighbor_id, _ in neighbors:
                if neighbor_id == primary_id:
                    continue

                pair_key = tuple(sorted((primary_id, neighbor_id)))

                if pair_key in seen_pairs:
                    continue

                neighbor_profile = await self.get_profile(neighbor_id)
                if not neighbor_profile:
                    continue

                neighbor_name = neighbor_profile.canonical_name
                score = fuzz.WRatio(primary_name, neighbor_name)
                is_substring = is_substring_match(primary_name, neighbor_name)
                primary_tokens = set(primary_name.lower().split()) - generic_tokens
                neighbor_tokens = set(neighbor_name.lower().split()) - generic_tokens
                sparse_substring_name = is_substring and (
                    len(primary_tokens) == 1 or len(neighbor_tokens) == 1
                )

                passes_threshold = (
                    is_substring
                    and score >= self.fuzzy_substring_threshold
                    and not sparse_substring_name
                ) or score >= self.fuzzy_non_substring_threshold

                if not passes_threshold:
                    emb_a = (
                        primary_profile.embedding
                        or await self.get_embedding_for_id(primary_id)
                    )
                    emb_b = (
                        neighbor_profile.embedding
                        or await self.get_embedding_for_id(neighbor_id)
                    )
                    if emb_a and emb_b:
                        cos_sim = cosine_similarity(emb_a, emb_b)
                        if cos_sim >= VECTOR_MERGE_SIM_THRESHOLD:
                            logger.info(
                                "Cosine-first candidate: "
                                f"({primary_id}, {neighbor_id}) "
                                f"names='{primary_name}'/'{neighbor_name}' "
                                f"cos={cos_sim:.3f}"
                            )
                            seen_pairs[pair_key] = {
                                "fuzz_score": 0,
                                "is_substring": False,
                                "cosine_score": cos_sim,
                                "reasons": ["vector_similarity"],
                            }
                            continue
                    continue

                if not (primary_tokens & neighbor_tokens):
                    continue

                if (
                    pair_key not in seen_pairs
                    or score > seen_pairs[pair_key]["fuzz_score"]
                ):
                    seen_pairs[pair_key] = {
                        "fuzz_score": score,
                        "is_substring": is_substring,
                        "reasons": ["name_similarity"],
                    }

        return {
            pair: {
                "fuzz_score": metadata["fuzz_score"],
                "cosine_score": metadata.get("cosine_score"),
                "reasons": metadata["reasons"],
            }
            for pair, metadata in seen_pairs.items()
        }

    async def _classify_pair(
        self,
        id_a: int,
        id_b: int,
        candidate_meta: dict,
        facts_by_entity: Dict[int, List[FactRecord]],
    ) -> Optional[dict]:
        """
        Evaluate one pair for merge or hierarchy relationship.
        Returns candidate dict or None to skip.
        """
        direct_edge = await self.knowledge_store.has_direct_edge(
            id_a,
            id_b,
            visible_project_ids=self.readable_project_ids,
        )
        if direct_edge:
            return None
        hierarchy_edge = await self.knowledge_store.has_hierarchy_edge(
            id_a,
            id_b,
            visible_project_ids=self.readable_project_ids,
        )
        if hierarchy_edge:
            return None

        profile_a = await self.get_profile(id_a)
        profile_b = await self.get_profile(id_b)

        if not profile_a or not profile_b:
            return None

        type_a = profile_a.entity_type
        type_b = profile_b.entity_type
        topic_a = profile_a.topic
        topic_b = profile_b.topic
        fuzz_score = candidate_meta["fuzz_score"]
        cosine_score = candidate_meta.get("cosine_score")
        reasons = set(candidate_meta.get("reasons") or [])
        vector_only = "vector_similarity" in reasons and "name_similarity" not in reasons

        type_compatible = self._merge_type_compatible(type_a, type_b)
        topic_compatible = self._merge_topic_compatible(topic_a, topic_b)

        if vector_only and not (type_compatible and topic_compatible):
            return None

        is_cross_topic = topic_a != topic_b
        if is_cross_topic:
            if not (fuzz_score >= 85 and type_a == type_b):
                return None

        neighbors_a = await self.knowledge_store.get_neighbor_ids(
            id_a,
            visible_project_ids=self.readable_project_ids,
        )
        neighbors_b = await self.knowledge_store.get_neighbor_ids(
            id_b,
            visible_project_ids=self.readable_project_ids,
        )
        neighbors_a.discard(1)  # ignore user node
        neighbors_b.discard(1)

        shared_neighbors = neighbors_a & neighbors_b
        if shared_neighbors:
            # Shared neighbors often imply these entities are already distinct
            # and connected within the graph (e.g., co-occurring), so we require
            # extremely high confidence to suggest a merge.
            high_confidence = (
                fuzz_score >= 95 and type_a and type_b and type_a == type_b
            )
            if not high_confidence:
                return None

        facts_a = facts_by_entity.get(id_a, [])
        facts_b = facts_by_entity.get(id_b, [])
        fact_support, fact_support_pairs = await self._classify_fact_support(
            facts_a,
            facts_b,
        )

        if vector_only:
            if fact_support == "contradiction":
                return None
            if fact_support == "neutral":
                return None
            if fact_support == "insufficient_facts" and (
                cosine_score is None
                or cosine_score < VECTOR_MERGE_SPARSE_FACT_SIM_THRESHOLD
            ):
                return None

        return {
            "primary_id": id_a,
            "secondary_id": id_b,
            "primary_name": profile_a.canonical_name or "Unknown",
            "secondary_name": profile_b.canonical_name or "Unknown",
            "primary_type": type_a,
            "secondary_type": type_b,
            "topic_a": topic_a,
            "topic_b": topic_b,
            "facts_a": facts_a,
            "facts_b": facts_b,
            "fuzz_score": fuzz_score,
            "cosine_score": cosine_score,
            "fact_support": fact_support,
            "fact_support_pairs": fact_support_pairs,
            "shared_neighbor_count": len(shared_neighbors),
        }

    @staticmethod
    def _merge_type_compatible(type_a: str, type_b: str) -> bool:
        norm_a = (type_a or "").strip().casefold()
        norm_b = (type_b or "").strip().casefold()
        return not norm_a or not norm_b or norm_a == norm_b

    @staticmethod
    def _merge_topic_compatible(topic_a: str, topic_b: str) -> bool:
        norm_a = (topic_a or "General").strip().casefold()
        norm_b = (topic_b or "General").strip().casefold()
        return norm_a == norm_b or "general" in {norm_a, norm_b}

    async def _classify_fact_support(
        self,
        facts_a: List[FactRecord],
        facts_b: List[FactRecord],
    ) -> Tuple[str, List[dict]]:
        pairs = self._fact_text_pairs(facts_a, facts_b)
        if not pairs:
            return "insufficient_facts", []

        try:
            classifications = await self.embedding_service.classify_text_pairs(
                [(left.content, right.content) for left, right in pairs]
            )
        except Exception as exc:
            logger.warning(f"Merge fact NLI support failed: {exc}")
            return "neutral", []

        support_pairs = []
        labels = []
        for (left, right), classification in zip(pairs, classifications):
            label = str(classification.label or "").casefold()
            labels.append(label)
            support_pairs.append(
                {
                    "fact_a_id": left.id,
                    "fact_b_id": right.id,
                    "label": label,
                    "scores": dict(classification.scores or {}),
                }
            )

        if "contradiction" in labels:
            return "contradiction", support_pairs
        if "entailment" in labels:
            return "entailment", support_pairs
        return "neutral", support_pairs

    @staticmethod
    def _fact_text_pairs(
        facts_a: List[FactRecord],
        facts_b: List[FactRecord],
    ) -> List[Tuple[FactRecord, FactRecord]]:
        active_a = [fact for fact in facts_a if str(fact.content or "").strip()]
        active_b = [fact for fact in facts_b if str(fact.content or "").strip()]
        pairs = []
        for fact_a in active_a:
            for fact_b in active_b:
                pairs.append((fact_a, fact_b))
                if len(pairs) >= MERGE_FACT_NLI_PAIR_LIMIT:
                    return pairs
        return pairs
