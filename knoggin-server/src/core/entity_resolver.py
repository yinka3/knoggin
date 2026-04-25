from collections import defaultdict
from loguru import logger
import threading
from typing import Dict, List, Optional, Tuple, Set
from rapidfuzz import fuzz, process
from db.store import MemGraphStore
from common.rag.embedding import EmbeddingService
from core.utils import is_substring_match
from common.schema.dtypes import Fact
from common.utils.events import emit_sync
from jobs.utils import cosine_similarity
from cachetools import LRUCache, cached, TTLCache

class EntityResolver:
    

    def __init__(self, store: 'MemGraphStore', embedding_service: EmbeddingService, 
                session_id: str = None, hierarchy_config: dict = None,
                fuzzy_substring_threshold: int = 75,
                fuzzy_non_substring_threshold: int = 91,
                generic_token_freq: int = 10,
                candidate_fuzzy_threshold: int = 85,
                candidate_vector_threshold: int = 0.85):
        
        self.store = store
        self.hierarchy_config = hierarchy_config or {}
        self.session_id = session_id
        self.embedding_service = embedding_service
        self.entity_profiles = LRUCache(maxsize=1000000)
        self._name_to_id = LRUCache(maxsize=3000000)
        self._id_to_names = LRUCache(maxsize=1000000)
        self._lock = threading.RLock()

        self.candidate_fuzzy_threshold = candidate_fuzzy_threshold
        self.candidate_vector_threshold = candidate_vector_threshold
        self.fuzzy_substring_threshold = fuzzy_substring_threshold
        self.fuzzy_non_substring_threshold = fuzzy_non_substring_threshold
        self.generic_token_freq = generic_token_freq
        
    
    def update_settings(self, fuzzy_substring_threshold: int = None, 
                        fuzzy_non_substring_threshold: int = None, 
                        generic_token_freq: int = None,
                        candidate_fuzzy_threshold: int = None,
                        candidate_vector_threshold: float = None):
        
        """Update resolution thresholds on the fly."""
        if fuzzy_substring_threshold is not None:
            self.fuzzy_substring_threshold = fuzzy_substring_threshold
        if fuzzy_non_substring_threshold is not None:
            self.fuzzy_non_substring_threshold = fuzzy_non_substring_threshold
        if generic_token_freq is not None:
            self.generic_token_freq = generic_token_freq
        
        if candidate_fuzzy_threshold is not None:
            self.candidate_fuzzy_threshold = candidate_fuzzy_threshold
        if candidate_vector_threshold is not None:
            self.candidate_vector_threshold = candidate_vector_threshold
            
        logger.info(f"EntityResolver settings updated: sub={self.fuzzy_substring_threshold}, non-sub={self.fuzzy_non_substring_threshold}, freq={self.generic_token_freq}")
    
    async def get_id(self, name: str) -> Optional[int]:
        if not name:
            return None
            
        lower_name = name.lower()
        
        with self._lock:
            stored_id = self._name_to_id.get(lower_name)
            if stored_id is not None:
                return stored_id
        found = await self.store.get_entities_by_names([name])
        if found:
            entity = found[0]
            eid = entity["id"]
            
            with self._lock:
                self._name_to_id[lower_name] = eid
                if eid not in self._id_to_names:
                    self._id_to_names[eid] = set()
                self._id_to_names[eid].add(lower_name)

                if entity.get("aliases"):
                    for a in entity["aliases"]:
                        self._name_to_id[a.lower()] = eid
                        self._id_to_names[eid].add(a.lower())
                
                if eid not in self.entity_profiles:
                    self.entity_profiles[eid] = {
                        "canonical_name": entity.get("canonical_name", name),
                        "type": entity.get("type"),
                        "topic": "General",
                        "session_id": None
                    }
                        
            return eid
        return None
    
    async def get_profile(self, entity_id: int) -> Optional[dict]:
        with self._lock:
            profile = self.entity_profiles.get(entity_id)
            if profile:
                return profile
        
        # Cache miss: fetch from store
        entity = await self.store.get_entity_by_id(entity_id)
        if entity:
            eid = entity["id"]
            canonical = entity.get("canonical_name")
            
            with self._lock:
                profile = {
                    "canonical_name": canonical,
                    "type": entity.get("type"),
                    "topic": entity.get("topic", "General"),
                    "session_id": entity.get("session_id"),
                    "embedding": entity.get("embedding")
                }
                self.entity_profiles[eid] = profile
                
                if canonical:
                    lower_canonical = canonical.lower()
                    self._name_to_id[lower_canonical] = eid
                    if eid not in self._id_to_names:
                        self._id_to_names[eid] = set()
                    self._id_to_names[eid].add(lower_canonical)
                
                aliases = entity.get("aliases") or []
                for a in aliases:
                    lower_a = a.lower()
                    self._name_to_id[lower_a] = eid
                    self._id_to_names[eid].add(lower_a)
                    
                return profile
        return None
    

    def get_profiles(self) -> Dict[int, Dict]:
        with self._lock:
            return dict(list(self.entity_profiles.items()))
    
    def get_mentions_for_id(self, entity_id: int) -> List[str]:
        with self._lock:
            return list(self._id_to_names.get(entity_id, set()))

    def get_known_aliases(self) -> Dict[str, int]:
        with self._lock:
            return dict(list(self._name_to_id.items()))
    
    async def get_embedding_for_id(self, entity_id: int) -> List[float]:
        """Retrieve embedding from graph by ID."""
        with self._lock:
            profile = self.entity_profiles.get(entity_id)
            if profile and profile.get("embedding"):
                return profile["embedding"]
        return await self.store.get_entity_embedding(entity_id)
        

    async def compute_batch_embeddings(self, texts: List[str]) -> List[List[float]]:
        """
        Compute embeddings for a batch of texts (used by Processor).
        """
        if not texts:
            return []
            
        return await self.embedding_service.encode(texts)
        
    
    def validate_existing(self, canonical_name: str, mentions: List[str]) -> Tuple[Optional[int], bool, List[str]]:
        """
        Check if canonical_name exists. If yes, register mention aliases and return ID.
        If no, return None (caller handles demotion).
        
        Returns:
            Tuple of (entity_id, aliases_added, new_aliases_list)
        """
        if not canonical_name:
            return None, False, []
        
        with self._lock:
            entity_id = self._name_to_id.get(canonical_name.lower())
            logger.debug(f"validate_existing: '{canonical_name}' -> id={entity_id}")
            if entity_id is None:
                return None, False, []
            
            new_aliases = []
            for mention in mentions:
                if mention.lower() not in self._name_to_id:
                    new_aliases.append(mention)

            return entity_id, len(new_aliases) > 0, new_aliases
    
    def commit_new_aliases(self, entity_id: int, aliases: List[str]):
        """Explicitly commit aliases after Graph validation."""
        if not aliases:
            return

        with self._lock:
            if entity_id not in self.entity_profiles:
                return
            for mention in aliases:
                self._name_to_id[mention.lower()] = entity_id
                if entity_id not in self._id_to_names:
                    self._id_to_names[entity_id] = set()
                self._id_to_names[entity_id].add(mention.lower())
    
    @cached(cache=TTLCache(maxsize=1, ttl=300))
    def _build_generic_tokens(self) -> set:
        """Tokens appearing in N+ distinct entities are generic. Cached for 5 minutes."""
        token_to_entities = defaultdict(set)
    
        with self._lock:
            profiles_snapshot = dict(self.entity_profiles)
            aliases_snapshot = {eid: list(self._id_to_names.get(eid, set())) for eid in profiles_snapshot}
        
        for ent_id, profile in profiles_snapshot.items():
            canonical = profile.get("canonical_name", "").lower()
            for token in canonical.split():
                token_to_entities[token].add(ent_id)
            
            for alias in aliases_snapshot.get(ent_id, []):
                for token in alias.lower().split():
                    token_to_entities[token].add(ent_id)
        
        return {token for token, ent_ids in token_to_entities.items() 
                if len(ent_ids) >= self.generic_token_freq}
    
    async def get_candidate_ids(
        self, 
        mention: str,
        precomputed_embedding: List[float] = None
    ) -> List[Tuple[int, float]]:
        
        if not mention:
            return []

        candidate_scores: Dict[int, float] = {}
        mention_lower = mention.lower()
        
        with self._lock:
            if mention_lower in self._name_to_id:
                candidate_scores[self._name_to_id[mention_lower]] = 1.0
            
            choices = list(self._name_to_id.keys())
            scorer = fuzz.ratio if len(mention_lower) < 4 else fuzz.WRatio
            results = process.extract(
                mention_lower,
                choices,
                limit=50,
                score_cutoff=self.candidate_fuzzy_threshold,
                scorer=scorer
            )

            for alias, fuzz_score, _ in results:
                eid = self._name_to_id.get(alias)
                if eid is not None:
                    normalized = fuzz_score / 100.0
                    candidate_scores[eid] = max(candidate_scores.get(eid, 0), normalized)
        
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
                vector_results = await self.store.search_entities_by_embedding(
                        vector, 
                        limit=5, 
                        score_threshold=self.candidate_vector_threshold
                    )
            except Exception as e:
                logger.warning(f"Vector search failed, using fuzzy only: {e}")
                vector_results = []
        for eid, vec_score in vector_results:
            if eid:
                candidate_scores[eid] = max(candidate_scores.get(eid, 0), vec_score)

        with self._lock:
            valid_scores = {
                eid: score for eid, score in candidate_scores.items()
                if eid in self.entity_profiles
            }
        return sorted(valid_scores.items(), key=lambda x: x[1], reverse=True)
    
    async def register_entity(
        self, 
        entity_id: int, 
        canonical_name: str, 
        mentions: List[str], 
        entity_type: str, 
        topic: str,
        session_id: str = None,
        source_context: str = None
    ) -> List[float]:
        """
        Register new entity: update all indexes and return embedding.
        """

        session_id = session_id or self.session_id
        text_to_embed = None
        if source_context:
            text_to_embed = f"{canonical_name} ({entity_type}). Context: {source_context}"
        else:
            text_to_embed = f"{canonical_name} ({entity_type})"

        embedding = await self.embedding_service.encode_single(text_to_embed)
        
        with self._lock:
            logger.info(f"Adding entity {entity_id}-{canonical_name} to resolver indexes.")
            
            self.entity_profiles[entity_id] = {
                "canonical_name": canonical_name,
                "type": entity_type,
                "topic": topic or "General",
                "session_id": session_id,
                "embedding": embedding
            }

            if entity_id not in self._id_to_names:
                self._id_to_names[entity_id] = set()
            self._id_to_names[entity_id].add(canonical_name.lower())

            self._name_to_id[canonical_name.lower()] = entity_id
            for mention in mentions:
                mention_lower = mention.lower()
                existing_id = self._name_to_id.get(mention_lower)
                if existing_id and existing_id != entity_id:
                    logger.warning(f"Alias collision: '{mention}' belongs to {existing_id}, skipping for {entity_id}")
                    continue
                self._name_to_id[mention_lower] = entity_id
                self._id_to_names[entity_id].add(mention_lower)
        
        return embedding
    

    async def compute_embedding(self, entity_id: int, resolution_text: str) -> List[float]:
        with self._lock:
            profile = self.entity_profiles.get(entity_id)
            if not profile:
                logger.warning(f"Cannot update profile for unknown entity {entity_id}")
                return []
            
            logger.info(f"Updating embedding for entity {entity_id}-{profile['canonical_name']}")
        
        embedding = await self.embedding_service.encode_single(resolution_text)
        
        with self._lock:
            if entity_id in self.entity_profiles:
                self.entity_profiles[entity_id]["embedding"] = embedding
        
        return embedding
        
    def merge_into(self, primary_id: int, secondary_id: int):
        """Transfer secondary entity's aliases to primary, remove secondary from indexes."""
        with self._lock:
            secondary_aliases = list(self._id_to_names.get(secondary_id, set()))
            
            for alias in secondary_aliases:
                self._name_to_id[alias] = primary_id

            secondary_names = self._id_to_names.pop(secondary_id, set())
            if primary_id not in self._id_to_names:
                self._id_to_names[primary_id] = set()
            self._id_to_names[primary_id].update(secondary_names)
            
            if secondary_id in self.entity_profiles:
                del self.entity_profiles[secondary_id]
            
            logger.info(f"Merged entity {secondary_id} into {primary_id}, transferred {len(secondary_aliases)} aliases")
            emit_sync(self.session_id, "resolver", "entity_merged", {
                "primary_id": primary_id,
                "secondary_id": secondary_id,
                "aliases_transferred": len(secondary_aliases)
            })
    
    async def find_alias_collisions_targeted(self, target_ids: set) -> List[Tuple[int, int]]:
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
            names.append(profile["canonical_name"].lower())
            
            for name in names:
                with self._lock:
                    mapped_id = self._name_to_id.get(name.lower())
                if mapped_id and mapped_id != eid:
                    pair = tuple(sorted((eid, mapped_id)))
                    if pair not in collisions:
                        collisions.append(pair)
        
        return collisions
    
    async def resolve_entity_name(self, entity: str) -> Optional[str]:
        """Resolve user input to canonical entity name via exact or fuzzy match."""
        candidates = await self.get_candidate_ids(entity)
    
        if not candidates:
            return None
        
        top_id, _ = candidates[0]
        
        profile = await self.get_profile(top_id)
        return profile["canonical_name"] if profile else None
            

    async def detect_merge_entity_candidates(self, dirty_ids: set = None) -> list:
        """Detect potential entity merges using vector search + fuzzy matching."""
        # Note: with lazy loading, we scan what's in memory or what's specifically passed.
        # If dirty_ids is None, we scan the current memory cache.
        with self._lock:
            scan_targets = dirty_ids if dirty_ids else list(self.entity_profiles.keys())
        
        if not scan_targets:
            logger.debug("Merge detection skipped: No entities to check.")
            return []

        logger.info(f"Merge detection started. Scanning {len(scan_targets)} entities against graph.")

        generic_tokens = self._build_generic_tokens()
        candidate_pairs = await self._collect_candidate_pairs(scan_targets, generic_tokens)

        if not candidate_pairs:
            return []

        entity_ids = set()
        for id_a, id_b in candidate_pairs.keys():
            entity_ids.add(id_a)
            entity_ids.add(id_b)
        
        facts_by_entity = await self.store.get_facts_for_entities(list(entity_ids), active_only=True)

        candidates = []
        for (id_a, id_b), fuzz_score in candidate_pairs.items():
            result = await self._classify_pair(id_a, id_b, fuzz_score, facts_by_entity)
            if result:
                candidates.append(result)

        logger.info(f"Detection complete: {len(candidates)} candidates found")
        return candidates
    
    def remove_entities(self, entity_ids: List[int]) -> int:
        """Remove entities from resolver indexes. Call after Memgraph deletion."""
        if not entity_ids:
            return 0
        
        removed = 0
        with self._lock:
            for eid in entity_ids:
                if eid in self.entity_profiles:
                    del self.entity_profiles[eid]
                    removed += 1
                
                to_remove = list(self._id_to_names.get(eid, set()))
                for alias in to_remove:
                    self._name_to_id.pop(alias, None)
                
                self._id_to_names.pop(eid, None)
        
        if removed > 0:
            logger.info(f"Removed {removed} entities from resolver")
            emit_sync(self.session_id, "resolver", "entities_removed", {
                "requested": len(entity_ids),
                "removed": removed
            })
        return removed


    async def _collect_candidate_pairs(
        self,
        target_ids: list,
        generic_tokens: set
    ) -> Dict[Tuple[int, int], int]:
        """
        Vector search + fuzzy filter.
        Returns {(id_a, id_b): fuzz_score}
        """
        seen_pairs = {}

        for primary_id in target_ids:
            primary_profile = await self.get_profile(primary_id)
            if not primary_profile:
                continue

            primary_name = primary_profile["canonical_name"]
            
            neighbors = await self.store.search_similar_entities(primary_id, limit=50)

            for neighbor_id, _ in neighbors:
                if neighbor_id == primary_id:
                    continue

                pair_key = tuple(sorted((primary_id, neighbor_id)))
                
                if pair_key in seen_pairs:
                    continue

                neighbor_profile = await self.get_profile(neighbor_id)
                if not neighbor_profile:
                    continue

                neighbor_name = neighbor_profile["canonical_name"]
                score = fuzz.WRatio(primary_name, neighbor_name)
                is_substring = is_substring_match(primary_name, neighbor_name)

                passes_threshold = (
                    (is_substring and score >= self.fuzzy_substring_threshold) or
                    score >= self.fuzzy_non_substring_threshold
                )

                if not passes_threshold:
                    emb_a = primary_profile.get("embedding") or await self.get_embedding_for_id(primary_id)
                    emb_b = neighbor_profile.get("embedding") or await self.get_embedding_for_id(neighbor_id)
                    if emb_a and emb_b:
                        
                        cos_sim = cosine_similarity(emb_a, emb_b)
                        if cos_sim >= 0.90:
                            logger.info(
                                f"Cosine-first candidate: ({primary_id}, {neighbor_id}) "
                                f"names='{primary_name}'/'{neighbor_name}' cos={cos_sim:.3f}"
                            )
                            seen_pairs[pair_key] = (0, False)
                            continue
                    continue

                tokens_i = set(primary_name.lower().split()) - generic_tokens
                tokens_j = set(neighbor_name.lower().split()) - generic_tokens

                if not (tokens_i & tokens_j):
                    continue

                if pair_key not in seen_pairs or score > seen_pairs[pair_key][0]:
                    seen_pairs[pair_key] = (score, is_substring)

        return {k: v[0] for k, v in seen_pairs.items()}


    async def _classify_pair(
        self,
        id_a: int,
        id_b: int,
        fuzz_score: int,
        facts_by_entity: Dict[int, List[Fact]]
    ) -> Optional[dict]:
        """
        Evaluate one pair for merge or hierarchy relationship.
        Returns candidate dict or None to skip.
        """
        direct_edge = await self.store.has_direct_edge(id_a, id_b)
        if direct_edge:
            return None
        hierarchy_edge = await self.store.has_hierarchy_edge(id_a, id_b)
        if hierarchy_edge:
            return None

        profile_a = await self.get_profile(id_a)
        profile_b = await self.get_profile(id_b)
        
        if not profile_a or not profile_b:
            return None

        type_a = profile_a.get("type")
        type_b = profile_b.get("type")
        topic_a = profile_a.get("topic", "General")
        topic_b = profile_b.get("topic", "General")

        is_cross_topic = topic_a != topic_b
        if is_cross_topic:
            if not (fuzz_score >= 85 and type_a == type_b):
                return None
        
        neighbors_a = await self.store.get_neighbor_ids(id_a)
        neighbors_b = await self.store.get_neighbor_ids(id_b)
        neighbors_a.discard(1) # ignore user node
        neighbors_b.discard(1)

        shared_neighbors = neighbors_a & neighbors_b
        if shared_neighbors:
            high_confidence = fuzz_score >= 95 and type_a and type_b and type_a == type_b
            if not high_confidence:
                return None

        return {
            "primary_id": id_a,
            "secondary_id": id_b,
            "primary_name": profile_a.get("canonical_name", "Unknown"),
            "secondary_name": profile_b.get("canonical_name", "Unknown"),
            "primary_type": type_a,
            "secondary_type": type_b,
            "primary_session": profile_a.get("session_id"),
            "secondary_session": profile_b.get("session_id"),
            "topic_a": topic_a,
            "topic_b": topic_b,
            "facts_a": facts_by_entity.get(id_a, []),
            "facts_b": facts_by_entity.get(id_b, []),
            "fuzz_score": fuzz_score,
            "shared_neighbor_count": len(shared_neighbors)
        }
    