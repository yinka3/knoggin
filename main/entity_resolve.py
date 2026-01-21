from collections import defaultdict
from loguru import logger
import threading
from typing import Dict, List, Optional, Tuple
from rapidfuzz import fuzz, process
import numpy as np
from sentence_transformers import SentenceTransformer, CrossEncoder
import torch
from db.memgraph import MemGraphStore
from main.utils import is_substring_match
from schema.dtypes import Fact


class EntityResolver:
    

    def __init__(self, store: 'MemGraphStore', session_id: str = None, hierarchy_config: dict = None, embedding_model='dunzhang/stella_en_400M_v5'):
        self.store = store
        self.hierarchy_config = hierarchy_config or {}
        self.session_id = session_id
        device = 'cuda' if torch.cuda.is_available() else 'cpu'
        logger.info(f"EntityResolver using device: {device}")
        self.embedding_model = SentenceTransformer(embedding_model, trust_remote_code=True, device=device, model_kwargs={"torch_dtype": torch.float16})
        self.cross_encoder = CrossEncoder('BAAI/bge-reranker-base', device=device)
        self.embedding_dim = 1024
        self.entity_profiles = {}
        self._name_to_id = {}
        self._lock = threading.RLock()
    
        self._hydrate_from_store()

    def _hydrate_from_store(self):
        """Populate all resolver structures from Memgraph."""
        try:
            entities = self.store.get_all_entities_for_hydration()
            
            if not entities:
                logger.info("No entities in Memgraph. Starting fresh.")
                return
            
            ids = []
            vectors = []
            
            with self._lock:
                for ent in entities:
                    ent_id = ent["id"]
                    canonical = ent["canonical_name"]
                    aliases = ent["aliases"] or []
                    embedding = ent["embedding"]
                    
                    self._name_to_id[canonical.lower()] = ent_id
                    for alias in aliases:
                        self._name_to_id[alias.lower()] = ent_id
                    
                    # no facts in cache anymore
                    self.entity_profiles[ent_id] = {
                        "canonical_name": canonical,
                        "type": ent["type"],
                        "topic": ent.get("topic", "General"),
                        "session_id": ent.get("session_id")
                    }
                    
                    if embedding and len(embedding) == self.embedding_dim:
                        ids.append(ent_id)
                        vectors.append(embedding)
            
            logger.info(f"Hydrated {len(self.entity_profiles)} entities, {len(ids)} vectors from Memgraph")
            
        except Exception as e:
            logger.error(f"Hydration failed: {e}")
            raise
    
    def get_id(self, name: str) -> Optional[int]:
        return self._name_to_id.get(name.lower())
    
    def get_profiles(self) -> Dict[int, Dict]:
        return self.entity_profiles
    
    def get_mentions_for_id(self, entity_id: int) -> List[str]:
        with self._lock:
            items = list(self._name_to_id.items())
        return [mention for mention, eid in items if eid == entity_id]
    
    def get_embedding_for_id(self, entity_id: int) -> List[float]:
        """Retrieve embedding from graph by ID."""
        profile = self.entity_profiles.get(entity_id)
        if profile and profile.get("embedding"):
            return profile["embedding"]
        return self.store.get_entity_embedding(entity_id)
            
    def get_hierarchy_relationship(self, type_a: str, type_b: str, topic: str) -> Optional[str]:
        """
        Check if two types have a parent/child relationship within a topic.
        
        Returns:
            "parent" if type_a is parent of type_b
            "child" if type_a is child of type_b
            None if no hierarchy relationship
        """
        topic_hierarchy = self.hierarchy_config.get(topic, {})
        
        if not topic_hierarchy:
            return None
        
        if type_a in topic_hierarchy:
            if type_b in topic_hierarchy[type_a]:
                return "parent"
        
        if type_b in topic_hierarchy:
            if type_a in topic_hierarchy[type_b]:
                return "child"
        
        return None
    

    def compute_batch_embeddings(self, texts: List[str]) -> List[List[float]]:
        """
        Compute embeddings for a batch of texts (used by Processor).
        """
        if not texts:
            return []
            
        embeddings = self.embedding_model.encode(texts).astype(np.float32)
        return embeddings.tolist()
        

    def _search_messages(self, query: str, k: int = 10) -> list[tuple[str, float]]:

        results = {}
        query_embedding = self.embedding_model.encode([query]).astype(np.float32)[0].tolist()
        sem_results = self.store.search_messages_vector(query_embedding, limit=50)

        for msg_id, score in sem_results:
            msg_key = f"msg_{msg_id}" if msg_id < 1_000_000_000 else f"turn_{msg_id - 1_000_000_000}"
            results[msg_key] = ("semantic", float(score))
        
        fts_results = self.store.search_messages_fts(query, limit=50)
        max_fts = max([s for _, s in fts_results]) if fts_results else 1.0

        for msg_id, raw_score in fts_results:
            msg_key = f"msg_{msg_id}"
            
            norm_score = raw_score / max_fts if max_fts > 0 else 0
            
            logger.debug(f"FTS result: {msg_key} score={norm_score:.3f}")

            if msg_key in results:
                _, sem_score = results[msg_key]
                results[msg_key] = ("both", sem_score + norm_score)
            else:
                results[msg_key] = ("keyword", norm_score)
        
        if not results:
            return []
        
        if len(results) > 1:
            candidate_keys = list(results.keys())[:45]
            pairs = []
            for msg_key in candidate_keys:
                msg_id = int(msg_key.split("_")[1])
                text = self.store.get_message_text(msg_id)
                pairs.append((query, text))
            
            scores = self.cross_encoder.predict(pairs)
            reranked = sorted(zip(candidate_keys, scores), key=lambda x: x[1], reverse=True)
            return [(msg_key, float(score)) for msg_key, score in reranked[:k]]
            
        # Fallback: single result
        sorted_results = sorted(results.items(), key=lambda x: x[1][1], reverse=True)[:k]
        return [(key, score) for key, (_, score) in sorted_results]
    
    def validate_existing(self, canonical_name: str, mentions: List[str]) -> Tuple[Optional[int], bool]:
        """
        Check if canonical_name exists. If yes, register mention aliases and return ID.
        If no, return None (caller handles demotion).
        """
        with self._lock:
            entity_id = self.get_id(canonical_name)
            logger.debug(f"validate_existing: '{canonical_name}' -> id={entity_id}")
            if entity_id is None:
                return None, False
            
            new_aliases = {}
            for mention in mentions:
                if mention.lower() not in self._name_to_id:
                    self._name_to_id[mention.lower()] = entity_id
                    new_aliases[mention] = entity_id

            return entity_id, len(new_aliases) > 0
    
    def _build_generic_tokens(self, min_entity_freq: int = 10) -> set:
        """Tokens appearing in N+ distinct entities are generic."""
        token_to_entities = defaultdict(set)
        
        for ent_id, profile in self.entity_profiles.items():
            canonical = profile.get("canonical_name", "").lower()
            for token in canonical.split():
                token_to_entities[token].add(ent_id)
            
            for alias in self.get_mentions_for_id(ent_id):
                for token in alias.lower().split():
                    token_to_entities[token].add(ent_id)
        
        return {token for token, ent_ids in token_to_entities.items() 
                if len(ent_ids) >= min_entity_freq}
    
    def get_candidate_ids(
        self, 
        mention: str,
        fuzzy_threshold: int = 75,
    ) -> List[int]:

        candidates = set()
        mention_lower = mention.lower()

        with self._lock:
            if mention_lower in self._name_to_id:
                candidates.add(self._name_to_id[mention_lower])
            
            choices = list(self._name_to_id.keys())

        results = process.extract(
            mention_lower,
            choices,
            limit=50,
            score_cutoff=fuzzy_threshold,
            scorer=fuzz.WRatio
        )

        for alias, _, _ in results:
            eid = self._name_to_id.get(alias)
            if eid is not None:
                candidates.add(eid)
        
        return list(candidates)
    
    def register_entity(
        self, 
        entity_id: int, 
        canonical_name: str, 
        mentions: List[str], 
        entity_type: str, 
        topic: str,
        session_id: str = None
    ) -> List[float]:
        """
        Register new entity: update all indexes and return embedding.
        """

        sessionID = session_id or self.session_id
        profile = {
            "canonical_name": canonical_name,
            "type": entity_type,
            "topic": topic,
            "facts": [],
            "session_id": sessionID
        }
        
        embedding = self._add_entity(entity_id, profile, sessionID)
        
        with self._lock:
            self._name_to_id[canonical_name.lower()] = entity_id
            for mention in mentions:
                mention_lower = mention.lower()
                existing_id = self._name_to_id.get(mention_lower)
                if existing_id and existing_id != entity_id:
                    logger.warning(f"Alias collision: '{mention}' belongs to {existing_id}, skipping for {entity_id}")
                    continue
                self._name_to_id[mention_lower] = entity_id

        return embedding


    def _add_entity(self, entity_id: int, profile: Dict, session_id: str) -> List[float]:
        canonical_name = profile.get("canonical_name", "")
        
        # Build resolution text without facts (new facts added on profile cycle)
        resolution_text = canonical_name
        embedding_np = self.embedding_model.encode([resolution_text]).astype(np.float32)[0]

        with self._lock:                
            logger.info(f"Adding entity {entity_id}-{profile['canonical_name']} to resolver indexes.")

            profile.setdefault("topic", "General")

            # Store profile without facts
            self.entity_profiles[entity_id] = {
                "canonical_name": profile["canonical_name"],
                "type": profile.get("type"),
                "topic": profile.get("topic", "General"),
                "session_id": session_id,
                "embedding": embedding_np.tolist()
            }
        
        return embedding_np.tolist()
    

    def compute_embedding(self, entity_id: int, resolution_text: str) -> List[float]:
        """
        Update entity facts and recompute embedding.
        Returns new embedding.
        """
        with self._lock:
            profile = self.entity_profiles.get(entity_id)
            if not profile:
                logger.warning(f"Cannot update profile for unknown entity {entity_id}")
                return []
            
            logger.info(f"Updating embedding for entity {entity_id}-{profile['canonical_name']}")
            
            embedding_np = self.embedding_model.encode([resolution_text]).astype(np.float32)[0]
            
        return embedding_np.tolist()
        
    def merge_into(self, primary_id: int, secondary_id: int):
        """Transfer secondary entity's aliases to primary, remove secondary from indexes."""
        with self._lock:
            secondary_aliases = [
                alias for alias, eid in self._name_to_id.items() 
                if eid == secondary_id
            ]
            
            for alias in secondary_aliases:
                self._name_to_id[alias] = primary_id
            
            if secondary_id in self.entity_profiles:
                del self.entity_profiles[secondary_id]
    
    def resolve_entity_name(self, entity: str) -> Optional[str]:
        """Resolve user input to canonical entity name via exact or fuzzy match."""
        candidates = self.get_candidate_ids(entity, fuzzy_threshold=85)
        
        if not candidates:
            return None
        
        with self._lock:
            profile = self.entity_profiles.get(candidates[0])
        
        return profile["canonical_name"] if profile else None
            

    def detect_merge_candidates(self) -> list:
        """Detect potential entity merges using vector search + fuzzy matching."""
        logger.info(f"Merge detection started. Scanning {len(self.entity_profiles)} entities.")

        generic_tokens = self._build_generic_tokens()
        candidate_pairs = self._collect_candidate_pairs(generic_tokens)

        if not candidate_pairs:
            return []

        entity_ids = set()
        for id_a, id_b in candidate_pairs.keys():
            entity_ids.add(id_a)
            entity_ids.add(id_b)

        facts_by_entity = self.store.get_facts_for_entities(list(entity_ids), active_only=True)

        candidates = []
        for (id_a, id_b), (fuzz_score, is_substring) in candidate_pairs.items():
            result = self._classify_pair(id_a, id_b, fuzz_score, is_substring, facts_by_entity)
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
                
                to_remove = [alias for alias, id_ in self._name_to_id.items() if id_ == eid]
                for alias in to_remove:
                    del self._name_to_id[alias]
        
        if removed > 0:
            logger.info(f"Removed {removed} entities from resolver")
        return removed


    def _collect_candidate_pairs(
        self, 
        generic_tokens: set,
        fuzzy_substring_threshold: int = 75,
        fuzzy_non_substring_threshold: int = 91,
    ) -> Dict[Tuple[int, int], Tuple[int, bool]]:
        """
        Vector search + fuzzy filter.
        Returns {(id_a, id_b): (fuzz_score, is_substring)}
        """
        seen_pairs = {}

        with self._lock:
            snapshot_ids = list(self.entity_profiles.keys())

        for primary_id in snapshot_ids:
            primary_profile = self.entity_profiles.get(primary_id)
            if not primary_profile:
                continue

            primary_name = primary_profile["canonical_name"]
            neighbors = self.store.search_similar_entities(primary_id, limit=50)

            for neighbor_id, _ in neighbors:
                if neighbor_id == primary_id or neighbor_id < primary_id:
                    continue

                neighbor_profile = self.entity_profiles.get(neighbor_id)
                if not neighbor_profile:
                    continue

                neighbor_name = neighbor_profile["canonical_name"]
                score = fuzz.WRatio(primary_name, neighbor_name)
                is_substring = is_substring_match(primary_name, neighbor_name)

                # threshold check
                passes_threshold = (
                    (is_substring and score >= fuzzy_substring_threshold) or
                    score >= fuzzy_non_substring_threshold
                )

                if not passes_threshold:
                    continue

                # generic token overlap check
                tokens_i = set(primary_name.lower().split()) - generic_tokens
                tokens_j = set(neighbor_name.lower().split()) - generic_tokens

                if not (tokens_i & tokens_j):
                    logger.warning(f"Skipping {primary_id}-{neighbor_id}: generic token overlap only")
                    continue

                pair_key = (primary_id, neighbor_id)
                if pair_key not in seen_pairs or score > seen_pairs[pair_key][0]:
                    seen_pairs[pair_key] = (score, is_substring)

        return seen_pairs


    def _classify_pair(
        self,
        id_a: int,
        id_b: int,
        fuzz_score: int,
        is_substring: bool,
        facts_by_entity: Dict[int, List[Fact]],
        hierarchy_substring_threshold: int = 80,
        hierarchy_non_substring_threshold: int = 91,
    ) -> Optional[dict]:
        """
        Evaluate one pair for merge or hierarchy relationship.
        Returns candidate dict or None to skip.
        """
        if self.store.has_direct_edge(id_a, id_b):
            return None
        if self.store.has_hierarchy_edge(id_a, id_b):
            return None

        profile_a = self.entity_profiles.get(id_a, {})
        profile_b = self.entity_profiles.get(id_b, {})

        type_a = profile_a.get("type")
        type_b = profile_b.get("type")
        topic_a = profile_a.get("topic", "General")
        topic_b = profile_b.get("topic", "General")

        is_cross_topic = topic_a != topic_b
        if is_cross_topic:
            if not (fuzz_score >= 96 and type_a == type_b):
                return None

        relationship = "merge"
        parent_id = None
        child_id = None
        
        # only consider hierarchy if same topic and both types known
        if topic_a == topic_b and type_a and type_b:
            hierarchy_rel = self.get_hierarchy_relationship(type_a, type_b, topic_a)
            if hierarchy_rel == "parent":
                relationship = "hierarchy"
                parent_id = id_a
                child_id = id_b
            elif hierarchy_rel == "child":
                relationship = "hierarchy"
                parent_id = id_b
                child_id = id_a

            
        if relationship == "hierarchy":
            if is_cross_topic:
                return None
            if is_substring and fuzz_score < hierarchy_substring_threshold:
                return None
            if not is_substring and fuzz_score < hierarchy_non_substring_threshold:
                return None

        # shared neighbor check
        if relationship == "merge":
            neighbors_a = self.store.get_neighbor_ids(id_a)
            neighbors_b = self.store.get_neighbor_ids(id_b)
            neighbors_a.discard(1) # ignore user node
            neighbors_b.discard(1)

            shared_neighbors = neighbors_a & neighbors_b
            if shared_neighbors:
                high_confidence = fuzz_score >= 95 and type_a and type_b and type_a == type_b
                if not high_confidence:
                    return None
        else:
            shared_neighbors = set()


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
            "shared_neighbor_count": len(shared_neighbors),
            "relationship": relationship,
            "parent_id": parent_id,
            "child_id": child_id,
        }
    