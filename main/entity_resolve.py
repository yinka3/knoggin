from collections import defaultdict
from datetime import datetime, timezone
from loguru import logger
import threading
from rank_bm25 import BM25Okapi
from typing import Dict, List, Optional, Tuple
from rapidfuzz import fuzz
import faiss
import numpy as np
from sentence_transformers import SentenceTransformer, CrossEncoder
import torch
from db.memgraph import MemGraphStore



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
        self.index_id_map = faiss.IndexIDMap2(faiss.IndexFlatIP(self.embedding_dim))
        self.entity_profiles = {}
        self._name_to_id = {}
        self.msg_index = faiss.IndexIDMap2(faiss.IndexScalarQuantizer(self.embedding_dim, faiss.ScalarQuantizer.QT_fp16, faiss.METRIC_INNER_PRODUCT))
        self.msg_int_to_id: dict[int, str] = {}
        self._lock = threading.RLock()
        self.bm25_index: Optional[BM25Okapi] = None
        self.msg_corpus: list[list[str]] = []
        self.msg_id_order: list[str] = []
        self._bm25_dirty = True
    
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
                    
                    self.entity_profiles[ent_id] = {
                        "canonical_name": canonical,
                        "type": ent["type"],
                        "facts": ent["facts"] or []
                    }
                    
                    if embedding and len(embedding) == self.embedding_dim:
                        ids.append(ent_id)
                        vectors.append(embedding)
                
                if ids:
                    self.index_id_map.add_with_ids(
                        np.array(vectors, dtype=np.float32),
                        np.array(ids, dtype=np.int64)
                    )
            
            logger.info(f"Hydrated {len(self.entity_profiles)} entities, {len(ids)} vectors from Memgraph")
            
        except Exception as e:
            logger.error(f"Hydration failed: {e}")
            raise
    
    def get_id(self, name: str) -> Optional[int]:
        return self._name_to_id.get(name.lower())
    
    def get_mentions_for_id(self, entity_id: int) -> List[str]:
        with self._lock:
            items = list(self._name_to_id.items())
        return [mention for mention, eid in items if eid == entity_id]
    
    def get_embedding_for_id(self, entity_id: int) -> List[float]:
        """Retrieve embedding from FAISS by ID."""
        with self._lock:
            try:
                embedding = self.index_id_map.reconstruct(entity_id)
                return embedding.tolist()
            except Exception as e:
                logger.warning(f"Could not retrieve embedding for {entity_id}: {e}")
                return []
            
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
    
    def hydrate_messages(self, messages: dict[str, dict]):
        if not messages:
            return
        
        ids, tokens = [], []
        for msg_key, data in messages.items():
            prefix, num = msg_key.split("_")
            num = int(num)
            int_id = num if prefix == "msg" else num + 1_000_000
            
            text = data.get("message") or data.get("content", "")
            self.msg_int_to_id[int_id] = msg_key
            ids.append(int_id)
            tokens.append(text.lower().split())
            self.msg_id_order.append(msg_key)
        
        texts = [data.get("message") or data.get("content", "") for data in messages.values()]
        embs = self.embedding_model.encode(texts).astype(np.float32)
        faiss.normalize_L2(embs)
        self.msg_index.add_with_ids(embs, np.array(ids, dtype=np.int64))
        

        self.msg_corpus.extend(tokens)
        self.bm25_index = BM25Okapi(self.msg_corpus)

    
    def add_message(self, msg_key: str, text: str):
        prefix, num = msg_key.split("_")
        num = int(num)
        
        # hacky solution: offset to 1 mill
        int_id = num if prefix == "msg" else num + 1_000_000
        
        self.msg_int_to_id[int_id] = msg_key

        emb = self.embedding_model.encode([text]).astype(np.float32)
        faiss.normalize_L2(emb)
        self.msg_index.add_with_ids(emb, np.array([int_id], dtype=np.int64))
        
        self.msg_corpus.append(text.lower().split())
        self.msg_id_order.append(msg_key)
        self._bm25_dirty = True

    def _search_messages(self, query: str, k: int = 10) -> list[tuple[str, float]]:
        if not self.msg_int_to_id:
            return []
        
        if self._bm25_dirty:
            self.bm25_index = BM25Okapi(self.msg_corpus)
            self._bm25_dirty = False
        
        results = {}
        
        q_emb = self.embedding_model.encode([query]).astype(np.float32)
        faiss.normalize_L2(q_emb)
        sem_scores, sem_ids = self.msg_index.search(q_emb, 50) #hardcoded for convienence
        
        for idx, score in zip(sem_ids[0], sem_scores[0]):
            if idx >= 0:
                if int(idx) not in self.msg_int_to_id:
                    logger.warning(f"FAISS returned ID {idx} not in msg_int_to_id")
                    continue
                msg_key = self.msg_int_to_id[int(idx)]
                results[msg_key] = ("semantic", float(score))
        
        if self.bm25_index:
            tokens = query.lower().split()
            bm25_scores = self.bm25_index.get_scores(tokens)
            top_indices = np.argsort(bm25_scores)[::-1][:75] #hardcoded for convienence
            max_bm25 = max(bm25_scores) if max(bm25_scores) > 0 else 1.0
            
            for idx in top_indices:
                if bm25_scores[idx] > 0:
                    msg_key = self.msg_id_order[idx]
                    norm_score = bm25_scores[idx] / max_bm25
                    if msg_key in results:
                        _, sem_score = results[msg_key]
                        results[msg_key] = ("both", sem_score + norm_score)
                    else:
                        results[msg_key] = ("keyword", norm_score)
        
        if not results:
            return []
        
        if len(results) > 1:
            candidate_keys = list(results.keys())[:45] #hardcoded for convienence
            pairs = []
            for msg_key in candidate_keys:
                msg_id = int(msg_key.split("_")[1]) 
                text = self.store.get_message_text(msg_id)
                pairs.append((query, text))
            scores = self.cross_encoder.predict(pairs)
            reranked = list(zip(candidate_keys, scores))
            reranked.sort(key=lambda x: x[1], reverse=True)
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
        
        if mention_lower in self._name_to_id:
            candidates.add(self._name_to_id[mention_lower])
        
        for alias, eid in self._name_to_id.items():
            if fuzz.WRatio(mention_lower, alias) >= fuzzy_threshold:
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
        profile = {
            "canonical_name": canonical_name,
            "type": entity_type,
            "topic": topic,
            "facts": [],
            "session_id": session_id or self.session_id
        }
        
        embedding = self.add_entity(entity_id, profile)
        
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

    def add_entity(self, entity_id: int, profile: Dict) -> List[float]:

        canonical_name = profile.get("canonical_name", "")
        facts = profile.get("facts", [])

        resolution_text = f"{canonical_name}. " + " ".join(facts)
        embedding_np = self.embedding_model.encode([resolution_text]).astype(np.float32)[0]
        faiss.normalize_L2(embedding_np.reshape(1, -1))

        with self._lock:

            #TODO: eventually need to make a better LRU system
            if len(self.entity_profiles) >= 10000:
                oldest_id = next(iter(self.entity_profiles))
                del self.entity_profiles[oldest_id]
                
            logger.info(f"Adding entity {entity_id}-{profile['canonical_name']} to resolver indexes.")

            profile.setdefault("topic", "General")
            profile.setdefault("facts", [])
            profile.setdefault("first_seen", datetime.now(timezone.utc).isoformat())
            profile["last_seen"] = datetime.now(timezone.utc).isoformat()
            
            self.index_id_map.add_with_ids(
                np.array([embedding_np]), 
                np.array([entity_id], dtype=np.int64)
            )

            self.entity_profiles[entity_id] = profile
        
        return embedding_np.tolist()
    

    def update_profile_embedding(self, entity_id: int, resolution_text: str) -> List[float]:
        """
        Update entity facts and recompute embedding.
        Returns new embedding.
        """
        with self._lock:
            profile = self.entity_profiles.get(entity_id)
            if not profile:
                logger.warning(f"Cannot update profile for unknown entity {entity_id}")
                return []
            
            profile["last_seen"] = datetime.now(timezone.utc).isoformat()
            
            embedding_np = self.embedding_model.encode([resolution_text]).astype(np.float32)[0]
            faiss.normalize_L2(embedding_np.reshape(1, -1))

            self.index_id_map.remove_ids(np.array([entity_id], dtype=np.int64))
            self.index_id_map.add_with_ids(
                np.array([embedding_np]),
                np.array([entity_id], dtype=np.int64)
            )
            
            return embedding_np.tolist()

    def detect_merge_candidates(self) -> list:
        """Detect potential entity merges using name matching and facts similarity."""
        
        logger.info(f"Merge detection started, {len(self.entity_profiles)} entities to scan")

        generic = self._build_generic_tokens()
        logger.debug(f"Generic tokens (10+ entities): {generic}")
        
        candidates = []
        seen_pairs = {}
        aliases = list(self._name_to_id.keys())
        
        for i in range(len(aliases)):
            for j in range(i + 1, len(aliases)):
                id_i = self._name_to_id[aliases[i]]
                id_j = self._name_to_id[aliases[j]]
                if id_i == id_j:
                    continue
                
                score = fuzz.WRatio(aliases[i], aliases[j])
                is_substring = aliases[i] in aliases[j] or aliases[j] in aliases[i]
                
                # Relaxed gate for substring matches
                if is_substring and score >= 75:
                    pass
                elif score >= 85:
                    pass
                else:
                    continue
                
                pair_key = tuple(sorted([id_i, id_j]))
                tokens_i = set(aliases[i].lower().split()) - generic
                tokens_j = set(aliases[j].lower().split()) - generic
                
                if not (tokens_i & tokens_j):
                    logger.debug(f"Skipped ({id_i}, {id_j}): no meaningful token overlap")
                    continue
                
                if pair_key not in seen_pairs or score > seen_pairs[pair_key]:
                    seen_pairs[pair_key] = score

        for (id_a, id_b), fuzz_score in seen_pairs.items():
            if self.store.has_direct_edge(id_a, id_b):
                logger.debug(f"Blocked ({id_a}, {id_b}) | Direct edge exists")
                continue

            if self.store.has_hierarchy_edge(id_a, id_b):
                logger.debug(f"Blocked ({id_a}, {id_b}) | Hierarchy edge exists")
                continue

            profile_a = self.entity_profiles.get(id_a, {})
            profile_b = self.entity_profiles.get(id_b, {})
            type_a = profile_a.get("type")
            type_b = profile_b.get("type")
            topic_a = profile_a.get("topic", "General")
            topic_b = profile_b.get("topic", "General")

            canonical_a = profile_a.get("canonical_name", "").lower()
            canonical_b = profile_b.get("canonical_name", "").lower()
            is_substring = canonical_a in canonical_b or canonical_b in canonical_a

            relationship = "merge"
            parent_id = None
            child_id = None
            
            # Check hierarchy if same topic
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
            
            # Second gate: stricter thresholds before LLM
            if relationship == "merge":                
                if is_substring and fuzz_score >= 75:
                    pass
                elif fuzz_score < 91:
                    continue
            
            if relationship == "hierarchy":
                if is_substring and fuzz_score < 80:
                    continue
                if not is_substring and fuzz_score < 90:
                    continue

            neighbors_a = self.store.get_neighbor_ids(id_a)
            neighbors_b = self.store.get_neighbor_ids(id_b)
            neighbors_a.discard(1)  # user id - hardcoded for now
            neighbors_b.discard(1)
            
            shared_neighbors = neighbors_a & neighbors_b
            if shared_neighbors and relationship == "merge":
                high_confidence = fuzz_score >= 95 and type_a and type_b and type_a == type_b
            
                if not high_confidence:
                    logger.info(f"Blocked ({id_a}, {id_b}) | Shared neighbors: {shared_neighbors} (score={fuzz_score}, types={type_a}/{type_b})")
                    continue
                else:
                    logger.info(f"Passed ({id_a}, {id_b}) | Shared neighbors as supporting evidence (score={fuzz_score}, type={type_a})")
            
            candidates.append({
                "primary_id": id_a,
                "secondary_id": id_b,
                "primary_name": profile_a.get("canonical_name", "Unknown"),
                "secondary_name": profile_b.get("canonical_name", "Unknown"),
                "primary_type": type_a,
                "secondary_type": type_b,
                "primary_session": profile_a.get("session_id"),
                "secondary_session": profile_b.get("session_id"),
                "topic": topic_a,
                "profile_a": {
                    **profile_a,
                    "aliases": self.get_mentions_for_id(id_a)
                },
                "profile_b": {
                    **profile_b,
                    "aliases": self.get_mentions_for_id(id_b)
                },
                "fuzz_score": fuzz_score,
                "shared_neighbor_count": len(shared_neighbors),
                "relationship": relationship,
                "parent_id": parent_id,
                "child_id": child_id,
            })
            
            logger.info(f"Candidate ({id_a}, {id_b}) {profile_a.get('canonical_name')} <-> {profile_b.get('canonical_name')} | score={fuzz_score}")

        merge_count = sum(1 for c in candidates if c["relationship"] == "merge")
        hierarchy_count = sum(1 for c in candidates if c["relationship"] == "hierarchy")
        logger.info(f"Detection complete: {merge_count} merge, {hierarchy_count} hierarchy candidates")
        
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
                try:
                    self.index_id_map.remove_ids(np.array(entity_ids, dtype=np.int64))
                except Exception as e:
                    logger.warning(f"FAISS removal failed: {e}")
        
        if removed > 0:
            logger.info(f"Removed {removed} entities from resolver")
        return removed
    
    def debug_search(self, query: str, target_msg_id: str, k: int = 20):
        """Check if a specific message appears in search results and where."""
        
        # BM25
        tokens = query.lower().split()
        bm25_scores = self.bm25_index.get_scores(tokens)
        bm25_ranked = sorted(enumerate(bm25_scores), key=lambda x: x[1], reverse=True)
        bm25_position = None
        for rank, (idx, score) in enumerate(bm25_ranked):
            if self.msg_id_order[idx] == target_msg_id:
                bm25_position = (rank, score)
                break
        
        # Vector
        q_emb = self.embedding_model.encode([query]).astype(np.float32)
        faiss.normalize_L2(q_emb)
        scores, ids = self.msg_index.search(q_emb, 100)
        
        vector_position = None
        for rank, (idx, score) in enumerate(zip(ids[0], scores[0])):
            if idx >= 0 and self.msg_int_to_id.get(int(idx)) == target_msg_id:
                vector_position = (rank, score)
                break
        
        # Full search result
        results = self._search_messages(query, k)
        final_position = None
        for rank, (msg_id, score) in enumerate(results):
            if msg_id == target_msg_id:
                final_position = (rank, score)
                break
        
        return {
            "query": query,
            "target": target_msg_id,
            "bm25": bm25_position,      # (rank, score) or None
            "vector": vector_position,   # (rank, score) or None
            "final": final_position      # (rank, score) or None
        }