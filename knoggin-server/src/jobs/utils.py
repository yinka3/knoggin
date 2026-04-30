
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from loguru import logger
import numpy as np
from rapidfuzz import fuzz
from sklearn.metrics.pairwise import cosine_similarity as sklearn_cosine_similarity

from db.store import MemGraphStore
from common.schema.dtypes import Fact, FactRecord, FactMergeResult



def cosine_similarity(vec_a: List[float], vec_b: List[float]) -> float:
    if not vec_a or not vec_b or None in vec_a or None in vec_b:
        return 0.0
    
    a = np.array(vec_a).reshape(1, -1)
    b = np.array(vec_b).reshape(1, -1)
    
    return float(sklearn_cosine_similarity(a, b)[0][0])

def find_duplicate_facts(
    facts_a: List[FactRecord], 
    facts_b: List[FactRecord],
    threshold: float = 0.96
) -> List[str]:
    """
    Find facts in B that are semantic duplicates of facts in A.
    Returns fact IDs from B to invalidate after merge.
    """
    if not facts_a or not facts_b:
        return []
    
    active_a = [f for f in facts_a if f.invalid_at is None and f.embedding]
    active_b = [f for f in facts_b if f.invalid_at is None and f.embedding]
    
    if not active_a or not active_b:
        return []
    
    emb_a = [f.embedding for f in active_a]
    emb_b = [f.embedding for f in active_b]
    
    similarity_matrix = sklearn_cosine_similarity(emb_b, emb_a)
    
    to_invalidate = []
    
    for i, fact_b in enumerate(active_b):
        max_sim = similarity_matrix[i].max()
        
        if max_sim >= threshold:
            to_invalidate.append(fact_b.id)
            logger.info(f"Marked duplicate fact for invalidation: '{fact_b.content[:50]}...' (sim={max_sim:.3f})")
    
    return to_invalidate


def has_sufficient_facts(candidate: dict, min_facts: int = 1) -> bool:
    facts_a = candidate.get("facts_a", [])
    facts_b = candidate.get("facts_b", [])
    return len(facts_a) >= min_facts and len(facts_b) >= min_facts


def process_extracted_facts(
    existing_facts: List[FactRecord],
    new_facts: List[Fact]
) -> FactMergeResult:
    """
    Process structured LLM-extracted facts against existing FactRecord nodes.
    Returns IDs to invalidate and new content strings (Fact objects).
    """
    if not new_facts:
        return FactMergeResult(to_invalidate=[], new_contents=[])

    to_invalidate = []
    # We'll return the Fact objects themselves as they now contain all metadata
    updates_to_keep = []

    active_facts = [f for f in existing_facts if f.invalid_at is None]

    for fact_update in new_facts:
        content = fact_update.content.strip()
        
        # Handle supersedes
        if fact_update.supersedes:
            old_text = fact_update.supersedes.strip()
            matched_fact = _find_matching_fact(old_text, active_facts)
            if matched_fact:
                to_invalidate.append(matched_fact.id)
                active_facts = [f for f in active_facts if f.id != matched_fact.id]
            else:
                logger.warning(f"SUPERSEDES target not found: '{old_text}'")
            
            if not _is_duplicate(content, active_facts):
                updates_to_keep.append(fact_update)
            continue

        # Handle invalidates
        if fact_update.invalidates:
            old_text = fact_update.invalidates.strip()
            matched_fact = _find_matching_fact(old_text, active_facts)
            if matched_fact:
                to_invalidate.append(matched_fact.id)
                active_facts = [f for f in active_facts if f.id != matched_fact.id]
            continue

        # Normal new fact
        if not _is_duplicate(content, active_facts):
            updates_to_keep.append(fact_update)
            logger.debug(f"Adding new fact: {content}")

    return FactMergeResult(to_invalidate=to_invalidate, new_contents=updates_to_keep)


def extract_fact_with_source(fact_update: Fact) -> Tuple[str, Optional[int]]:
    """
    Helper to extract content and source msg_id from a Fact.
    """
    return fact_update.content, fact_update.source_msg_id


def _find_matching_fact(text: str, facts: List[FactRecord], threshold: int = 90) -> FactRecord | None:
    """Find existing fact matching the text via fuzzy match."""
    text_lower = text.lower().strip()
    
    for fact in facts:
        if fuzz.ratio(text_lower, fact.content.lower().strip()) > threshold:
            return fact
    
    return None

def _is_duplicate(content: str, facts: List[FactRecord]) -> bool:
    """Check if content already exists in facts (exact match)."""
    content_lower = content.lower().strip()
    return any(f.content.lower().strip() == content_lower for f in facts)






def format_recorded_date(recorded: str) -> str:
    """Format ISO timestamp to YYYY-MM-DD, with fallback."""
    if not recorded:
        return "unknown"
    try:
        dt = datetime.fromisoformat(recorded.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except (ValueError, AttributeError):
        return str(recorded)[:10]

