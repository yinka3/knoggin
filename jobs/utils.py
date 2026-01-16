from datetime import datetime, timezone
import re
from typing import List, Optional
from loguru import logger
import numpy as np
from rapidfuzz import fuzz

from schema.dtypes import BatchProfileResponse, FactMergeResult, ProfileUpdate, Fact


def cosine_similarity(vec_a: List[float], vec_b: List[float]) -> float:
    if not vec_a or not vec_b:
        return 0.0
    a = np.array(vec_a)
    b = np.array(vec_b)
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))

def find_duplicate_facts(
    facts_a: List[Fact], 
    facts_b: List[Fact],
    threshold: float = 0.85
) -> List[str]:
    """
    Find facts in B that are semantic duplicates of facts in A.
    Returns fact IDs from B to invalidate after merge.
    """
    if not facts_a or not facts_b:
        return []
    
    active_a = [f for f in facts_a if f.invalid_at is None]
    active_b = [f for f in facts_b if f.invalid_at is None]
    
    if not active_a or not active_b:
        return []
    
    to_invalidate = []
    
    emb_a = np.array([f.embedding for f in active_a])
    emb_b = np.array([f.embedding for f in active_b])
    
    emb_a = emb_a / np.linalg.norm(emb_a, axis=1, keepdims=True)
    emb_b = emb_b / np.linalg.norm(emb_b, axis=1, keepdims=True)
    similarity_matrix = emb_b @ emb_a.T
    
    for i, fact_b in enumerate(active_b):
        max_sim = similarity_matrix[i].max()
        if max_sim >= threshold:
            to_invalidate.append(fact_b.id)
            logger.debug(f"Duplicate fact: '{fact_b.content[:50]}...' (sim={max_sim:.3f})")
    
    return to_invalidate


def has_sufficient_facts(candidate: dict, min_facts: int = 2) -> bool:
    facts_a = candidate["profile_a"].get("facts", [])
    facts_b = candidate["profile_b"].get("facts", [])
    return len(facts_a) >= min_facts and len(facts_b) >= min_facts


def process_extracted_facts(
    existing_facts: List[Fact],
    new_facts: List[str]
) -> FactMergeResult:
    """
    Process LLM-extracted facts against existing Fact nodes.
    Returns IDs to invalidate and new content strings to create.
    """
    if not new_facts:
        return FactMergeResult(to_invalidate=[], new_contents=[])

    to_invalidate = []
    new_contents = []

    active_facts = [f for f in existing_facts if f.invalid_at is None]

    for fact_str in new_facts:
        fact_str = fact_str.strip()

        inv_match = re.match(r"^\[INVALIDATES:\s*(.+?)\]$", fact_str)
        if inv_match:
            old_text = inv_match.group(1).strip()
            matched_fact = _find_matching_fact(old_text, active_facts)
            if matched_fact:
                to_invalidate.append(matched_fact.id)
            continue

        if not _is_duplicate(fact_str, active_facts):
            new_contents.append(fact_str)

    return FactMergeResult(to_invalidate=to_invalidate, new_contents=new_contents)



def _find_matching_fact(text: str, facts: List[Fact], threshold: int = 92) -> Fact | None:
    """Find existing fact matching the text via fuzzy match."""
    text_lower = text.lower().strip()
    
    for fact in facts:
        if fuzz.ratio(text_lower, fact.content.lower().strip()) > threshold:
            return fact
    
    return None


def _is_duplicate(content: str, facts: List[Fact]) -> bool:
    """Check if content already exists in facts (exact match)."""
    content_lower = content.lower().strip()
    return any(f.content.lower().strip() == content_lower for f in facts)

def parse_new_facts(reasoning: str) -> Optional[BatchProfileResponse]:
    """Parse <new_facts> block and validate via Pydantic."""
    match = re.search(r"<new_facts>(.*?)</new_facts>", reasoning, re.DOTALL)
    
    if match:
        content = match.group(1).strip()
    else:
        lines = []
        for line in reasoning.strip().split("\n"):
            if ":" in line and "|" in line:
                lines.append(line.strip())
        content = "\n".join(lines)
    
    if not content:
        return None
    
    profiles = []
    for line in content.split("\n"):
        if ":" not in line:
            continue
        entity_name, facts_part = line.split(":", 1)
        facts = [f.strip() for f in facts_part.split("|") if f.strip()]
        if facts:
            profiles.append(ProfileUpdate(
                canonical_name=entity_name.strip(),
                facts=facts
            ))
    
    if not profiles:
        return None
    
    return BatchProfileResponse(profiles=profiles)

