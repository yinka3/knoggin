from datetime import datetime, timezone
import re
from typing import List, Optional
import numpy as np
from rapidfuzz import fuzz

from schema.dtypes import BatchProfileResponse, ProfileUpdate


def cosine_similarity(vec_a: List[float], vec_b: List[float]) -> float:
    if not vec_a or not vec_b:
        return 0.0
    a = np.array(vec_a)
    b = np.array(vec_b)
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))


def has_sufficient_facts(candidate: dict, min_facts: int = 2) -> bool:
    facts_a = candidate["profile_a"].get("facts", [])
    facts_b = candidate["profile_b"].get("facts", [])
    return len(facts_a) >= min_facts and len(facts_b) >= min_facts

def _facts_match(existing: str, reference: str) -> bool:
    """Check if existing fact matches the reference from [UPDATES: ...]."""
    existing_clean = re.sub(r"\s*\[INVALIDATED:.*?\]$", "", existing).strip().lower()
    reference_clean = reference.strip().lower()
    
    if existing_clean == reference_clean:
        return True
    
    return fuzz.ratio(existing_clean, reference_clean) > 92


def process_extracted_facts(
    existing_facts: List[str], 
    new_facts: List[str],
    timestamp: str = None
) -> List[str]:
    """
    Merge extracted facts with existing, handling INVALIDATES and SPECIFIES.
    """
    if not new_facts:
        return existing_facts
    
    date_str = timestamp or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    
    to_invalidate = set()
    to_remove = set()
    clean_new_facts = []
    
    for fact in new_facts:
        inv_match = re.match(r"^\[INVALIDATES:\s*(.+?)\]$", fact.strip())
        if inv_match:
            old_fact_text = inv_match.group(1).strip()
            for existing in existing_facts:
                if _facts_match(existing, old_fact_text):
                    to_invalidate.add(existing)
                    break
            continue
        
        spec_match = re.match(r"(.+?)\s*\[SPECIFIES:\s*(.+?)\]$", fact.strip())
        if spec_match:
            new_fact = spec_match.group(1).strip()
            old_fact_text = spec_match.group(2).strip()
            for existing in existing_facts:
                if _facts_match(existing, old_fact_text):
                    to_remove.add(existing)
                    break
            clean_new_facts.append(new_fact)
            continue
        
        clean_new_facts.append(fact.strip())
    
    merged = []
    for fact in existing_facts:
        if fact in to_remove:
            continue
        if fact in to_invalidate:
            if "[INVALIDATED:" not in fact:
                merged.append(f"{fact} [INVALIDATED: {date_str}]")
            else:
                merged.append(fact)
        else:
            merged.append(fact)
    
    existing_lower = {f.lower().strip() for f in merged}
    for fact in clean_new_facts:
        if fact.lower().strip() not in existing_lower:
            merged.append(fact)
            existing_lower.add(fact.lower().strip())
    
    return merged


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

