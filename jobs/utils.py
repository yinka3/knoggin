from datetime import datetime, timezone
import re
from typing import List, Optional
from rapidfuzz import fuzz

from schema.dtypes import BatchProfileResponse, ProfileUpdate

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
    Merge extracted facts with existing, handling [UPDATES: ...] flags.
    Returns the new complete fact list.
    """
    if not new_facts:
        return existing_facts
    
    date_str = timestamp or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    
    to_invalidate = set()
    clean_new_facts = []
    
    for fact in new_facts:
        match = re.match(r"(.+?)\s*\[UPDATES:\s*(.+?)\]$", fact)
        
        if match:
            new_fact = match.group(1).strip()
            old_fact_text = match.group(2).strip()
            
            for existing in existing_facts:
                if _facts_match(existing, old_fact_text):
                    to_invalidate.add(existing)
                    break
            
            clean_new_facts.append(new_fact)
        else:
            clean_new_facts.append(fact)
    
    merged = []
    
    for fact in existing_facts:
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
    if not match:
        return None
    
    profiles = []
    for line in match.group(1).strip().split("\n"):
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

