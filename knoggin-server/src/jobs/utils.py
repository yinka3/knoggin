
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from loguru import logger
import numpy as np
from rapidfuzz import fuzz
from sklearn.metrics.pairwise import cosine_similarity as sklearn_cosine_similarity

from db.store import MemGraphStore
from common.schema.dtypes import Fact, FactMergeResult, FactUpdate



def cosine_similarity(vec_a: List[float], vec_b: List[float]) -> float:
    if not vec_a or not vec_b or None in vec_a or None in vec_b:
        return 0.0
    
    a = np.array(vec_a).reshape(1, -1)
    b = np.array(vec_b).reshape(1, -1)
    
    return float(sklearn_cosine_similarity(a, b)[0][0])

def find_duplicate_facts(
    facts_a: List[Fact], 
    facts_b: List[Fact],
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
    existing_facts: List[Fact],
    new_facts: List[FactUpdate]
) -> FactMergeResult:
    """
    Process structured LLM-extracted facts against existing Fact nodes.
    Returns IDs to invalidate and new content strings (FactUpdate objects).
    """
    if not new_facts:
        return FactMergeResult(to_invalidate=[], new_contents=[])

    to_invalidate = []
    # We'll return the FactUpdate objects themselves as they now contain all metadata
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


def extract_fact_with_source(fact_update: FactUpdate) -> Tuple[str, Optional[int]]:
    """
    Helper to extract content and source msg_id from a FactUpdate.
    """
    return fact_update.content, fact_update.msg_id


def _find_matching_fact(text: str, facts: List[Fact], threshold: int = 90) -> Fact | None:
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






def format_recorded_date(recorded: str) -> str:
    """Format ISO timestamp to YYYY-MM-DD, with fallback."""
    if not recorded:
        return "unknown"
    try:
        dt = datetime.fromisoformat(recorded.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except (ValueError, AttributeError):
        return str(recorded)[:10]

def _format_entity_block(ent: Dict[str, Any], label: Optional[str] = None) -> List[str]:
    name = ent.get("canonical_name", ent.get("entity_name", "Unknown"))
    etype = ent.get("type", ent.get("entity_type", "Unknown"))
    
    header = f"### {label}: {name} [{etype}]" if label else f"### {name} [{etype}]"
    output = [header]
    
    aliases = ent.get("aliases", ent.get("known_aliases", []))
    if aliases:
        output.append(f"Aliases: {', '.join(aliases)}")
    else:
        output.append("Aliases: (none)")
        
    facts = ent.get("facts", ent.get("existing_facts", []))
    if facts:
        output.append("Facts:")
        for f in facts:
            content = f.get("content", "")
            recorded = f.get("recorded_at", "")
            source = f.get("source_message")
            
            if recorded:
                recorded_str = format_recorded_date(recorded)
            else:
                recorded_str = "unknown"
            
            source_info = f", source: \"{source}\"" if source else ""
            output.append(f"  - {content} (recorded: {recorded_str}{source_info})")
            
    return output

def format_vp04_input(
    entities: List[Dict],
    conversation_text: str
) -> str:
    """Format prompt for extraction verification phase."""
    lines = []
    lines.append("## Entities")
    
    for ent in entities:
        lines.extend(_format_entity_block(ent))
        lines.append("")
        
    lines.append("## Prior Conversation For Context")
    lines.append(conversation_text)
    
    return "\n".join(lines)


def format_vp05_input(
    entity_a: Dict,
    entity_b: Dict
) -> str:
    """Format prompt for merge profile validation phase."""
    
    output = []
    output.extend(_format_entity_block(entity_a, "Entity A"))
    output.append("")
    output.extend(_format_entity_block(entity_b, "Entity B"))
    
    return "\n".join(output)


async def enrich_facts_with_sources(
    facts: List[Fact], 
    store: MemGraphStore
) -> List[Dict]:
    """Enrich facts with timestamps and source message content."""
    enriched = []
    msg_id_to_indices: Dict[int, List[int]] = {}
    
    for i, fact in enumerate(facts):
        entry = {
            "content": fact.content,
            "recorded_at": fact.valid_at.isoformat() if fact.valid_at else None,
            "source_message": None
        }
        enriched.append(entry)
        
        if fact.source_msg_id:
            if fact.source_msg_id not in msg_id_to_indices:
                msg_id_to_indices[fact.source_msg_id] = []
            msg_id_to_indices[fact.source_msg_id].append(i)
    
    if msg_id_to_indices:
        try:
            messages = await store.get_messages_by_ids(list(msg_id_to_indices.keys()))
            msg_text_map = {m["id"]: m.get("content", "") for m in messages}
            
            for msg_id, indices in msg_id_to_indices.items():
                text = msg_text_map.get(msg_id)
                if text:
                    for idx in indices:
                        enriched[idx]["source_message"] = text
        except Exception as e:
            logger.debug(f"Could not batch fetch source messages: {e}")
    
    return enriched

