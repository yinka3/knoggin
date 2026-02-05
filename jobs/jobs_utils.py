import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import re
from typing import Dict, List, Optional, Tuple
from loguru import logger
import numpy as np
from rapidfuzz import fuzz
from sklearn.metrics.pairwise import cosine_similarity as sklearn_cosine_similarity

from db.store import MemGraphStore
from schema.dtypes import Fact, FactMergeResult, ProfileUpdate



def cosine_similarity(vec_a: List[float], vec_b: List[float]) -> float:
    if not vec_a or not vec_b:
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

        sup_match = re.search(r"^\[SUPERSEDES:\s*(.+?)\]\s*(.+)$", fact_str)
        if sup_match:
            old_text = sup_match.group(1).strip()
            new_text = sup_match.group(2).strip()
            
            new_text_clean = re.sub(r"\s*\[MSG_?\d+\]\s*$", "", new_text).strip()

            if not old_text:
                logger.warning(f"SUPERSEDES with empty target, treating as new fact: {new_text_clean}")
                if not _is_duplicate(new_text_clean, active_facts):
                    new_contents.append(new_text)
                continue
            
            matched_fact = _find_matching_fact(old_text, active_facts)
            if matched_fact:
                to_invalidate.append(matched_fact.id)
                active_facts = [f for f in active_facts if f.id != matched_fact.id]
            else:
                logger.warning(f"SUPERSEDES target not found: '{old_text}' — adding new fact anyway")
            
            content_clean = re.sub(r"\s*\[MSG_?\d+\]\s*$", "", fact_str).strip()
            if not _is_duplicate(content_clean, active_facts):
                new_contents.append(fact_str)
            continue

        inv_match = re.search(r"^\[INVALIDATES:\s*(.+?)\](?:\s*\[MSG_?\d+\])?\s*$", fact_str)
        if inv_match:
            old_text = inv_match.group(1).strip()
            if not old_text:
                logger.warning(f"INVALIDATES with empty target, skipping")
                continue
            matched_fact = _find_matching_fact(old_text, active_facts)
            if matched_fact:
                to_invalidate.append(matched_fact.id)
                active_facts = [f for f in active_facts if f.id != matched_fact.id]
            continue

        content_clean = re.sub(r"\s*\[MSG_?\d+\]\s*$", "", fact_str).strip()
        if not _is_duplicate(content_clean, active_facts):
            new_contents.append(fact_str)
            logger.debug(f"Adding new content of fact: {fact_str}")

    return FactMergeResult(to_invalidate=to_invalidate, new_contents=new_contents)

def extract_fact_with_source(raw_fact: str) -> Tuple[str, Optional[int]]:
    """
    Parse fact string to extract content and source msg_id.
    Returns: (content, msg_id as int or None)
    """
    cleaned = re.sub(r"^\[(?:SUPERSEDES|INVALIDATES):\s*[^\]]+\]\s*", "", raw_fact.strip())
    
    match = re.search(r"^(.*?)\[MSG_?(\d+)\]", cleaned)
    if match:
        content = match.group(1).strip()
        msg_id = int(match.group(2))
        return content, msg_id
    return cleaned, None


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

def parse_new_facts(reasoning: str) -> Optional[List[ProfileUpdate]]:
    """
    Parse <new_facts> block.
    Lenient on closing tag, strict on opening tag.
    """
    if not reasoning:
        return None

    start_match = re.search(r"(?i)<new_facts>", reasoning)
    if not start_match:
        return None
    
    content_start = start_match.end()
    remaining = reasoning[content_start:]
    
    end_match = re.search(r"(?i)</new_facts>", remaining)
    if end_match:
        content = remaining[:end_match.start()].strip()
    else:
        logger.warning("Missing </new_facts> closing tag. Using truncated content.")
        content = remaining.strip()
    
    if not content:
        return None
    
    profiles = []
    for line in content.split("\n"):
        line = line.strip()
        
        if ":" not in line:
            continue
        
        try:
            entity_name, facts_part = line.split(":", 1)
            facts = [f.strip() for f in facts_part.split("|") if f.strip()]
            
            if facts:
                profiles.append(ProfileUpdate(
                    canonical_name=entity_name.strip(),
                    facts=facts
                ))
        except ValueError:
            continue
    
    if not profiles:
        return None
    
    return profiles

def parse_merge_score(reasoning: str) -> Optional[float]:
    """
    Parses <score>0.XX</score> from text. 
    Lenient on closing tag. Strict on numeric bounds.
    """
    if not reasoning:
        return None
        
    match = re.search(r"(?i)<score>\s*(.*?)(?:</score>|$)", reasoning, re.DOTALL)
    
    if not match:
        return None
        
    score_str = match.group(1).strip()
    
    try:
        # sanity check: "0.9.5" is invalid
        if score_str.count('.') > 1:
            return None
            
        score = float(score_str)
        
        if 0.0 <= score <= 1.0:
            return score
            
    except ValueError:
        pass
        
    return None


def format_recorded_date(recorded: str) -> str:
    """Format ISO timestamp to YYYY-MM-DD, with fallback."""
    if not recorded:
        return "unknown"
    try:
        dt = datetime.fromisoformat(recorded.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except (ValueError, AttributeError):
        return str(recorded)[:10]

def format_vp04_input(
    entities: List[Dict],
    conversation_text: str
) -> str:
    """
    Format input for VP-04 (Profile Extraction).
    
    entities: List of {
        "entity_name": str,
        "entity_type": str,
        "existing_facts": List[{"content", "recorded_at", "source_message"}],
        "known_aliases": List[str]
    }
    """
    lines = []
    
    lines.append("## Entities")
    
    for ent in entities:
        name = ent.get("entity_name", "Unknown")
        etype = ent.get("entity_type", "unknown")
        aliases = ent.get("known_aliases", [])
        facts = ent.get("existing_facts", [])
        
        lines.append(f"\n### {name} [{etype}]")
        
        if aliases:
            lines.append(f"Aliases: {', '.join(aliases)}")
        
        if facts:
            lines.append("Existing Facts:")
            for f in facts:
                content = f.get("content", "")
                recorded = f.get("recorded_at", "")
                source = f.get("source_message")
                
                if recorded:
                    recorded_str = format_recorded_date(f.get("recorded_at", ""))
                else:
                    recorded_str = "unknown"
                
                if source:
                    lines.append(f"  - {content} (recorded: {recorded_str}, source: \"{source}\")")
                else:
                    lines.append(f"  - {content} (recorded: {recorded_str})")
        else:
            lines.append("Existing Facts: (none)")
    
    lines.append("\n## Prior Conversation For Context")
    lines.append(conversation_text)
    
    return "\n".join(lines)


def format_vp05_input(
    entity_a: Dict,
    entity_b: Dict
) -> str:
    """
    Format input for VP-05 (Merge Judgment).
    
    entity: {
        "canonical_name": str,
        "type": str,
        "aliases": List[str],
        "facts": List[{"content", "recorded_at", "source_message"}]
    }
    """
    def _format_entity(ent: Dict, label: str) -> List[str]:
        lines = []
        name = ent.get("canonical_name", "Unknown")
        etype = ent.get("type", "unknown")
        aliases = ent.get("aliases", [])
        facts = ent.get("facts", [])
        
        lines.append(f"## {name} [{etype}]")
        
        if aliases:
            lines.append(f"Aliases: {', '.join(aliases)}")
        else:
            lines.append("Aliases: (none)")
        
        if facts:
            lines.append("Facts:")
            for f in facts:
                content = f.get("content", "")
                recorded = f.get("recorded_at", "")
                source = f.get("source_message")
                
                if recorded:
                   recorded_str = format_recorded_date(f.get("recorded_at", ""))
                else:
                    recorded_str = "unknown"
                
                if source:
                    lines.append(f"  - {content} (recorded: {recorded_str}, source: \"{source}\")")
                else:
                    lines.append(f"  - {content} (recorded: {recorded_str})")
        else:
            lines.append("Facts: (none)")
        
        return lines
    
    output = []
    output.extend(_format_entity(entity_a, "Entity A"))
    output.append("")
    output.extend(_format_entity(entity_b, "Entity B"))
    
    return "\n".join(output)


async def enrich_facts_with_sources(
    facts: List[Fact], 
    store: MemGraphStore
) -> List[Dict]:
    """Enrich facts with timestamps and source message content."""
    loop = asyncio.get_running_loop()
    enriched = []
    msg_id_to_indices = {}
    
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
        sem = asyncio.Semaphore(2)
        
        async def fetch_one(msg_id):
            async with sem:
                try:
                    return msg_id, await loop.run_in_executor(
                        None, store.get_message_text, msg_id
                    )
                except Exception as e:
                    logger.debug(f"Could not fetch source for msg {msg_id}: {e}")
                    return msg_id, None
        
        results = await asyncio.gather(*[fetch_one(mid) for mid in msg_id_to_indices])
        
        for msg_id, text in results:
            if text:
                for idx in msg_id_to_indices[msg_id]:
                    enriched[idx]["source_message"] = text
    
    return enriched