import asyncio
import re
from typing import List, Optional, Tuple

from loguru import logger
from typing import Dict

from wordfreq import word_frequency
from shared.topics_config import TopicConfig
from spacy.lang.en.stop_words import STOP_WORDS as SPACY_STOPS
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS as SKLEARN_STOPS

from shared.schema.dtypes import EntityItem, EntityPair, MessageConnections


PRONOUNS = {
        "my", "his", "her", "their", "our", "your", "its",
        "he", "she", "they", "we", "i", "me", "him", "them", "us",
        "this", "that", "these", "those"
    }

STOP_WORDS = SPACY_STOPS | SKLEARN_STOPS

def handle_background_task_result(task: asyncio.Task):
    """Log any unhandled exceptions from background tasks."""
    if task.cancelled():
        return
    if exc := task.exception():
        logger.error(f"Background task failed: {exc}")


def is_substring_match(name_a: str, name_b: str) -> bool:
    """Case-insensitive substring check."""
    a, b = name_a.lower(), name_b.lower()
    return a in b or b in a

def is_generic_phrase(text: str, threshold: float = 5e-6) -> bool:
    """
    Returns True if phrase is generic (should filter).
    - Any rare word (< threshold) → pass (likely proper noun)
    - Single common word → filter
    - Multi-word all common → sum and check scaled threshold
    """
    words = text.lower().split()
    freqs = [word_frequency(w, 'en') for w in words]
    
    # Any rare word = likely name/proper noun → pass
    if any(f < threshold for f in freqs):
        return False
    
    # Single common word → filter
    if len(words) == 1:
        return True
    
    # Multi-word, all common: sum frequencies
    total = sum(freqs)
    return total > threshold * 100

def is_covered(candidate: str, covered_texts: set[str]) -> bool:
    """
    Check if candidate span is already covered by known entities.
    Uses text comparison, not index comparison.
    """
    candidate_lower = candidate.lower().strip()
    
    for covered in covered_texts:
        if candidate_lower == covered:
            return True
        if candidate_lower in covered:
            return True
        if covered in candidate_lower:
            return True
    
    return False

def extract_xml_content(text: str, tag: str) -> Optional[str]:
    """
    Lenient XML Extraction.
    1. MUST have <tag>.
    2. captures everything after <tag>.
    3. If </tag> exists, stops there. If not, takes to end of string.
    """
    if not text:
        return None
    
    start_match = re.search(f"<{tag}>", text, re.IGNORECASE)
    if not start_match:
        return None
    
    content_start = start_match.end()
    remaining_text = text[content_start:]
    
    end_match = re.search(f"</{tag}>", remaining_text, re.IGNORECASE)
    
    if end_match:
        return remaining_text[:end_match.start()].strip()
    else:
        logger.warning(f"Missing closing </{tag}> tag. Parsing available content.")
        return remaining_text.strip()


def validate_entity(name: str, topic: str, topic_config: TopicConfig) -> bool:
    """Filter invalid mentions before resolution."""
    
    if not name or len(name) < 2:
        return False
    
    if len(name) > 100:
        return False
    
    if name.lower() in STOP_WORDS:
        return False
    
    if name.lower() in PRONOUNS:
        return False
    
    if is_generic_phrase(name):
        return False
    
    if not any(c.isalpha() for c in name):
        return False
    
    if topic and topic != "General":
        normalized = topic_config.normalize_topic(topic)
        if normalized == "General" and topic.lower() not in topic_config.alias_lookup:
            logger.debug(f"Invalid topic '{topic}' for entity '{name}'")
            return False
    
    return True


def format_vp01_input(
    messages: List[Dict],
    known_ents: List[Tuple[str, int]],
    gliner_ents: List[Tuple[int, str, str]],
    ambiguous: List[Tuple[int, str, str, List[str]]],
    covered_texts: Dict[int, set],
    label_block: str
) -> str:
    
    lines = []
    lines.append("## Label Schema\n")
    lines.append(label_block)

    lines.append("\n## Messages\n")
    for msg in messages:
        label = msg.get("role_label")
        if not label:
            label = "USER" if msg.get("role") == "user" else "AGENT"
            
        content = msg.get("message") or msg.get("content") or ""
        lines.append(f"[MSG {msg['id']}] [{label}]: \"{content}\"")
    
    lines.append("\n## Known Entities (from graph - do not override)\n")
    if known_ents:
        for span_text, eid in known_ents:
            lines.append(f"- \"{span_text}\" -> entity_id={eid}")
    else:
        lines.append("(none)")
    
    lines.append("\n## GLiNER Extractions (can override if wrong)\n")
    gliner_resolved = []
    for msg_id, span, label in gliner_ents:
        if span.lower() in covered_texts.get(msg_id, set()):
            if not any(span == a[1] for a in ambiguous):
                gliner_resolved.append((msg_id, span, label))
    
    if gliner_resolved:
        for msg_id, span, label in gliner_resolved:
            lines.append(f"- MSG {msg_id}: \"{span}\" -> {label}")
    else:
        lines.append("(none)")
    
    if ambiguous:
        lines.append("\n## Ambiguous (Task 1: assign topic)")
        for msg_id, span_text, label, topics in ambiguous:
            lines.append(f"- MSG {msg_id}: \"{span_text}\" ({label}) -> choose from: {topics}")
    
    lines.append("\n## Discovery (Task 2: find missed entities)")
    lines.append("Scan messages above for proper nouns not listed in Already Resolved.")
    lines.append("Include the MSG id where you found each entity.")
    
    return "\n".join(lines)

def parse_entities(reasoning: str, min_confidence: float = 0.8, topic_config: TopicConfig = None) -> Optional[List[EntityItem]]:
    """Parse <entities> block from VP-01 output. Expects: msg_id | name | label | topic | confidence
    Falls back to: msg_id | name | label | confidence (topic defaults to General)"""
    content = extract_xml_content(reasoning, "entities")
    
    if not content:
        return None
    
    entities = []
    malformed = 0
    
    for line in content.split("\n"):
        line = line.strip()
        if not line or line.lower().startswith("msg_id"):
            continue
        
        parts = line.split("|")
        
        if len(parts) == 5:
            msg_id_str, name, label, topic, conf_str = [p.strip() for p in parts]
        elif len(parts) == 4:
            msg_id_str, name, field3, conf_str = [p.strip() for p in parts]
            
            if topic_config and field3.lower() in topic_config.alias_lookup:
                label = ""
                topic = field3
                logger.debug(f"VP-01 missing label, inferred topic from field3: {name} -> {topic}")
            else:
                label = field3
                topic = "General"
                logger.warning(f"VP-01 missing topic field, defaulting to General: {name}")
        else:
            malformed += 1
            continue
        
        if not name:
            malformed += 1
            continue
        
        try:
            msg_id = int(msg_id_str.replace("MSG", "").replace("msg", "").strip())
            confidence = float(conf_str)
        except ValueError:
            malformed += 1
            continue
        
        if confidence < min_confidence:
            continue
        
        entities.append(EntityItem(
            msg_id=msg_id,
            name=name,
            label=label,
            topic=topic,
            confidence=confidence
        ))
    
    if malformed > 0:
        logger.warning(f"VP-01 malformed lines: {malformed}")
    
    if not entities:
        return None
    
    return entities

def format_vp03_input(
    candidates: List[Dict],
    messages: List[Dict],
    session_context: str
) -> str:
    lines = []
    
    lines.append("## Candidate Entities")
    if candidates:
        for c in candidates:
            msg_ids = c.get('source_msgs', [])
            if msg_ids:
                source = f" (from MSG {', '.join(str(m) for m in msg_ids)})"
            else:
                source = ""
            lines.append(f"{c['canonical_name']} [{c['type']}]{source}")
            if c.get('mentions'):
                lines.append(f"  Mentions: {', '.join(c['mentions'])}")
    else:
        lines.append("(none)")
    
    lines.append("\n## Messages")
    if messages:
        for msg in messages:
            label = msg.get("role_label")
            if not label:
                label = "USER" if msg.get("role") == "user" else "AGENT"
            
            content = msg.get("message") or msg.get("content") or msg.get("text") or ""
            lines.append(f"[MSG {msg['id']}] [{label}]: \"{content}\"")
    else:
        lines.append("(none)")
    
    lines.append("\n## Session Context (for pronoun resolution only)")
    if session_context:
        lines.append(session_context)
    else:
        lines.append("(none)")
    
    return "\n".join(lines)

def parse_connection_response(text: str) -> List[MessageConnections]:
    """
    Parses <connections> block.
    Format: MSG <id> | entity_a; entity_b | confidence | reason
    """
    content = extract_xml_content(text, "connections")
    if not content:
        return []
    
    connections = []
    skipped = 0
    for line in content.split('\n'):
        line = line.strip()
        if not line:
            continue
            
        parts = [p.strip() for p in line.split('|')]
        
        if len(parts) < 2:
            skipped += 1
            continue
            
        msg_match = re.search(r"MSG\s+(\d+)", parts[0], re.IGNORECASE)
        if not msg_match:
            skipped += 1
            continue
        msg_id = msg_match.group(1)
        
        mid_part = parts[1].strip()
        if mid_part.upper() == "NO CONNECTIONS":
            continue
            
        if len(parts) < 4:
            continue
        
        try:
            confidence = float(parts[2].strip())
            confidence = max(0.0, min(1.0, confidence))
        except ValueError:
            confidence = 0.8
        
        reason = parts[3].strip()
            
        if ";" in mid_part:
            ents = [e.strip() for e in mid_part.split(';')]
            if len(ents) >= 2:
                connections.append({
                    "msg_id": msg_id,
                    "entity_a": ents[0],
                    "entity_b": ents[1],
                    "confidence": confidence,
                    "reason": reason
                })
    
    if skipped > 0:
        logger.debug(f"parse_connection_response: skipped {skipped} malformed lines")

    return build_connection_response(connections)


def build_connection_response(parsed: List[dict]) -> List[MessageConnections]:
    from collections import defaultdict
    
    grouped = defaultdict(list)
    for item in parsed:
        grouped[item["msg_id"]].append(
            EntityPair(
                entity_a=item["entity_a"],
                entity_b=item["entity_b"],
                confidence=item["confidence"],
                context=item.get("reason")
            )
        )
    
    message_results = [
        MessageConnections(message_id=int(msg_id), entity_pairs=pairs)
        for msg_id, pairs in grouped.items()
    ]
    
    return message_results



