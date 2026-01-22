import re
from typing import List, Optional, Tuple

from loguru import logger
from typing import Dict

from wordfreq import word_frequency
from main.topics_config import TopicConfig
from schema.dtypes import ConnectionExtractionResponse, DisambiguationResult, EntityItem, EntityPair, ExtractionResponse, MessageConnections, ResolutionEntry
from spacy.lang.en.stop_words import STOP_WORDS as SPACY_STOPS
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS as SKLEARN_STOPS

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
    
    start_match = re.search(f"(?i)<{tag}>", text, re.IGNORECASE)
    if not start_match:
        return None
    
    content_start = start_match.end()
    remaining_text = text[content_start:]
    
    end_match = re.search(f"(?i)</{tag}>", remaining_text)
    
    if end_match:
        return remaining_text[:end_match.start()].strip()
    else:
        logger.warning(f"Missing closing </{tag}> tag. Parsing available content.")
        return remaining_text.strip()


def validate_entity(name: str, topic: str, topic_config: TopicConfig) -> bool:
    """Filter garbage before it reaches VP-02."""

    STOP_WORDS = SPACY_STOPS | SKLEARN_STOPS
    if not name or len(name) < 2:
        return False
    
    if len(name) > 100:
        return False
    
    if name.lower() in STOP_WORDS:
        return False
    
    if not any(c.isalpha() for c in name):
        return False
    
    if topic not in topic_config.raw and topic != "General":
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
    
    lines.append("## Label Schema")
    lines.append(label_block)

    lines = ["## Messages"]
    for msg in messages:
        lines.append(f"[MSG {msg['id']}]: \"{msg['message']}\"")
    
    lines.append("\n## Known Entities (from graph - do not override)")
    if known_ents:
        for span_text, eid in known_ents:
            lines.append(f"- \"{span_text}\" -> entity_id={eid}")
    else:
        lines.append("(none)")
    
    lines.append("\n## GLiNER Extractions (can override if wrong)")
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
        for span_text, label, topics in ambiguous:
            lines.append(f"- \"{span_text}\" ({label}) -> choose from: {topics}")
    
    lines.append("\n## Discovery (Task 2: find missed entities)")
    lines.append("Scan messages above for proper nouns not listed in Already Resolved.")
    lines.append("Include the MSG id where you found each entity.")
    
    return "\n".join(lines)

def parse_entities(reasoning: str, min_confidence: float = 0.8) -> Optional[ExtractionResponse]:
    """Parse <entities> block from VP-01 output. Expects: msg_id | name | label | topic | confidence"""
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
        if len(parts) != 5:
            malformed += 1
            continue
        
        msg_id_str, name, label, topic, conf_str = [p.strip() for p in parts]
        
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
    
    return ExtractionResponse(entities=entities)

def format_vp02_input(
    known_entities: List[Dict],
    mentions: List[Tuple[int, str, str, str]],
    messages: List[Dict],
    session_context: str
) -> str:
    lines = []
    
    if known_entities:
        lines.append("## Known Entities")
        for ent in known_entities:
            lines.append(f"{ent['canonical_name']}")
            if ent.get('facts'):
                lines.append(f"  Facts: {' | '.join(ent['facts'])}")
            if ent.get('connected_to'):
                lines.append(f"  Connected to: {', '.join(ent['connected_to'])}")
    else:
        lines.append("(none)")
    
    if mentions:
        lines.append("\n## Mentions")
        for msg_id, name, typ, topic in mentions:
            lines.append(f"MSG {msg_id} | {name} | {typ} | {topic}")
    
    if messages:
        lines.append("\n## Messages")
        for msg in messages:
            lines.append(f"[MSG {msg['id']}]: \"{msg['text']}\"")
    
    if session_context:
        lines.append("\n## Session Context")
        lines.append(session_context)
    
    return "\n".join(lines)

def parse_disambiguation(
    reasoning: str, 
    mentions: List[Tuple[str, str, str]]
) -> DisambiguationResult:
    """
    Parse VP-02 <resolution> block into DisambiguationResult.
    """
    content = extract_xml_content(reasoning, "resolution")
    if not content:
        return DisambiguationResult(entries=[])
    
    logger.debug(f"Disambiguation content:\n{content}")
    
    # Build lookup: (msg_id, name_lower) -> (type, topic)
    mention_lookup = {
        (m[0], m[1].lower()): (m[2], m[3]) 
        for m in mentions
    }
    
    entries = []
    for line in content.split("\n"):
        line = line.strip()
        if not line or "|" not in line:
            continue
        
        parts = [p.strip() for p in line.split("|")]
        verdict = parts[0].upper()
        
        if verdict == "EXISTING" and len(parts) >= 3:
            canonical = parts[1]
            mention_raw = parts[2]
            mention, msg_id = _parse_mention_with_msg_id(mention_raw)
            typ, topic = mention_lookup.get((msg_id, mention.lower()), ("unknown", "General"))
            
            entries.append(ResolutionEntry(
                verdict="EXISTING",
                canonical_name=canonical,
                mentions=[mention],
                entity_type=typ,
                topic=topic,
                msg_ids=[msg_id] if msg_id else []
            ))
        
        elif verdict == "NEW_GROUP" and len(parts) >= 2:
            mention_list = []
            msg_ids = []
            first_type, first_topic = None, None
            
            for raw in [m.strip() for m in parts[1].split(",")]:
                mention, msg_id = _parse_mention_with_msg_id(raw)
                mention_list.append(mention)
                if msg_id:
                    msg_ids.append(msg_id)
                if first_type is None:
                    first_type, first_topic = mention_lookup.get((msg_id, mention.lower()), ("unknown", "General"))
            
            entries.append(ResolutionEntry(
                verdict="NEW_GROUP",
                canonical_name=max(mention_list, key=lambda m: (len(m), m)),
                mentions=mention_list,
                entity_type=first_type or "unknown",
                topic=first_topic or "General",
                msg_ids=msg_ids
            ))
        
        elif verdict == "NEW_SINGLE" and len(parts) >= 2:
            mention_raw = parts[1]
            mention, msg_id = _parse_mention_with_msg_id(mention_raw)
            typ, topic = mention_lookup.get((msg_id, mention.lower()), ("unknown", "General"))
            
            entries.append(ResolutionEntry(
                verdict="NEW_SINGLE",
                canonical_name=mention,
                mentions=[mention],
                entity_type=typ,
                topic=topic,
                msg_ids=[msg_id] if msg_id else []
            ))
    
    return DisambiguationResult(entries=entries)

def _parse_mention_with_msg_id(raw: str) -> Tuple[str, int]:
    """
    Parse 'X (MSG_3)' -> ('X', 3)
    Falls back to msg_id=0 if not found.
    """
    match = re.search(r"^(.+?)\s*\(MSG[_]?(\d+)\)$", raw.strip(), re.IGNORECASE)
    if match:
        return match.group(1).strip(), int(match.group(2))
    
    # Fallback: no msg_id in output
    return raw.strip(), 0

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
            lines.append(f"[MSG {msg['id']}]: \"{msg['message']}\"")
    else:
        lines.append("(none)")
    
    lines.append("\n## Session Context (for pronoun resolution only)")
    if session_context:
        lines.append(session_context)
    else:
        lines.append("(none)")
    
    return "\n".join(lines)

def parse_connection_response(text: str) -> List[dict]:
    """
    Parses <connections> block.
    """
    content = extract_xml_content(text, "connections")
    if not content:
        return []
    
    connections = []
    for line in content.split('\n'):
        line = line.strip()
        if not line:
            continue
            
        parts = [p.strip() for p in line.split('|')]
        
        if len(parts) < 2:
            continue
            
        msg_match = re.search(r"MSG\s+(\d+)", parts[0], re.IGNORECASE)
        if not msg_match:
            continue
        msg_id = msg_match.group(1)
        
        mid_part = parts[1].strip()
        if mid_part.upper() == "NO CONNECTIONS":
            continue
            
        if len(parts) < 3:
            continue
            
        if ";" in mid_part:
            ents = [e.strip() for e in mid_part.split(';')]
            reason = parts[2].strip()
            
            if len(ents) >= 2:
                connections.append({
                    "msg_id": msg_id,
                    "entity_a": ents[0],
                    "entity_b": ents[1],
                    "reason": reason
                })
        else:
            if "," in mid_part:
                ents = [e.strip() for e in mid_part.split(',')]
                if len(ents) >= 2:
                     connections.append({
                        "msg_id": msg_id,
                        "entity_a": ents[0],
                        "entity_b": ents[1],
                        "reason": parts[2].strip()
                    })

    return connections


def build_connection_response(parsed: List[dict]) -> ConnectionExtractionResponse:
    from collections import defaultdict
    
    grouped = defaultdict(list)
    for item in parsed:
        grouped[item["msg_id"]].append(
            EntityPair(
                entity_a=item["entity_a"],
                entity_b=item["entity_b"],
                confidence=0.8
            )
        )
    
    message_results = [
        MessageConnections(message_id=int(msg_id), entity_pairs=pairs)
        for msg_id, pairs in grouped.items()
    ]
    
    return ConnectionExtractionResponse(message_results=message_results)



