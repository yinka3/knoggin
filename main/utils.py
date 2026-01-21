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

def parse_entities(reasoning: str, min_confidence: float = 0.8) -> Optional[ExtractionResponse]:
    """Parse <entities> block from VP-01 output."""
    content = extract_xml_content(reasoning, "entities")
    
    if not content:
        return None
    
    entities = []
    malformed = 0
    
    for line in content.split("\n"):
        line = line.strip()
        if not line or line.lower().startswith("name"):
            continue
        
        parts = line.split("|")
        if len(parts) != 4:
            malformed += 1
            continue
        
        name, label, topic, conf_str = [p.strip() for p in parts]
        
        if not name:
            malformed += 1
            continue
        
        try:
            confidence = float(conf_str)
        except ValueError:
            malformed += 1
            continue
        
        if confidence < min_confidence:
            continue
        
        entities.append(EntityItem(
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
    
    # Build lookup: mention_name -> (type, topic)
    mention_lookup = {m[0].lower(): (m[1], m[2]) for m in mentions}
    
    entries = []
    for line in content.split("\n"):
        line = line.strip()
        if not line or "|" not in line:
            continue
        
        parts = [p.strip() for p in line.split("|")]
        verdict = parts[0].upper()
        
        if verdict == "EXISTING" and len(parts) >= 3:
            canonical = parts[1]
            mention = parts[2]
            typ, topic = mention_lookup.get(mention.lower(), ("unknown", "General"))
            
            entries.append(ResolutionEntry(
                verdict="EXISTING",
                canonical_name=canonical,
                mentions=[mention],
                entity_type=typ,
                topic=topic
            ))
        
        elif verdict == "NEW_GROUP" and len(parts) >= 2:
            mention_list = [m.strip() for m in parts[1].split(",")]
            canonical = max(mention_list, key=lambda m: (len(m), m))
            typ, topic = mention_lookup.get(mention_list[0].lower(), ("unknown", "General"))
            
            entries.append(ResolutionEntry(
                verdict="NEW_GROUP",
                canonical_name=canonical,
                mentions=mention_list,
                entity_type=typ,
                topic=topic
            ))
        
        elif verdict == "NEW_SINGLE" and len(parts) >= 2:
            mention = parts[1]
            typ, topic = mention_lookup.get(mention.lower(), ("unknown", "General"))
            
            entries.append(ResolutionEntry(
                verdict="NEW_SINGLE",
                canonical_name=mention,
                mentions=[mention],
                entity_type=typ,
                topic=topic
            ))
    
    return DisambiguationResult(entries=entries)


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

def format_vp01_input(
    messages: List[Dict],
    known_ents: List[Tuple[str, int]],
    gliner_ents: List[Tuple[str, str]],
    ambiguous: List[Tuple[str, str, List[str]]],
    covered_texts: set[str]
) -> str:
    lines = ["## Messages"]
    for msg in messages:
        lines.append(f"[MSG {msg['id']}]: \"{msg['message']}\"")
    
    lines.append("\n## Already Resolved (skip these)")
    for span_text, eid in known_ents:
        lines.append(f"- \"{span_text}\" → entity_id={eid}")
    for span_text, label in gliner_ents:
        if span_text.lower() in covered_texts and not any(
            span_text == a[0] for a in ambiguous
        ):
            lines.append(f"- \"{span_text}\" → {label}")
    
    if ambiguous:
        lines.append("\n## Ambiguous (Task 1: assign topic)")
        for span_text, label, topics in ambiguous:
            lines.append(f"- \"{span_text}\" ({label}) → choose from: {topics}")
    
    lines.append("\n## Discovery (Task 2: find missed entities)")
    lines.append("Scan messages above for proper nouns not listed in Already Resolved.")
    
    return "\n".join(lines)

