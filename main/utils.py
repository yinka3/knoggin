import re
from typing import List, Optional, Tuple

from loguru import logger
from schema.dtypes import ConnectionExtractionResponse, DisambiguationResult, EntityPair, MessageConnections, ResolutionEntry


def is_substring_match(name_a: str, name_b: str) -> bool:
    """Case-insensitive substring check."""
    a, b = name_a.lower(), name_b.lower()
    return a in b or b in a


def build_label_block(topics_config: dict) -> str:
    lines = []
    for topic, config in topics_config.items():
        labels = config.get("labels", [])
        lines.append(f"Topic: {topic}")
        lines.append(f"  Labels: {', '.join(labels)}")
        lines.append("")
    return "\n".join(lines)

def extract_xml_content(text: str, tag: str) -> Optional[str]:
    """
    Lenient XML Extraction.
    1. MUST have <tag>.
    2. captures everything after <tag>.
    3. If </tag> exists, stops there. If not, takes to end of string.
    """
    if not text:
        return None
    
    start_match = re.search(f"(?i)<{tag}>", text)
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

def parse_ner_response(text: str) -> List[dict]:
    """
    Parses <entities> block from VP-01.
    Format: Name | label | topic
    """
    content = extract_xml_content(text, "entities")
    if not content:
        return []

    entities = []
    for line in content.split('\n'):
        line = line.strip()
        if not line:
            continue
            
        parts = [p.strip() for p in line.split('|')]
        
        if len(parts) == 3:
            entities.append({
                "name": parts[0],
                "label": parts[1],
                "topic": parts[2]
            })
        else:
            logger.warning(f"Skipping malformed NER line: {line}")
            
    return entities

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