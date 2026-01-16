import re
from typing import List, Tuple
from schema.dtypes import DisambiguationResult, ResolutionEntry

def build_label_block(topics_config: dict) -> str:
    lines = []
    for topic, config in topics_config.items():
        labels = config.get("labels", [])
        lines.append(f"Topic: {topic}")
        lines.append(f"  Labels: {', '.join(labels)}")
        lines.append("")
    return "\n".join(lines)


def parse_disambiguation(
    reasoning: str, 
    mentions: List[Tuple[str, str, str]]
) -> DisambiguationResult:
    """
    Parse VP-02 <resolution> block into DisambiguationResult.
    """
    match = re.search(r"<resolution>(.*?)</resolution>", reasoning, re.DOTALL)
    
    if match:
        content = match.group(1).strip()
    else:
        content = reasoning.strip()
    
    if not content:
        return DisambiguationResult(entries=[])
    
    # Build lookup: mention_name -> (type, topic)
    mention_lookup = {m[0].lower(): (m[1], m[2]) for m in mentions}
    
    entries = []
    for line in match.group(1).strip().split("\n"):
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