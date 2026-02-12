from datetime import datetime, timezone
from typing import Dict, List

# Timestamp bounds (Unix seconds)
TS_MIN = 946684800    # 2000-01-01 00:00:00 UTC
TS_MAX = 2524608000   # 2050-01-01 00:00:00 UTC


def _normalize_timestamp(ts: float) -> float | None:
    """Normalize timestamp to seconds. Returns None if out of bounds."""
    divisors = [1, 1_000, 1_000_000, 1_000_000_000]
    
    for divisor in divisors:
        normalized = ts / divisor
        if TS_MIN <= normalized <= TS_MAX:
            return normalized
    
    return None


def _format_timestamp(ts) -> str:
    """Convert timestamp to readable datetime string. Handles s, ms, us, ns."""
    if not ts:
        return "unknown"
    
    try:
        if isinstance(ts, str):
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%d %H:%M")
        
        if isinstance(ts, (int, float)):
            ts_normalized = _normalize_timestamp(ts)
            if ts_normalized is None:
                return "unknown"
            return datetime.fromtimestamp(ts_normalized, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            
    except (ValueError, OSError, OverflowError):
        pass
    
    return "unknown"


def format_retrieved_messages(messages: List[Dict]) -> str:
    if not messages:
        return "No messages found."

    output = []
    
    for idx, hit in enumerate(messages):
        score = hit.get('score', 0)
        context = hit.get('context', [])
        
        block = f"--- Search Result #{idx+1} (Relevance: {score:.2f}) ---\n"
        
        for msg in context:
            ts_str = msg.get('timestamp', "")
            ts_display = ts_str
            try:
                if "T" in ts_str:
                    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    ts_display = dt.strftime("%Y-%m-%d %H:%M")
            except (ValueError, TypeError):
                pass
            
            role = "USER" if msg['role'] == 'user' else "AGENT"
            content = msg.get('content', '')
            
            marker = ">> " if msg.get('is_hit') else "   "
            
            block += f"{marker}[{ts_display}] {role}: {content}\n"
            
        output.append(block)

    return "\n".join(output)


def format_entity_results(entities: List[Dict], evidence_limit: int = 5) -> str:
    """Format search_entity output."""
    if not entities:
        return "No entities found."
    
    blocks = []
    for ent in entities:
        name = ent.get("canonical_name", "Unknown")
        ent_type = ent.get("type", "unknown")
        aliases = ent.get("aliases", [])
        topic = ent.get("topic", "General")
        last_mentioned = _format_timestamp(ent.get("last_mentioned"))
        facts = ent.get("facts", [])
        
        block = f"=== {name} ({ent_type}) ===\n"
        
        if aliases:
            block += f"Aliases: {', '.join(aliases)}\n"
        
        block += f"Topic: {topic}\n"
        block += f"Last talked about: {last_mentioned}\n"
        
        if facts:
            block += "Facts:\n"
            for fact in facts:
                block += f"  - {fact}\n"
        else:
            block += "Facts: None recorded\n"
        
        connections = ent.get("top_connections", [])
        if connections:
            block += "\nConnections:\n"
            for conn in connections:
                conn_name = conn.get("canonical_name", "Unknown")
                conn_aliases = conn.get("aliases", [])
                weight = conn.get("weight", 0)
                
                alias_str = f" (aka {', '.join(conn_aliases)})" if conn_aliases else ""
                conn_context = conn.get("context")
                if conn_context:
                    block += f"  -> {conn_name}{alias_str} | Context: {conn_context} | weight: {weight}\n"
                else:
                    block += f"  -> {conn_name}{alias_str} | weight: {weight}\n"
                
                for ev in conn.get("evidence", [])[:evidence_limit]:
                    msg = ev.get("message", "")
                    ts = _format_timestamp(ev.get("timestamp"))
                    block += f"    \"{msg}\" [{ts}]\n"
        
        blocks.append(block)
    
    return "\n".join(blocks)


def format_graph_results(results: List[Dict]) -> str:
    """Format get_connections and get_activity output."""
    if not results:
        return "No connections found."
    
    blocks = []
    for r in results:
        if "source" in r and "target" in r:
            source = r.get("source", "?")
            target = r.get("target", "?")
            strength = r.get("connection_strength", 0)
            last_seen = _format_timestamp(r.get("last_seen"))
            
            block = f"--- {source} -> {target} ---\n"
            context = r.get("context")
            if context:
                block += f"Description: {context}\n"
            target_facts = r.get("target_facts", [])
            if target_facts:
                block += f"Facts: {' | '.join(target_facts[:3])}\n"
            block += f"Strength: {strength} | Last talked about: {last_seen}\n"
        
        elif "entity" in r:
            entity = r.get("entity", "?")
            last_seen = _format_timestamp(r.get("time"))
            
            block = f"--- Activity: {entity} ---\n"
            block += f"Last talked about: {last_seen}\n"
        
        else:
            continue
        
        for ev in r.get("evidence", []):
            msg = ev.get("message", "")
            ts = _format_timestamp(ev.get("timestamp"))
            block += f"  [{ts}] \"{msg}\"\n"
        
        blocks.append(block)
    
    return "\n".join(blocks)


def format_path_results(path: List[Dict]) -> str:
    """Format find_path output."""
    if not path:
        return "No path found."
    
    if len(path) == 1 and path[0].get("hidden"):
        return path[0].get("message", "Connection exists through inactive topics.")
    
    entities = [path[0].get("entity_a", "?")]
    for step in path:
        entities.append(step.get("entity_b", "?"))
    
    hops = len(path)
    header = f"Path: {' -> '.join(entities)} ({hops} hop{'s' if hops != 1 else ''})\n"
    
    steps = []
    for step in path:
        step_num = step.get("step", 0) + 1
        ent_a = step.get("entity_a", "?")
        ent_b = step.get("entity_b", "?")
        
        step_block = f"  [{step_num}] {ent_a} -> {ent_b}\n"
        
        if step.get("status") == "LOCKED":
            step_block += f"      [LOCKED: {step.get('locked_reason', 'Inactive topic')}]\n"
        else:
            for ev in step.get("evidence", []):
                msg = ev.get("message", "")
                ts = _format_timestamp(ev.get("timestamp"))
                step_block += f"      \"{msg}\" [{ts}]\n"
        
        steps.append(step_block)
    
    return header + "".join(steps)


def format_hot_topic_context(context: Dict[str, Dict]) -> str:
    """Format hot topic pre-fetched context."""
    if not context:
        return ""
    
    blocks = []
    for topic, data in context.items():
        entities = data.get("entities", [])
        
        block = f"[HOT: {topic}]\n"

        if entities:
            block += "Entities:\n"
            for ent in entities:
                name = ent.get("name", "")
                facts = ent.get("facts", [])
                
                if name:
                    if facts:
                        block += f"  - {name}: {' | '.join(facts[:3])}\n"
                    else:
                        block += f"  - {name}\n"
        
        blocks.append(block)
    
    return "\n".join(blocks)


def format_hierarchy_results(results: List[Dict]) -> str:
    if not results:
        return "No hierarchy found."
    
    blocks = []
    for h in results:
        entity = h.get("entity", "Unknown")
        block = f"=== {entity} ===\n"
        
        if h.get("ancestry"):
            block += f"Ancestry: {' → '.join(h['ancestry'])}\n"
        
        if h.get("parents"):
            block += "Parents:\n"
            for p in h["parents"]:
                facts = p.get("facts", [])
                fact_str = f" ({', '.join(facts[:2])})" if facts else ""
                block += f"  ↑ {p.get('canonical_name', '?')}{fact_str}\n"
        
        if h.get("children"):
            block += "Children:\n"
            for c in h["children"]:
                facts = c.get("facts", [])
                fact_str = f" ({', '.join(facts[:2])})" if facts else ""
                block += f"  ↓ {c.get('canonical_name', '?')}{fact_str}\n"
        
        blocks.append(block)
    
    return "\n".join(blocks)

def format_memory_context(blocks: dict) -> str:
    """Format memory blocks for prompt injection."""
    if not blocks:
        return ""
    
    sections = []
    for topic, entries in blocks.items():
        if not entries:
            continue
        
        lines = [f"[{topic}]"]
        for entry in entries:
            lines.append(f"  - ({entry['id']}) {entry['content']}")
        sections.append("\n".join(lines))
    
    if not sections:
        return ""
    
    return "\n".join(sections)


def format_preferences_context(preferences: list) -> str:
    """Format user-defined preferences for prompt injection."""
    if not preferences:
        return ""
    
    lines = []
    for pref in preferences:
        kind = pref.get("kind", "preference")
        content = pref.get("content", "")
        if kind == "ick":
            lines.append(f"- AVOID: {content}")
        else:
            lines.append(f"- {content}")
    
    return "\n".join(lines)

def format_files_context(files: list) -> str:
    """Format file manifest for prompt injection."""
    if not files:
        return ""
    
    lines = []
    for f in files:
        size_kb = f.get("size_bytes", 0) / 1024
        lines.append(f"- {f['original_name']} ({size_kb:.0f}KB, {f['chunk_count']} chunks)")
    
    return "\n".join(lines)