from datetime import datetime
from typing import Dict, List


def _format_timestamp(ts) -> str:
    """Convert timestamp to readable date string."""
    if not ts:
        return "unknown"
    
    try:
        if isinstance(ts, (int, float)):
            if ts > 1e14:  # microseconds
                ts = ts / 1_000_000
            elif ts > 1e11:  # milliseconds
                ts = ts / 1000
            return datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
        
        if isinstance(ts, str):
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%d")
    except (ValueError, OSError, OverflowError):
        return str(ts) if ts else "unknown"
    
    return str(ts)


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
            except:
                pass # Fallback to raw string if parsing fails
            
            role = "User" if msg['role'] == 'user' else "Stella"
            content = msg.get('content', '')
            
            marker = ">> " if msg.get('is_hit') else "   "
            
            block += f"{marker}[{ts_display}] {role}: {content}\n"
            
        output.append(block)

    return "\n".join(output)

def format_entity_results(entities: List[Dict], k: int = 5) -> str:
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
                block += f"  → {conn_name}{alias_str} | weight: {weight}\n"
                
                for ev in conn.get("evidence", [])[:k]:
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
            
            block = f"--- {source} → {target} ---\n"
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
    header = f"Path: {' → '.join(entities)} ({hops} hop{'s' if hops != 1 else ''})\n"
    
    steps = []
    for step in path:
        step_num = step.get("step", 0) + 1
        ent_a = step.get("entity_a", "?")
        ent_b = step.get("entity_b", "?")
        
        step_block = f"  [{step_num}] {ent_a} → {ent_b}\n"
        
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
        # messages = data.get("messages", [])
        
        entity_names = []
        for ent in entities:
            name = ent.get("name", "")
            if name:
                entity_names.append(name)
        
        block = f"[HOT: {topic}]\n"

        if entities:
            block += "Entities:\n"
            for ent in entities:
                name = ent.get("name", "")
                facts = ent.get("facts", [])
                
                if name:
                    if facts:
                        block += f"  • {name}: {' | '.join(facts[:3])}\n"
                    else:
                        block += f"  • {name}\n"
        
        blocks.append(block)
    
    return "\n".join(blocks)