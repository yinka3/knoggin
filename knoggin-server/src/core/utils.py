import asyncio
import json
import redis.asyncio as aioredis
from typing import Any, List, Optional, Tuple
import re

from common.infra.redis import RedisKeys
from loguru import logger
from typing import Dict

from wordfreq import word_frequency
from common.config.topics_config import TopicConfig
from spacy.lang.en.stop_words import STOP_WORDS as SPACY_STOPS
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS as SKLEARN_STOPS


PRONOUNS = {
        "my", "his", "her", "their", "our", "your", "its",
        "he", "she", "they", "we", "i", "me", "him", "them",
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
    
    # Single common word shouldn't be blocked here (handled by global Stop Word filters)
    if len(words) <= 1:
        return False
    
    # Multi-word, all common: sum frequencies
    total = sum(freqs)
    return total > threshold * 100

def is_covered(candidate: str, covered_texts: set[str]) -> bool:
    """
    Check if candidate span is already covered by known entities.
    Uses word-boundary text comparison.
    """
    candidate_lower = candidate.lower().strip()
    
    for covered in covered_texts:
        if candidate_lower == covered:
            return True
            
        cov_esc = re.escape(covered)
        cand_esc = re.escape(candidate_lower)
        
        if re.search(r'\b' + cov_esc + r'\b', candidate_lower):
            return True
        if re.search(r'\b' + cand_esc + r'\b', covered):
            return True
    
    return False




def validate_entity(name: str, topic: str, topic_config: TopicConfig, label: str = None) -> bool:
    """Filter invalid mentions before resolution."""
    
    if not name or len(name) < 2:
        return False
    
    if len(name) > 100:
        return False
    
    if name.lower() in STOP_WORDS:
        return False
    
    if name.lower() in PRONOUNS:
        return False
    
    has_specific_label = label and label.lower() not in ("", "general")
    if not has_specific_label and is_generic_phrase(name):
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
    known_spans = {k[0].lower() for k in known_ents}
    for msg_id, span, label in gliner_ents:
        if span.lower() not in known_spans:
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
    lines.append("Scan messages above for proper nouns not listed in Known Entities or GLiNER extractions.")
    lines.append("Include the MSG id where you found each entity.")
    
    return "\n".join(lines)



def format_vp02_input(
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




async def fetch_conversation_turns(
    redis_client: aioredis.Redis,
    user_name: str,
    session_id: str,
    num_turns: int,
    up_to_msg_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch conversation turns from Redis in chronological order.

    Shared between Context (agent prompt context) and ProfileRefinementJob
    (fact extraction context). Callers post-process the results for their
    specific formatting needs.

    Returns list of dicts with keys:
        turn_id, role, content, timestamp, user_msg_id, metadata
    """
    sorted_key = RedisKeys.recent_conversation(user_name, session_id)
    conv_key = RedisKeys.conversation(user_name, session_id)

    if up_to_msg_id:
        turn_key = await redis_client.hget(
            RedisKeys.msg_to_turn_lookup(user_name, session_id),
            f"msg_{up_to_msg_id}",
        )
        if turn_key:
            turn_score = await redis_client.zscore(sorted_key, turn_key)
            turn_ids = await redis_client.zrange(
                sorted_key,
                f"({turn_score}",
                "-inf",
                desc=True,
                byscore=True,
                offset=0,
                num=num_turns,
            )
            turn_ids = list(reversed(turn_ids))
        else:
            # DLQ Retry Guard: If up_to_msg_id isn't in DB, check if it's an old message by comparing to the latest.
            latest_turn_ids = await redis_client.zrange(sorted_key, 0, 0, desc=True)
            is_dlq_retry = False
            latest_msg_id = None
            
            if latest_turn_ids:
                latest_turn_data = await redis_client.hget(conv_key, latest_turn_ids[0])
                if latest_turn_data:
                    try:
                        parsed = json.loads(latest_turn_data)
                        latest_msg_id = parsed.get("user_msg_id")
                        if latest_msg_id is not None and int(latest_msg_id) >= int(up_to_msg_id):
                            is_dlq_retry = True
                    except (ValueError, TypeError, Exception) as e:
                        logger.warning(f"Failed to unpack latest turn for DLQ guard: {e}")
            
            if is_dlq_retry:
                logger.warning(
                    f"DLQ Guard: Msg {up_to_msg_id} missing from cache, but DB is already at msg {latest_msg_id}. "
                    "Returning empty context to prevent leaking future messages."
                )
                return []

            # If not a DLQ retry, it's a truly new message. Safe to grab the current state as context.
            turn_ids = await redis_client.zrange(
                sorted_key, 0, num_turns - 1, desc=True
            )
            turn_ids = list(turn_ids)
            turn_ids.reverse()
    else:
        turn_ids = await redis_client.zrange(
            sorted_key, 0, num_turns - 1, desc=True
        )
        turn_ids = list(turn_ids)
        turn_ids.reverse()

    if not turn_ids:
        return []

    turn_data = await redis_client.hmget(conv_key, *turn_ids)

    results = []
    for turn_id, data in zip(turn_ids, turn_data):
        if not data:
            continue
        try:
            parsed = json.loads(data)
            results.append(
                {
                    "turn_id": turn_id,
                    "role": parsed["role"],
                    "content": parsed["content"],
                    "timestamp": parsed["timestamp"],
                    "user_msg_id": parsed.get("user_msg_id"),
                    "metadata": parsed.get("metadata"),
                }
            )
        except Exception as e:
            logger.warning(f"Failed to parse turn data for {turn_id}: {e}")
            continue

    return results




