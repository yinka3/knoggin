import asyncio
import inspect
import json
import re
from functools import lru_cache
from typing import Any, Callable, Dict, List, Optional, Tuple

from loguru import logger
from wordfreq import word_frequency

from common.conf.topics_config import TopicConfig
from common.utils.time_utils import parse_iso_time_or_now

PRONOUNS = {
    "my",
    "his",
    "her",
    "their",
    "our",
    "your",
    "its",
    "he",
    "she",
    "they",
    "we",
    "i",
    "me",
    "him",
    "them",
    "this",
    "that",
    "these",
    "those",
}

STOP_WORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "to",
    "was",
    "with",
}


@lru_cache(maxsize=1)
def _stop_words() -> set[str]:
    stop_words = set(STOP_WORDS)
    try:
        from spacy.lang.en.stop_words import STOP_WORDS as SPACY_STOP_WORDS

        stop_words |= set(SPACY_STOP_WORDS)
    except Exception as exc:
        logger.debug(f"Falling back without spaCy stop words: {exc}")

    try:
        from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS

        stop_words |= set(ENGLISH_STOP_WORDS)
    except Exception as exc:
        logger.debug(f"Falling back without sklearn stop words: {exc}")

    return stop_words


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
    freqs = [word_frequency(w, "en") for w in words]

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

        if re.search(r"\b" + cov_esc + r"\b", candidate_lower):
            return True
        if re.search(r"\b" + cand_esc + r"\b", covered):
            return True

    return False


def validate_entity(
    name: str, topic: str, topic_config: TopicConfig, label: Optional[str] = None
) -> bool:
    """Filter invalid mentions before resolution."""

    if not name or len(name) < 2:
        return False

    if len(name) > 100:
        return False

    if name.lower() in _stop_words():
        return False

    if name.lower() in PRONOUNS:
        return False

    has_specific_label = label and label.lower() not in ("", "general")
    if not has_specific_label and is_generic_phrase(name):
        return False

    if not any(c.isalpha() for c in name):
        return False

    if topic:
        normalized = topic_config.normalize_topic(topic)
        if not normalized or not topic_config.is_active(normalized):
            logger.debug(f"Invalid topic '{topic}' for entity '{name}'")
            return False

    return True


async def fetch_conversation_turns(
    pg_client,
    user_name: str,
    session_id: str,
    num_turns: int,
    up_to_msg_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Fetch conversation turns natively from Postgres in chronological order."""
    query = """
        SELECT message_id, role, content, timestamp_ms as timestamp,
               user_msg_id, metadata
        FROM public.messages
        WHERE user_name = %(user_name)s AND session_id = %(session_id)s
    """
    params = {"user_name": user_name, "session_id": session_id, "limit": num_turns}

    if up_to_msg_id:
        query += " AND message_id <= %(up_to_msg_id)s "
        params["up_to_msg_id"] = up_to_msg_id

    query += " ORDER BY message_id DESC LIMIT %(limit)s "

    rows = await pg_client.fetch_all(query, params)

    # We want chronological order, but we fetched DESC to get the latest `limit` rows.
    # So we reverse the rows.
    rows = list(rows)
    rows.reverse()

    results = []
    for row in rows:
        meta = row.get("metadata")
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except (TypeError, json.JSONDecodeError):
                meta = {}

        # Keep the public conversation-turn shape stable while storing the
        # canonical timestamp as milliseconds in Postgres.
        from datetime import datetime, timezone

        ts = row["timestamp"]
        if ts:
            dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)
            ts_str = dt.isoformat()
        else:
            ts_str = ""

        results.append({
            "message_id": row["message_id"],
            "role": row["role"],
            "content": row["content"],
            "timestamp": ts_str,
            "user_msg_id": row.get("user_msg_id"),
            "metadata": meta or {},
        })

    return results


def format_recorded_date(recorded: str) -> str:
    """Format ISO timestamp to YYYY-MM-DD, with fallback."""
    if not recorded:
        return "unknown"
    try:
        dt = parse_iso_time_or_now(recorded)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return str(recorded)[:10]


def format_vp01_input(
    messages: List[Dict],
    known_ents: List[Tuple[str, int]],
    gliner_ents: List[Tuple[int, str, str]],
    ambiguous: List[Tuple[int, str, str, List[str]]],
    covered_texts: Dict[int, set],
    label_block: str,
) -> str:
    lines = []
    lines.append("## Label Schema\n")
    lines.append(label_block)

    lines.append("\n## Messages\n")
    valid_msg_ids = [msg["id"] for msg in messages]
    lines.append(f"Valid msg_id values: {valid_msg_ids}")
    for msg in messages:
        label = msg.get("role_label")
        if not label:
            label = "USER" if msg.get("role") == "user" else "AGENT"

        content = msg.get("message") or msg.get("content") or ""
        lines.append(f'[MSG {msg["id"]}] [{label}]: "{content}"')

    lines.append("\n## Known Entities (from graph - do not override)\n")
    if known_ents:
        for span_text, eid in known_ents:
            lines.append(f'- "{span_text}" -> entity_id={eid}')
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
            lines.append(f'- MSG {msg_id}: "{span}" -> {label}')
    else:
        lines.append("(none)")

    if ambiguous:
        lines.append("\n## Ambiguous (Task 1: assign topic)")
        for msg_id, span_text, label, topics in ambiguous:
            lines.append(
                f'- MSG {msg_id}: "{span_text}" ({label}) -> choose from: {topics}'
            )

    lines.append("\n## Discovery (Task 2: find missed entities)")
    lines.append(
        "Scan messages above for proper nouns not listed in Known Entities or "
        "GLiNER extractions."
    )
    lines.append("Include the MSG id where you found each entity.")
    lines.append("Only return msg_id values from the Valid msg_id list above.")

    return "\n".join(lines)


def format_vp02_input(
    candidates: List[Dict],
    messages: List[Dict],
    session_context: str,
    user_name: Optional[str] = None,
) -> str:
    lines = []

    lines.append("## Candidate Entities")
    canonical_names = [
        c.get("canonical_name") for c in candidates if c.get("canonical_name")
    ]
    lines.append(f"Valid canonical entity names: {canonical_names}")
    if candidates:
        for c in candidates:
            msg_ids = c.get("source_msgs", [])
            if msg_ids:
                source = f" (from MSG {', '.join(str(m) for m in msg_ids)})"
            else:
                source = ""
            lines.append(f"{c['canonical_name']} [{c['type']}]{source}")
            if c.get("mentions"):
                lines.append(f"  Mentions: {', '.join(c['mentions'])}")
    else:
        lines.append("(none)")

    lines.append("\n## Messages")
    valid_msg_ids = [msg["id"] for msg in messages]
    lines.append(f"Valid msg_id values: {valid_msg_ids}")
    if messages:
        for msg in messages:
            label = msg.get("role_label")
            if not label:
                label = "USER" if msg.get("role") == "user" else "AGENT"

            content = msg.get("message") or msg.get("content") or msg.get("text") or ""
            lines.append(f'[MSG {msg["id"]}] [{label}]: "{content}"')
    else:
        lines.append("(none)")

    lines.append("\n## Output Constraints")
    lines.append("Use only Valid canonical entity names for entity_a and entity_b.")
    lines.append(
        "Use only Valid canonical entity names for user_connections.entity_name."
    )
    lines.append("Use only Valid msg_id values from the Messages section.")
    if user_name:
        lines.append(
            f'Do not put "{user_name}" in entity_a or entity_b; use '
            "user_connections instead."
        )
    lines.append("Do not use Session Context as evidence for a connection.")

    lines.append("\n## Session Context (for pronoun resolution only)")
    if session_context:
        lines.append(session_context)
    else:
        lines.append("(none)")

    return "\n".join(lines)


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

            source_info = f', source: "{source}"' if source else ""
            output.append(f"  - {content} (recorded: {recorded_str}{source_info})")

    return output


def format_vp04_input(entities: List[Dict], conversation_text: str) -> str:
    """Format prompt for extraction verification phase."""
    lines = []
    lines.append("## Entities")
    entity_names = [
        ent.get("canonical_name", ent.get("entity_name", "Unknown"))
        for ent in entities
    ]
    lines.append(f"Valid canonical_name values: {entity_names}")

    for ent in entities:
        lines.extend(_format_entity_block(ent))
        lines.append("")

    lines.append("## Prior Conversation For Context")
    lines.append(conversation_text)
    lines.append("")
    lines.append("## Output Constraints")
    lines.append("Use only Valid canonical_name values.")
    lines.append(
        "Use source_msg_id only when the fact is grounded in a [MSG_<id>] or "
        "[MSG <id>] line above."
    )
    lines.append("Use exact existing fact text for supersedes or invalidates.")

    return "\n".join(lines)


def format_vp05_input(entity_a: Dict, entity_b: Dict) -> str:
    """Format prompt for merge profile validation phase."""

    output = []
    output.extend(_format_entity_block(entity_a, "Entity A"))
    output.append("")
    output.extend(_format_entity_block(entity_b, "Entity B"))
    return "\n".join(output)


def safe_update(target_method: Callable, settings_model: Any) -> Optional[Any]:
    """
    Safely calls target_method with the provided settings.
    Detects if the method expects:
    1. **kwargs (maps all fields)
    2. A single object (passes the model itself)
    3. Specific named parameters (maps matching fields)
    Accepts Pydantic models and dict config subtrees.
    """
    try:
        sig = inspect.signature(target_method)
        params = list(sig.parameters.values())
        if hasattr(settings_model, "model_dump"):
            all_settings = settings_model.model_dump()
        elif isinstance(settings_model, dict):
            all_settings = settings_model
        else:
            all_settings = {}

        if any(p.kind == p.VAR_KEYWORD for p in params):
            valid_updates = {k: v for k, v in all_settings.items() if v is not None}
            return target_method(**valid_updates)

        if len(params) == 1:
            return target_method(settings_model)

        valid_updates = {
            k: v
            for k, v in all_settings.items()
            if k in sig.parameters and v is not None
        }
        if valid_updates:
            return target_method(**valid_updates)

    except Exception as e:
        logger.error(f"Safe update failed for {target_method}: {e}")

    return None
