from typing import Dict, List

from loguru import logger

from common.utils.prompt_loader import render_prompt


def ner_reasoning_prompt(user_name: str) -> str:
    return render_prompt("pipeline/extract_entities.j2", user_name=user_name)


def get_connection_reasoning_prompt(user_name: str) -> str:
    return render_prompt("pipeline/extract_relationships.j2", user_name=user_name)


def get_profile_extraction_prompt(user_name: str) -> str:
    return render_prompt("pipeline/extract_facts.j2", user_name=user_name)


def get_merge_judgment_prompt() -> str:
    return render_prompt("pipeline/judge_merge.j2")


def get_contradiction_judgment_prompt() -> str:
    return render_prompt("pipeline/judge_contradiction.j2")


def get_topic_seed_prompt(user_name: str) -> str:
    return render_prompt("pipeline/generate_topic_seed.j2", user_name=user_name)


def get_lightweight_extraction_prompt(content: str) -> str:
    return render_prompt("pipeline/extract_ai_facts.j2", content=content)


def get_topic_evolution_prompt(user_name: str) -> str:
    return render_prompt("pipeline/generate_topic_evolution.j2", user_name=user_name)


async def enrich_facts_with_sources(facts: list, graph_client) -> List[Dict]:
    """Enrich facts with timestamps and source message content."""
    enriched = []
    msg_id_to_indices: Dict[int, List[int]] = {}

    for i, fact in enumerate(facts):
        entry = {
            "content": fact.content,
            "recorded_at": fact.valid_at.isoformat() if fact.valid_at else None,
            "source_message": None,
        }
        enriched.append(entry)

        if fact.source_msg_id:
            if fact.source_msg_id not in msg_id_to_indices:
                msg_id_to_indices[fact.source_msg_id] = []
            msg_id_to_indices[fact.source_msg_id].append(i)

    if msg_id_to_indices:
        try:
            messages = await graph_client.get_messages_by_ids(
                list(msg_id_to_indices.keys())
            )
            msg_text_map = {m["id"]: m.get("content", "") for m in messages}

            for msg_id, indices in msg_id_to_indices.items():
                text = msg_text_map.get(msg_id)
                if text:
                    for idx in indices:
                        enriched[idx]["source_message"] = text
        except Exception as e:
            logger.debug(f"Could not batch fetch source messages: {e}")

    return enriched
