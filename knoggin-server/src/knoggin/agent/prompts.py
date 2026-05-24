from typing import Dict, List, Tuple

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


async def enrich_facts_with_sources(
    facts: list,
    graph_client,
    user_name: str = None,
    session_id: str = None,
) -> List[Dict]:
    """Enrich facts with timestamps and source message content."""
    enriched = []
    scope_to_indices: Dict[Tuple[str, str, int], List[int]] = {}
    skipped_unscoped_sources = 0

    for i, fact in enumerate(facts):
        entry = {
            "content": fact.content,
            "recorded_at": fact.valid_at.isoformat() if fact.valid_at else None,
            "source_message": None,
        }
        enriched.append(entry)

        if fact.source_msg_id:
            fact_user = getattr(fact, "source_user_name", None) or user_name
            fact_session = getattr(fact, "source_session_id", None) or session_id
            if fact_user and fact_session:
                key = (fact_user, fact_session, fact.source_msg_id)
                if key not in scope_to_indices:
                    scope_to_indices[key] = []
                scope_to_indices[key].append(i)
            else:
                skipped_unscoped_sources += 1

    if skipped_unscoped_sources:
        logger.debug(
            f"Skipping {skipped_unscoped_sources} source message enrichments without user/session scope"
        )

    if scope_to_indices:
        by_scope: Dict[Tuple[str, str], List[int]] = {}
        for fact_user, fact_session, msg_id in scope_to_indices:
            by_scope.setdefault((fact_user, fact_session), []).append(msg_id)

        try:
            msg_text_map = {}
            for (fact_user, fact_session), msg_ids in by_scope.items():
                messages = await graph_client.get_messages_by_ids(
                    list(dict.fromkeys(msg_ids)),
                    user_name=fact_user,
                    session_ids=[fact_session],
                )
                for message in messages:
                    msg_text_map[
                        (
                            message.get("user_name", fact_user),
                            message.get("session_id", fact_session),
                            message["id"],
                        )
                    ] = message.get("content", "")

            for key, indices in scope_to_indices.items():
                text = msg_text_map.get(key)
                if text:
                    for idx in indices:
                        enriched[idx]["source_message"] = text
        except Exception as e:
            logger.debug(f"Could not batch fetch source messages: {e}")

    return enriched
