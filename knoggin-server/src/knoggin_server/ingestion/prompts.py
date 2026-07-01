from typing import Dict, List, Tuple

from loguru import logger

from common.utils.prompt_loader import load_named_prompt, render_prompt_text


def ner_reasoning_prompt(user_name: str) -> str:
    return load_named_prompt("extract_entities", user_name=user_name)


def get_connection_reasoning_prompt(user_name: str) -> str:
    return load_named_prompt("extract_relationships", user_name=user_name)


def get_profile_extraction_prompt(user_name: str) -> str:
    return load_named_prompt("extract_facts", user_name=user_name)


def get_merge_judgment_prompt() -> str:
    return load_named_prompt("judge_merge")


def get_contradiction_judgment_prompt() -> str:
    return load_named_prompt("judge_contradiction")


def get_relevance_judgment_prompt() -> str:
    return load_named_prompt("judge_relevance")


def render_configured_prompt(
    prompt_template: str,
    *,
    prompt_name: str,
    required: set[str] | None = None,
    **values,
) -> str:
    """Strictly render a prompt supplied through runtime configuration."""
    return render_prompt_text(
        prompt_template,
        values,
        required=required,
        prompt_name=prompt_name,
    )



async def enrich_facts_with_sources(
    facts: list,
    knowledge_store,
    visible_project_ids: List[str],
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
                messages = await knowledge_store.get_messages_by_ids(
                    list(dict.fromkeys(msg_ids)),
                    user_name=fact_user,
                    session_ids=[fact_session],
                    visible_project_ids=visible_project_ids,
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
