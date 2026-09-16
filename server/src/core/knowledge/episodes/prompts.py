"""Prompts used by project episode generation."""

from common.utils.prompt_loader import load_named_prompt


def get_episode_generation_prompt(
    user_name: str,
    *,
    prompt_narrative_chars: int,
    max_narrative_chars: int,
    max_episode_source_messages: int,
    max_episode_source_tokens: int,
) -> str:
    return load_named_prompt(
        "generate_episode",
        user_name=user_name,
        prompt_narrative_chars=prompt_narrative_chars,
        max_narrative_chars=max_narrative_chars,
        max_episode_source_messages=max_episode_source_messages,
        max_episode_source_tokens=max_episode_source_tokens,
    )


def get_episode_narrative_repair_prompt(
    user_name: str, *, max_narrative_chars: int
) -> str:
    return load_named_prompt(
        "repair_episode_narrative",
        user_name=user_name,
        max_narrative_chars=max_narrative_chars,
    )
