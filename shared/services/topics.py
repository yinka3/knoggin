import json
from typing import Optional
from loguru import logger


DEFAULT_TOPICS = {
    "General": {
        "active": True,
        "labels": [],
        "hierarchy": {},
        "aliases": []
    },
    "Identity": {
        "active": True,
        "labels": ["person"],
        "hierarchy": {},
        "aliases": []
    }
}


def _strip_code_fences(text: str) -> str:
    """Remove markdown code fences from LLM output."""
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("\n", 1)[-1]
    if cleaned.endswith("```"):
        cleaned = cleaned.rsplit("```", 1)[0]
    return cleaned.strip()


async def generate_topics(llm_service, text: str, max_topics: int = 6) -> dict:
    """
    Generate topic configuration from a text description using an LLM.
    Returns a merged dict with defaults (General, Identity) + generated topics.
    """
    from main.prompts import get_topic_seed_prompt

    system = get_topic_seed_prompt()
    raw = await llm_service.call_llm(system, text)

    if not raw:
        raise ValueError("LLM returned empty response")

    cleaned = _strip_code_fences(raw)

    try:
        generated = json.loads(cleaned)
    except json.JSONDecodeError:
        logger.error(f"Failed to parse topic generation output: {cleaned[:500]}")
        raise ValueError("Failed to parse generated topics")

    # Remove protected topics — they get re-added as defaults
    generated.pop("General", None)
    generated.pop("Identity", None)

    if len(generated) > max_topics:
        keys = list(generated.keys())[:max_topics]
        generated = {k: generated[k] for k in keys}

    for name, config in generated.items():
        config.setdefault("labels", [])
        config.setdefault("aliases", [])
        config.setdefault("hierarchy", {})
        config.setdefault("active", True)

    return {**DEFAULT_TOPICS, **generated}
