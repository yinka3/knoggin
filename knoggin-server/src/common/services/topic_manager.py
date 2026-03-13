import json
import re
from loguru import logger
from core.prompts import get_topic_seed_prompt

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
    if "```" in cleaned:
        cleaned = cleaned.split("```")[0]
    return cleaned.strip()


async def generate_topics(llm_service, text: str, max_topics: int = 6) -> dict:
    

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

    if not isinstance(generated, dict):
        raise ValueError(f"Expected dict from topic generation, got {type(generated).__name__}")

    generated.pop("General", None)
    generated.pop("Identity", None)

    if len(generated) > max_topics:
        keys = list(generated.keys())[:max_topics]
        generated = {k: generated[k] for k in keys}

    for name, config in generated.items():
        if not isinstance(config, dict):
            generated[name] = {"labels": [], "aliases": [], "hierarchy": {}, "active": True}
            continue

        config.setdefault("active", True)
        config.setdefault("hierarchy", {})

        if not isinstance(config.get("labels"), list):
            config["labels"] = []
        if not isinstance(config.get("aliases"), list):
            config["aliases"] = []
        if not isinstance(config.get("hierarchy"), dict):
            config["hierarchy"] = {}

        clean_labels = []
        for label in config["labels"]:
            if not isinstance(label, str):
                continue
            label = label.strip().lower()
            if not label or len(label) > 30:
                continue
            if not re.match(r'^[a-z][a-z0-9 _-]*$', label):
                continue
            clean_labels.append(label)
        config["labels"] = clean_labels

    return {**DEFAULT_TOPICS, **generated}
