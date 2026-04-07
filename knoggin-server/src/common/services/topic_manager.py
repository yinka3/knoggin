from loguru import logger
from core.prompts import get_topic_seed_prompt
from common.schema.dtypes import TopicConfigResult

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

async def generate_topics(llm_service, text: str, user_name: str, max_topics: int = 6) -> dict:
    

    system = get_topic_seed_prompt(user_name)
    result: TopicConfigResult = await llm_service.call_llm(
        response_model=TopicConfigResult,
        system=system,
        user=text
    )

    if not result:
        raise ValueError("LLM returned empty response")

    # The result is already validated by Pydantic (TopicConfigResult)
    generated = {name: detail.model_dump() for name, detail in result.topics.items()}

    # Remove duplicates or defaults that might have been returned
    generated.pop("General", None)
    generated.pop("Identity", None)

    if len(generated) > max_topics:
        keys = list(generated.keys())[:max_topics]
        generated = {k: generated[k] for k in keys}

    # Merge DEFAULT_TOPICS with the generated topics
    # Fix 3: Logic is now fully delegated to Pydantic models for robustness
    return {**DEFAULT_TOPICS, **generated}
