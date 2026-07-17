from common.utils.prompt_loader import load_named_prompt


def ner_prompt(user_name: str) -> str:
    return load_named_prompt("extract_entities", user_name=user_name)


def get_connection_reasoning_prompt(user_name: str) -> str:
    return load_named_prompt("extract_relationships", user_name=user_name)


def get_episode_generation_prompt(user_name: str) -> str:
    return load_named_prompt("generate_episode", user_name=user_name)


def get_episode_consolidation_prompt(user_name: str) -> str:
    return load_named_prompt(
        "regenerate_episode_consolidation",
        user_name=user_name,
    )
