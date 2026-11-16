from common.utils.prompt_loader import load_named_prompt


def entity_prompt(user_name: str) -> str:
    return load_named_prompt("extract_entities", user_name=user_name)


def relationship_prompt(user_name: str) -> str:
    return load_named_prompt("extract_relationships", user_name=user_name)
