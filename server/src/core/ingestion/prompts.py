from common.utils.prompt_loader import load_named_prompt


def ner_prompt(user_name: str) -> str:
    return load_named_prompt("extract_entities", user_name=user_name)


def get_connection_reasoning_prompt(user_name: str) -> str:
    return load_named_prompt("extract_relationships", user_name=user_name)


def get_context_connection_reasoning_prompt(user_name: str) -> str:
    """Load the Context-native VP-02 contract, not the legacy message prompt."""

    return load_named_prompt("extract_context_relationships", user_name=user_name)
