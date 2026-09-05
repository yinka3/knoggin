"""Prompts for the project Context reconciliation operation."""

from common.utils.prompt_loader import load_named_prompt


def get_context_update_prompt(user_name: str) -> str:
    """Load the server-owned Context reconciliation instructions."""

    return load_named_prompt("update_context", user_name=user_name)
