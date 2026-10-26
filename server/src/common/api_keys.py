"""Provider credentials come only from the environment, never saved settings.

Keeping secrets out of ``knoggin.yml`` means the settings API can never write
one, and backups or shared config files never carry one.
"""

import os

LLM_API_KEY_ENV = "OPENROUTER_API_KEY"
JEV_API_KEY_ENV = "JEV_API_KEY"
BRAVE_API_KEY_ENV = "BRAVE_API_KEY"
TAVILY_API_KEY_ENV = "TAVILY_API_KEY"


def _env(name: str) -> str:
    return os.environ.get(name, "").strip()


def llm_api_key() -> str:
    return _env(LLM_API_KEY_ENV)


def jev_api_key(endpoint: str = "") -> str:
    """JEV's own key; OpenRouter's Decisions API can reuse the OpenRouter key."""

    return _env(JEV_API_KEY_ENV) or (
        llm_api_key() if "openrouter.ai" in endpoint else ""
    )


def brave_api_key() -> str:
    return _env(BRAVE_API_KEY_ENV)


def tavily_api_key() -> str:
    return _env(TAVILY_API_KEY_ENV)
