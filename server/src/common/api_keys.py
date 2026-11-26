"""Provider credentials: from the environment first, then saved logins.

Secrets never go in ``knoggin.yml``: saved logins live in their own owner-only
file (see common.credential_store), so the settings API, shared config files
and backups never carry one.
"""

from common import credential_store

LLM_API_KEY_ENV = "OPENROUTER_API_KEY"
JEV_API_KEY_ENV = "JEV_API_KEY"
BRAVE_API_KEY_ENV = "BRAVE_API_KEY"
TAVILY_API_KEY_ENV = "TAVILY_API_KEY"


def _env(name: str) -> str:
    return credential_store.get(name)


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
