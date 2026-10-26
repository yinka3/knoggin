import pytest
from pydantic import ValidationError

from common.schema.agent.settings import (
    AgentModelPromptMemory,
    PromptMemoryBudget,
    PromptMemorySettings,
)
from core.agent.prompt_budget import (
    PromptBudgetExceeded,
    allocate_memory,
    token_count,
)


def test_selected_agent_and_model_override_or_global_default():
    override = PromptMemoryBudget(input_tokens=2048)
    settings = PromptMemorySettings(overrides=[
        AgentModelPromptMemory(agent_id="research", model="model-a", budget=override)
    ])

    assert settings.for_run("research", "model-a") == override
    assert settings.for_run("research", "model-b") == settings.default
    assert settings.for_run("other", "model-a") == settings.default

    with pytest.raises(ValidationError):
        PromptMemorySettings(overrides=[
            AgentModelPromptMemory(agent_id="research", model="model-a", budget=override),
            AgentModelPromptMemory(agent_id="research", model="model-a", budget=override),
        ])


def test_unused_project_share_is_lent_to_conversation_without_erasing_notebook():
    budget = PromptMemoryBudget(
        input_tokens=2048,
        conversation_tokens=4,
        project_context_tokens=16,
        run_notebook_tokens=4,
        selected_notebook_tokens=0,
    )
    parts = {
        "conversation": "C" * 80,
        "project_context": "",
        "run_notebook": "N" * 16,
        "selected_notebook": "",
    }
    memory = allocate_memory(parts, budget, 12, None)

    assert token_count(memory["conversation"], None) == 8
    assert memory["run_notebook"] == parts["run_notebook"]
    assert sum(token_count(text, None) for text in memory.values()) <= 12


def test_fixed_prompt_over_budget_fails_and_fallback_rounds_up():
    assert token_count("abcde", None) == 2
    with pytest.raises(PromptBudgetExceeded):
        allocate_memory({}, PromptMemoryBudget(), -1, None)
