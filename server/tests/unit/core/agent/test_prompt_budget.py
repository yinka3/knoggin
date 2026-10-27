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
    fit_to_context,
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


def test_trimmed_memory_tells_the_model_content_was_omitted():
    from core.agent.prompt_budget import trim_to_tokens

    text = "".join(f"Context line {index}.\n" for index in range(200))

    head = trim_to_tokens(text, 100, None)
    tail = trim_to_tokens(text, 100, None, tail=True)

    assert head.startswith("Context line 0.")
    assert head.endswith("omitted to fit the prompt budget]")
    assert tail.startswith("[…earlier content omitted")
    assert tail.rstrip().endswith("Context line 199.")
    assert token_count(head, None) <= 100
    assert token_count(tail, None) <= 100
    retrimmed = trim_to_tokens(head, 60, None)
    assert retrimmed.count("omitted to fit the prompt budget") == 1
    assert token_count(retrimmed, None) <= 60


def test_budget_scales_to_the_model_context_and_keeps_proportions():
    budget = PromptMemoryBudget()
    large = fit_to_context(budget, 262_144)

    assert large.input_tokens == int(262_144 * 0.6)
    scale = large.input_tokens / budget.input_tokens
    assert large.project_context_tokens == int(budget.project_context_tokens * scale)
    assert large.conversation_tokens == int(budget.conversation_tokens * scale)
    assert large.response_reserve_tokens == budget.response_reserve_tokens

    # A 1M-token model stops at the ceiling instead of 629K per step.
    assert fit_to_context(budget, 1_048_576).input_tokens == budget.max_fitted_input_tokens

    small = fit_to_context(budget, 8_192)
    assert small.input_tokens == int(8_192 * 0.6)
    assert small.run_notebook_tokens < budget.run_notebook_tokens

    # Unknown size, or the user opted out: the configured numbers stand.
    assert fit_to_context(budget, None) is budget
    fixed = PromptMemoryBudget(fit_to_model=False)
    assert fit_to_context(fixed, 262_144) is fixed
