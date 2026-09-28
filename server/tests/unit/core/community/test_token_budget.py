import pytest

from core.community.token_budget import AACTokenBudget


@pytest.mark.runtime
@pytest.mark.no_network
def test_aac_token_budget_is_shared_and_soft():
    budget = AACTokenBudget(limit=100)

    assert budget.allow_call() is True
    assert budget.record({"prompt_tokens": 40, "completion_tokens": 30}) == 70
    assert budget.allow_call() is True
    assert budget.record({"total_tokens": 50, "approximate": True}) == 120
    assert budget.approximate is True
    assert budget.allow_call() is False


@pytest.mark.runtime
@pytest.mark.no_network
def test_aac_token_budget_rejects_invalid_limits_and_usage():
    with pytest.raises(ValueError):
        AACTokenBudget(limit=-1)
    budget = AACTokenBudget(limit=10)
    with pytest.raises(ValueError):
        budget.record({"total_tokens": -1})


@pytest.mark.parametrize("field", ["prompt_tokens", "completion_tokens"])
@pytest.mark.parametrize("value", [-1, True, "3", 1.5])
@pytest.mark.parametrize("total", [None, 5])
def test_invalid_components_are_rejected_without_changing_budget(field, value, total):
    budget = AACTokenBudget(limit=10, used=2)
    with pytest.raises(ValueError, match="non-negative token counts"):
        budget.record({field: value, "total_tokens": total, "approximate": True})
    assert budget.used == 2
    assert budget.approximate is False
