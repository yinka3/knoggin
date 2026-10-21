import pytest
from pydantic import ValidationError

from common.schema.settings import (
    EntityResolutionSettings,
    LLMModelPricing,
    LLMSpendingBudgetSettings,
    RootConfig,
)


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize("value", [float("inf"), "Infinity", float("nan")])
def test_spending_limit_must_be_finite_at_persisted_config_boundary(value):
    with pytest.raises(ValidationError):
        RootConfig.model_validate(
            {
                "llm": {
                    "spending_budget": {
                        "limit_usd": value,
                        "fallback_pricing": {
                            "input_usd_per_million_tokens": 1,
                            "output_usd_per_million_tokens": 1,
                        },
                    }
                }
            }
        )


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize(
    "field", ["input_usd_per_million_tokens", "output_usd_per_million_tokens"]
)
def test_model_pricing_must_be_finite(field):
    with pytest.raises(ValidationError):
        LLMModelPricing.model_validate(
            {
                "input_usd_per_million_tokens": 1,
                "output_usd_per_million_tokens": 1,
                field: float("inf"),
            }
        )


@pytest.mark.unit
@pytest.mark.no_network
def test_common_word_threshold_must_be_finite():
    with pytest.raises(ValidationError):
        EntityResolutionSettings(common_word_frequency_threshold=float("inf"))


@pytest.mark.unit
@pytest.mark.no_network
def test_finite_values_remain_accepted():
    assert LLMSpendingBudgetSettings(
        limit_usd=5,
        fallback_pricing=LLMModelPricing(
            input_usd_per_million_tokens=1,
            output_usd_per_million_tokens=0,
        ),
    ).limit_usd == 5
    assert EntityResolutionSettings(common_word_frequency_threshold=0.25).common_word_frequency_threshold == 0.25
