import pytest

from common.schema.settings import LLMSpendingBudgetSettings
from infrastructure.external_model_budget import ExternalModelSpendingLedger


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_durable_ledger_records_the_billed_cost(real_postgres_client):
    ledger = ExternalModelSpendingLedger(
        LLMSpendingBudgetSettings(), postgres_client=real_postgres_client
    )
    for cost in (0.0042, 0.0008):
        reservation = await ledger.reserve(model="google/gemini-2.5-flash", estimated_prompt_tokens=100)
        await ledger.record(
            reservation, model="google/gemini-2.5-flash", prompt_tokens=100,
            completion_tokens=10, approximate_usage=False, failed=False,
            reported_cost_usd=cost,
        )

    snapshot = await ledger.snapshot()
    assert snapshot["spent_usd"] == pytest.approx(0.005)
    assert snapshot["request_count"] == 2
