import pytest

from common.exceptions import LLMBudgetExceededError
from common.schema.settings import LLMModelPricing, LLMSpendingBudgetSettings
from infrastructure.external_model_budget import ExternalModelSpendingLedger


def settings(limit=None, reset_key="", price=1):
    return LLMSpendingBudgetSettings(
        limit_usd=limit,
        reset_key=reset_key,
        fallback_pricing=LLMModelPricing(
            input_usd_per_million_tokens=price, output_usd_per_million_tokens=0
        ),
    )


async def reserve(ledger):
    return await ledger.reserve(
        model="synthetic", estimated_prompt_tokens=100, estimated_output_tokens=0
    )


async def record(ledger, reservation):
    await ledger.record(
        reservation,
        model="synthetic",
        prompt_tokens=100,
        completion_tokens=0,
        approximate_usage=False,
        failed=False,
    )


async def test_memory_reservation_is_settled_once_after_cap_and_price_change():
    ledger = ExternalModelSpendingLedger(settings())
    reservation = await reserve(ledger)
    assert reservation.storage == "memory"
    await ledger.update_settings(settings(limit=1, price=5))
    await record(ledger, reservation)
    await record(ledger, reservation)
    snapshot = await ledger.snapshot()
    assert snapshot["spent_usd"] == pytest.approx(0.0001)
    assert snapshot["reserved_usd"] == 0
    assert snapshot["request_count"] == 1


async def test_disabling_cap_keeps_inflight_reservations_and_existing_spend():
    ledger = ExternalModelSpendingLedger(settings(limit=1))
    first = await reserve(ledger)
    await record(ledger, first)
    second = await reserve(ledger)
    await ledger.update_settings(settings())
    await record(ledger, second)
    await ledger.update_settings(settings(limit=0.0002))
    with pytest.raises(LLMBudgetExceededError):
        await reserve(ledger)
    assert (await ledger.snapshot())["spent_usd"] == pytest.approx(0.0002)


async def test_old_memory_reservation_does_not_charge_new_reset_period():
    ledger = ExternalModelSpendingLedger(settings(reset_key="old"))
    old = await reserve(ledger)
    await ledger.update_settings(settings(reset_key="new"))
    await record(ledger, old)
    snapshot = await ledger.snapshot()
    assert snapshot["spent_usd"] == 0
    assert snapshot["reserved_usd"] == 0
    assert snapshot["request_count"] == 0
