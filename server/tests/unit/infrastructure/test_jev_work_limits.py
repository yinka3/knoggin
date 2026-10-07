"""Optional model work cannot exceed admitted budgets or dispatch after closing."""

import pytest

from infrastructure.jev_client import JevWorkBudget
from tests.unit.infrastructure.test_jev_client import evaluate, setup_client


@pytest.mark.parametrize(
    "updates",
    [
        {"max_calls": 0},
        {"calls": -1},
        {"max_observations": 0},
        {"observations": -1},
        {"max_elapsed_seconds": 0},
        {"max_elapsed_seconds": float("nan")},
        {"max_elapsed_seconds": float("inf")},
    ],
)
def test_invalid_work_budgets_are_rejected(updates):
    with pytest.raises(ValueError, match="positive call and elapsed limits"):
        JevWorkBudget(**{"max_calls": 1, **updates})


@pytest.mark.parametrize("looser", ["calls", "elapsed"])
async def test_caller_cannot_relax_admitted_work_limits(looser):
    def forbidden(_request):
        pytest.fail("Invalid budgets must not dispatch HTTP")

    client, policy, ledger = setup_client(forbidden)
    budget = JevWorkBudget(
        policy.max_calls_per_window + int(looser == "calls"),
        policy.max_elapsed_seconds_per_window + int(looser == "elapsed"),
    )
    try:
        result = await evaluate(client, policy, budget)
        assert result.reason == "invalid_work_budget"
        assert budget.calls == 0
        assert (await ledger.snapshot())["request_count"] == 0
    finally:
        await client.close()


async def test_close_during_reservation_releases_usage_without_http(monkeypatch):
    def forbidden(_request):
        pytest.fail("Closing admission must not dispatch HTTP")

    client, policy, ledger = setup_client(forbidden)
    reserve = ledger.reserve

    async def close_after_reserve(**kwargs):
        reservation = await reserve(**kwargs)
        client._closing = True
        return reservation

    monkeypatch.setattr(ledger, "reserve", close_after_reserve)
    try:
        result = await evaluate(client, policy)
        assert result.reason == "closing"
        assert result.attempts == 0
        assert result.input_tokens == result.output_tokens == 0
        assert (await ledger.snapshot())["reserved_usd"] == 0
    finally:
        await client.close()
