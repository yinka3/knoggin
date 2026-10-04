import asyncio
import json
import time
from copy import deepcopy

import httpx
import pytest

from common.schema.jev import ChoiceQuestion, JevSettings, NoulQuestion
from common.schema.settings import LLMModelPricing, LLMSpendingBudgetSettings
from infrastructure.external_model_budget import ExternalModelSpendingLedger
from infrastructure.jev_client import JevClient, JevWorkBudget

QUESTIONS = {
    "type": ChoiceQuestion(
        instructions="Select type", criteria={"person": "A person", "none": "No entity"}
    ),
    "entity": NoulQuestion(instructions="This is an entity."),
}
RESPONSE = {
    "model": "jev-1.13.0",
    "answers": {
        "type": {
            "type": "choice",
            "choice": "person",
            "probabilities": {"person": 0.9, "none": 0.1},
            "confidence": 0.7,
        },
        "entity": {"type": "noul", "noul": 0.95},
    },
    "usage": {"input_tokens": 100, "output_tokens": 20},
}


def setup_client(handler, **updates):
    settings = JevSettings(api_key="private-key", extraction_mode="observe", **updates)
    ledger = ExternalModelSpendingLedger(
        LLMSpendingBudgetSettings(
            model_pricing={
                "jev-1.13.0": LLMModelPricing(
                    input_usd_per_million_tokens=0.042, output_usd_per_million_tokens=0
                )
            },
        )
    )
    client = JevClient(
        settings, spending_ledger=ledger, transport=httpx.MockTransport(handler)
    )
    return client, settings.capture_policy(), ledger


async def evaluate(client, policy, budget=None):
    return await client.evaluate(
        state={"text": "Alex"},
        questions=QUESTIONS,
        capability="extraction",
        policy=policy,
        work_budget=budget or JevWorkBudget(policy.max_calls_per_window),
    )


async def test_valid_choice_noul_and_shared_accounting():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json=RESPONSE)

    client, policy, ledger = setup_client(handler)
    result = await evaluate(client, policy)
    assert result.outcome == "available"
    assert result.response.answers["entity"].noul == 0.95
    assert result.input_tokens == 100
    assert result.cost_usd == pytest.approx(0.0000042)
    assert not result.approximate_usage
    assert json.loads(requests[0].content)["model"] == policy.model
    assert requests[0].headers["authorization"] == "Bearer private-key"
    snapshot = await ledger.snapshot()
    assert snapshot["reserved_usd"] == 0
    assert snapshot["spent_usd"] == pytest.approx(0.0000042)
    assert snapshot["request_count"] == 1
    await client.close()


async def test_disabled_missing_key_and_closed_make_no_requests():
    def handler(_):
        pytest.fail("No network work expected")

    client, policy, _ = setup_client(handler)
    assert client._client is None
    result = await evaluate(
        client, policy.model_copy(update={"extraction_mode": "disabled"})
    )
    assert result.reason == "disabled"
    client.update_settings(JevSettings(extraction_mode="observe"))
    assert (await evaluate(client, policy)).reason == "missing_api_key"
    await client.close()
    assert (await evaluate(client, policy)).reason == "closing"
    assert client._client is None


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_answer",
        "extra_answer",
        "wrong_type",
        "wrong_handle",
        "extra_option",
        "missing_option",
        "nan",
        "infinity",
        "negative",
        "above_one",
        "bad_sum",
        "wrong_winner",
        "wrong_model",
        "boolean_noul",
        "invalid_usage",
    ],
)
async def test_invalid_answers_fail_without_retry_and_record_usage(mutation):
    payload = deepcopy(RESPONSE)
    answer = payload["answers"]["type"]
    if mutation == "missing_answer":
        del payload["answers"]["entity"]
    elif mutation == "extra_answer":
        payload["answers"]["extra"] = {"type": "noul", "noul": 0.2}
    elif mutation == "wrong_type":
        payload["answers"]["entity"] = deepcopy(answer)
    elif mutation == "wrong_handle":
        answer["choice"] = "invented"
    elif mutation == "extra_option":
        answer["probabilities"]["invented"] = 0.0
    elif mutation == "missing_option":
        del answer["probabilities"]["none"]
    elif mutation in {"nan", "infinity", "negative", "above_one"}:
        answer["confidence"] = {
            "nan": float("nan"),
            "infinity": float("inf"),
            "negative": -0.1,
            "above_one": 1.1,
        }[mutation]
    elif mutation == "bad_sum":
        answer["probabilities"]["none"] = 0.5
    elif mutation == "wrong_winner":
        answer["choice"] = "none"
    elif mutation == "wrong_model":
        payload["model"] = "jev-1.14.0"
    elif mutation == "boolean_noul":
        payload["answers"]["entity"]["noul"] = True
    elif mutation == "invalid_usage":
        payload["usage"]["input_tokens"] = -1

    # HTTPX's JSON encoder rejects NaN, so simulate the raw malformed provider body.
    client, policy, ledger = setup_client(
        lambda _: httpx.Response(200, content=json.dumps(payload))
    )
    result = await evaluate(client, policy)
    assert result.reason == "invalid_response"
    assert result.attempts == 1
    snapshot = await ledger.snapshot()
    assert snapshot["request_count"] == 1
    assert snapshot["reserved_usd"] == 0
    await client.close()


@pytest.mark.parametrize("status", [401, 422, 500, 302])
async def test_nonretry_statuses_do_not_follow_redirects_or_retry(status):
    client, policy, ledger = setup_client(
        lambda _: httpx.Response(status, headers={"location": "https://example.com"})
    )
    result = await evaluate(client, policy)
    assert result.reason == f"http_{status}"
    assert result.attempts == 1
    assert (await ledger.snapshot())["reserved_usd"] == 0
    await client.close()


@pytest.mark.parametrize("status", [429, 529])
async def test_retry_is_bounded_and_charges_each_attempt(status):
    calls = []

    def handler(_):
        calls.append(1)
        return httpx.Response(status if len(calls) == 1 else 200, json=RESPONSE)

    client, policy, ledger = setup_client(handler)
    result = await evaluate(client, policy)
    assert result.outcome == "available"
    assert len(calls) == 2
    assert (await ledger.snapshot())["request_count"] == 2
    await client.close()


async def test_work_budget_shared_across_requests_and_retries():
    client, policy, ledger = setup_client(lambda _: httpx.Response(429))
    budget = JevWorkBudget(1)
    result = await evaluate(client, policy, budget)
    assert result.reason == "work_budget_exhausted"
    assert result.attempts == 1
    assert (await evaluate(client, policy, budget)).attempts == 0
    assert (await ledger.snapshot())["request_count"] == 1
    await client.close()


async def test_elapsed_work_budget_is_shared_across_requests():
    calls = 0

    async def handler(_):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01 if calls == 1 else 0.3)
        return httpx.Response(200, json=RESPONSE)

    client, policy, _ = setup_client(
        handler,
        request_timeout_seconds=0.5,
        total_timeout_seconds=0.5,
        max_elapsed_seconds_per_window=1.0,
        max_retries=0,
    )
    budget = JevWorkBudget(12, 1.0)

    assert (await evaluate(client, policy, budget)).outcome == "available"
    # Leave a short shared allowance without relying on the first call's
    # scheduling time, which varies under CI load.
    budget._started_at = time.monotonic() - 0.9
    second = await evaluate(client, policy, budget)

    assert second.reason == "work_budget_exhausted"
    await client.close()


async def test_openrouter_decisions_response_and_dated_model_are_supported():
    requests = []
    response = deepcopy(RESPONSE)
    response.update(
        {
            "model": "typesafe/jev-1.13-20260917",
            "id": "gen-dec-test",
            "provider": "TypeSafe",
        }
    )
    response["usage"]["cost"] = 0.0000042

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json=response)

    client, policy, _ = setup_client(
        handler,
        endpoint="https://openrouter.ai/api/alpha/decisions",
        model="typesafe/jev-1.13",
    )
    result = await evaluate(client, policy)

    assert result.outcome == "available"
    assert result.reported_model == "typesafe/jev-1.13-20260917"
    assert result.cost_usd == pytest.approx(0.0000042)
    assert str(requests[0].url) == "https://openrouter.ai/api/alpha/decisions"
    assert json.loads(requests[0].content)["model"] == "typesafe/jev-1.13"
    await client.close()


def test_observation_budget_is_independent_of_provider_calls():
    budget = JevWorkBudget(1, max_observations=2)

    assert budget.take_observation()
    assert budget.take_observation()
    assert not budget.take_observation()
    assert budget.calls == 0


async def test_spending_limit_is_shared_and_blocks_before_http():
    client, policy, ledger = setup_client(
        lambda _: pytest.fail("budget must block HTTP")
    )
    await ledger.update_settings(LLMSpendingBudgetSettings(limit_usd=0))
    assert (await evaluate(client, policy)).reason == "spending_budget_exhausted"
    assert client._client is None


async def test_unknown_pricing_does_not_claim_free_usage():
    client, policy, ledger = setup_client(lambda _: httpx.Response(200, json=RESPONSE))
    await ledger.update_settings(LLMSpendingBudgetSettings())
    result = await evaluate(client, policy)
    assert result.outcome == "available"
    assert result.cost_usd is None
    await client.close()


@pytest.mark.parametrize(
    "body", [b"invalid json", b"x" * 128_001], ids=["malformed", "oversized"]
)
async def test_invalid_or_oversized_response_is_unavailable(body):
    client, policy, ledger = setup_client(lambda _: httpx.Response(200, content=body))
    result = await evaluate(client, policy)
    assert result.reason == "invalid_response"
    assert result.approximate_usage
    assert (await ledger.snapshot())["reserved_usd"] == 0
    await client.close()


async def test_close_surfaces_failed_accounting_and_keeps_http_owner():
    from unittest.mock import AsyncMock

    entered = asyncio.Event()

    async def handler(_):
        entered.set()
        await asyncio.Event().wait()

    client, policy, ledger = setup_client(handler)
    original_record = ledger.record
    ledger.record = AsyncMock(side_effect=RuntimeError("accounting unavailable"))
    task = asyncio.create_task(evaluate(client, policy))
    await entered.wait()
    with pytest.raises(RuntimeError, match="cleanup failed"):
        await client.close()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert client._client is not None
    assert client._pending_accounting
    ledger.record = original_record
    await client.close()
    assert client._client is None
    assert not client._pending_accounting
    assert (await ledger.snapshot())["reserved_usd"] == 0


async def test_request_limits_and_unknown_pricing_skip_http():
    client, policy, ledger = setup_client(lambda _: pytest.fail("must skip HTTP"))
    assert (
        await evaluate(client, policy.model_copy(update={"max_request_bytes": 1}))
    ).reason == "request_limit"
    assert (
        await evaluate(client, policy.model_copy(update={"max_options_per_choice": 1}))
    ).reason == "invalid_questions"
    await ledger.update_settings(
        LLMSpendingBudgetSettings(
            limit_usd=1,
            model_pricing={
                "another-model": LLMModelPricing(
                    input_usd_per_million_tokens=1, output_usd_per_million_tokens=0
                )
            },
        )
    )
    assert (await evaluate(client, policy)).reason == "missing_model_pricing"
    assert client._client is None


async def test_deadline_honors_long_retry_after():
    client, policy, _ = setup_client(
        lambda _: httpx.Response(429, headers={"retry-after": "100"}),
        request_timeout_seconds=0.02,
        total_timeout_seconds=0.05,
    )
    assert (await evaluate(client, policy)).reason == "deadline_exceeded"
    await client.close()


@pytest.mark.parametrize("cancel_by_close", [False, True])
async def test_cancellation_propagates_and_settles_accounting(cancel_by_close):
    entered = asyncio.Event()

    async def handler(_):
        entered.set()
        await asyncio.Event().wait()

    client, policy, ledger = setup_client(handler)
    task = asyncio.create_task(evaluate(client, policy))
    await entered.wait()
    if cancel_by_close:
        await client.close()
    else:
        task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await client.close()
    snapshot = await ledger.snapshot()
    assert snapshot["reserved_usd"] == 0
    assert snapshot["request_count"] == 1
    await client.close()


async def test_broken_gzip_returns_unavailable_and_settles_usage():
    client, policy, ledger = setup_client(
        lambda _: httpx.Response(
            200, headers={"Content-Encoding": "gzip"}, content=b"not gzip"
        )
    )
    result = await evaluate(client, policy)
    assert result.reason == "invalid_response"
    assert result.attempts == 1
    assert result.approximate_usage
    assert (await ledger.snapshot())["reserved_usd"] == 0
    await client.close()


async def test_request_deadline_returns_while_accounting_stays_owned():
    entered, release = asyncio.Event(), asyncio.Event()
    client, policy, ledger = setup_client(
        lambda _: httpx.Response(200, json=RESPONSE),
        request_timeout_seconds=0.01,
        total_timeout_seconds=0.02,
        accounting_timeout_seconds=0.5,
    )
    original_record = ledger.record

    async def delayed_record(*args, **kwargs):
        entered.set()
        await release.wait()
        await original_record(*args, **kwargs)

    ledger.record = delayed_record
    task = asyncio.create_task(evaluate(client, policy))
    await entered.wait()
    try:
        done, _ = await asyncio.wait((task,), timeout=0.2)
        assert task in done
        result = task.result()
        assert result.reason == "deadline_exceeded"
        assert result.accounting_pending
        assert client._accounting_tasks
        assert (await ledger.snapshot())["reserved_usd"] > 0
    finally:
        release.set()
        await client.close()
    assert (await ledger.snapshot())["request_count"] == 1
    assert (await ledger.snapshot())["reserved_usd"] == 0


async def test_accounting_timeout_retains_reservation_for_retry():
    cancelled = asyncio.Event()
    client, policy, ledger = setup_client(
        lambda _: httpx.Response(200, json=RESPONSE),
        accounting_timeout_seconds=0.02,
    )
    original_record = ledger.record

    async def blocked_record(*args, **kwargs):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    ledger.record = blocked_record
    result = await evaluate(client, policy)
    assert result.reason == "accounting_unavailable"
    assert result.accounting_pending
    assert cancelled.is_set()
    assert (await ledger.snapshot())["reserved_usd"] > 0
    ledger.record = original_record
    await client.close()
    assert (await ledger.snapshot())["reserved_usd"] == 0
    assert (await ledger.snapshot())["request_count"] == 1


async def test_pending_recovery_blocks_new_provider_work_and_is_idempotent():
    calls = []

    def handler(_):
        calls.append(1)
        return httpx.Response(200, json=RESPONSE)

    client, policy, ledger = setup_client(handler)
    original_record = ledger.record

    async def ambiguous_completion(*args, **kwargs):
        await original_record(*args, **kwargs)
        raise RuntimeError("Completion acknowledgement lost")

    ledger.record = ambiguous_completion
    assert (await evaluate(client, policy)).reason == "accounting_unavailable"
    ledger.record = original_record
    assert (await evaluate(client, policy)).reason == "accounting_pending"
    assert len(calls) == 1
    await asyncio.gather(*tuple(client._accounting_tasks))
    assert (await ledger.snapshot())["request_count"] == 1
    assert (await evaluate(client, policy)).outcome == "available"
    assert len(calls) == 2
    assert (await ledger.snapshot())["request_count"] == 2
    await client.close()


async def test_shutdown_deadline_retains_noncooperative_accounting_owner():
    entered, release = asyncio.Event(), asyncio.Event()
    client, policy, ledger = setup_client(
        lambda _: httpx.Response(200, json=RESPONSE),
        accounting_timeout_seconds=0.01,
        shutdown_timeout_seconds=0.02,
    )
    original_record = ledger.record

    async def delayed_record(*args, **kwargs):
        entered.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            await release.wait()
        await original_record(*args, **kwargs)

    ledger.record = delayed_record
    task = asyncio.create_task(evaluate(client, policy))
    await entered.wait()
    try:
        closing = asyncio.create_task(client.close())
        done, _ = await asyncio.wait((closing,), timeout=0.2)
        assert closing in done
        with pytest.raises(RuntimeError, match="deadline exceeded"):
            closing.result()
        assert client._pending_accounting
        assert client._accounting_tasks
        assert client._client is not None
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        release.set()
        await asyncio.gather(*tuple(client._accounting_tasks))
        await client.close()
    assert (await ledger.snapshot())["request_count"] == 1


async def test_http_close_has_bounded_wait_and_retains_owner():
    from unittest.mock import AsyncMock

    release = asyncio.Event()
    client, policy, _ = setup_client(
        lambda _: httpx.Response(200, json=RESPONSE), shutdown_timeout_seconds=0.02
    )
    assert (await evaluate(client, policy)).outcome == "available"
    original_close = client._client.aclose

    async def delayed_close():
        await release.wait()
        await original_close()

    client._client.aclose = AsyncMock(side_effect=delayed_close)
    with pytest.raises(RuntimeError, match="deadline exceeded"):
        await client.close()
    assert client._close_task is not None
    assert client._client is not None
    release.set()
    await client.close()
    assert client._client is None


async def test_late_reservation_cannot_dispatch_provider_after_deadline():
    entered, release = asyncio.Event(), asyncio.Event()
    client, policy, ledger = setup_client(
        lambda _: pytest.fail("Expired request must never reach the provider"),
        request_timeout_seconds=0.01,
        total_timeout_seconds=0.02,
    )
    original_reserve = ledger.reserve

    async def delayed_reserve(*args, **kwargs):
        entered.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            await release.wait()
        return await original_reserve(*args, **kwargs)

    ledger.reserve = delayed_reserve
    task = asyncio.create_task(evaluate(client, policy))
    await entered.wait()
    try:
        done, _ = await asyncio.wait((task,), timeout=0.2)
        assert task in done
        assert task.result().reason == "deadline_exceeded"
        assert task.result().accounting_pending
        assert client._tasks
        assert (await evaluate(client, policy)).reason == "accounting_pending"
    finally:
        release.set()
        await client.close()
    snapshot = await ledger.snapshot()
    assert snapshot["spent_usd"] == 0
    assert snapshot["reserved_usd"] == 0


async def test_settings_update_keeps_inflight_credentials_and_next_call_uses_new_key():
    entered, release = asyncio.Event(), asyncio.Event()
    keys = []

    async def handler(request):
        keys.append(request.headers["authorization"])
        if len(keys) == 1:
            entered.set()
            await release.wait()
        return httpx.Response(200, json=RESPONSE)

    client, policy, _ = setup_client(handler)
    first = asyncio.create_task(evaluate(client, policy))
    await entered.wait()
    client.update_settings(JevSettings(api_key="new-key", extraction_mode="observe"))
    release.set()
    assert (await first).outcome == "available"
    assert (await evaluate(client, policy)).outcome == "available"
    assert keys == ["Bearer private-key", "Bearer new-key"]
    await client.close()
