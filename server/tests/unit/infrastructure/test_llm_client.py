import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import instructor
import pytest
from openai import APIConnectionError, BadRequestError
from pydantic import BaseModel

from common.exceptions import (
    ConfigurationError,
    LLMBudgetExceededError,
    LLMProviderError,
    LLMResponseError,
)
from common.schema.settings import (
    LLMModelPricing,
    LLMSettings,
    LLMSpendingBudgetSettings,
)
from infrastructure.llm_client import (
    TRANSPORT_RETRIES,
    LLMService,
)


class DummyModel(BaseModel):
    name: str


async def test_jev_and_llm_use_one_spending_balance(llm_service, monkeypatch):
    from common.schema.jev import JevSettings, NoulQuestion
    from infrastructure.jev_client import JevClient, JevWorkBudget

    service, raw_client, *_ = llm_service
    await service.spending_ledger.update_settings(LLMSpendingBudgetSettings(
        fallback_pricing=LLMModelPricing(input_usd_per_million_tokens=0.042, output_usd_per_million_tokens=0),
    ))
    raw_client.chat.completions.create.return_value = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content="ok"))],
        usage=SimpleNamespace(prompt_tokens=100, completion_tokens=20),
    )
    assert await service.generate_text(system="system", user="user") == "ok"
    monkeypatch.setenv("JEV_API_KEY", "fake")
    settings = JevSettings(identity_mode="observe")
    client = JevClient(settings, spending_ledger=service.spending_ledger,
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json={
            "model": "jev-1.13.0", "answers": {"support": {"type": "noul", "noul": 0.8}},
            "usage": {"input_tokens": 100, "output_tokens": 20},
        })))
    result = await client.evaluate(state="Synthetic input", questions={"support": NoulQuestion(instructions="Evidence is sufficient.")},
        capability="identity", policy=settings.capture_policy(), work_budget=JevWorkBudget(1))
    assert result.outcome == "available"
    snapshot = await service.spending_snapshot()
    assert snapshot["request_count"] == 2
    assert snapshot["spent_usd"] == pytest.approx(0.0000084)
    await client.close()


async def test_failed_client_close_retains_handle_for_retry(llm_service):
    service, raw, *_ = llm_service
    raw.close.side_effect = [RuntimeError("close failed"), None]
    with pytest.raises(RuntimeError, match="cleanup failed"):
        await service.close()
    assert raw in service._retired_clients
    await service.close()
    assert raw not in service._retired_clients
    assert raw.close.await_count == 2


async def async_chunks(*chunks):
    for chunk in chunks:
        yield chunk


def stream_chunk(*, content=None, tool_calls=None, usage=None, chunk_id="response-1"):
    choices = []
    if content is not None or tool_calls is not None:
        choices = [
            SimpleNamespace(
                delta=SimpleNamespace(
                    content=content,
                    tool_calls=tool_calls,
                    reasoning_details=None,
                )
            )
        ]
    return SimpleNamespace(id=chunk_id, choices=choices, usage=usage)


@pytest.fixture
def llm_service():
    with (
        patch("infrastructure.llm_client.instructor.from_openai") as from_openai,
        patch("infrastructure.llm_client.AsyncOpenAI") as openai,
    ):
        raw_client = MagicMock()
        raw_client.chat.completions.create = AsyncMock()
        raw_client.close = AsyncMock()
        openai.return_value = raw_client

        instructor_client = MagicMock()
        instructor_client.chat.completions.create_with_completion = AsyncMock()
        from_openai.return_value = instructor_client

        service = LLMService(
            api_key="test-key",
            agent_model="agent-v1",
            extraction_model="extract-v1",
            merge_model="merge-v1",
            base_url="https://test.com",
            trace_logger=MagicMock(),
        )
        yield service, raw_client, instructor_client, openai, from_openai


@pytest.mark.unit
@pytest.mark.no_network
async def test_generate_structured_uses_instructor_validation_retries(llm_service):
    service, _, instructor_client, _, _ = llm_service
    completion = MagicMock()
    instructor_client.chat.completions.create_with_completion.return_value = (
        DummyModel(name="test"),
        completion,
    )

    result = await service.generate_structured(
        system="System prompt",
        user="User prompt",
        response_model=DummyModel,
        temperature=0.5,
    )

    assert result.name == "test"
    kwargs = instructor_client.chat.completions.create_with_completion.call_args.kwargs
    assert kwargs["model"] == "extract-v1"
    assert kwargs["temperature"] == 0.5
    assert kwargs["response_model"] is DummyModel
    # The service owns validation retries so every provider attempt can be
    # budgeted separately.
    assert kwargs["max_retries"] == 0
    assert len(kwargs["messages"]) == 2


@pytest.mark.unit
@pytest.mark.no_network
async def test_generate_structured_classifies_provider_failure(llm_service, monkeypatch):
    service, _, instructor_client, _, _ = llm_service
    monkeypatch.setattr("infrastructure.llm_client.asyncio.sleep", AsyncMock())
    instructor_client.chat.completions.create_with_completion.side_effect = (
        APIConnectionError(request=httpx.Request("POST", "https://llm.test"))
    )

    with pytest.raises(LLMProviderError, match="provider request failed"):
        await service.generate_structured(
            response_model=DummyModel,
            system="system",
            user="user",
        )
    assert (
        instructor_client.chat.completions.create_with_completion.await_count
        == TRANSPORT_RETRIES
    )


@pytest.mark.unit
@pytest.mark.no_network
async def test_generate_structured_retries_transient_provider_failure(
    llm_service, monkeypatch
):
    service, _, instructor_client, _, _ = llm_service
    monkeypatch.setattr("infrastructure.llm_client.asyncio.sleep", AsyncMock())
    instructor_client.chat.completions.create_with_completion.side_effect = [
        APIConnectionError(request=httpx.Request("POST", "https://llm.test")),
        (DummyModel(name="recovered"), MagicMock()),
    ]

    result = await service.generate_structured(
        response_model=DummyModel, system="system", user="user",
    )

    assert result.name == "recovered"
    assert instructor_client.chat.completions.create_with_completion.await_count == 2


@pytest.mark.unit
@pytest.mark.no_network
async def test_generate_structured_does_not_retry_client_request_errors(
    llm_service, monkeypatch
):
    service, _, instructor_client, _, _ = llm_service
    monkeypatch.setattr("infrastructure.llm_client.asyncio.sleep", AsyncMock())
    request = httpx.Request("POST", "https://llm.test")
    instructor_client.chat.completions.create_with_completion.side_effect = (
        BadRequestError(
            "context too long", response=httpx.Response(400, request=request), body=None
        )
    )

    with pytest.raises(LLMProviderError):
        await service.generate_structured(
            response_model=DummyModel, system="system", user="user",
        )
    assert instructor_client.chat.completions.create_with_completion.await_count == 1


@pytest.mark.unit
@pytest.mark.no_network
async def test_generate_text_uses_raw_client_without_instructor_arguments(llm_service):
    service, raw_client, instructor_client, _, _ = llm_service
    raw_client.chat.completions.create.return_value = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content="plain response"))]
    )

    result = await service.generate_text(
        system="System prompt",
        user="User prompt",
        temperature=0.2,
    )

    assert result == "plain response"
    kwargs = raw_client.chat.completions.create.call_args.kwargs
    assert kwargs["model"] == "extract-v1"
    assert kwargs["temperature"] == 0.2
    assert "max_retries" not in kwargs
    assert "response_model" not in kwargs
    instructor_client.chat.completions.create_with_completion.assert_not_called()


@pytest.mark.unit
@pytest.mark.no_network
async def test_global_spending_budget_records_usage_then_blocks_later_calls(llm_service):
    service, raw_client, _, _, _ = llm_service
    await service._spending_ledger.update_settings(
        LLMSpendingBudgetSettings(
            limit_usd=0.000001,
            fallback_pricing=LLMModelPricing(
                input_usd_per_million_tokens=1.0,
                output_usd_per_million_tokens=1.0,
            ),
        )
    )
    raw_client.chat.completions.create.return_value = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content="ok"))]
    )

    assert await service.generate_text(system="system", user="user") == "ok"

    with pytest.raises(LLMBudgetExceededError, match="budget has been reached"):
        await service.generate_text(system="system", user="user")

    snapshot = await service.spending_snapshot()
    assert snapshot["enforced"] is True
    assert snapshot["request_count"] == 1
    assert snapshot["spent_usd"] > snapshot["configured_limit_usd"]
    assert raw_client.chat.completions.create.await_count == 1


@pytest.mark.unit
@pytest.mark.no_network
async def test_budgeted_call_rejects_an_unpriced_model_before_provider_request(
    llm_service,
):
    service, raw_client, _, _, _ = llm_service
    await service._spending_ledger.update_settings(
        LLMSpendingBudgetSettings(
            limit_usd=1.0,
            model_pricing={
                "agent-v1": LLMModelPricing(
                    input_usd_per_million_tokens=1.0,
                    output_usd_per_million_tokens=1.0,
                )
            },
        )
    )

    with pytest.raises(ConfigurationError, match="no price for model 'extract-v1'"):
        await service.generate_text(system="system", user="user")

    raw_client.chat.completions.create.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.no_network
def test_positive_spending_limit_requires_user_supplied_pricing():
    with pytest.raises(ValueError, match="requires model_pricing"):
        LLMSpendingBudgetSettings(limit_usd=10.0)


@pytest.mark.unit
@pytest.mark.no_network
async def test_generate_text_retries_transport_failures_once_per_service_policy(
    llm_service,
    monkeypatch,
):
    service, raw_client, _, _, _ = llm_service
    raw_client.chat.completions.create.side_effect = RuntimeError("provider down")

    async def no_sleep(_delay):
        return None

    monkeypatch.setattr("infrastructure.llm_client.asyncio.sleep", no_sleep)

    with pytest.raises(LLMProviderError, match="failed after retries"):
        await service.generate_text(system="system", user="user")

    assert raw_client.chat.completions.create.call_count == TRANSPORT_RETRIES


@pytest.mark.unit
@pytest.mark.no_network
async def test_generate_text_rejects_empty_provider_response(llm_service):
    service, raw_client, _, _, _ = llm_service
    raw_client.chat.completions.create.return_value = SimpleNamespace(choices=[])

    with pytest.raises(LLMResponseError, match="no completion choices"):
        await service.generate_text(system="system", user="user")


@pytest.mark.unit
@pytest.mark.no_network
async def test_stream_with_tools_preserves_ids_and_exact_usage(llm_service):
    service, raw_client, _, _, _ = llm_service
    tool_call = SimpleNamespace(
        index=0,
        id="call-1",
        function=SimpleNamespace(name="search_messages", arguments='{"query":"x"}'),
    )
    usage = SimpleNamespace(
        prompt_tokens=10,
        completion_tokens=4,
        total_tokens=14,
    )
    raw_client.chat.completions.create.return_value = async_chunks(
        stream_chunk(content="Need evidence. "),
        stream_chunk(tool_calls=[tool_call]),
        stream_chunk(usage=usage),
    )

    events = [
        event
        async for event in service.stream_with_tools(
            system="system",
            user="user",
            tools=[],
        )
    ]

    assert all(set(event) == {"event", "data"} for event in events)
    assert events[-2]["data"]["calls"] == [
        {
            "id": "call-1",
            "name": "search_messages",
            "arguments": '{"query":"x"}',
        }
    ]
    assert events[-1] == {
        "event": "step_completed",
        "data": {
            "content": "Need evidence. ",
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 4,
                "total_tokens": 14,
                "cached_prompt_tokens": 0,
                "cache_write_tokens": 0,
                "approximate": False,
            },
        },
    }


@pytest.mark.unit
@pytest.mark.no_network
async def test_stream_with_tools_marks_estimated_usage(llm_service):
    service, raw_client, _, _, _ = llm_service
    service._tokenizer = None
    raw_client.chat.completions.create.return_value = async_chunks(
        stream_chunk(content="plain text")
    )

    events = [
        event
        async for event in service.stream_with_tools(
            system="system",
            user="user",
            tools=[],
        )
    ]

    assert events[-1]["event"] == "step_completed"
    assert events[-1]["data"]["usage"]["approximate"] is True


@pytest.mark.unit
@pytest.mark.no_network
async def test_stream_with_tools_rejects_incomplete_calls(
    llm_service,
):
    service, raw_client, _, _, _ = llm_service
    incomplete_call = SimpleNamespace(
        index=0,
        id=None,
        function=SimpleNamespace(name=None, arguments="{}"),
    )
    raw_client.chat.completions.create.return_value = async_chunks(
        stream_chunk(tool_calls=[incomplete_call])
    )

    events = [
        event
        async for event in service.stream_with_tools(
            system="system",
            user="user",
            tools=[],
        )
    ]

    assert events == [
        {
            "event": "step_error",
            "data": {
                "kind": "provider",
                "message": "LLM returned incomplete tool calls at indexes [0]",
            },
        }
    ]


@pytest.mark.unit
@pytest.mark.no_network
async def test_stream_retries_only_before_output(llm_service, monkeypatch):
    service, raw_client, _, _, _ = llm_service
    raw_client.chat.completions.create.side_effect = [
        RuntimeError("connect failed"),
        async_chunks(stream_chunk(content="recovered")),
    ]

    async def no_sleep(_delay):
        return None

    monkeypatch.setattr("infrastructure.llm_client.asyncio.sleep", no_sleep)
    events = [
        event
        async for event in service.stream_with_tools(
            system="system",
            user="user",
            tools=[],
        )
    ]

    assert raw_client.chat.completions.create.call_count == 2
    assert events[-1]["event"] == "step_completed"


@pytest.mark.unit
@pytest.mark.no_network
async def test_stream_does_not_retry_after_output(llm_service):
    service, raw_client, _, _, _ = llm_service

    async def interrupted_stream():
        yield stream_chunk(content="partial")
        raise RuntimeError("connection dropped")

    raw_client.chat.completions.create.return_value = interrupted_stream()
    events = [
        event
        async for event in service.stream_with_tools(
            system="system",
            user="user",
            tools=[],
        )
    ]

    assert raw_client.chat.completions.create.call_count == 1
    assert events[-1] == {
        "event": "step_error",
        "data": {
            "kind": "provider",
            "message": "Stream interrupted mid-generation: connection dropped",
        },
    }


@pytest.mark.unit
@pytest.mark.no_network
def test_update_settings_swaps_client_and_supports_clearing_credentials(
    llm_service, monkeypatch
):
    service, raw_client, _, openai, from_openai = llm_service
    replacement_raw = MagicMock()
    replacement_raw.close = AsyncMock()
    replacement_instructor = MagicMock()
    openai.return_value = replacement_raw
    from_openai.return_value = replacement_instructor

    monkeypatch.setenv("OPENROUTER_API_KEY", "new-key")
    service.update_settings(
        LLMSettings(
            base_url="https://new.test.com",
            agent_model="new-agent",
            extraction_model="new-extract",
            merge_model="new-merge",
        )
    )

    assert service._raw_client is replacement_raw
    assert service._client is replacement_instructor
    assert raw_client in service._retired_clients
    assert service.agent_model == "new-agent"
    assert service.extraction_model == "new-extract"
    assert service.merge_model == "new-merge"
    assert openai.call_count == 2
    assert from_openai.call_count == 2

    monkeypatch.delenv("OPENROUTER_API_KEY")
    service.update_settings(
        LLMSettings(
            base_url=None,
            agent_model="new-agent",
            extraction_model="new-extract",
            merge_model="new-merge",
        )
    )

    assert service.is_configured is False
    assert replacement_raw in service._retired_clients
    with pytest.raises(ConfigurationError):
        service._get_clients()


@pytest.mark.unit
@pytest.mark.no_network
def test_update_settings_initializes_client_when_key_is_added(monkeypatch):
    with (
        patch("infrastructure.llm_client.instructor.from_openai") as from_openai,
        patch("infrastructure.llm_client.AsyncOpenAI") as openai,
    ):
        raw_client = MagicMock()
        openai.return_value = raw_client
        service = LLMService(api_key="")

        monkeypatch.setenv("OPENROUTER_API_KEY", "added-later")
        service.update_settings(LLMSettings())

    assert service.is_configured is True
    openai.assert_called_once()
    from_openai.assert_called_once_with(raw_client, mode=instructor.Mode.JSON)


@pytest.mark.unit
@pytest.mark.no_network
async def test_close_closes_only_owned_raw_client(llm_service):
    service, raw_client, _, _, _ = llm_service

    await service.close()
    await service.close()

    raw_client.close.assert_awaited_once()
    assert service.is_configured is False


@pytest.mark.unit
@pytest.mark.no_network
async def test_hot_reload_does_not_close_client_used_by_inflight_request(
    llm_service, monkeypatch
):
    service, raw_client, _, openai, from_openai = llm_service
    request_started = asyncio.Event()
    release_request = asyncio.Event()

    async def delayed_completion(**_kwargs):
        request_started.set()
        await release_request.wait()
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content="old client"))]
        )

    raw_client.chat.completions.create.side_effect = delayed_completion
    request = asyncio.create_task(service.generate_text(system="system", user="user"))
    await request_started.wait()

    replacement_raw = MagicMock()
    replacement_raw.close = AsyncMock()
    openai.return_value = replacement_raw
    from_openai.return_value = MagicMock()
    monkeypatch.setenv("OPENROUTER_API_KEY", "replacement-key")
    service.update_settings(LLMSettings())

    raw_client.close.assert_not_awaited()
    release_request.set()
    assert await request == "old client"
    await asyncio.sleep(0)
    raw_client.close.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.no_network
async def test_closed_service_rejects_requests_and_updates(llm_service):
    service, _, _, _, _ = llm_service
    await service.close()

    with pytest.raises(ConfigurationError, match="closed"):
        await service.generate_text(system="system", user="user")
    with pytest.raises(RuntimeError, match="closed"):
        service.update_settings(LLMSettings())


@pytest.mark.unit
@pytest.mark.no_network
def test_trace_output_is_bounded(llm_service):
    service, _, _, _, _ = llm_service
    service._trace_exchange("model", "u" * 25_000, "r" * 25_000)

    message = service._trace.debug.call_args.args[0]
    assert "characters omitted" in message
    assert len(message) < 41_000


@pytest.mark.unit
@pytest.mark.no_network
async def test_generate_structured_runs_through_real_instructor(monkeypatch):
    """Mocked Instructor clients cannot catch kwargs the real library rejects."""
    import json

    from openai import AsyncOpenAI as RealAsyncOpenAI

    requests = []

    def respond(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={
            "id": "chatcmpl-1", "object": "chat.completion", "created": 0,
            "model": "local-model",
            "choices": [{"index": 0, "finish_reason": "stop", "message": {
                "role": "assistant", "content": '{"name": "Alice"}'}}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8},
        })

    def openai_over_fake_transport(**kwargs):
        return RealAsyncOpenAI(
            **kwargs, http_client=httpx.AsyncClient(transport=httpx.MockTransport(respond))
        )

    monkeypatch.setattr("infrastructure.llm_client.AsyncOpenAI", openai_over_fake_transport)
    service = LLMService(
        api_key="local", base_url="http://localhost:11434/v1",
        agent_model="local-model", extraction_model="local-model", merge_model="local-model",
    )

    result = await service.generate_structured(
        response_model=DummyModel, system="Extract.", user="Alice is here.",
    )

    assert result.name == "Alice"
    assert requests[0]["model"] == "local-model"
    await service.close()


@pytest.mark.unit
@pytest.mark.no_network
def test_local_provider_needs_no_api_key_but_openrouter_does():
    with (
        patch("infrastructure.llm_client.instructor.from_openai"),
        patch("infrastructure.llm_client.AsyncOpenAI") as openai,
    ):
        local = LLMService(api_key="", base_url="http://localhost:11434/v1")
        hosted = LLMService(api_key="")

    assert local.is_configured
    assert openai.call_args.kwargs["api_key"] == "no-key-required"
    assert not hosted.is_configured


@pytest.mark.unit
@pytest.mark.no_network
async def test_wrapped_provider_timeout_is_a_transient_transport_failure(
    llm_service, monkeypatch
):
    from instructor.core.exceptions import FailedAttempt, InstructorRetryException
    from openai import APITimeoutError

    service, _, instructor_client, _, _ = llm_service
    monkeypatch.setattr("infrastructure.llm_client.asyncio.sleep", AsyncMock())
    timeout = APITimeoutError(request=httpx.Request("POST", "https://llm.test"))
    wrapped = InstructorRetryException(
        "Request timed out.", n_attempts=1, total_usage=0,
        failed_attempts=[FailedAttempt(1, timeout, None)],
    )
    instructor_client.chat.completions.create_with_completion.side_effect = wrapped

    with pytest.raises(LLMProviderError) as caught:
        await service.generate_structured(
            response_model=DummyModel, system="system", user="user",
        )

    assert caught.value.transient is True
    # Retried as transport, not as invalid output.
    assert (
        instructor_client.chat.completions.create_with_completion.await_count
        == TRANSPORT_RETRIES
    )


@pytest.mark.unit
@pytest.mark.no_network
async def test_cached_prompt_tokens_are_reported_per_model(llm_service):
    service, raw_client, _, _, _ = llm_service
    raw_client.chat.completions.create.return_value = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content="ok"))],
        usage=SimpleNamespace(
            prompt_tokens=1000, completion_tokens=10,
            prompt_tokens_details=SimpleNamespace(cached_tokens=900),
        ),
    )

    await service.generate_text(system="system", user="user", model="m1")
    await service.generate_text(system="system", user="user", model="m1")

    # Gemini-style usage: a write is also counted inside cached_tokens.
    raw_client.chat.completions.create.return_value = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content="ok"))],
        usage=SimpleNamespace(
            prompt_tokens=1000, completion_tokens=10,
            prompt_tokens_details=SimpleNamespace(cached_tokens=1000, cache_write_tokens=1000),
        ),
    )
    await service.generate_text(system="system", user="user", model="m1")

    cache = (await service.spending_snapshot())["prompt_cache"]
    assert cache["m1"] == {
        "requests": 3, "prompt_tokens": 3000, "cached_prompt_tokens": 1800,
        "cache_write_tokens": 1000, "hit_rate": 0.6,
    }


@pytest.mark.unit
@pytest.mark.no_network
async def test_silently_truncated_prompt_fails_loudly(llm_service):
    from common.exceptions import LLMContextTruncatedError

    service, raw_client, instructor_client, _, _ = llm_service
    long_prompt = "word " * 5_000
    # A local server with a small context reports far fewer prompt tokens.
    instructor_client.chat.completions.create_with_completion.return_value = (
        DummyModel(name="partial"),
        SimpleNamespace(usage=SimpleNamespace(prompt_tokens=2_048, completion_tokens=5)),
    )
    with pytest.raises(LLMContextTruncatedError, match="OLLAMA_CONTEXT_LENGTH"):
        await service.generate_structured(
            response_model=DummyModel, system=long_prompt, user="user",
        )

    raw_client.chat.completions.create.return_value = async_chunks(
        stream_chunk(content="partial"),
        stream_chunk(usage=SimpleNamespace(prompt_tokens=2_048, completion_tokens=1, total_tokens=2_049)),
    )
    events = [
        event async for event in service.stream_with_tools(
            system=long_prompt, user="user", tools=[],
        )
    ]
    assert events[-1]["event"] == "step_error"
    assert events[-1]["data"]["code"] == "provider_context_limit"
    assert raw_client.chat.completions.create.call_args.kwargs["stream_options"] == {
        "include_usage": True
    }


@pytest.mark.unit
@pytest.mark.no_network
def test_local_thinking_switch_maps_to_reasoning_effort():
    with (
        patch("infrastructure.llm_client.instructor.from_openai"),
        patch("infrastructure.llm_client.AsyncOpenAI"),
    ):
        local = LLMService(base_url="http://localhost:11434/v1", thinking="planning_only")
        hosted = LLMService(api_key="key", thinking="off")
        full = LLMService(base_url="http://localhost:11434/v1", thinking="provider_default")

    assert local._provider_extra_body("high") is None
    assert local._provider_extra_body("medium") == {"reasoning_effort": "none"}
    assert local._provider_extra_body(None) == {"reasoning_effort": "none"}
    # OpenRouter keeps its own reasoning budgets.
    assert "reasoning_effort" not in hosted._provider_extra_body("medium")
    assert full._provider_extra_body("medium") is None


@pytest.mark.unit
@pytest.mark.no_network
def test_local_models_are_fast_by_default_and_full_thinking_is_warned():
    from common.schema.settings import LLMSettings
    from infrastructure.llm_client import local_thinking_warning

    assert LLMSettings().thinking == "planning_only"
    local = "http://localhost:11434/v1"
    assert local_thinking_warning(local, "planning_only") is None
    assert "slower" in local_thinking_warning(local, "provider_default")
    # OpenRouter is unaffected by local thinking control.
    assert local_thinking_warning(None, "provider_default") is None
