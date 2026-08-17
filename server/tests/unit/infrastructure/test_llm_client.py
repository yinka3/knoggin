import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from openai import APIConnectionError
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
async def test_generate_structured_classifies_provider_failure(llm_service):
    service, _, instructor_client, _, _ = llm_service
    instructor_client.chat.completions.create_with_completion.side_effect = (
        APIConnectionError(request=httpx.Request("POST", "https://llm.test"))
    )

    with pytest.raises(LLMProviderError, match="provider request failed"):
        await service.generate_structured(
            response_model=DummyModel,
            system="system",
            user="user",
        )


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
def test_update_settings_swaps_client_and_supports_clearing_credentials(llm_service):
    service, raw_client, _, openai, from_openai = llm_service
    replacement_raw = MagicMock()
    replacement_raw.close = AsyncMock()
    replacement_instructor = MagicMock()
    openai.return_value = replacement_raw
    from_openai.return_value = replacement_instructor

    service.update_settings(
        LLMSettings(
            api_key="new-key",
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

    service.update_settings(
        LLMSettings(
            api_key="",
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
def test_update_settings_initializes_client_when_key_is_added():
    with (
        patch("infrastructure.llm_client.instructor.from_openai") as from_openai,
        patch("infrastructure.llm_client.AsyncOpenAI") as openai,
    ):
        raw_client = MagicMock()
        openai.return_value = raw_client
        service = LLMService(api_key="")

        service.update_settings(LLMSettings(api_key="added-later"))

    assert service.is_configured is True
    openai.assert_called_once()
    from_openai.assert_called_once_with(raw_client)


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
async def test_hot_reload_does_not_close_client_used_by_inflight_request(llm_service):
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
    service.update_settings(LLMSettings(api_key="replacement-key"))

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
        service.update_settings(LLMSettings(api_key="new-key"))


@pytest.mark.unit
@pytest.mark.no_network
def test_trace_output_is_bounded(llm_service):
    service, _, _, _, _ = llm_service
    service._trace_exchange("model", "u" * 25_000, "r" * 25_000)

    message = service._trace.debug.call_args.args[0]
    assert "characters omitted" in message
    assert len(message) < 41_000
