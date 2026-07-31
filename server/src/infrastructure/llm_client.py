import asyncio
from contextlib import asynccontextmanager
from typing import (
    Any,
    AsyncIterator,
    Dict,
    List,
    Optional,
    TypeVar,
)

import instructor
import tiktoken
from instructor.core import AsyncInstructor, InstructorRetryException
from loguru import logger
from openai import (
    APIConnectionError,
    APIStatusError,
    APITimeoutError,
    AsyncOpenAI,
    RateLimitError,
)

from common.exceptions import (
    ConfigurationError,
    LLMProviderError,
    LLMResponseError,
)
from common.schema.agent_stream import (
    InternalAgentStreamEvent,
    StreamToolCall,
    StreamUsage,
)
from common.schema.settings import LLMSettings

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
TRANSPORT_RETRIES = 3
VALIDATION_RETRIES = 3
TRACE_MAX_CHARS = 20_000
CLIENT_SHUTDOWN_TIMEOUT = 5.0
ResponseT = TypeVar("ResponseT")


class LLMService:
    def __init__(
        self,
        api_key: str = None,
        base_url: str = None,
        trace_logger: Optional[Any] = None,
        agent_model: str = "google/gemini-3-flash-preview",
        extraction_model: str = "google/gemini-2.5-flash",
        merge_model: str = "google/gemini-2.5-pro",
    ):
        self._api_key = (api_key or "").strip()
        self._base_url = base_url or OPENROUTER_BASE_URL
        self._is_openrouter = "openrouter.ai" in self._base_url
        self._trace = trace_logger
        self._agent_model = agent_model
        self._extraction_model = extraction_model
        self._merge_model = merge_model
        self._client = None
        self._raw_client = None
        self._tokenizer = None
        self._retired_clients: set[AsyncOpenAI] = set()
        self._retirement_tasks: dict[AsyncOpenAI, asyncio.Task] = {}
        self._client_usage: dict[AsyncOpenAI, int] = {}
        self._active_requests: set[asyncio.Task] = set()
        self._closed = False

        if self._api_key:
            self._raw_client, self._client = self._build_clients(
                self._api_key,
                self._base_url,
            )

            provider_label = "OpenRouter" if self._is_openrouter else self._base_url
            logger.info(
                f"LLMService initialized ({provider_label}) | "
                f"extraction={extraction_model} | merge={merge_model} | "
                f"agent={agent_model}"
            )
        else:
            logger.warning("LLMService initialized without API key")

    @staticmethod
    def _build_clients(
        api_key: str,
        base_url: str,
    ) -> tuple[AsyncOpenAI, AsyncInstructor]:
        client = AsyncOpenAI(
            base_url=base_url,
            api_key=api_key,
            timeout=60.0,
            max_retries=0,
        )
        return client, instructor.from_openai(client)

    async def load_tokenizer(self):
        """Load tiktoken encoding for token estimation."""
        if self._tokenizer:
            return

        try:
            self._tokenizer = tiktoken.get_encoding("cl100k_base")
            logger.info("Tiktoken loaded (cl100k_base)")
        except Exception as e:
            logger.warning(f"Failed to load tiktoken: {e}")

    @property
    def agent_model(self) -> str:
        return self._agent_model

    @property
    def extraction_model(self) -> str:
        return self._extraction_model

    @property
    def merge_model(self) -> str:
        return self._merge_model

    @property
    def is_configured(self) -> bool:
        return bool(
            not self._closed and self._api_key and self._client and self._raw_client
        )

    def _get_clients(self) -> tuple[AsyncOpenAI, AsyncInstructor]:
        if self._closed:
            raise ConfigurationError("LLM service has been closed")
        if not self.is_configured:
            raise ConfigurationError(
                "LLM API key not configured. "
                "Please add your API key in Settings > Configuration."
            )
        return self._raw_client, self._client

    def _retire_client(self, client: Optional[AsyncOpenAI]) -> None:
        if client is None:
            return
        self._retired_clients.add(client)
        if self._client_usage.get(client, 0) > 0:
            return
        self._schedule_client_retirement(client)

    def _schedule_client_retirement(self, client: AsyncOpenAI) -> None:
        if client in self._retirement_tasks:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        task = loop.create_task(
            self._close_retired_client(client),
            name="llm-client-retirement",
        )
        self._retirement_tasks[client] = task
        task.add_done_callback(
            lambda _task, retired=client: self._retirement_tasks.pop(
                retired,
                None,
            )
        )

    async def _close_retired_client(self, client: AsyncOpenAI) -> None:
        try:
            await client.close()
        except Exception as exc:
            logger.warning(f"Failed to close retired LLM client: {exc}")
        finally:
            self._retired_clients.discard(client)

    @asynccontextmanager
    async def _client_snapshot(self):
        raw_client, instructor_client = self._get_clients()
        self._client_usage[raw_client] = self._client_usage.get(raw_client, 0) + 1
        request_task = asyncio.current_task()
        if request_task is not None:
            self._active_requests.add(request_task)
        try:
            yield raw_client, instructor_client
        finally:
            remaining = self._client_usage.get(raw_client, 1) - 1
            if remaining > 0:
                self._client_usage[raw_client] = remaining
            else:
                self._client_usage.pop(raw_client, None)
                if raw_client in self._retired_clients:
                    self._schedule_client_retirement(raw_client)
            if request_task is not None:
                self._active_requests.discard(request_task)

    def update_settings(self, settings: LLMSettings) -> None:
        if self._closed:
            raise RuntimeError("Cannot update a closed LLM service")

        api_key = settings.api_key.strip()
        base_url = settings.base_url or OPENROUTER_BASE_URL
        connection_changed = api_key != self._api_key or base_url != self._base_url

        replacement_raw = self._raw_client
        replacement_instructor = self._client
        if connection_changed:
            if api_key:
                replacement_raw, replacement_instructor = self._build_clients(
                    api_key,
                    base_url,
                )
            else:
                replacement_raw = None
                replacement_instructor = None

        previous_raw = self._raw_client
        self._api_key = api_key
        self._base_url = base_url
        self._is_openrouter = "openrouter.ai" in self._base_url

        if connection_changed:
            self._raw_client = replacement_raw
            self._client = replacement_instructor
            self._retire_client(previous_raw)
            logger.info(f"LLMService: API configuration updated ({self._base_url})")

        if settings.agent_model != self._agent_model:
            logger.info(
                f"LLMService: agent model {self._agent_model} -> {settings.agent_model}"
            )
            self._agent_model = settings.agent_model

        if settings.extraction_model != self._extraction_model:
            logger.info(
                f"LLMService: extraction model {self._extraction_model} -> "
                f"{settings.extraction_model}"
            )
            self._extraction_model = settings.extraction_model

        if settings.merge_model != self._merge_model:
            logger.info(
                f"LLMService: merge model {self._merge_model} -> {settings.merge_model}"
            )
            self._merge_model = settings.merge_model

    @staticmethod
    def _openrouter_extra_body(
        reasoning: Optional[str] = "low",
    ) -> Dict[str, Any]:
        """Extra body parameters for OpenRouter."""
        body: Dict[str, Any] = {
            "provider": {"require_parameters": True},
            "cache_control": {"type": "ephemeral"},
        }
        if reasoning == "high":
            body["reasoning"] = {"max_tokens": 4096}
        elif reasoning == "medium":
            body["reasoning"] = {"max_tokens": 1024}
        elif reasoning not in (None, "low"):
            raise ValueError(f"Unsupported reasoning level: {reasoning}")
        return body

    def count_tokens(self, text: str) -> int:
        if not text:
            return 0
        if not self._tokenizer:
            # Fallback to rough estimation if tokenizer failed to load
            return len(text) // 4
        return len(self._tokenizer.encode(text))

    def _trace_exchange(self, model: str, user: str, response: Any) -> None:
        if not self._trace:
            return

        def bounded(value: Any) -> str:
            text = str(value)
            if len(text) <= TRACE_MAX_CHARS:
                return text
            omitted = len(text) - TRACE_MAX_CHARS
            return f"{text[:TRACE_MAX_CHARS]}\n...[{omitted} characters omitted]"

        self._trace.debug(
            f"MODEL: {model}\nUSER:\n{bounded(user)}\nRESPONSE:\n{bounded(response)}"
        )

    async def generate_text(
        self,
        system: str,
        user: str,
        model: Optional[str] = None,
        temperature: float = 1.0,
        reasoning: Optional[str] = None,
    ) -> str:
        """Generate an unstructured text completion."""
        model = model or self._extraction_model
        is_openrouter = self._is_openrouter

        async with self._client_snapshot() as (raw_client, _):
            for attempt in range(TRANSPORT_RETRIES):
                try:
                    create_kwargs: Dict[str, Any] = {
                        "model": model,
                        "messages": [
                            {"role": "system", "content": system},
                            {"role": "user", "content": user},
                        ],
                        "temperature": temperature,
                    }

                    if is_openrouter:
                        create_kwargs["extra_body"] = self._openrouter_extra_body(
                            reasoning
                        )

                    response = await raw_client.chat.completions.create(**create_kwargs)
                    if not response.choices:
                        raise LLMResponseError(
                            "LLM provider returned no completion choices",
                            details={"model": model},
                        )

                    content = response.choices[0].message.content
                    if not content or not content.strip():
                        raise LLMResponseError(
                            "LLM provider returned an empty completion",
                            details={"model": model},
                        )

                    self._trace_exchange(model, user, content)
                    return content
                except LLMResponseError:
                    raise
                except Exception as exc:
                    if attempt < TRANSPORT_RETRIES - 1:
                        logger.warning(
                            f"LLM text call failed: {exc}. Retrying "
                            f"({attempt + 1}/{TRANSPORT_RETRIES})"
                        )
                        await asyncio.sleep(0.5 * (attempt + 1))
                    else:
                        raise LLMProviderError(
                            "LLM text generation failed after retries",
                            details={"model": model, "error": str(exc)},
                        ) from exc

        raise AssertionError("unreachable")

    async def generate_structured(
        self,
        *,
        response_model: type[ResponseT],
        system: str,
        user: str,
        model: Optional[str] = None,
        temperature: float = 1.0,
        reasoning: Optional[str] = None,
        mode: Optional[instructor.Mode] = None,
    ) -> ResponseT:
        """Generate and validate a structured completion with Instructor."""
        model = model or self._extraction_model
        is_openrouter = self._is_openrouter
        create_kwargs: Dict[str, Any] = {
            "model": model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": temperature,
            "response_model": response_model,
            "mode": mode or instructor.Mode.JSON,
            "max_retries": VALIDATION_RETRIES,
        }
        if is_openrouter:
            create_kwargs["extra_body"] = self._openrouter_extra_body(reasoning)

        async with self._client_snapshot() as (_, instructor_client):
            try:
                (
                    response,
                    _completion,
                ) = await instructor_client.chat.completions.create_with_completion(
                    **create_kwargs
                )
            except InstructorRetryException as exc:
                raise LLMResponseError(
                    "LLM structured response failed validation",
                    details=self._structured_validation_error_details(model, exc),
                ) from exc
            except (
                APIConnectionError,
                APIStatusError,
                APITimeoutError,
                RateLimitError,
            ) as exc:
                raise LLMProviderError(
                    "LLM structured provider request failed",
                    details={"model": model, "error": str(exc)},
                ) from exc
            except Exception as exc:
                raise LLMResponseError(
                    "LLM structured generation failed",
                    details={"model": model, "error": str(exc)},
                ) from exc

        self._trace_exchange(model, user, response)
        return response

    @staticmethod
    def _structured_validation_error_details(
        model: str, exc: InstructorRetryException
    ) -> Dict[str, str]:
        """Preserve bounded model-output diagnostics for the retry/DLQ path."""

        details = {"model": model, "error": str(exc)}
        last_completion = getattr(exc, "last_completion", None)
        if last_completion is not None:
            excerpt = str(last_completion)
            if excerpt:
                details["response_excerpt"] = excerpt[:4096]
        return details

    async def stream_with_tools(
        self,
        system: str,
        user: str,
        tools: List[Dict],
        model: Optional[str] = None,
        temperature: float = 0.0,
        reasoning: Optional[str] = "low",
    ) -> AsyncIterator[InternalAgentStreamEvent]:
        """Streaming completion with tools. Defaults to agent model."""
        model = model or self._agent_model
        is_openrouter = self._is_openrouter
        async with self._client_snapshot() as (raw_client, _):
            async for event in self._stream_with_client(
                raw_client=raw_client,
                system=system,
                user=user,
                tools=tools,
                model=model,
                temperature=temperature,
                reasoning=reasoning,
                is_openrouter=is_openrouter,
            ):
                yield event

    async def _stream_with_client(
        self,
        *,
        raw_client: AsyncOpenAI,
        system: str,
        user: str,
        tools: List[Dict],
        model: str,
        temperature: float,
        reasoning: Optional[str],
        is_openrouter: bool,
    ) -> AsyncIterator[InternalAgentStreamEvent]:
        for attempt in range(TRANSPORT_RETRIES):
            content = ""
            tool_calls_by_index: Dict[int, StreamToolCall] = {}
            tool_calls_detected = False
            emitted_output = False
            usage: Optional[StreamUsage] = None

            try:
                create_kwargs: Dict[str, Any] = dict(
                    model=model,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user},
                    ],
                    tools=tools,
                    tool_choice="auto",
                    temperature=temperature,
                    stream=True,
                )
                if is_openrouter:
                    create_kwargs.update(
                        {
                            "stream_options": {"include_usage": True},
                            "extra_body": self._openrouter_extra_body(
                                reasoning=reasoning
                            ),
                        }
                    )

                response = await raw_client.chat.completions.create(**create_kwargs)

                async for chunk in response:
                    if getattr(chunk, "usage", None):
                        usage = {
                            "prompt_tokens": chunk.usage.prompt_tokens,
                            "completion_tokens": chunk.usage.completion_tokens,
                            "total_tokens": chunk.usage.total_tokens,
                            "approximate": False,
                        }
                    if not chunk.choices:
                        continue

                    delta = chunk.choices[0].delta

                    if delta.content:
                        content += delta.content
                        if not tool_calls_detected:
                            emitted_output = True
                            yield {
                                "event": "token",
                                "data": {"content": delta.content},
                            }

                    if delta.tool_calls:
                        tool_calls_detected = True
                        for tc in delta.tool_calls:
                            idx = tc.index
                            if idx not in tool_calls_by_index:
                                tool_calls_by_index[idx] = {"name": "", "arguments": ""}
                            if getattr(tc, "id", None):
                                tool_calls_by_index[idx]["id"] = tc.id
                            if tc.function.name:
                                tool_calls_by_index[idx]["name"] = tc.function.name
                            if tc.function.arguments:
                                tool_calls_by_index[idx]["arguments"] += (
                                    tc.function.arguments
                                )

                    if hasattr(delta, "reasoning_details") and delta.reasoning_details:
                        for rd in delta.reasoning_details:
                            if hasattr(rd, "content") and rd.content:
                                emitted_output = True
                                yield {
                                    "event": "thinking",
                                    "data": {"content": rd.content},
                                }

                if tool_calls_by_index:
                    calls = [
                        tool_calls_by_index[i]
                        for i in sorted(tool_calls_by_index.keys())
                    ]
                    invalid_indexes = [
                        index
                        for index, call in enumerate(calls)
                        if not call["name"].strip()
                    ]
                    if invalid_indexes:
                        yield {
                            "event": "step_error",
                            "data": {
                                "kind": "provider",
                                "message": (
                                    "LLM returned incomplete tool calls at indexes "
                                    f"{invalid_indexes}"
                                ),
                            },
                        }
                        return
                    for call in calls:
                        if not call["arguments"].strip():
                            call["arguments"] = "{}"

                    yield {
                        "event": "tool_calls",
                        "data": {
                            "content": content,
                            "calls": calls,
                        },
                    }

                if not usage:
                    p_tokens = self.count_tokens(f"{system}\n{user}")
                    c_tokens = self.count_tokens(content)
                    usage = {
                        "prompt_tokens": p_tokens,
                        "completion_tokens": c_tokens,
                        "total_tokens": p_tokens + c_tokens,
                        "approximate": True,
                    }

                yield {
                    "event": "step_completed",
                    "data": {
                        "content": content,
                        "usage": usage,
                    },
                }
                return

            except Exception as exc:
                if attempt < TRANSPORT_RETRIES - 1:
                    if emitted_output or tool_calls_detected:
                        logger.error(
                            f"Stream failed ({model}) mid-generation: {exc}. "
                            "Cannot retry safely."
                        )
                        yield {
                            "event": "step_error",
                            "data": {
                                "kind": "provider",
                                "message": (
                                    f"Stream interrupted mid-generation: {str(exc)}"
                                ),
                            },
                        }
                        return
                    logger.warning(
                        f"Stream failed ({model}): {exc}. Retrying in "
                        f"{0.5 * (attempt + 1)}s..."
                    )
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                logger.error(
                    f"Stream failed ({model}) after {TRANSPORT_RETRIES} retries: {exc}"
                )
                yield {
                    "event": "step_error",
                    "data": {"kind": "provider", "message": str(exc)},
                }
                return

    async def close(self):
        if self._closed:
            return

        self._closed = True
        active_client = self._raw_client
        self._raw_client = None
        self._client = None
        self._api_key = ""

        current_task = asyncio.current_task()
        active_requests = [
            task
            for task in self._active_requests
            if task is not current_task and not task.done()
        ]
        if active_requests:
            _, pending = await asyncio.wait(
                active_requests,
                timeout=CLIENT_SHUTDOWN_TIMEOUT,
            )
            if pending:
                logger.warning(
                    f"Closing LLM clients with {len(pending)} requests still active"
                )

        if self._retirement_tasks:
            await asyncio.gather(
                *tuple(self._retirement_tasks.values()),
                return_exceptions=True,
            )

        clients = set(self._retired_clients)
        if active_client is not None:
            clients.add(active_client)
        self._retired_clients.clear()

        if clients:
            await asyncio.gather(
                *(client.close() for client in clients),
                return_exceptions=True,
            )
