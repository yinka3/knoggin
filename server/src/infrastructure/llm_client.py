import asyncio
import json
import math
from contextlib import asynccontextmanager
from typing import (
    Any,
    AsyncIterator,
    Dict,
    List,
    Optional,
    Sequence,
    TypeVar,
    Union,
)

import httpx
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

from common.api_keys import llm_api_key
from common.exceptions import (
    ConfigurationError,
    LLMBudgetExceededError,
    LLMContextTruncatedError,
    LLMProviderError,
    LLMResponseError,
)
from common.schema.agent.stream import (
    InternalAgentStreamEvent,
    StreamToolCall,
    StreamUsage,
)
from common.schema.settings import LLMSettings, LLMSpendingBudgetSettings
from infrastructure.external_model_budget import (
    ExternalModelSpendingLedger,
    _BudgetReservation,
)

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
TRANSPORT_RETRIES = 3
VALIDATION_RETRIES = 3
TRACE_MAX_CHARS = 20_000
CLIENT_SHUTDOWN_TIMEOUT = 5.0
ResponseT = TypeVar("ResponseT")


# A system prompt is one string or ordered parts, most reusable first.
SystemPrompt = Union[str, Sequence[str]]


def local_thinking_warning(base_url: str | None, thinking: str) -> str | None:
    if thinking != "provider_default" or "openrouter.ai" in (base_url or OPENROUTER_BASE_URL):
        return None
    return (
        "llm.thinking is provider_default: the local model may think on every "
        "agent step and memory call, so responses will be several times slower. "
        "Use planning_only for faster responses."
    )


def _reported_window(row: Any) -> int | None:
    # OpenRouter says context_length; vLLM says max_model_len.
    for key in ("context_length", "max_model_len", "context_window"):
        value = getattr(row, key, None)
        if isinstance(value, int) and value > 0:
            return value
    return None


def system_text(system: SystemPrompt) -> str:
    return system if isinstance(system, str) else "".join(system)


def reported_cost(usage: Any) -> float | None:
    """The cost in USD the provider billed for one call, when it says.
    OpenRouter reports it, cache discounts included; local servers do not."""

    value = usage.get("cost_usd", usage.get("cost")) if isinstance(usage, dict) else getattr(usage, "cost", None)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if math.isfinite(value) and value >= 0 else None


def prompt_cache_tokens(usage: Any) -> tuple[int, int]:
    """Return (tokens read from the prompt cache, tokens written to it).

    Some providers (Gemini via OpenRouter) count a cache write inside
    ``cached_tokens`` and report it again as ``cache_write_tokens``; only the
    remainder was actually read from the cache.
    """

    if isinstance(usage, dict):
        if "cached_prompt_tokens" in usage:
            return (
                int(usage["cached_prompt_tokens"] or 0),
                int(usage.get("cache_write_tokens") or 0),
            )
        details = usage.get("prompt_tokens_details") or {}
        if not isinstance(details, dict):
            return 0, 0
        cached, written = details.get("cached_tokens"), details.get("cache_write_tokens")
    else:
        details = getattr(usage, "prompt_tokens_details", None)
        cached = getattr(details, "cached_tokens", 0) if details is not None else 0
        written = getattr(details, "cache_write_tokens", 0) if details is not None else 0
    cached = cached if isinstance(cached, int) else 0
    written = written if isinstance(written, int) else 0
    return max(cached - written, 0), written


class LLMService:
    @property
    def spending_ledger(self) -> ExternalModelSpendingLedger:
        """The shared server-wide ledger, also used by optional JEV requests."""
        return self._spending_ledger

    def __init__(
        self,
        api_key: str = None,
        base_url: str = None,
        trace_logger: Optional[Any] = None,
        agent_model: str = "google/gemini-3-flash-preview",
        extraction_model: str = "google/gemini-2.5-flash",
        merge_model: str = "google/gemini-2.5-pro",
        spending_budget: LLMSpendingBudgetSettings | None = None,
        postgres_client=None,
        request_timeout_seconds: float = 300.0,
        thinking: str = "planning_only",
    ):
        self._api_key = (api_key or "").strip()
        self._base_url = base_url or OPENROUTER_BASE_URL
        self._is_openrouter = "openrouter.ai" in self._base_url
        self._request_timeout_seconds = request_timeout_seconds
        self._thinking = thinking
        self._trace = trace_logger
        self._agent_model = agent_model
        self._extraction_model = extraction_model
        self._merge_model = merge_model
        self._spending_ledger = ExternalModelSpendingLedger(
            spending_budget, postgres_client=postgres_client
        )
        self._client = None
        self._raw_client = None
        self._tokenizer = None
        self._retired_clients: set[AsyncOpenAI] = set()
        self._retirement_tasks: dict[AsyncOpenAI, asyncio.Task] = {}
        # Process-lifetime prompt cache counters per model, for visibility.
        self._prompt_cache: dict[str, dict[str, int]] = {}
        self._client_usage: dict[AsyncOpenAI, int] = {}
        # Context sizes from the provider's model list, fetched once.
        self._listed_windows: dict[str, int | None] | None = None
        self._serves_ollama_ps: bool | None = None
        self._active_requests: set[asyncio.Task] = set()
        self._closed = False
        self._close_lock = asyncio.Lock()

        if self._api_key or not self._is_openrouter:
            self._raw_client, self._client = self._build_clients(
                self._api_key,
                self._base_url,
                self._request_timeout_seconds,
            )

            if self.thinking_warning:
                logger.warning(self.thinking_warning)
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
        timeout_seconds: float,
    ) -> tuple[AsyncOpenAI, AsyncInstructor]:
        client = AsyncOpenAI(
            base_url=base_url,
            # Local OpenAI-compatible servers (Ollama, LM Studio, vLLM) need no
            # key, but the client requires a value.
            api_key=api_key or "no-key-required",
            timeout=timeout_seconds,
            max_retries=0,
        )
        # Instructor applies the client's mode itself; a per-call ``mode``
        # kwarg is passed twice and rejected before any request is sent.
        return client, instructor.from_openai(client, mode=instructor.Mode.JSON)

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

    async def spending_snapshot(self) -> dict[str, Any]:
        """Return bounded server-wide LLM spend and reservation state."""

        snapshot = await self._spending_ledger.snapshot()
        snapshot["prompt_cache"] = {
            model: {
                **counts,
                "hit_rate": round(
                    counts["cached_prompt_tokens"] / counts["prompt_tokens"], 4
                ) if counts["prompt_tokens"] else 0.0,
            }
            for model, counts in sorted(self._prompt_cache.items())
        }
        return snapshot

    @property
    def is_configured(self) -> bool:
        return bool(not self._closed and self._client and self._raw_client)

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
        else:
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

        api_key = llm_api_key()
        base_url = settings.base_url or OPENROUTER_BASE_URL
        connection_changed = (
            api_key != self._api_key
            or base_url != self._base_url
            or settings.request_timeout_seconds != self._request_timeout_seconds
        )

        replacement_raw = self._raw_client
        replacement_instructor = self._client
        if connection_changed:
            if api_key or "openrouter.ai" not in base_url:
                replacement_raw, replacement_instructor = self._build_clients(
                    api_key,
                    base_url,
                    settings.request_timeout_seconds,
                )
            else:
                replacement_raw = None
                replacement_instructor = None

        previous_raw = self._raw_client
        self._api_key = api_key
        self._base_url = base_url
        self._is_openrouter = "openrouter.ai" in self._base_url
        self._request_timeout_seconds = settings.request_timeout_seconds
        if settings.thinking != self._thinking:
            self._thinking = settings.thinking
            if self.thinking_warning:
                logger.warning(self.thinking_warning)

        if connection_changed:
            self._raw_client = replacement_raw
            self._client = replacement_instructor
            self._listed_windows = None
            self._serves_ollama_ps = None
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

        self._schedule_budget_settings_update(settings.spending_budget)

    def _schedule_budget_settings_update(
        self,
        settings: LLMSpendingBudgetSettings,
    ) -> None:
        """Apply hot-reloaded budget policy without requiring an async callback."""

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            self._spending_ledger.replace_settings_without_lock(settings)
            return
        loop.create_task(
            self._spending_ledger.update_settings(settings),
            name="llm-spending-budget-update",
        )

    def _system_message(self, system: SystemPrompt) -> Dict[str, Any]:
        """Mark reusable system text for provider prompt caching.

        OpenRouter forwards explicit cache markers to providers that need them
        (Gemini, Anthropic). Callers keep per-step text out of the system
        message, because Gemini caches it as one unit. Other OpenAI-compatible
        servers (Ollama, vLLM) get one string in the same order, which still
        lets them reuse the identical prompt start.
        """

        parts = [system] if isinstance(system, str) else [part for part in system if part]
        if not self._is_openrouter:
            return {"role": "system", "content": "".join(parts)}
        return {
            "role": "system",
            "content": [
                {"type": "text", "text": part, "cache_control": {"type": "ephemeral"}}
                for part in parts
            ],
        }

    @property
    def thinking_warning(self) -> str | None:
        """Explain the cost of letting a local server think on every step."""

        return local_thinking_warning(self._base_url, self._thinking)

    def _provider_extra_body(self, reasoning: Optional[str]) -> Dict[str, Any] | None:
        """Provider-specific request fields, including thinking control."""

        if self._is_openrouter:
            return self._openrouter_extra_body(reasoning)
        # Local servers expose thinking only as on/off via reasoning_effort.
        if self._thinking == "off" or (
            self._thinking == "planning_only" and reasoning != "high"
        ):
            return {"reasoning_effort": "none"}
        return None

    @staticmethod
    def _openrouter_extra_body(
        reasoning: Optional[str] = "low",
    ) -> Dict[str, Any]:
        """Extra body parameters for OpenRouter."""
        body: Dict[str, Any] = {
            "provider": {"require_parameters": True},
        }
        if reasoning == "high":
            body["reasoning"] = {"max_tokens": 4096}
        elif reasoning == "medium":
            body["reasoning"] = {"max_tokens": 1024}
        elif reasoning not in (None, "low"):
            raise ValueError(f"Unsupported reasoning level: {reasoning}")
        return body

    async def context_window(self, model: str) -> int | None:
        """The model's context size in tokens, or None when the provider
        does not say. Ollama reports the size it actually serves only while
        the model is loaded, so that answer is read fresh each time."""

        client = self._raw_client
        if client is None:
            return None
        if not self._is_openrouter and self._serves_ollama_ps is not False:
            loaded = await self._ollama_loaded_window(model)
            if loaded is not None:
                return loaded
        if self._listed_windows is None:
            try:
                page = await client.models.list()
            except Exception as exc:
                logger.debug("LLMService: model list unavailable ({})", exc)
                return None
            self._listed_windows = {
                str(row.id): _reported_window(row) for row in page.data
            }
        return self._listed_windows.get(model)

    async def _ollama_loaded_window(self, model: str) -> int | None:
        root = self._base_url.rstrip("/").removesuffix("/v1")
        try:
            async with httpx.AsyncClient(timeout=2.0) as http:
                response = await http.get(f"{root}/api/ps")
            response.raise_for_status()
            rows = response.json()["models"]
        except Exception:
            # Not Ollama (or not reachable); stop asking until the URL changes.
            if self._serves_ollama_ps is None:
                self._serves_ollama_ps = False
            return None
        self._serves_ollama_ps = True
        names = {model, f"{model}:latest"}
        for row in rows:
            if row.get("name") in names or row.get("model") in names:
                value = row.get("context_length")
                return value if isinstance(value, int) and value > 0 else None
        return None

    def count_tokens(self, text: str) -> int:
        if not text:
            return 0
        if not self._tokenizer:
            # Fallback to rough estimation if tokenizer failed to load
            return (len(text) + 3) // 4
        return len(self._tokenizer.encode(text))

    def count_tokens_batch(self, texts: list[str]) -> list[int]:
        """Count independent rendered prefixes with the same tokenizer rule."""
        if not self._tokenizer:
            return [(len(value) + 3) // 4 for value in texts]
        return [
            len(tokens)
            for tokens in self._tokenizer.encode_batch(texts, num_threads=4)
        ]

    async def _reserve_external_attempt(
        self,
        *,
        model: str,
        system: str,
        user: str,
        additional_prompt_tokens: int = 0,
    ) -> _BudgetReservation:
        return await self._spending_ledger.reserve(
            model=model,
            estimated_prompt_tokens=(
                self.count_tokens(f"{system}\n{user}")
                + max(additional_prompt_tokens, 0)
            ),
        )

    async def _record_external_attempt(
        self,
        reservation: _BudgetReservation,
        *,
        model: str,
        system: str,
        user: str,
        content: str = "",
        provider_usage: Any = None,
        failed: bool,
        additional_prompt_tokens: int = 0,
        aac_budget: Any = None,
    ) -> None:
        declared_approximate = None
        if isinstance(provider_usage, dict):
            prompt_tokens = provider_usage.get("prompt_tokens")
            completion_tokens = provider_usage.get("completion_tokens")
            declared_approximate = provider_usage.get("approximate")
        else:
            prompt_tokens = getattr(provider_usage, "prompt_tokens", None)
            completion_tokens = getattr(provider_usage, "completion_tokens", None)
        approximate = (
            bool(declared_approximate)
            if declared_approximate is not None
            else prompt_tokens is None or completion_tokens is None
        )
        if approximate:
            prompt_tokens = (
                self.count_tokens(f"{system}\n{user}")
                + max(additional_prompt_tokens, 0)
            )
            completion_tokens = self.count_tokens(content)
        elif not failed:
            counts = self._prompt_cache.setdefault(
                model,
                {
                    "requests": 0,
                    "prompt_tokens": 0,
                    "cached_prompt_tokens": 0,
                    "cache_write_tokens": 0,
                },
            )
            read, written = prompt_cache_tokens(provider_usage)
            counts["requests"] += 1
            counts["prompt_tokens"] += int(prompt_tokens or 0)
            counts["cached_prompt_tokens"] += read
            counts["cache_write_tokens"] += written
        await self._spending_ledger.record(
            reservation,
            model=model,
            prompt_tokens=int(prompt_tokens or 0),
            completion_tokens=int(completion_tokens or 0),
            approximate_usage=approximate,
            failed=failed,
            reported_cost_usd=None if approximate else reported_cost(provider_usage),
        )
        if aac_budget is not None:
            aac_budget.record(
                {
                    "prompt_tokens": int(prompt_tokens or 0),
                    "completion_tokens": int(completion_tokens or 0),
                    "total_tokens": int(prompt_tokens or 0)
                    + int(completion_tokens or 0),
                    "approximate": approximate,
                }
            )

    def _truncation_message(
        self, usage: Any, *, system: str, user: str, additional_prompt_tokens: int = 0
    ) -> str | None:
        """Explain a provider that silently processed only part of the prompt."""

        if isinstance(usage, dict):
            if usage.get("approximate"):
                return None
            reported = usage.get("prompt_tokens")
        else:
            reported = getattr(usage, "prompt_tokens", None)
        if not isinstance(reported, int) or isinstance(reported, bool):
            return None
        estimate = self.count_tokens(f"{system}\n{user}") + max(additional_prompt_tokens, 0)
        # Tokenizers differ by model, so only a large gap means truncation.
        if estimate < 1_024 or reported >= estimate * 0.6:
            return None
        return (
            f"The model processed only {reported} of about {estimate} prompt tokens: "
            "its context window is smaller than the prompt, so part of it was "
            "silently dropped. Use a model with a larger context, or raise the "
            "server's context length (for Ollama, set OLLAMA_CONTEXT_LENGTH)."
        )

    @staticmethod
    def _check_aac_budget(aac_budget: Any) -> None:
        if aac_budget is not None and not aac_budget.allow_call():
            raise LLMBudgetExceededError(
                "The AAC discussion token budget has been reached.",
                details={"scope": "aac"},
            )

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
        system: SystemPrompt,
        user: str,
        model: Optional[str] = None,
        temperature: float = 1.0,
        reasoning: Optional[str] = None,
        aac_budget: Any = None,
    ) -> str:
        """Generate an unstructured text completion."""
        model = model or self._extraction_model
        system_message = self._system_message(system)
        system = system_text(system)

        async with self._client_snapshot() as (raw_client, _):
            for attempt in range(TRANSPORT_RETRIES):
                self._check_aac_budget(aac_budget)
                reservation = await self._reserve_external_attempt(
                    model=model,
                    system=system,
                    user=user,
                )
                recorded = False
                response = None
                content = ""
                try:
                    create_kwargs: Dict[str, Any] = {
                        "model": model,
                        "messages": [
                            system_message,
                            {"role": "user", "content": user},
                        ],
                        "temperature": temperature,
                    }

                    if extra_body := self._provider_extra_body(reasoning):
                        create_kwargs["extra_body"] = extra_body

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

                    await self._record_external_attempt(
                        reservation,
                        model=model,
                        system=system,
                        user=user,
                        content=content,
                        provider_usage=getattr(response, "usage", None),
                        failed=False,
                        aac_budget=aac_budget,
                    )
                    recorded = True
                    truncated = self._truncation_message(
                        getattr(response, "usage", None), system=system, user=user
                    )
                    if truncated:
                        raise LLMContextTruncatedError(truncated, details={"model": model})
                    self._trace_exchange(model, user, content)
                    return content
                except (LLMBudgetExceededError, LLMContextTruncatedError):
                    raise
                except LLMResponseError:
                    if not recorded:
                        await self._record_external_attempt(
                            reservation,
                            model=model,
                            system=system,
                            user=user,
                            content=content or "",
                            provider_usage=getattr(response, "usage", None),
                            failed=True,
                            aac_budget=aac_budget,
                        )
                        recorded = True
                    raise
                except Exception as exc:
                    if not recorded:
                        await self._record_external_attempt(
                            reservation,
                            model=model,
                            system=system,
                            user=user,
                            content=content or "",
                            provider_usage=getattr(response, "usage", None),
                            failed=True,
                            aac_budget=aac_budget,
                        )
                        recorded = True
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
                finally:
                    if not recorded:
                        await self._record_external_attempt(
                            reservation,
                            model=model,
                            system=system,
                            user=user,
                            content=content or "",
                            provider_usage=getattr(response, "usage", None),
                            failed=True,
                            aac_budget=aac_budget,
                        )

        raise AssertionError("unreachable")

    async def generate_structured(
        self,
        *,
        response_model: type[ResponseT],
        system: SystemPrompt,
        user: str,
        model: Optional[str] = None,
        temperature: float = 1.0,
        reasoning: Optional[str] = None,
        aac_budget: Any = None,
    ) -> ResponseT:
        """Generate and validate a structured completion with Instructor."""
        model = model or self._extraction_model
        system_message = self._system_message(system)
        system = system_text(system)
        create_kwargs: Dict[str, Any] = {
            "model": model,
            "messages": [
                system_message,
                {"role": "user", "content": user},
            ],
            "temperature": temperature,
            "response_model": response_model,
            # Validation retries are controlled below so each provider request
            # receives its own budget reservation and usage record.
            "max_retries": 0,
        }
        if extra_body := self._provider_extra_body(reasoning):
            create_kwargs["extra_body"] = extra_body

        async with self._client_snapshot() as (_, instructor_client):
            response = None
            validation_failures = 0
            transport_failures = 0
            while True:
                self._check_aac_budget(aac_budget)
                reservation = await self._reserve_external_attempt(
                    model=model,
                    system=system,
                    user=user,
                )
                recorded = False
                completion = None
                try:
                    (
                        response,
                        completion,
                    ) = await instructor_client.chat.completions.create_with_completion(
                        **create_kwargs
                    )
                    await self._record_external_attempt(
                        reservation,
                        model=model,
                        system=system,
                        user=user,
                        content=str(response),
                        provider_usage=getattr(completion, "usage", None),
                        failed=False,
                        aac_budget=aac_budget,
                    )
                    recorded = True
                    break
                except InstructorRetryException as exc:
                    completion = getattr(exc, "last_completion", None)
                    if not recorded:
                        await self._record_external_attempt(
                            reservation,
                            model=model,
                            system=system,
                            user=user,
                            provider_usage=getattr(completion, "usage", None),
                            failed=True,
                            aac_budget=aac_budget,
                        )
                        recorded = True
                    # Instructor also wraps timeouts and provider errors;
                    # those are transport failures, not invalid output.
                    provider_exc = self._wrapped_provider_error(exc)
                    if provider_exc is not None:
                        if (
                            self._is_transient_provider_error(provider_exc)
                            and transport_failures < TRANSPORT_RETRIES - 1
                        ):
                            transport_failures += 1
                            logger.warning(
                                "LLM structured provider request failed: {}. Retrying "
                                "({}/{})",
                                type(provider_exc).__name__,
                                transport_failures,
                                TRANSPORT_RETRIES,
                            )
                            await asyncio.sleep(0.5 * transport_failures)
                            continue
                        raise LLMProviderError(
                            "LLM structured provider request failed",
                            details={"model": model, "error": str(provider_exc)},
                            transient=self._is_transient_provider_error(provider_exc),
                        ) from exc
                    if validation_failures < VALIDATION_RETRIES:
                        validation_failures += 1
                        logger.warning(
                            "LLM structured response failed validation; retrying "
                            "({}/{})",
                            validation_failures,
                            VALIDATION_RETRIES,
                        )
                        continue
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
                    if not recorded:
                        await self._record_external_attempt(
                            reservation,
                            model=model,
                            system=system,
                            user=user,
                            provider_usage=getattr(completion, "usage", None),
                            failed=True,
                            aac_budget=aac_budget,
                        )
                        recorded = True
                    if (
                        self._is_transient_provider_error(exc)
                        and transport_failures < TRANSPORT_RETRIES - 1
                    ):
                        transport_failures += 1
                        logger.warning(
                            "LLM structured provider request failed: {}. Retrying "
                            "({}/{})",
                            type(exc).__name__,
                            transport_failures,
                            TRANSPORT_RETRIES,
                        )
                        await asyncio.sleep(0.5 * transport_failures)
                        continue
                    raise LLMProviderError(
                        "LLM structured provider request failed",
                        details={"model": model, "error": str(exc)},
                        transient=self._is_transient_provider_error(exc),
                    ) from exc
                except Exception as exc:
                    if not recorded:
                        await self._record_external_attempt(
                            reservation,
                            model=model,
                            system=system,
                            user=user,
                            provider_usage=getattr(completion, "usage", None),
                            failed=True,
                            aac_budget=aac_budget,
                        )
                        recorded = True
                    raise LLMResponseError(
                        "LLM structured generation failed",
                        details={"model": model, "error": str(exc)},
                    ) from exc
                finally:
                    if not recorded:
                        await self._record_external_attempt(
                            reservation,
                            model=model,
                            system=system,
                            user=user,
                            provider_usage=getattr(completion, "usage", None),
                            failed=True,
                            aac_budget=aac_budget,
                        )

        if response is None:
            raise AssertionError("structured generation completed without a response")
        truncated = self._truncation_message(
            getattr(completion, "usage", None), system=system, user=user
        )
        if truncated:
            raise LLMContextTruncatedError(truncated, details={"model": model})
        self._trace_exchange(model, user, response)
        return response

    @staticmethod
    def _wrapped_provider_error(exc: InstructorRetryException) -> Exception | None:
        attempts = getattr(exc, "failed_attempts", None) or ()
        candidates = [attempt.exception for attempt in attempts[-1:]]
        candidates.append(exc.__cause__)
        for candidate in candidates:
            if isinstance(candidate, (APIConnectionError, APIStatusError)):
                return candidate
        return None

    @staticmethod
    def _is_transient_provider_error(exc: Exception) -> bool:
        """Retry dropped connections, timeouts, rate limits, and provider 5xx."""

        if isinstance(exc, (APIConnectionError, RateLimitError)):
            return True
        return isinstance(exc, APIStatusError) and exc.status_code >= 500

    @staticmethod
    def _structured_validation_error_details(
        model: str, exc: InstructorRetryException
    ) -> Dict[str, str]:
        """Preserve bounded model-output diagnostics for retry handling."""

        details = {"model": model, "error": str(exc)}
        last_completion = getattr(exc, "last_completion", None)
        if last_completion is not None:
            excerpt = str(last_completion)
            if excerpt:
                details["response_excerpt"] = excerpt[:4096]
        return details

    async def stream_with_tools(
        self,
        system: SystemPrompt,
        user: str,
        tools: List[Dict],
        model: Optional[str] = None,
        temperature: float = 0.0,
        reasoning: Optional[str] = "low",
        aac_budget: Any = None,
        max_completion_tokens: int | None = None,
    ) -> AsyncIterator[InternalAgentStreamEvent]:
        """Streaming completion with tools. Defaults to agent model."""
        model = model or self._agent_model
        is_openrouter = self._is_openrouter
        async with self._client_snapshot() as (raw_client, _):
            async for event in self._stream_with_client(
                raw_client=raw_client,
                system=system_text(system),
                system_message=self._system_message(system),
                user=user,
                tools=tools,
                model=model,
                temperature=temperature,
                reasoning=reasoning,
                is_openrouter=is_openrouter,
                aac_budget=aac_budget,
                max_completion_tokens=max_completion_tokens,
            ):
                yield event

    async def _stream_with_client(
        self,
        *,
        raw_client: AsyncOpenAI,
        system: str,
        system_message: Dict[str, Any],
        user: str,
        tools: List[Dict],
        model: str,
        temperature: float,
        reasoning: Optional[str],
        is_openrouter: bool,
        aac_budget: Any = None,
        max_completion_tokens: int | None = None,
    ) -> AsyncIterator[InternalAgentStreamEvent]:
        tool_schema_tokens = self.count_tokens(
            json.dumps(tools, sort_keys=True, default=str, separators=(",", ":"))
        )
        for attempt in range(TRANSPORT_RETRIES):
            content = ""
            tool_calls_by_index: Dict[int, StreamToolCall] = {}
            tool_calls_detected = False
            emitted_output = False
            usage: Optional[StreamUsage] = None
            try:
                self._check_aac_budget(aac_budget)
                reservation = await self._reserve_external_attempt(
                    model=model,
                    system=system,
                    user=user,
                    additional_prompt_tokens=tool_schema_tokens,
                )
            except LLMBudgetExceededError as exc:
                yield {
                    "event": "step_error",
                    "data": {"kind": "budget", "message": str(exc)},
                }
                return
            except ConfigurationError as exc:
                yield {
                    "event": "step_error",
                    "data": {"kind": "configuration", "message": str(exc)},
                }
                return
            recorded = False

            try:
                create_kwargs: Dict[str, Any] = dict(
                    model=model,
                    messages=[
                        system_message,
                        {"role": "user", "content": user},
                    ],
                    tools=tools,
                    tool_choice="auto",
                    temperature=temperature,
                    stream=True,
                )
                if max_completion_tokens is not None:
                    create_kwargs["max_completion_tokens"] = max_completion_tokens
                # Standard OpenAI option, also honored by Ollama, vLLM, and
                # LM Studio; usage is needed for accounting and truncation checks.
                create_kwargs["stream_options"] = {"include_usage": True}
                if extra_body := self._provider_extra_body(reasoning):
                    create_kwargs["extra_body"] = extra_body

                response = await raw_client.chat.completions.create(**create_kwargs)

                async for chunk in response:
                    if getattr(chunk, "usage", None):
                        usage = {
                            "prompt_tokens": chunk.usage.prompt_tokens,
                            "completion_tokens": chunk.usage.completion_tokens,
                            "total_tokens": chunk.usage.total_tokens,
                            "cached_prompt_tokens": prompt_cache_tokens(chunk.usage)[0],
                            "cache_write_tokens": prompt_cache_tokens(chunk.usage)[1],
                            "cost_usd": reported_cost(chunk.usage),
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

                if not usage:
                    p_tokens = self.count_tokens(f"{system}\n{user}")
                    c_tokens = self.count_tokens(content)
                    usage = {
                        "prompt_tokens": p_tokens,
                        "completion_tokens": c_tokens,
                        "total_tokens": p_tokens + c_tokens,
                        "approximate": True,
                    }

                await self._record_external_attempt(
                    reservation,
                    model=model,
                    system=system,
                    user=user,
                    content=content,
                    provider_usage=usage,
                    failed=False,
                    additional_prompt_tokens=tool_schema_tokens,
                    aac_budget=aac_budget,
                )
                recorded = True
                truncated = self._truncation_message(
                    usage, system=system, user=user,
                    additional_prompt_tokens=tool_schema_tokens,
                )
                if truncated:
                    yield {
                        "event": "step_error",
                        "data": {
                            "kind": "provider",
                            "message": truncated,
                            "code": "provider_context_limit",
                            "retryable": False,
                        },
                    }
                    return

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

                yield {
                    "event": "step_completed",
                    "data": {
                        "content": content,
                        "usage": usage,
                    },
                }
                return

            except Exception as exc:
                if not recorded:
                    await self._record_external_attempt(
                        reservation,
                        model=model,
                        system=system,
                        user=user,
                        content=content,
                        provider_usage=usage,
                        failed=True,
                        additional_prompt_tokens=tool_schema_tokens,
                        aac_budget=aac_budget,
                    )
                    recorded = True
                if isinstance(exc, APIStatusError) and exc.status_code in {400, 413, 422}:
                    detail = str(getattr(exc, "body", "") or exc).lower()
                    if any(marker in detail for marker in (
                        "context_length", "context length", "context window",
                        "maximum context", "too many tokens", "token limit",
                    )):
                        yield {
                            "event": "step_error",
                            "data": {
                                "kind": "provider",
                                "message": "Configured prompt and response reserve exceed this model's context limit.",
                                "code": "provider_context_limit",
                                "retryable": False,
                            },
                        }
                        return
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
            finally:
                if not recorded:
                    await self._record_external_attempt(
                        reservation,
                        model=model,
                        system=system,
                        user=user,
                        content=content,
                        provider_usage=usage,
                        failed=True,
                        additional_prompt_tokens=tool_schema_tokens,
                        aac_budget=aac_budget,
                    )

    async def close(self):
        async with self._close_lock:
            await self._close_owned_clients()

    async def _close_owned_clients(self):
        self._closed = True
        active_client = self._raw_client
        if active_client is not None:
            self._retired_clients.add(active_client)
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
        failures = []
        for client in clients:
            try:
                await client.close()
            except Exception as exc:
                failures.append(exc)
            else:
                self._retired_clients.discard(client)
        if failures:
            raise RuntimeError("LLM client cleanup failed") from failures[0]
