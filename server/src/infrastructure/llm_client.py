import asyncio
import json
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
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
    LLMBudgetExceededError,
    LLMProviderError,
    LLMResponseError,
)
from common.schema.agent.stream import (
    InternalAgentStreamEvent,
    StreamToolCall,
    StreamUsage,
)
from common.schema.settings import LLMSettings, LLMSpendingBudgetSettings

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
TRANSPORT_RETRIES = 3
VALIDATION_RETRIES = 3
TRACE_MAX_CHARS = 20_000
CLIENT_SHUTDOWN_TIMEOUT = 5.0
ResponseT = TypeVar("ResponseT")


@dataclass(frozen=True, slots=True)
class LLMUsageRecord:
    """One provider attempt recorded by the server-wide LLM service."""

    model: str
    prompt_tokens: int
    completion_tokens: int
    cost_usd: float
    approximate_usage: bool
    failed: bool
    recorded_at: datetime


@dataclass(frozen=True, slots=True)
class _BudgetReservation:
    token: Any
    reserved_cost_usd: float
    price: Any
    generation: int


class _LLMSpendingLedger:
    """Concurrency-safe in-process accounting for all external model attempts."""

    MAX_RECORDS = 10_000

    def __init__(
        self, settings: LLMSpendingBudgetSettings | None = None, *, postgres_client=None
    ) -> None:
        self._lock = asyncio.Lock()
        self._settings = settings or LLMSpendingBudgetSettings()
        self._spent_usd = 0.0
        self._reserved_usd = 0.0
        self._records: list[LLMUsageRecord] = []
        self._next_token = 0
        self._reset_key = self._settings.reset_key
        self._generation = 0
        self._postgres = postgres_client

    async def update_settings(self, settings: LLMSpendingBudgetSettings) -> None:
        async with self._lock:
            self._replace_settings_unlocked(settings)

    def replace_settings_without_lock(self, settings: LLMSpendingBudgetSettings) -> None:
        """Use only before the service enters an event loop."""

        self._replace_settings_unlocked(settings)

    def _replace_settings_unlocked(self, settings: LLMSpendingBudgetSettings) -> None:
        if settings.reset_key != self._reset_key:
            self._spent_usd = 0.0
            self._reserved_usd = 0.0
            self._records.clear()
            self._reset_key = settings.reset_key
            self._generation += 1
        self._settings = settings

    async def reserve(
        self,
        *,
        model: str,
        estimated_prompt_tokens: int,
    ) -> _BudgetReservation:
        if self._postgres is not None and self._settings.limit_usd is not None:
            return await self._reserve_durable(
                model=model, estimated_prompt_tokens=estimated_prompt_tokens
            )
        async with self._lock:
            limit = self._settings.limit_usd
            if limit is not None and self._spent_usd + self._reserved_usd >= limit:
                raise LLMBudgetExceededError(
                    "The configured global LLM spending budget has been reached. "
                    "Increase the limit or change its reset key before starting "
                    "new LLM-backed work.",
                    details=self._snapshot_unlocked(),
                )
            price = self._price_for(model)
            if limit is not None and price is None:
                raise ConfigurationError(
                    "The configured LLM spending budget has no price for model "
                    f"'{model}'. Add model_pricing for it or configure "
                    "fallback_pricing before starting LLM-backed work."
                )
            reserved_cost = self._cost_for(
                estimated_prompt_tokens,
                self._settings.reservation_output_tokens,
                price,
            )
            self._next_token += 1
            self._reserved_usd += reserved_cost
            return _BudgetReservation(
                self._next_token,
                reserved_cost,
                price,
                self._generation,
            )

    async def record(
        self,
        reservation: _BudgetReservation,
        *,
        model: str,
        prompt_tokens: int,
        completion_tokens: int,
        approximate_usage: bool,
        failed: bool,
    ) -> None:
        if self._postgres is not None and self._settings.limit_usd is not None:
            await self._record_durable(
                reservation,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
            )
            return
        async with self._lock:
            # A reset explicitly starts a new user-controlled accounting period.
            # Do not let an older in-flight request charge that new period.
            if reservation.generation != self._generation:
                return
            self._reserved_usd = max(
                0.0,
                self._reserved_usd - reservation.reserved_cost_usd,
            )
            cost = self._cost_for(
                prompt_tokens,
                completion_tokens,
                reservation.price,
            )
            self._spent_usd += cost
            self._records.append(
                LLMUsageRecord(
                    model=model,
                    prompt_tokens=max(prompt_tokens, 0),
                    completion_tokens=max(completion_tokens, 0),
                    cost_usd=cost,
                    approximate_usage=approximate_usage,
                    failed=failed,
                    recorded_at=datetime.now(timezone.utc),
                )
            )
            if len(self._records) > self.MAX_RECORDS:
                del self._records[: len(self._records) - self.MAX_RECORDS]

    async def _reserve_durable(self, *, model: str, estimated_prompt_tokens: int) -> _BudgetReservation:
        price = self._price_for(model)
        if price is None:
            raise ConfigurationError(f"The configured LLM spending budget has no price for model '{model}'.")
        reserved = self._cost_for(estimated_prompt_tokens, self._settings.reservation_output_tokens, price)
        reservation_id = str(uuid.uuid4())
        reset_key = self._settings.reset_key
        async with self._postgres.transaction() as cur:
            await cur.execute("INSERT INTO public.llm_budget_windows (reset_key) VALUES (%s) ON CONFLICT DO NOTHING", (reset_key,))
            await cur.execute("SELECT spent_usd, reserved_usd FROM public.llm_budget_windows WHERE reset_key = %s FOR UPDATE", (reset_key,))
            window = await cur.fetchone()
            await cur.execute("""WITH expired AS (UPDATE public.llm_budget_reservations SET status = 'expired' WHERE reset_key = %s AND status = 'active' AND expires_at <= now() RETURNING reserved_usd) UPDATE public.llm_budget_windows SET reserved_usd = GREATEST(0, reserved_usd - COALESCE((SELECT sum(reserved_usd) FROM expired), 0)), updated_at = now() WHERE reset_key = %s""", (reset_key, reset_key))
            await cur.execute("SELECT spent_usd, reserved_usd FROM public.llm_budget_windows WHERE reset_key = %s", (reset_key,))
            window = await cur.fetchone()
            if float(window["spent_usd"]) + float(window["reserved_usd"]) + reserved > float(self._settings.limit_usd):
                raise LLMBudgetExceededError("The configured global LLM spending budget has been reached.")
            await cur.execute("INSERT INTO public.llm_budget_reservations (reservation_id, reset_key, reserved_usd, expires_at, status) VALUES (%s, %s, %s, now() + interval '15 minutes', 'active')", (reservation_id, reset_key, reserved))
            await cur.execute("UPDATE public.llm_budget_windows SET reserved_usd = reserved_usd + %s, updated_at = now() WHERE reset_key = %s", (reserved, reset_key))
        return _BudgetReservation(reservation_id, reserved, price, self._generation)

    async def _record_durable(self, reservation: _BudgetReservation, *, prompt_tokens: int, completion_tokens: int) -> None:
        actual = self._cost_for(prompt_tokens, completion_tokens, reservation.price)
        async with self._postgres.transaction() as cur:
            await cur.execute("SELECT reset_key, reserved_usd FROM public.llm_budget_reservations WHERE reservation_id = %s AND status = 'active' FOR UPDATE", (reservation.token,))
            row = await cur.fetchone()
            if row is None:
                return
            await cur.execute("UPDATE public.llm_budget_reservations SET status = 'recorded', recorded_at = now() WHERE reservation_id = %s", (reservation.token,))
            await cur.execute("UPDATE public.llm_budget_windows SET reserved_usd = GREATEST(0, reserved_usd - %s), spent_usd = spent_usd + %s, updated_at = now() WHERE reset_key = %s", (float(row["reserved_usd"]), actual, row["reset_key"]))

    async def snapshot(self) -> dict[str, Any]:
        async with self._lock:
            return self._snapshot_unlocked()

    def _snapshot_unlocked(self) -> dict[str, Any]:
        limit = self._settings.limit_usd
        return {
            "configured_limit_usd": limit,
            "spent_usd": round(self._spent_usd, 8),
            "reserved_usd": round(self._reserved_usd, 8),
            "remaining_usd": (
                None
                if limit is None
                else round(max(limit - self._spent_usd - self._reserved_usd, 0.0), 8)
            ),
            "request_count": len(self._records),
            "enforced": limit is not None,
        }

    def _price_for(self, model: str):
        return self._settings.model_pricing.get(
            model,
            self._settings.fallback_pricing,
        )

    @staticmethod
    def _cost_for(prompt_tokens: int, completion_tokens: int, price) -> float:
        if price is None:
            return 0.0
        return (
            (max(prompt_tokens, 0) * price.input_usd_per_million_tokens)
            + (max(completion_tokens, 0) * price.output_usd_per_million_tokens)
        ) / 1_000_000


class LLMService:
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
    ):
        self._api_key = (api_key or "").strip()
        self._base_url = base_url or OPENROUTER_BASE_URL
        self._is_openrouter = "openrouter.ai" in self._base_url
        self._trace = trace_logger
        self._agent_model = agent_model
        self._extraction_model = extraction_model
        self._merge_model = merge_model
        self._spending_ledger = _LLMSpendingLedger(spending_budget, postgres_client=postgres_client)
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

    async def spending_snapshot(self) -> dict[str, Any]:
        """Return bounded server-wide LLM spend and reservation state."""

        return await self._spending_ledger.snapshot()

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
        await self._spending_ledger.record(
            reservation,
            model=model,
            prompt_tokens=int(prompt_tokens or 0),
            completion_tokens=int(completion_tokens or 0),
            approximate_usage=approximate,
            failed=failed,
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
        system: str,
        user: str,
        model: Optional[str] = None,
        temperature: float = 1.0,
        reasoning: Optional[str] = None,
        aac_budget: Any = None,
    ) -> str:
        """Generate an unstructured text completion."""
        model = model or self._extraction_model
        is_openrouter = self._is_openrouter

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
                    self._trace_exchange(model, user, content)
                    return content
                except LLMBudgetExceededError:
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
        system: str,
        user: str,
        model: Optional[str] = None,
        temperature: float = 1.0,
        reasoning: Optional[str] = None,
        mode: Optional[instructor.Mode] = None,
        aac_budget: Any = None,
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
            # Validation retries are controlled below so each provider request
            # receives its own budget reservation and usage record.
            "max_retries": 0,
        }
        if is_openrouter:
            create_kwargs["extra_body"] = self._openrouter_extra_body(reasoning)

        async with self._client_snapshot() as (_, instructor_client):
            response = None
            for attempt in range(VALIDATION_RETRIES + 1):
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
                    if attempt < VALIDATION_RETRIES:
                        logger.warning(
                            "LLM structured response failed validation; retrying "
                            "({}/{})",
                            attempt + 1,
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
                    raise LLMProviderError(
                        "LLM structured provider request failed",
                        details={"model": model, "error": str(exc)},
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
        self._trace_exchange(model, user, response)
        return response

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
        system: str,
        user: str,
        tools: List[Dict],
        model: Optional[str] = None,
        temperature: float = 0.0,
        reasoning: Optional[str] = "low",
        aac_budget: Any = None,
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
                aac_budget=aac_budget,
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
        aac_budget: Any = None,
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
