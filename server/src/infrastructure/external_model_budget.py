"""Shared reservation/accounting seam for external model providers."""

import asyncio
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from common.exceptions import ConfigurationError, LLMBudgetExceededError
from common.schema.settings import LLMSpendingBudgetSettings


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
    storage: str
    reset_key: str


class ExternalModelSpendingLedger:
    """Shared accounting with durable storage when PostgreSQL is available."""

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
        # Storage is an ownership decision, not a property of the current cap.
        # Keep uncapped calls durable too so later caps see the same balance.
        self._storage = "postgres" if postgres_client is not None else "memory"
        self._pending_reservations: dict[Any, _BudgetReservation] = {}

    async def update_settings(self, settings: LLMSpendingBudgetSettings) -> None:
        async with self._lock:
            self._replace_settings_unlocked(settings)

    def replace_settings_without_lock(
        self, settings: LLMSpendingBudgetSettings
    ) -> None:
        """Use only before the service enters an event loop."""

        self._replace_settings_unlocked(settings)

    def _replace_settings_unlocked(self, settings: LLMSpendingBudgetSettings) -> None:
        if settings.reset_key != self._reset_key:
            self._spent_usd = 0.0
            self._reserved_usd = 0.0
            self._records.clear()
            self._pending_reservations.clear()
            self._reset_key = settings.reset_key
            self._generation += 1
        self._settings = settings

    async def reserve(
        self,
        *,
        model: str,
        estimated_prompt_tokens: int,
        estimated_output_tokens: int | None = None,
    ) -> _BudgetReservation:
        async with self._lock:
            if self._storage == "postgres":
                return await self._reserve_durable(
                    model=model,
                    estimated_prompt_tokens=estimated_prompt_tokens,
                    estimated_output_tokens=estimated_output_tokens,
                )
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
                (
                    self._settings.reservation_output_tokens
                    if estimated_output_tokens is None
                    else estimated_output_tokens
                ),
                price,
            )
            # Explicit estimates (JEV) must fit before sending. Preserve the
            # pre-existing LLM in-memory admission behavior in this extraction.
            if (
                estimated_output_tokens is not None
                and limit is not None
                and self._spent_usd + self._reserved_usd + reserved_cost > limit
            ):
                raise LLMBudgetExceededError(
                    "The configured global LLM spending budget cannot cover this request.",
                    details=self._snapshot_unlocked(),
                )
            self._next_token += 1
            self._reserved_usd += reserved_cost
            reservation = _BudgetReservation(
                self._next_token,
                reserved_cost,
                price,
                self._generation,
                self._storage,
                self._reset_key,
            )
            self._pending_reservations[reservation.token] = reservation
            return reservation

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
        async with self._lock:
            if reservation.storage == "postgres":
                if self._postgres is None:
                    raise RuntimeError("Durable reservation has no storage owner")
                await self._record_durable(
                    reservation,
                    prompt_tokens=prompt_tokens,
                    completion_tokens=completion_tokens,
                )
                return
            # A reset explicitly starts a new user-controlled accounting period.
            # Do not let an older in-flight request charge that new period.
            if reservation.generation != self._generation:
                return
            if self._pending_reservations.pop(reservation.token, None) is None:
                return  # Recovery may retry after an ambiguous completion.
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

    async def _reserve_durable(
        self,
        *,
        model: str,
        estimated_prompt_tokens: int,
        estimated_output_tokens: int | None = None,
    ) -> _BudgetReservation:
        price = self._price_for(model)
        if self._settings.limit_usd is not None and price is None:
            raise ConfigurationError(
                f"The configured LLM spending budget has no price for model '{model}'."
            )
        reserved = self._cost_for(
            estimated_prompt_tokens,
            self._settings.reservation_output_tokens
            if estimated_output_tokens is None
            else estimated_output_tokens,
            price,
        )
        reservation_id = str(uuid.uuid4())
        reset_key = self._settings.reset_key
        async with self._postgres.transaction() as cur:
            await cur.execute(
                "INSERT INTO public.llm_budget_windows (reset_key) VALUES (%s) ON CONFLICT DO NOTHING",
                (reset_key,),
            )
            await cur.execute(
                "SELECT spent_usd, reserved_usd FROM public.llm_budget_windows WHERE reset_key = %s FOR UPDATE",
                (reset_key,),
            )
            window = await cur.fetchone()
            await cur.execute(
                # Unsettled work may already have reached the provider. Charge
                # its estimate once rather than silently refunding it on expiry.
                """WITH expired AS (UPDATE public.llm_budget_reservations SET status = 'recorded', recorded_at = now() WHERE reset_key = %s AND status = 'active' AND expires_at <= now() RETURNING reserved_usd) UPDATE public.llm_budget_windows SET reserved_usd = GREATEST(0, reserved_usd - COALESCE((SELECT sum(reserved_usd) FROM expired), 0)), spent_usd = spent_usd + COALESCE((SELECT sum(reserved_usd) FROM expired), 0), updated_at = now() WHERE reset_key = %s""",
                (reset_key, reset_key),
            )
            await cur.execute(
                "SELECT spent_usd, reserved_usd FROM public.llm_budget_windows WHERE reset_key = %s",
                (reset_key,),
            )
            window = await cur.fetchone()
            if self._settings.limit_usd is not None and (
                float(window["spent_usd"]) + float(window["reserved_usd"]) + reserved
                > self._settings.limit_usd
                or float(window["spent_usd"]) + float(window["reserved_usd"])
                >= self._settings.limit_usd
            ):
                raise LLMBudgetExceededError(
                    "The configured global LLM spending budget has been reached."
                )
            await cur.execute(
                "INSERT INTO public.llm_budget_reservations (reservation_id, reset_key, reserved_usd, expires_at, status) VALUES (%s, %s, %s, now() + interval '15 minutes', 'active')",
                (reservation_id, reset_key, reserved),
            )
            await cur.execute(
                "UPDATE public.llm_budget_windows SET reserved_usd = reserved_usd + %s, updated_at = now() WHERE reset_key = %s",
                (reserved, reset_key),
            )
        return _BudgetReservation(
            reservation_id, reserved, price, self._generation, self._storage, reset_key
        )

    async def _record_durable(
        self,
        reservation: _BudgetReservation,
        *,
        prompt_tokens: int,
        completion_tokens: int,
    ) -> None:
        actual = self._cost_for(prompt_tokens, completion_tokens, reservation.price)
        async with self._postgres.transaction() as cur:
            # Same lock order as reservation admission/expiry, including callers
            # in other processes: window first, then reservation rows.
            await cur.execute(
                "SELECT spent_usd, reserved_usd FROM public.llm_budget_windows WHERE reset_key = %s FOR UPDATE",
                (reservation.reset_key,),
            )
            await cur.execute(
                "SELECT reset_key, reserved_usd FROM public.llm_budget_reservations WHERE reservation_id = %s AND status = 'active' FOR UPDATE",
                (reservation.token,),
            )
            row = await cur.fetchone()
            if row is None:
                return
            await cur.execute(
                "UPDATE public.llm_budget_reservations SET status = 'recorded', recorded_at = now() WHERE reservation_id = %s",
                (reservation.token,),
            )
            await cur.execute(
                "UPDATE public.llm_budget_windows SET reserved_usd = GREATEST(0, reserved_usd - %s), spent_usd = spent_usd + %s, updated_at = now() WHERE reset_key = %s",
                (float(row["reserved_usd"]), actual, row["reset_key"]),
            )

    async def snapshot(self) -> dict[str, Any]:
        async with self._lock:
            if self._storage == "postgres":
                async with self._postgres.transaction() as cur:
                    await cur.execute(
                        "SELECT spent_usd, reserved_usd FROM public.llm_budget_windows WHERE reset_key = %s",
                        (self._reset_key,),
                    )
                    row = await cur.fetchone()
                    await cur.execute(
                        "SELECT count(*) AS request_count FROM public.llm_budget_reservations WHERE reset_key = %s AND status = 'recorded'",
                        (self._reset_key,),
                    )
                    count = await cur.fetchone()
                return self._snapshot_values(
                    float(row["spent_usd"]) if row else 0,
                    float(row["reserved_usd"]) if row else 0,
                    int(count["request_count"]),
                )
            return self._snapshot_unlocked()

    def _snapshot_unlocked(self) -> dict[str, Any]:
        return self._snapshot_values(
            self._spent_usd, self._reserved_usd, len(self._records)
        )

    def _snapshot_values(self, spent_usd, reserved_usd, request_count):
        limit = self._settings.limit_usd
        return {
            "configured_limit_usd": limit,
            "spent_usd": round(spent_usd, 8),
            "reserved_usd": round(reserved_usd, 8),
            "remaining_usd": (
                None
                if limit is None
                else round(max(limit - spent_usd - reserved_usd, 0.0), 8)
            ),
            "request_count": request_count,
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
