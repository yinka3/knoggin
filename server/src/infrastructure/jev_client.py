"""Optional bounded JEV HTTP client; callers own semantic decisions."""

from __future__ import annotations

import asyncio
import json
import math
import time
from dataclasses import dataclass, field

import httpx
from pydantic import ValidationError

from common.exceptions import ConfigurationError, LLMBudgetExceededError
from common.schema.jev import (
    Capability,
    ChoiceAnswer,
    ChoiceQuestion,
    JevPolicy,
    JevResponse,
    JevResult,
    JevSettings,
    JevUsage,
    NoulQuestion,
)
from infrastructure.external_model_budget import (
    ExternalModelSpendingLedger,
    _BudgetReservation,
)

MAX_RESPONSE_BYTES = 128_000


@dataclass
class _AccountingWork:
    reservation: _BudgetReservation
    values: dict
    timeout_seconds: float
    task: asyncio.Task | None = None


@dataclass
class JevWorkBudget:
    """One shared window budget; retry attempts and both passes consume it."""

    max_calls: int
    max_elapsed_seconds: float = 30
    max_observations: int = 128
    calls: int = 0
    observations: int = 0
    _started_at: float | None = field(default=None, init=False, repr=False)

    def __post_init__(self):
        if (
            self.max_calls < 1
            or self.calls < 0
            or self.max_observations < 1
            or self.observations < 0
            or not math.isfinite(self.max_elapsed_seconds)
            or self.max_elapsed_seconds <= 0
        ):
            raise ValueError("Work budget must have positive call and elapsed limits")

    def remaining_seconds(self) -> float:
        if self._started_at is None:
            return self.max_elapsed_seconds
        return max(0, self.max_elapsed_seconds - (time.monotonic() - self._started_at))

    def take_observation(self) -> bool:
        """Bound diagnostics even when a request exits before spending a call."""

        if self.observations >= self.max_observations:
            return False
        self.observations += 1
        return True

    def take(self) -> bool:
        if self.calls >= self.max_calls or self.remaining_seconds() <= 0:
            return False
        if self._started_at is None:
            self._started_at = time.monotonic()
        self.calls += 1
        return True


class JevClient:
    def __init__(
        self,
        settings: JevSettings,
        *,
        spending_ledger: ExternalModelSpendingLedger,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        self._settings = settings.model_copy(deep=True)
        self._ledger = spending_ledger
        self._transport = transport
        self._client: httpx.AsyncClient | None = None
        self._tasks: set[asyncio.Task] = set()
        self._accounting_tasks: set[asyncio.Task] = set()
        self._pending_accounting: dict[object, _AccountingWork] = {}
        self._close_task: asyncio.Task | None = None
        self._closing = False

    def update_settings(self, settings: JevSettings) -> None:
        # Requests capture a settings copy; changing credentials/endpoint does
        # not modify headers or close the shared transport beneath an active call.
        self._settings = settings.model_copy(deep=True)

    async def evaluate(
        self,
        *,
        state: str | dict | list,
        questions: dict[str, ChoiceQuestion | NoulQuestion],
        capability: Capability,
        policy: JevPolicy,
        work_budget: JevWorkBudget,
    ) -> JevResult:
        started = time.monotonic()
        if policy.mode_for(capability) == "disabled":
            return JevResult(outcome="skipped", reason="disabled")
        if self._closing:
            return JevResult(outcome="unavailable", reason="closing")
        settings = self._settings.model_copy(deep=True)
        if not settings.api_key.strip():
            return JevResult(outcome="unavailable", reason="missing_api_key")
        if self._pending_accounting or any(task.cancelling() for task in self._tasks):
            # Recovery is owned by this client. Do not issue more optional
            # provider work while previous usage is still unsettled.
            self._retry_pending_accounting()
            return JevResult(
                outcome="unavailable",
                reason="accounting_pending",
                accounting_pending=True,
            )
        if (
            not questions
            or len(questions) > policy.max_questions_per_request
            or any(not isinstance(qid, str) or not qid.strip() for qid in questions)
            or any(
                not isinstance(q, (ChoiceQuestion, NoulQuestion))
                or (
                    isinstance(q, ChoiceQuestion)
                    and len(q.criteria) > policy.max_options_per_choice
                )
                for q in questions.values()
            )
        ):
            return JevResult(outcome="unavailable", reason="invalid_questions")
        try:
            if not isinstance(state, (str, dict, list)):
                return JevResult(outcome="unavailable", reason="invalid_state")
            questions = {
                qid: question.model_copy(deep=True)
                for qid, question in questions.items()
            }
            # Serialize now to freeze caller-owned dictionaries through retries.
            body = json.dumps(
                {
                    "model": policy.model,
                    "state": state,
                    "questions": {qid: q.model_dump() for qid, q in questions.items()},
                },
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
        except (TypeError, ValueError):
            return JevResult(outcome="unavailable", reason="invalid_state")
        if len(body) > policy.max_request_bytes:
            return JevResult(outcome="unavailable", reason="request_limit")
        # Work limits are admitted policy; never trust a looser caller budget.
        if (
            work_budget.max_calls > policy.max_calls_per_window
            or work_budget.max_elapsed_seconds > policy.max_elapsed_seconds_per_window
        ):
            return JevResult(outcome="unavailable", reason="invalid_work_budget")
        progress = {
            "attempts": 0,
            "reservation": None,
            "totals": {
                "input_tokens": 0,
                "output_tokens": 0,
                "cost_usd": 0.0,
                "approximate_usage": False,
                "reported_model": None,
            },
        }
        task = asyncio.create_task(
            self._evaluate(
                settings, policy, questions, body, work_budget, started, progress
            )
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        try:
            request_remaining = max(
                0, policy.total_timeout_seconds - (time.monotonic() - started)
            )
            budget_remaining = work_budget.remaining_seconds()
            budget_limits_wait = budget_remaining <= request_remaining
            remaining = max(
                0,
                min(request_remaining, budget_remaining),
            )
            async with asyncio.timeout(remaining):
                return await asyncio.shield(task)
        except TimeoutError:
            # Even transaction rollback or a noncooperative transport cannot
            # hold the caller past its deadline. Cleanup stays with this owner.
            task.cancel()
            reservation = progress["reservation"]
            return JevResult(
                outcome="unavailable",
                reason=(
                    "work_budget_exhausted"
                    if budget_limits_wait
                    else "deadline_exceeded"
                ),
                attempts=progress["attempts"],
                elapsed_seconds=time.monotonic() - started,
                **progress["totals"],
                accounting_pending=not task.done()
                or (
                    reservation is not None
                    and reservation.token in self._pending_accounting
                ),
            )
        except asyncio.CancelledError:
            task.cancel()
            raise

    def _retry_pending_accounting(self):
        for work in tuple(self._pending_accounting.values()):
            if work.task is None or work.task.done():
                self._start_accounting(work)

    def _start_accounting(self, work: _AccountingWork):
        async def record():
            try:
                async with asyncio.timeout(work.timeout_seconds):
                    await self._ledger.record(work.reservation, **work.values)
            except Exception:
                # The reservation remains protected and the typed work item is
                # retained for another call/close to retry. No private payload logs.
                return False
            self._pending_accounting.pop(work.reservation.token, None)
            return True

        work.task = asyncio.create_task(record(), name="jev-usage-accounting")
        self._accounting_tasks.add(work.task)
        work.task.add_done_callback(self._accounting_tasks.discard)

    async def _evaluate(
        self, settings, policy, questions, body, work_budget, started, progress
    ):
        attempts = 0
        reason = "provider_error"
        work = None
        totals = progress["totals"]
        budget_limits_work = work_budget.remaining_seconds() <= max(
            0, policy.total_timeout_seconds - (time.monotonic() - started)
        )
        try:
            async with asyncio.timeout(
                min(policy.total_timeout_seconds, work_budget.remaining_seconds())
            ):
                for retry in range(policy.max_retries + 1):
                    if not work_budget.take():
                        reason = "work_budget_exhausted"
                        break
                    try:
                        # UTF-8 bytes are a conservative input estimate, not the
                        # provider tokenizer. Include framing slack. Output is free.
                        reservation = await self._ledger.reserve(
                            model=policy.model,
                            estimated_prompt_tokens=len(body) + 512,
                            estimated_output_tokens=0,
                        )
                    except LLMBudgetExceededError:
                        reason = "spending_budget_exhausted"
                        break
                    except ConfigurationError:
                        reason = "missing_model_pricing"
                        break
                    progress["reservation"] = reservation
                    usage = None
                    failed = True
                    response_data = None
                    retry_delay = min(0.25 * 2**retry, 2.0)
                    try:
                        if (
                            self._closing
                            or time.monotonic() - started
                            >= policy.total_timeout_seconds
                            or work_budget.remaining_seconds() <= 0
                        ):
                            # Admission may have completed after cancellation.
                            # No provider work was dispatched; release at zero usage.
                            usage = JevUsage(input_tokens=0, output_tokens=0)
                            reason = (
                                "closing"
                                if self._closing
                                else (
                                    "work_budget_exhausted"
                                    if work_budget.remaining_seconds() <= 0
                                    else "deadline_exceeded"
                                )
                            )
                            break
                        attempts += 1
                        progress["attempts"] = attempts
                        if self._client is None:
                            self._client = httpx.AsyncClient(
                                transport=self._transport,
                                follow_redirects=False,
                                trust_env=False,
                            )
                        async with self._client.stream(
                            "POST",
                            settings.endpoint,
                            content=body,
                            headers={
                                "Authorization": f"Bearer {settings.api_key.strip()}",
                                "Content-Type": "application/json",
                            },
                            timeout=policy.request_timeout_seconds,
                        ) as response:
                            if response.status_code in {429, 529}:
                                reason = f"http_{response.status_code}"
                                retry_after = response.headers.get("retry-after")
                                if retry_after is not None:
                                    try:
                                        value = float(retry_after)
                                        if math.isfinite(value):
                                            retry_delay = max(retry_delay, value)
                                    except ValueError:
                                        pass
                            elif not response.is_success:
                                reason = f"http_{response.status_code}"
                                break
                            else:
                                data = bytearray()
                                async for chunk in response.aiter_bytes():
                                    data.extend(chunk)
                                    if len(data) > MAX_RESPONSE_BYTES:
                                        raise ValueError("response_limit")
                                payload = json.loads(data)
                                # Retain valid usage even if semantic shape fails.
                                if isinstance(payload, dict):
                                    if isinstance(payload.get("model"), str):
                                        totals["reported_model"] = payload["model"]
                                    usage = JevUsage.model_validate(
                                        payload.get("usage")
                                    )
                                response_data = JevResponse.model_validate(payload)
                                self._validate_response(
                                    response_data, questions, policy
                                )
                                failed = False
                    except (httpx.TimeoutException, httpx.TransportError):
                        reason = "transport_error"
                    except (httpx.DecodingError, ValueError, ValidationError):
                        reason = "invalid_response"
                        break
                    finally:
                        # Accounting has its own bound and remains owned even
                        # when the foreground request's deadline/cancellation wins.
                        input_tokens = usage.input_tokens if usage else len(body) + 512
                        output_tokens = usage.output_tokens if usage else 0
                        totals["input_tokens"] += input_tokens
                        totals["output_tokens"] += output_tokens
                        totals["approximate_usage"] |= usage is None
                        if usage is not None and usage.cost is not None:
                            if totals["cost_usd"] is not None:
                                totals["cost_usd"] += usage.cost
                        elif reservation.price is None:
                            totals["cost_usd"] = None
                        elif totals["cost_usd"] is not None:
                            totals["cost_usd"] += (
                                input_tokens
                                * reservation.price.input_usd_per_million_tokens
                                + output_tokens
                                * reservation.price.output_usd_per_million_tokens
                            ) / 1_000_000
                        work = _AccountingWork(
                            reservation,
                            dict(
                                model=policy.model,
                                prompt_tokens=input_tokens,
                                completion_tokens=output_tokens,
                                approximate_usage=usage is None,
                                failed=failed,
                            ),
                            policy.accounting_timeout_seconds,
                        )
                        self._pending_accounting[reservation.token] = work
                        self._start_accounting(work)
                        accounting_ok = await asyncio.shield(work.task)
                        if not accounting_ok:
                            reason = "accounting_unavailable"
                    if not accounting_ok:
                        break
                    if not failed:
                        return JevResult(
                            outcome="available",
                            response=response_data,
                            attempts=attempts,
                            elapsed_seconds=time.monotonic() - started,
                            **totals,
                        )
                    if retry < policy.max_retries:
                        await asyncio.sleep(retry_delay)
        except TimeoutError:
            reason = (
                "work_budget_exhausted"
                if budget_limits_work
                else "deadline_exceeded"
            )
        return JevResult(
            outcome="unavailable",
            reason=reason,
            attempts=attempts,
            elapsed_seconds=time.monotonic() - started,
            **totals,
            accounting_pending=work is not None
            and work.reservation.token in self._pending_accounting,
        )

    @staticmethod
    def _validate_response(response, questions, policy):
        dated_suffix = response.model.removeprefix(f"{policy.model}-")
        reported_model_is_allowed = response.model == policy.model or (
            policy.model.startswith("typesafe/jev-")
            and response.model.startswith(f"{policy.model}-")
            and len(dated_suffix) == 8
            and dated_suffix.isdigit()
        )
        if not reported_model_is_allowed:
            raise ValueError("Unexpected provider model version")
        if set(response.answers) != set(questions):
            raise ValueError("Missing or extra answer IDs")
        for qid, question in questions.items():
            answer = response.answers[qid]
            if answer.type != question.type:
                raise ValueError("Answer type mismatch")
            if isinstance(answer, ChoiceAnswer):
                if set(answer.probabilities) != set(question.criteria):
                    raise ValueError("Option distribution mismatch")
                if answer.choice not in question.criteria:
                    raise ValueError("Unknown choice")
                if not math.isclose(
                    sum(answer.probabilities.values()), 1, abs_tol=1e-4
                ):
                    raise ValueError("Probabilities must sum to one")
                if answer.probabilities[answer.choice] < max(
                    answer.probabilities.values()
                ):
                    raise ValueError("Choice must have highest probability")

    async def close(self) -> None:
        self._closing = True
        deadline = time.monotonic() + self._settings.shutdown_timeout_seconds

        async def wait_owned(tasks):
            if not tasks:
                return
            done, pending = await asyncio.wait(
                tasks, timeout=max(0, deadline - time.monotonic())
            )
            if pending:
                raise RuntimeError("JEV cleanup deadline exceeded; owners retained")
            for task in done:
                if not task.cancelled() and task.exception() is not None:
                    raise RuntimeError(
                        "JEV request cleanup failed"
                    ) from task.exception()

        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        await wait_owned(tasks)
        self._retry_pending_accounting()
        await wait_owned(tuple(self._accounting_tasks))
        if self._pending_accounting:
            raise RuntimeError("JEV accounting cleanup failed; reservations retained")
        if self._client is not None:
            if self._close_task is None:
                self._close_task = asyncio.create_task(self._client.aclose())
            try:
                await wait_owned((self._close_task,))
            except RuntimeError:
                if self._close_task.done():
                    self._close_task = None
                raise
            self._client = None
            self._close_task = None
