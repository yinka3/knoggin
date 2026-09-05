"""Deterministic project-level admission for durable semantic windows."""

from __future__ import annotations

from dataclasses import dataclass
from time import time
from typing import Callable, Protocol
from uuid import uuid4

from common.conf.domain_config import CompiledDomain
from common.schema.semantic_window import (
    SemanticWindowClaimResult,
    SemanticWindowMessage,
    SemanticWindowOrigin,
    SemanticWindowRecord,
    SemanticWindowStage,
)
from common.schema.settings import EpisodeSettings, IngestionSettings
from core.ingestion.policy import IngestionPolicy
from core.knowledge.episodes.policy import EpisodeGenerationPolicy


class SemanticWindowStore(Protocol):
    async def get_unclaimed_project_semantic_exchange_rows(
        self, *, user_name: str, project_id: str
    ) -> list[dict]: ...

    async def claim_project_semantic_window(
        self,
        window: SemanticWindowRecord,
        messages: list[SemanticWindowMessage],
    ) -> SemanticWindowClaimResult: ...


TokenCounter = Callable[[str], int]


@dataclass(frozen=True, slots=True)
class ProjectSemanticWindowAdmission:
    """One frozen membership proposal, before its atomic durable claim."""

    window: SemanticWindowRecord
    messages: tuple[SemanticWindowMessage, ...]
    close_reason: str


@dataclass(frozen=True, slots=True)
class _Exchange:
    user_message_id: int
    session_id: str
    source_timestamp_ms: int | None
    closed_at_ms: int
    session_closed: bool
    messages: tuple[dict, ...]


class SemanticWindowAdmission:
    """Select whole, FIFO-safe exchanges without becoming a semantic writer.

    The 128K setting is the only size control.  A crossing exchange is included
    whole and ends selection immediately; the retained overfill documents an
    unavoidable boundary rather than a second configurable cap.
    """

    IDLE_FLUSH_SECONDS = 300
    TOKEN_ESTIMATOR = "llm.count_tokens"
    TOKEN_ESTIMATOR_VERSION = "runtime-v1"
    POLICY_VERSION = 1
    EPISODE_POLICY_REFERENCE_WINDOW_SIZE = 24

    def __init__(
        self,
        knowledge_store: SemanticWindowStore,
        settings: IngestionSettings,
        *,
        token_counter: TokenCounter,
        episode_settings: EpisodeSettings | None = None,
        now_ms: Callable[[], int] | None = None,
    ) -> None:
        if not callable(token_counter):
            raise TypeError("Semantic window admission requires an LLM token counter")
        self.knowledge_store = knowledge_store
        self._token_counter = token_counter
        self._now_ms = now_ms or (lambda: int(time() * 1000))
        self.update_settings(settings)
        self.update_episode_settings(episode_settings or EpisodeSettings())

    def update_settings(self, settings: IngestionSettings) -> None:
        if not isinstance(settings, IngestionSettings):
            raise TypeError("settings must be IngestionSettings")
        self._settings = settings

    def update_episode_settings(self, settings: EpisodeSettings) -> None:
        if not isinstance(settings, EpisodeSettings):
            raise TypeError("episode settings must be EpisodeSettings")
        self._episode_settings = settings

    async def select(
        self,
        *,
        user_name: str,
        project_id: str,
        domain: CompiledDomain,
        ingestion_policy: IngestionPolicy | None = None,
        force_flush: bool = False,
    ) -> ProjectSemanticWindowAdmission | None:
        """Build one proposal from currently admissible complete exchanges."""

        if not isinstance(domain, CompiledDomain):
            raise TypeError("domain must be a CompiledDomain snapshot")
        if ingestion_policy is not None:
            if not isinstance(ingestion_policy, IngestionPolicy):
                raise TypeError("ingestion_policy must be an IngestionPolicy")
            if ingestion_policy.domain != domain:
                raise ValueError("ingestion policy must use the admitted domain snapshot")
        rows = await self.knowledge_store.get_unclaimed_project_semantic_exchange_rows(
            user_name=user_name,
            project_id=project_id,
        )
        exchanges = self._eligible_exchanges(rows)
        if not exchanges:
            return None

        selected: list[_Exchange] = []
        target = self._settings.semantic_window_tokens
        close_reason: str | None = None
        for exchange in exchanges:
            prospective = [*selected, exchange]
            prospective_tokens = self._count_tokens(prospective)
            selected.append(exchange)
            if prospective_tokens >= target:
                close_reason = (
                    "oversized_exchange" if len(selected) == 1 else "target_crossed"
                )
                break

        if close_reason is None:
            latest_close = max(exchange.closed_at_ms for exchange in selected)
            if force_flush:
                close_reason = "explicit_flush"
            elif any(exchange.session_closed for exchange in selected):
                close_reason = "session_closed"
            elif self._now_ms() >= latest_close + (self.IDLE_FLUSH_SECONDS * 1000):
                close_reason = "idle_flush"
            else:
                return None

        source_token_count = self._count_tokens(selected)
        overfill = max(0, source_token_count - target)
        # A flattened comprehension cannot retain one monotonic ordinal across
        # exchange bundles without obscuring the invariant; keep it explicit.
        flattened: list[SemanticWindowMessage] = []
        for exchange in selected:
            for message in exchange.messages:
                flattened.append(
                    SemanticWindowMessage(
                        message_id=int(message["message_id"]),
                        session_id=exchange.session_id,
                        exchange_user_message_id=exchange.user_message_id,
                        role=str(message["role"]),
                        ordinal=len(flattened),
                    )
                )
        messages = tuple(flattened)
        policy_snapshot: dict[str, object] = {
            "admission_policy": {
                "version": self.POLICY_VERSION,
                "semantic_window_tokens": target,
                "idle_flush_seconds": self.IDLE_FLUSH_SECONDS,
                "close_reason": close_reason,
                "whole_exchange_only": True,
            },
            "episode_generation_policy": EpisodeGenerationPolicy.capture(
                settings=self._episode_settings,
                episode_window_size=self.EPISODE_POLICY_REFERENCE_WINDOW_SIZE,
            ).semantic_window_snapshot(),
            "compiled_domain": domain.to_dict(),
        }
        if ingestion_policy is not None:
            policy_snapshot["ingestion_policy"] = (
                ingestion_policy.semantic_window_snapshot()
            )
        window = SemanticWindowRecord(
            window_id=uuid4(),
            user_name=user_name,
            project_id=project_id,
            origin=SemanticWindowOrigin.CONVERSATION,
            stage=SemanticWindowStage.CLAIMED,
            domain_version=domain.version,
            policy_snapshot=policy_snapshot,
            source_token_count=source_token_count,
            token_estimator=self.TOKEN_ESTIMATOR,
            token_estimator_version=self.TOKEN_ESTIMATOR_VERSION,
            overfill_tokens=overfill,
            overfill_ratio=(overfill / target) if target else 0.0,
        )
        return ProjectSemanticWindowAdmission(
            window=window,
            messages=messages,
            close_reason=close_reason,
        )

    async def claim_next(
        self,
        *,
        user_name: str,
        project_id: str,
        domain: CompiledDomain,
        ingestion_policy: IngestionPolicy | None = None,
        force_flush: bool = False,
    ) -> SemanticWindowClaimResult | None:
        """Atomically claim the next selected proposal, if its trigger is due."""

        admission = await self.select(
            user_name=user_name,
            project_id=project_id,
            domain=domain,
            ingestion_policy=ingestion_policy,
            force_flush=force_flush,
        )
        if admission is None:
            return None
        return await self.knowledge_store.claim_project_semantic_window(
            admission.window,
            list(admission.messages),
        )

    def _eligible_exchanges(self, rows: list[dict]) -> list[_Exchange]:
        """Apply per-session FIFO first, then merge eligible session heads."""

        streams: dict[str, list[_Exchange]] = {}
        blocked_sessions: set[str] = set()
        for row in rows:
            session_id = str(row["session_id"])
            if bool(row.get("already_claimed")):
                # A completed durable window is no longer a barrier for later
                # work.  An active one is rejected by the writer's project lock.
                continue
            if session_id in blocked_sessions:
                continue
            exchange = self._exchange_from_row(row)
            if exchange is None:
                blocked_sessions.add(session_id)
                continue
            streams.setdefault(session_id, []).append(exchange)

        exchanges = [exchange for stream in streams.values() for exchange in stream]
        return sorted(
            exchanges,
            key=lambda exchange: (
                exchange.source_timestamp_ms is None,
                exchange.source_timestamp_ms or 0,
                exchange.user_message_id,
            ),
        )

    @staticmethod
    def _exchange_from_row(row: dict) -> _Exchange | None:
        """Return a sealed, closed exchange or mark its session FIFO-blocked."""

        if (
            row.get("user_exchange_state") != "closed"
            or row.get("user_lifecycle_state") != "sealed"
            or row.get("user_exchange_outcome") is None
            or row.get("exchange_closed_at_ms") is None
        ):
            return None
        outcome = str(row["user_exchange_outcome"])
        assistant_id = row.get("assistant_message_id")
        if outcome == "assistant_final":
            if (
                assistant_id is None
                or row.get("assistant_lifecycle_state") != "sealed"
            ):
                return None
        elif assistant_id is not None:
            # A non-final terminal exchange must not manufacture an assistant
            # evidence row.  Treat corruption as a FIFO barrier until repaired.
            return None

        messages = [
            {
                "message_id": int(row["user_message_id"]),
                "role": "user",
                "content": str(row.get("user_content") or ""),
            }
        ]
        if assistant_id is not None:
            messages.append(
                {
                    "message_id": int(assistant_id),
                    "role": "assistant",
                    "content": str(row.get("assistant_content") or ""),
                }
            )
        return _Exchange(
            user_message_id=int(row["user_message_id"]),
            session_id=str(row["session_id"]),
            source_timestamp_ms=(
                None
                if row.get("user_timestamp_ms") is None
                else int(row["user_timestamp_ms"])
            ),
            closed_at_ms=int(row["exchange_closed_at_ms"]),
            session_closed=row.get("session_status") != "open",
            messages=tuple(messages),
        )

    def _count_tokens(self, exchanges: list[_Exchange]) -> int:
        rendered = "\n\n".join(
            f"{message['role'].upper()}:\n{message['content']}"
            for exchange in exchanges
            for message in exchange.messages
        )
        count = self._token_counter(rendered)
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            raise ValueError("LLM token counter must return a non-negative integer")
        return count
