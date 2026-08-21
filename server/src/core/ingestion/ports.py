"""Focused subsystem contracts used by ingestion workflows."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from common.schema.ingestion.contracts import GraphWriteSummary, IngestionCommit


@runtime_checkable
class IngestionPersistence(Protocol):
    """Durable operations owned by the ingestion worker."""

    async def seal_due_user_messages(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        settle_delay_seconds: float,
    ) -> int: ...

    async def claim_next_ingestion_batch(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        batch_size: int,
    ) -> Any | None: ...

    async def release_ingestion_claim(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        batch_id: str,
        blocked: bool,
    ) -> bool: ...

    async def fail_ingestion_claim(
        self,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        batch_id: str,
        failure_stage: str,
        failure_code: str,
        error_summary: str,
        retryable: bool,
        max_attempts: int,
    ) -> bool: ...


@runtime_checkable
class IngestionGraphPersistence(Protocol):
    """Graph validation and commit operations required by one sealed batch."""

    async def validate_existing_ids(
        self,
        ids: list[int],
        *,
        visible_project_ids: list[str],
    ) -> set[int] | None: ...

    async def commit_ingestion(self, commit: IngestionCommit) -> GraphWriteSummary: ...
