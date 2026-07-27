"""Durable writes for source context attached to assistant responses."""

import hashlib
import json
import uuid
from contextlib import asynccontextmanager
from typing import Sequence

from common.schema.source_reference import SourceReference, SourceReferenceCandidate
from common.scoping import require_scope_value


class SourceReferenceWriter:
    """Persists one completed response's source candidates atomically."""

    def __init__(self, client) -> None:
        self.client = client

    @asynccontextmanager
    async def _cursor(self, cursor):
        if cursor is not None:
            yield cursor
            return
        async with self.client.transaction() as transaction_cursor:
            yield transaction_cursor

    async def write_for_assistant_message(
        self,
        message_id: int,
        candidates: Sequence[SourceReferenceCandidate],
        *,
        user_name: str,
        project_id: str,
        session_id: str,
        cursor=None,
    ) -> list[SourceReference]:
        """Write candidates only for an assistant message in the given scope.

        The insert joins the canonical message and session so a caller cannot
        attach references to another user's scope or a non-assistant message.
        It also checks session-document visibility before inserting a document
        reference; that visibility rule cannot be expressed by the document's
        project-level foreign key alone.
        """

        if not candidates:
            return []
        if message_id <= 0:
            raise ValueError("message_id must be positive")

        user_name = require_scope_value(
            user_name, "user_name", "write_for_assistant_message"
        )
        project_id = require_scope_value(
            project_id, "project_id", "write_for_assistant_message"
        )
        session_id = require_scope_value(
            session_id, "session_id", "write_for_assistant_message"
        )
        self._validate_candidates(candidates, project_id, session_id)

        references: list[SourceReference] = []
        async with self._cursor(cursor) as cur:
            for candidate in candidates:
                source_ref_id = str(uuid.uuid4())
                idempotency_key = self.idempotency_key(candidate)
                await cur.execute(
                    self._insert_sql(),
                    self._insert_params(
                        source_ref_id,
                        idempotency_key,
                        message_id,
                        candidate,
                        user_name=user_name,
                        project_id=project_id,
                        session_id=session_id,
                    ),
                )
                row = await cur.fetchone()
                if row is None:
                    raise ValueError(
                        "assistant message or document is not visible in source "
                        "reference scope"
                    )
                references.append(self._reference_from_row(row))
        return references

    @staticmethod
    def idempotency_key(candidate: SourceReferenceCandidate) -> str:
        """Return a stable retry key from the run and immutable source origin."""

        if candidate.tool_call_id is not None:
            origin = f"tool:{candidate.tool_call_id}:{candidate.result_position}"
        else:
            locator = candidate.locator
            origin = (
                "pasted:"
                f"{candidate.source_message_id}:{locator.start_char}:{locator.end_char}"
            )
        raw = "|".join((candidate.agent_run_id, candidate.source_kind, origin))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @staticmethod
    def _validate_candidates(
        candidates: Sequence[SourceReferenceCandidate],
        project_id: str,
        session_id: str,
    ) -> None:
        for candidate in candidates:
            if not isinstance(candidate, SourceReferenceCandidate):
                raise TypeError("candidates must be SourceReferenceCandidate instances")
            if candidate.project_id != project_id or candidate.session_id != session_id:
                raise ValueError(
                    "source candidate scope must match assistant message scope"
                )

    @staticmethod
    def _insert_sql() -> str:
        return """
        INSERT INTO public.message_source_refs (
            source_ref_id,
            project_id,
            session_id,
            message_id,
            source_kind,
            document_id,
            canonical_url,
            source_message_id,
            content_hash,
            locator,
            excerpt,
            metadata,
            encounter_kind,
            agent_run_id,
            tool_call_id,
            result_position,
            idempotency_key,
            created_at
        )
        SELECT
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s::jsonb,
            %s, %s, %s, %s, %s, clock_timestamp()
        FROM public.messages AS message
        JOIN public.sessions AS session
          ON session.session_id = message.session_id
         AND session.project_id = message.project_id
        WHERE message.message_id = %s
          AND message.project_id = %s
          AND message.session_id = %s
          AND message.role = 'assistant'
          AND session.user_name = %s
          AND (
              %s IS NULL
              OR EXISTS (
                  SELECT 1
                  FROM public.project_documents AS document
                  WHERE document.document_id = %s
                    AND document.project_id = %s
                    AND (
                        document.visibility_scope = 'project'
                        OR (
                            document.visibility_scope = 'session'
                            AND document.session_id = %s
                        )
                    )
              )
          )
        ON CONFLICT (idempotency_key) DO UPDATE
        SET idempotency_key = EXCLUDED.idempotency_key
        RETURNING
            source_ref_id,
            project_id,
            session_id,
            message_id,
            source_kind,
            document_id,
            canonical_url,
            source_message_id,
            content_hash,
            locator,
            excerpt,
            metadata,
            encounter_kind,
            agent_run_id,
            tool_call_id,
            result_position,
            idempotency_key,
            created_at
        """

    @staticmethod
    def _insert_params(
        source_ref_id: str,
        idempotency_key: str,
        message_id: int,
        candidate: SourceReferenceCandidate,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> tuple:
        return (
            source_ref_id,
            project_id,
            session_id,
            message_id,
            candidate.source_kind,
            candidate.document_id,
            candidate.canonical_url,
            candidate.source_message_id,
            candidate.content_hash,
            json.dumps(candidate.locator.model_dump(mode="json")),
            candidate.excerpt,
            json.dumps(candidate.metadata),
            candidate.encounter_kind,
            candidate.agent_run_id,
            candidate.tool_call_id,
            candidate.result_position,
            idempotency_key,
            message_id,
            project_id,
            session_id,
            user_name,
            candidate.document_id,
            candidate.document_id,
            project_id,
            session_id,
        )

    @staticmethod
    def _reference_from_row(row: dict) -> SourceReference:
        payload = dict(row)
        for field in ("source_ref_id", "document_id"):
            if payload.get(field) is not None:
                payload[field] = str(payload[field])
        for field in ("locator", "metadata"):
            if isinstance(payload[field], str):
                payload[field] = json.loads(payload[field])
        return SourceReference.model_validate(payload)
