"""Scoped reads for source context attached to responses and episodes."""

import hashlib
import json
from typing import Any

from common.schema.source.references import (
    AssistantMessageWithSources,
    SourceConsulted,
    SourceReference,
)
from common.scoping import require_scope_value


class SourceReferenceReader:
    """Reads source references only through owned message or episode scope."""

    def __init__(self, client) -> None:
        self.client = client

    async def get_message_source_refs(
        self,
        message_id: int,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> list[SourceConsulted]:
        if message_id <= 0:
            raise ValueError("message_id must be positive")
        scope = self._scope(
            user_name,
            project_id,
            session_id,
            "get_message_source_refs",
        )
        user_name, project_id, session_id = scope
        rows = await self.client.fetch_all(
            """
            SELECT
                ref.source_ref_id,
                ref.project_id,
                ref.session_id,
                ref.message_id,
                ref.source_kind,
                ref.document_id,
                ref.source_project_id,
                ref.canonical_url,
                ref.source_message_id,
                ref.content_hash,
                ref.locator,
                ref.excerpt,
                ref.metadata,
                ref.encounter_kind,
                ref.agent_run_id,
                ref.tool_call_id,
                ref.result_position,
                ref.idempotency_key,
                ref.created_at,
                document.status AS document_status,
                document.content_hash AS document_content_hash
            FROM public.message_source_refs AS ref
            JOIN public.messages AS message
              ON message.message_id = ref.message_id
             AND message.project_id = ref.project_id
             AND message.session_id = ref.session_id
            JOIN public.sessions AS session
              ON session.session_id = message.session_id
             AND session.project_id = message.project_id
            LEFT JOIN public.project_documents AS document
              ON document.document_id = ref.document_id
             AND document.project_id = ref.source_project_id
            WHERE ref.message_id = %s
              AND ref.project_id = %s
              AND ref.session_id = %s
              AND session.user_name = %s
            ORDER BY ref.created_at ASC, ref.result_position ASC, ref.source_ref_id ASC
            """,
            (message_id, project_id, session_id, user_name),
        )
        return [
            self._present_reference(
                self._reference_from_row(row),
                document_status=row.get("document_status"),
                document_content_hash=row.get("document_content_hash"),
                document_status_resolved="document_status" in row,
            )
            for row in rows
        ]

    async def get_assistant_message_with_sources(
        self,
        message_id: int,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> AssistantMessageWithSources | None:
        """Read one owned assistant response with its source provenance."""

        if message_id <= 0:
            raise ValueError("message_id must be positive")
        scope = self._scope(
            user_name,
            project_id,
            session_id,
            "get_assistant_message_with_sources",
        )
        user_name, project_id, session_id = scope
        row = await self.client.fetch_one(
            """
            SELECT message.message_id, message.content
            FROM public.messages AS message
            JOIN public.sessions AS session
              ON session.session_id = message.session_id
             AND session.project_id = message.project_id
            WHERE message.message_id = %s
              AND message.project_id = %s
              AND message.session_id = %s
              AND message.role = 'assistant'
              AND session.user_name = %s
            """,
            (message_id, project_id, session_id, user_name),
        )
        if row is None:
            return None
        return AssistantMessageWithSources(
            message_id=int(row["message_id"]),
            content=str(row["content"]),
            sources_consulted=await self.get_message_source_refs(
                message_id,
                user_name=user_name,
                project_id=project_id,
                session_id=session_id,
            ),
        )

    async def get_episode_source_refs(
        self,
        episode_id: str,
        *,
        user_name: str,
        project_id: str,
        session_id: str,
    ) -> list[SourceConsulted]:
        episode_id = require_scope_value(
            episode_id, "episode_id", "get_episode_source_refs"
        )
        scope = self._scope(
            user_name,
            project_id,
            session_id,
            "get_episode_source_refs",
        )
        user_name, project_id, session_id = scope
        rows = await self.client.fetch_all(
            """
            SELECT
                ref.source_ref_id,
                ref.project_id,
                ref.session_id,
                ref.message_id,
                ref.source_kind,
                ref.document_id,
                ref.source_project_id,
                ref.canonical_url,
                ref.source_message_id,
                ref.content_hash,
                ref.locator,
                ref.excerpt,
                ref.metadata,
                ref.encounter_kind,
                ref.agent_run_id,
                ref.tool_call_id,
                ref.result_position,
                ref.idempotency_key,
                ref.created_at,
                document.status AS document_status,
                document.content_hash AS document_content_hash
            FROM public.episode_messages AS attachment
            JOIN public.episodes AS episode
              ON episode.episode_id = attachment.episode_id
             AND episode.project_id = attachment.project_id
            JOIN public.sessions AS session
              ON session.session_id = attachment.session_id
             AND session.project_id = attachment.project_id
            JOIN public.message_source_refs AS ref
              ON ref.message_id = attachment.message_id
             AND ref.project_id = attachment.project_id
             AND ref.session_id = attachment.session_id
            LEFT JOIN public.project_documents AS document
              ON document.document_id = ref.document_id
             AND document.project_id = ref.source_project_id
            WHERE attachment.episode_id = %s
              AND episode.project_id = %s
              AND attachment.session_id = %s
              AND session.user_name = %s
            ORDER BY attachment.message_position ASC, ref.created_at ASC,
                ref.result_position ASC, ref.source_ref_id ASC
            """,
            (episode_id, project_id, session_id, user_name),
        )
        presented = []
        seen = set()
        for row in rows:
            reference = self._reference_from_row(row)
            key = self._episode_deduplication_key(reference)
            if key in seen:
                continue
            seen.add(key)
            presented.append(
                self._present_reference(
                    reference,
                    document_status=row.get("document_status"),
                    document_content_hash=row.get("document_content_hash"),
                    document_status_resolved="document_status" in row,
                )
            )
        return presented

    async def get_project_episode_source_refs(
        self, episode_id: str, *, user_name: str, project_id: str
    ) -> list[SourceConsulted]:
        rows = await self.client.fetch_all(
            """
            SELECT
                ref.*,
                document.status AS document_status,
                document.content_hash AS document_content_hash
            FROM public.episode_messages attachment
            JOIN public.episodes episode
              ON episode.episode_id = attachment.episode_id
             AND episode.project_id = attachment.project_id
            JOIN public.projects project ON project.project_id = episode.project_id
            JOIN public.message_source_refs ref
              ON ref.message_id = attachment.message_id
             AND ref.project_id = attachment.project_id
             AND ref.session_id = attachment.session_id
            LEFT JOIN public.project_documents AS document
              ON document.document_id = ref.document_id
             AND document.project_id = ref.source_project_id
            WHERE attachment.episode_id = %s AND episode.project_id = %s
              AND project.user_name = %s
            ORDER BY attachment.message_position, ref.created_at,
                     ref.result_position, ref.source_ref_id
            """,
            (episode_id, project_id, user_name),
        )
        presented, seen = [], set()
        for row in rows:
            reference = self._reference_from_row(row)
            key = self._episode_deduplication_key(reference)
            if key not in seen:
                seen.add(key)
                presented.append(
                    self._present_reference(
                        reference,
                    document_status=row.get("document_status"),
                    document_content_hash=row.get("document_content_hash"),
                    document_status_resolved="document_status" in row,
                    )
                )
        return presented

    @staticmethod
    def _scope(
        user_name: str,
        project_id: str,
        session_id: str,
        operation: str,
    ) -> tuple[str, str, str]:
        return (
            require_scope_value(user_name, "user_name", operation),
            require_scope_value(project_id, "project_id", operation),
            require_scope_value(session_id, "session_id", operation),
        )

    @staticmethod
    def _reference_from_row(row: dict[str, Any]) -> SourceReference:
        payload = dict(row)
        payload.pop("document_status", None)
        payload.pop("document_content_hash", None)
        for field in ("source_ref_id", "document_id"):
            if payload.get(field) is not None:
                payload[field] = str(payload[field])
        for field in ("locator", "metadata"):
            value = payload[field]
            if isinstance(value, str):
                payload[field] = json.loads(value)
        return SourceReference.model_validate(payload)

    @staticmethod
    def _present_reference(
        reference: SourceReference,
        *,
        document_status: str | None = None,
        document_content_hash: str | None = None,
        document_status_resolved: bool = False,
    ) -> SourceConsulted:
        if reference.source_kind in {"pdf_document", "text_document"}:
            source_status = (
                "unavailable"
                if document_status_resolved
                and (
                    document_status in {None, "deleted"}
                    or document_content_hash != reference.content_hash
                )
                else "available"
            )
        elif reference.source_kind in {"user_pasted_text", "web_page", "web_pdf"}:
            source_status = "available"
        else:
            source_status = "search_result_snippet"

        return SourceConsulted(
            source_kind=reference.source_kind,
            locator=reference.locator,
            excerpt=reference.excerpt,
            document_id=reference.document_id,
            source_project_id=reference.source_project_id,
            canonical_url=reference.canonical_url,
            source_message_id=reference.source_message_id,
            source_status=source_status,
            contributing_message_id=reference.message_id,
        )

    @staticmethod
    def _episode_deduplication_key(reference: SourceReference) -> tuple:
        """Remove retry duplicates without merging distinct answer passages."""

        stable_identity = (
            reference.document_id
            or reference.canonical_url
            or reference.source_message_id
        )
        locator = json.dumps(
            reference.locator.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        )
        excerpt_hash = hashlib.sha256(reference.excerpt.encode("utf-8")).hexdigest()
        return (
            reference.message_id,
            reference.source_kind,
            stable_identity,
            reference.content_hash,
            locator,
            excerpt_hash,
        )
