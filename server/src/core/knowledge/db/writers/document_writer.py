"""Write queries for the document knowledge base."""

import json
import uuid
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from infrastructure.postgres_client import PostgresClient

if TYPE_CHECKING:
    from core.knowledge.documents.storage import DocumentChunk, DocumentParseSnapshot


class DocumentWriter:
    """All INSERT / UPDATE / DELETE queries scoped to a single project."""

    def __init__(self, client: PostgresClient, project_id: str) -> None:
        self._client = client
        self._project_id = project_id

    @staticmethod
    def _validate_chunk_embeddings(
        chunks: List[Union["DocumentChunk", str]],
        embeddings: List[List[float]],
        operation: str,
    ) -> None:
        if len(chunks) != len(embeddings):
            raise ValueError(
                f"{operation}: chunks and embeddings must have the same length"
            )

    @staticmethod
    async def _copy_chunk_rows(cur, rows: List[tuple]) -> None:
        """Load chunk rows with one COPY stream instead of per-row executes."""
        if not rows:
            return
        async with cur.copy(
            """
            COPY public.document_chunks (
                chunk_id,
                document_id,
                snapshot_id,
                chunk_index,
                content,
                relative_path,
                embedding,
                language,
                chunk_kind,
                symbol_name,
                page_number,
                start_line,
                end_line,
                start_row,
                end_row,
                section_path,
                start_paragraph,
                end_paragraph,
                layout_region
            ) FROM STDIN
            """
        ) as copy:
            for row in rows:
                await copy.write_row(row)

    @staticmethod
    def _chunk_copy_row(
        *,
        document_id: str,
        snapshot_id: str,
        relative_path: str,
        chunk_index: int,
        chunk: Union["DocumentChunk", str],
        embedding: List[float],
    ) -> tuple:
        if isinstance(chunk, str):
            from core.knowledge.documents.storage import DocumentChunk

            chunk = DocumentChunk(content=chunk)
        return (
            str(uuid.uuid4()),
            document_id,
            snapshot_id,
            chunk_index,
            chunk.content,
            relative_path,
            json.dumps(embedding),
            chunk.language,
            chunk.chunk_kind,
            chunk.symbol_name,
            chunk.page_number,
            chunk.start_line,
            chunk.end_line,
            chunk.start_row,
            chunk.end_row,
            list(chunk.section_path) if chunk.section_path is not None else None,
            chunk.start_paragraph,
            chunk.end_paragraph,
            json.dumps(chunk.layout_region) if chunk.layout_region is not None else None,
        )

    async def insert_document(
        self,
        *,
        document_id: str,
        original_name: str,
        relative_path: str,
        extension: str,
        size_bytes: int,
        content_hash: str,
        created_at: str,
    ) -> None:
        """
        Insert one project-owned document catalog row atomically.
        """
        async with self._client.transaction() as cur:
            await cur.execute(
                """
                INSERT INTO public.project_documents (
                    document_id,
                    project_id,
                    original_name,
                    relative_path,
                    extension,
                    size_bytes,
                    content_hash,
                    status,
                    created_at,
                    updated_at
                )
                VALUES (
                    %s, %s,
                    %s, %s, %s, %s, %s, 'queued', %s, %s
                )
                """,
                (
                    document_id,
                    self._project_id,
                    original_name,
                    relative_path,
                    extension,
                    size_bytes,
                    content_hash,
                    created_at,
                    created_at,
                ),
            )

    async def delete_document(
        self,
        *,
        document_id: str,
    ) -> Optional[Dict]:
        """Tombstone a document while retaining immutable parse evidence."""
        async with self._client.transaction() as cur:
            await cur.execute(
                """
            UPDATE public.project_documents
            SET status = 'deleted',
                deleted_at = COALESCE(deleted_at, now()),
                indexed_at = NULL,
                error_message = NULL,
                updated_at = now()
            WHERE document_id = %s
              AND project_id = %s
              AND status <> 'deleted'
            RETURNING
                document_id,
                project_id,
                original_name,
                relative_path,
                extension,
                size_bytes,
                content_hash,
                current_snapshot_id,
                status,
                created_at,
                updated_at,
                indexed_at,
                error_message,
                deleted_at
                """,
                (document_id, self._project_id),
            )
            row = await cur.fetchone()
            if row is None:
                return None
            await cur.execute(
                """
                DELETE FROM public.document_chunks
                WHERE document_id = %s
                """,
                (document_id,),
            )
            return dict(row)

    async def transition_index_status(
        self,
        *,
        document_id: str,
        status: str,
        allowed_statuses: tuple[str, ...],
        updated_at: str,
    ) -> Optional[Dict]:
        """Atomically transition one project-owned document into a work state."""
        allowed = tuple(allowed_statuses)
        async with self._client.transaction() as cur:
            await cur.execute(
                """
                UPDATE public.project_documents
                SET
                    status = %s,
                    indexed_at = CASE
                        WHEN current_snapshot_id IS NULL THEN NULL
                        ELSE indexed_at
                    END,
                    error_message = NULL,
                    updated_at = %s
                WHERE document_id = %s
                  AND project_id = %s
                  AND status = ANY(%s)
                RETURNING
                    document_id,
                    project_id,
                    original_name,
                    relative_path,
                    extension,
                    size_bytes,
                    content_hash,
                    current_snapshot_id,
                    status,
                    created_at,
                    updated_at,
                    indexed_at,
                    error_message
                """,
                (
                    status,
                    updated_at,
                    document_id,
                    self._project_id,
                    list(allowed),
                ),
            )
            row = await cur.fetchone()
            return dict(row) if row else None

    async def requeue_interrupted_indexes(self, *, updated_at: str) -> int:
        """Make work left in ``indexing`` by a stopped process recoverable."""
        rows = await self._client.fetch_all(
            """
            UPDATE public.project_documents
            SET
                status = CASE
                    WHEN current_snapshot_id IS NULL THEN 'queued'
                    ELSE 'indexed'
                END,
                updated_at = %s
            WHERE project_id = %s
              AND status = 'indexing'
            RETURNING document_id
            """,
            (updated_at, self._project_id),
        )
        return len(rows)

    async def requeue_index_claims(
        self,
        *,
        document_ids: List[str],
        updated_at: str,
    ) -> int:
        """Release specific cancelled index claims back to the durable queue."""
        if not document_ids:
            return 0
        rows = await self._client.fetch_all(
            """
            UPDATE public.project_documents
            SET
                status = CASE
                    WHEN current_snapshot_id IS NULL THEN 'queued'
                    ELSE 'indexed'
                END,
                indexed_at = CASE
                    WHEN current_snapshot_id IS NULL THEN NULL
                    ELSE indexed_at
                END,
                error_message = NULL,
                updated_at = %s
            WHERE project_id = %s
              AND document_id = ANY(%s)
              AND status = 'indexing'
            RETURNING document_id
            """,
            (updated_at, self._project_id, document_ids),
        )
        return len(rows)

    async def persist_indexed_chunks(
        self,
        *,
        document_id: str,
        chunks: List[Union["DocumentChunk", str]],
        embeddings: List[List[float]],
        parse_snapshot: "DocumentParseSnapshot",
        indexed_at: str,
        read_content_hash: str,
    ) -> Optional[Dict]:
        """Publish one immutable parse snapshot and its current chunk projection.

        The transaction verifies the exact bytes that were parsed, records the
        snapshot first, replaces only the current retrieval projection, and
        moves the document pointer last.  Earlier snapshots remain available to
        existing source references after a reindex or tombstone.
        """
        self._validate_chunk_embeddings(
            chunks,
            embeddings,
            "persist_indexed_chunks",
        )
        snapshot_id = str(uuid.uuid4())
        snapshot_payload = json.dumps(parse_snapshot.to_storage_payload())
        async with self._client.transaction() as cur:
            await cur.execute(
                """
                SELECT
                    document_id,
                    project_id,
                    original_name,
                    relative_path,
                    extension,
                    size_bytes,
                    content_hash,
                    current_snapshot_id,
                    status,
                    created_at,
                    updated_at,
                    indexed_at,
                    error_message,
                    (
                        SELECT COUNT(*)::INTEGER
                        FROM public.document_chunks AS dc
                        WHERE dc.document_id = pd.document_id
                    ) AS chunk_count
                FROM public.project_documents AS pd
                WHERE pd.document_id = %s
                  AND pd.project_id = %s
                FOR UPDATE
                """,
                (document_id, self._project_id),
            )
            locked = await cur.fetchone()
            if locked is None:
                return None
            if locked["content_hash"] != read_content_hash:
                return None
            if locked["status"] == "indexed":
                return dict(locked)
            if locked["status"] != "indexing":
                return None

            await cur.execute(
                """
                INSERT INTO public.document_parse_snapshots (
                    snapshot_id,
                    document_id,
                    source_content_hash,
                    parser_name,
                    parser_version,
                    parser_fingerprint,
                    snapshot
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                """,
                (
                    snapshot_id,
                    document_id,
                    read_content_hash,
                    parse_snapshot.parser_name,
                    parse_snapshot.parser_version,
                    parse_snapshot.parser_fingerprint,
                    snapshot_payload,
                ),
            )
            await cur.execute(
                """
                DELETE FROM public.document_chunks
                WHERE document_id = %s
                """,
                (document_id,),
            )
            await self._copy_chunk_rows(
                cur,
                [
                    self._chunk_copy_row(
                        document_id=document_id,
                        snapshot_id=snapshot_id,
                        relative_path=locked["relative_path"],
                        chunk_index=chunk_index,
                        chunk=chunk,
                        embedding=embedding,
                    )
                    for chunk_index, (chunk, embedding) in enumerate(
                        zip(chunks, embeddings)
                    )
                ],
            )

            await cur.execute(
                """
                UPDATE public.project_documents
                SET
                    current_snapshot_id = %s,
                    status = 'indexed',
                    indexed_at = %s,
                    error_message = NULL,
                    updated_at = %s
                WHERE document_id = %s
                RETURNING
                    document_id,
                    project_id,
                    original_name,
                    relative_path,
                    extension,
                    size_bytes,
                    content_hash,
                    current_snapshot_id,
                    status,
                    created_at,
                    updated_at,
                    indexed_at,
                    error_message
                """,
                (snapshot_id, indexed_at, indexed_at, document_id),
            )
            updated = await cur.fetchone()
            if updated is None:
                raise RuntimeError("Indexed document status update failed")
            result = dict(updated)
            result["chunk_count"] = len(chunks)
            return result

    async def record_index_failure(
        self,
        *,
        document_id: str,
        error_message: str,
        updated_at: str,
    ) -> None:
        """
        A failed first index has no usable projection and becomes ``failed``.
        A failed reindex keeps the previous snapshot and stays readable as
        ``indexed``; the error records why the newer parse was not published.
        """
        async with self._client.transaction() as cur:
            await cur.execute(
                """
                        SELECT status, current_snapshot_id
                        FROM public.project_documents
                        WHERE document_id = %s
                          AND project_id = %s
                        FOR UPDATE
                        """,
                (document_id, self._project_id),
            )
            row = await cur.fetchone()
            if row is None or row["status"] == "indexed":
                return

            if row["current_snapshot_id"] is not None:
                await cur.execute(
                    """
                    UPDATE public.project_documents
                    SET
                        status = 'indexed',
                        error_message = %s,
                        updated_at = %s
                    WHERE document_id = %s
                      AND status = 'indexing'
                    """,
                    (error_message, updated_at, document_id),
                )
                return

            await cur.execute(
                """
                        DELETE FROM public.document_chunks
                        WHERE document_id = %s
                        """,
                (document_id,),
            )
            await cur.execute(
                """
                        UPDATE public.project_documents
                        SET
                            status = 'failed',
                            indexed_at = NULL,
                            error_message = %s,
                            updated_at = %s
                        WHERE document_id = %s
                          AND status <> 'indexed'
                        """,
                (error_message, updated_at, document_id),
            )

    async def upsert_scan_settings(
        self,
        *,
        settings_json: str,
        saved_at: str,
    ) -> None:
        """Insert or update project scan settings."""
        await self._client.execute(
            """
            INSERT INTO public.project_document_scan_settings (
                project_id,
                settings,
                created_at,
                updated_at
            )
            VALUES (%s, %s::jsonb, %s, %s)
            ON CONFLICT (project_id) DO UPDATE
            SET
                settings = EXCLUDED.settings,
                updated_at = EXCLUDED.updated_at
            """,
            (self._project_id, settings_json, saved_at, saved_at),
        )

    async def delete_scan_settings(self) -> None:
        """Remove saved scan settings for this project."""
        await self._client.execute(
            """
            DELETE FROM public.project_document_scan_settings
            WHERE project_id = %s
            """,
            (self._project_id,),
        )

    async def insert_saved_web_link(
        self,
        *,
        link_id: str,
        url: str,
        title: str | None,
        summary: str | None,
        created_at: str,
    ) -> Dict:
        """Persist one project-owned bookmark without document derivation."""
        async with self._client.transaction() as cur:
            await cur.execute(
                """
                INSERT INTO public.saved_web_links (
                    link_id,
                    project_id,
                    url,
                    title,
                    summary,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING
                    link_id,
                    project_id,
                    url,
                    title,
                    summary,
                    created_at,
                    updated_at
                """,
                (
                    link_id,
                    self._project_id,
                    url,
                    title,
                    summary,
                    created_at,
                    created_at,
                ),
            )
            row = await cur.fetchone()
        if row is None:
            raise RuntimeError("Saved web link insert did not return a row")
        return dict(row)

    async def update_saved_web_link(
        self,
        *,
        link_id: str,
        title: str | None,
        summary: str | None,
        updated_at: str,
    ) -> Optional[Dict]:
        """Replace bookmark presentation fields within the active project."""
        async with self._client.transaction() as cur:
            await cur.execute(
                """
                UPDATE public.saved_web_links
                SET
                    title = %s,
                    summary = %s,
                    updated_at = %s
                WHERE link_id = %s
                  AND project_id = %s
                RETURNING
                    link_id,
                    project_id,
                    url,
                    title,
                    summary,
                    created_at,
                    updated_at
                """,
                (title, summary, updated_at, link_id, self._project_id),
            )
            row = await cur.fetchone()
        return dict(row) if row is not None else None

    async def delete_saved_web_link(self, *, link_id: str) -> bool:
        """Remove a project bookmark without touching source provenance."""
        async with self._client.transaction() as cur:
            await cur.execute(
                """
                DELETE FROM public.saved_web_links
                WHERE link_id = %s
                  AND project_id = %s
                RETURNING link_id
                """,
                (link_id, self._project_id),
            )
            return await cur.fetchone() is not None
