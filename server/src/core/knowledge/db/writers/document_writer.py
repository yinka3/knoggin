"""Write queries for the document knowledge base."""

import json
import uuid
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from infrastructure.postgres_client import PostgresClient

if TYPE_CHECKING:
    from core.knowledge.documents.storage import DocumentChunk


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
                end_paragraph
            ) FROM STDIN
            """
        ) as copy:
            for row in rows:
                await copy.write_row(row)

    @staticmethod
    def _chunk_copy_row(
        *,
        document_id: str,
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
        content: bytes,
        created_at: str,
    ) -> None:
        """
        Insert one project-owned document row and its raw bytes atomically.
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
            await cur.execute(
                """
                INSERT INTO public.document_content (document_id, content)
                VALUES (%s, %s)
                """,
                (document_id, content),
            )

    async def delete_document(
        self,
        *,
        document_id: str,
    ) -> Optional[Dict]:
        """Purge document content while retaining a provenance tombstone."""
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
            await cur.execute(
                """
                DELETE FROM public.document_content
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
                    indexed_at = NULL,
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
            SET status = 'queued', updated_at = %s
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
                status = 'queued',
                indexed_at = NULL,
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
        extracted_text: str,
        indexed_at: str,
        expected_content_hash: Optional[str] = None,
    ) -> Optional[Dict]:
        """
        Within a single transaction: lock the document row FOR UPDATE, skip if
        already indexed, replace existing chunks, insert new chunks, and mark
        the document as indexed.  Returns the updated document row, or None if
        the document was not found.
        """
        self._validate_chunk_embeddings(
            chunks,
            embeddings,
            "persist_indexed_chunks",
        )
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
            if (
                expected_content_hash is not None
                and locked["content_hash"] != expected_content_hash
            ):
                return None
            if locked["status"] == "indexed":
                return dict(locked)
            if locked["status"] != "indexing":
                return None

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
                UPDATE public.document_content
                SET
                    extracted_text = %s,
                    extracted_content_hash = %s
                WHERE document_id = %s
                """,
                (extracted_text, locked["content_hash"], document_id),
            )

            await cur.execute(
                """
                        UPDATE public.project_documents
                        SET
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
                            status,
                            created_at,
                            updated_at,
                            indexed_at,
                            error_message
                        """,
                (indexed_at, indexed_at, document_id),
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
        Within a single transaction: lock the document row FOR UPDATE, skip if
        already indexed, clear any partial chunks, and mark the document as
        failed.
        """
        async with self._client.transaction() as cur:
            await cur.execute(
                """
                        SELECT status
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
