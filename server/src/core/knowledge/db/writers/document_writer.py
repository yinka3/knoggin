"""Write queries for the document knowledge base."""

import json
import uuid
from typing import Dict, List, Optional

from infrastructure.postgres_client import PostgresClient


class DocumentWriter:
    """All INSERT / UPDATE / DELETE queries scoped to a single project."""

    def __init__(self, client: PostgresClient, project_id: str) -> None:
        self._client = client
        self._project_id = project_id

    async def insert_document(
        self,
        *,
        document_id: str,
        session_id: Optional[str],
        visibility_scope: str,
        original_name: str,
        relative_path: str,
        extension: str,
        size_bytes: int,
        content_hash: str,
        content: bytes,
        created_at: str,
    ) -> None:
        """
        Insert one manual-upload document row and its raw bytes in a single
        transaction.
        """
        async with self._client.transaction() as cur:
            await cur.execute(
                """
                INSERT INTO public.project_documents (
                    document_id,
                    project_id,
                    session_id,
                    visibility_scope,
                    folder_root_id,
                    source_kind,
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
                    %s, %s, %s, %s, NULL, 'manual_upload',
                    %s, %s, %s, %s, %s, 'uploaded', %s, %s
                )
                """,
                (
                    document_id,
                    self._project_id,
                    session_id,
                    visibility_scope,
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
        session_id: Optional[str],
    ) -> Optional[Dict]:
        """
        Delete one visible document row and return it, or None if not found.
        Cascades automatically delete document_content and document_chunks rows.
        """
        rows = await self._client.fetch_all(
            """
            DELETE FROM public.project_documents
            WHERE document_id = %s
              AND project_id = %s
              AND (
                  visibility_scope = 'project'
                  OR (
                      visibility_scope = 'session'
                      AND session_id = %s
                  )
              )
            RETURNING
                document_id,
                project_id,
                session_id,
                visibility_scope,
                folder_root_id,
                source_kind,
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
            (document_id, self._project_id, session_id),
        )
        return rows[0] if rows else None

    async def transition_index_status(
        self,
        *,
        document_id: str,
        session_id: Optional[str],
        status: str,
        allowed_statuses: tuple[str, ...],
        updated_at: str,
    ) -> Optional[Dict]:
        """Atomically transition one visible document into a work state."""
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
                  AND (
                      visibility_scope = 'project'
                      OR (
                          visibility_scope = 'session'
                          AND session_id = %s
                      )
                  )
                  AND status = ANY(%s)
                RETURNING
                    document_id,
                    project_id,
                    session_id,
                    visibility_scope,
                    folder_root_id,
                    source_kind,
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
                    session_id,
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

    async def insert_folder_batch(
        self,
        *,
        folder_root_id: str,
        session_id: Optional[str],
        visibility_scope: str,
        folder_name: str,
        candidate_count: int,
        candidate_bytes: int,
        excluded_count: int,
        excluded_bytes: int,
        excluded_directory_count: int,
        excluded_reason_counts: Dict,
        scan_settings: Dict,
        documents: List[Dict],
        indexed_at: str,
    ) -> None:
        """
        Atomically insert one folder-upload batch record together with all of
        its documents, raw bytes, and chunks.

        Each element of `documents` must contain:
            document_id, original_name, relative_path, extension,
            size_bytes, content_hash, content (bytes),
            chunks: List[Tuple[str, List[float]]]  (text, embedding)
        """
        async with self._client.transaction() as cur:
            await cur.execute(
                """
                        INSERT INTO public.document_folder_uploads (
                            folder_root_id,
                            project_id,
                            session_id,
                            visibility_scope,
                            folder_name,
                            candidate_count,
                            candidate_bytes,
                            document_count,
                            total_size_bytes,
                            excluded_count,
                            excluded_bytes,
                            excluded_directory_count,
                            excluded_reason_counts,
                            scan_settings,
                            created_at,
                            indexed_at
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s::jsonb, %s::jsonb,
                            %s, %s
                        )
                        """,
                (
                    folder_root_id,
                    self._project_id,
                    session_id,
                    visibility_scope,
                    folder_name,
                    candidate_count,
                    candidate_bytes,
                    len(documents),
                    sum(d["size_bytes"] for d in documents),
                    excluded_count,
                    excluded_bytes,
                    excluded_directory_count,
                    json.dumps(excluded_reason_counts),
                    json.dumps(scan_settings),
                    indexed_at,
                    indexed_at,
                ),
            )

            for document in documents:
                await cur.execute(
                    """
                            INSERT INTO public.project_documents (
                                document_id,
                                project_id,
                                session_id,
                                visibility_scope,
                                folder_root_id,
                                source_kind,
                                original_name,
                                relative_path,
                                extension,
                                size_bytes,
                                content_hash,
                                status,
                                indexed_at,
                                created_at,
                                updated_at
                            )
                            VALUES (
                                %s, %s, %s, %s, %s, 'folder_upload',
                                %s, %s, %s, %s, %s, 'indexed',
                                %s, %s, %s
                            )
                            """,
                    (
                        document["document_id"],
                        self._project_id,
                        session_id,
                        visibility_scope,
                        folder_root_id,
                        document["original_name"],
                        document["relative_path"],
                        document["extension"],
                        document["size_bytes"],
                        document["content_hash"],
                        indexed_at,
                        indexed_at,
                        indexed_at,
                    ),
                )
                await cur.execute(
                    """
                            INSERT INTO public.document_content (document_id, content)
                            VALUES (%s, %s)
                            """,
                    (document["document_id"], document["content"]),
                )
                for chunk_index, (chunk_text, embedding) in enumerate(
                    document["chunks"]
                ):
                    await cur.execute(
                        """
                                INSERT INTO public.document_chunks (
                                    chunk_id,
                                    document_id,
                                    chunk_index,
                                    content,
                                    embedding
                                )
                                VALUES (%s, %s, %s, %s, %s::vector)
                                """,
                        (
                            str(uuid.uuid4()),
                            document["document_id"],
                            chunk_index,
                            chunk_text,
                            json.dumps(embedding),
                        ),
                    )

    async def persist_indexed_chunks(
        self,
        *,
        document_id: str,
        session_id: Optional[str],
        chunks: List[str],
        embeddings: List[List[float]],
        indexed_at: str,
    ) -> Optional[Dict]:
        """
        Within a single transaction: lock the document row FOR UPDATE, skip if
        already indexed, replace existing chunks, insert new chunks, and mark
        the document as indexed.  Returns the updated document row, or None if
        the document was not found.
        """
        async with self._client.transaction() as cur:
            await cur.execute(
                """
                        SELECT
                            document_id,
                            project_id,
                            session_id,
                            visibility_scope,
                            folder_root_id,
                            source_kind,
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
                          AND (
                              pd.visibility_scope = 'project'
                              OR (
                                  pd.visibility_scope = 'session'
                                  AND pd.session_id = %s
                              )
                          )
                        FOR UPDATE
                        """,
                (document_id, self._project_id, session_id),
            )
            locked = await cur.fetchone()
            if locked is None:
                return None
            if locked["status"] == "indexed":
                return dict(locked)

            await cur.execute(
                """
                        DELETE FROM public.document_chunks
                        WHERE document_id = %s
                        """,
                (document_id,),
            )
            for chunk_index, (chunk_text, embedding) in enumerate(
                zip(chunks, embeddings)
            ):
                await cur.execute(
                    """
                            INSERT INTO public.document_chunks (
                                chunk_id,
                                document_id,
                                chunk_index,
                                content,
                                embedding
                            )
                            VALUES (%s, %s, %s, %s, %s::vector)
                            """,
                    (
                        str(uuid.uuid4()),
                        document_id,
                        chunk_index,
                        chunk_text,
                        json.dumps(embedding),
                    ),
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
                            session_id,
                            visibility_scope,
                            folder_root_id,
                            source_kind,
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
        session_id: Optional[str],
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
                          AND (
                              visibility_scope = 'project'
                              OR (
                                  visibility_scope = 'session'
                                  AND session_id = %s
                              )
                          )
                        FOR UPDATE
                        """,
                (document_id, self._project_id, session_id),
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
