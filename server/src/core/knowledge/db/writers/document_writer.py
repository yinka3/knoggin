"""Write queries for the document knowledge base."""

import hashlib
import json
import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from common.exceptions import WorkspaceConflictError
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

    async def insert_workspace_source(
        self,
        *,
        source_id: str,
        session_id: Optional[str],
        visibility_scope: str,
        display_name: str,
        created_at: str,
    ) -> None:
        """Persist the stable identity for a synchronizable workspace."""
        async with self._client.transaction() as cur:
            await cur.execute(
                """
                INSERT INTO public.document_workspace_sources (
                    source_id,
                    project_id,
                    session_id,
                    visibility_scope,
                    display_name,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    source_id,
                    self._project_id,
                    session_id,
                    visibility_scope,
                    display_name,
                    created_at,
                    created_at,
                ),
            )

    async def insert_managed_workspace_source(
        self,
        *,
        source_id: str,
        display_name: str,
        created_at: str,
    ) -> None:
        """Create the single project-owned managed workspace source."""
        async with self._client.transaction() as cur:
            await cur.execute(
                """
                INSERT INTO public.document_workspace_sources (
                    source_id,
                    project_id,
                    session_id,
                    visibility_scope,
                    ownership_mode,
                    display_name,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, NULL, 'project',
                        'managed_project_workspace', %s, %s, %s)
                """,
                (source_id, self._project_id, display_name, created_at, created_at),
            )

    async def insert_managed_workspace_source_and_file(
        self,
        *,
        source_id: str,
        display_name: str,
        relative_path: str,
        original_name: str,
        extension: str,
        content: bytes,
        content_hash: str,
        created_at: str,
        cursor: Optional[Any] = None,
    ) -> Dict:
        """Atomically create a managed source and its first queued file.

        ``cursor`` is supplied by project creation so the project row, source,
        document metadata, and raw content share one transaction. When omitted,
        this method provides the same operation as a standalone transaction.
        """
        if cursor is not None:
            return await self._insert_managed_workspace_source_and_file(
                cursor,
                source_id=source_id,
                display_name=display_name,
                relative_path=relative_path,
                original_name=original_name,
                extension=extension,
                content=content,
                content_hash=content_hash,
                created_at=created_at,
            )
        async with self._client.transaction() as cur:
            return await self._insert_managed_workspace_source_and_file(
                cur,
                source_id=source_id,
                display_name=display_name,
                relative_path=relative_path,
                original_name=original_name,
                extension=extension,
                content=content,
                content_hash=content_hash,
                created_at=created_at,
            )

    async def _insert_managed_workspace_source_and_file(
        self,
        cur,
        *,
        source_id: str,
        display_name: str,
        relative_path: str,
        original_name: str,
        extension: str,
        content: bytes,
        content_hash: str,
        created_at: str,
    ) -> Dict:
        await cur.execute(
            """
            INSERT INTO public.document_workspace_sources (
                source_id,
                project_id,
                session_id,
                visibility_scope,
                ownership_mode,
                display_name,
                created_at,
                updated_at
            )
            VALUES (%s, %s, NULL, 'project',
                    'managed_project_workspace', %s, %s, %s)
            """,
            (source_id, self._project_id, display_name, created_at, created_at),
        )
        document_id = str(uuid.uuid4())
        await cur.execute(
            """
            INSERT INTO public.project_documents (
                document_id,
                project_id,
                session_id,
                visibility_scope,
                folder_root_id,
                source_id,
                source_kind,
                original_name,
                relative_path,
                extension,
                size_bytes,
                content_hash,
                status,
                indexed_at,
                error_message,
                created_at,
                updated_at
            )
            VALUES (
                %s, %s, NULL, 'project', NULL, %s, 'workspace',
                %s, %s, %s, %s, %s, 'queued', NULL, NULL, %s, %s
            )
            """,
            (
                document_id,
                self._project_id,
                source_id,
                original_name,
                relative_path,
                extension,
                len(content),
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
        return {
            "source_id": source_id,
            "document_id": document_id,
            "project_id": self._project_id,
            "session_id": None,
            "visibility_scope": "project",
            "ownership_mode": "managed_project_workspace",
            "source_kind": "workspace",
            "original_name": original_name,
            "relative_path": relative_path,
            "extension": extension,
            "size_bytes": len(content),
            "content_hash": content_hash,
            "status": "queued",
            "indexed_at": None,
            "error_message": None,
            "created_at": created_at,
            "updated_at": created_at,
            "chunk_count": 0,
        }

    async def insert_managed_workspace_file(
        self,
        *,
        source_id: str,
        relative_path: str,
        original_name: str,
        extension: str,
        content: bytes,
        content_hash: str,
        updated_at: str,
    ) -> Dict:
        """Atomically insert one managed file and queue its indexing state."""
        document_id = str(uuid.uuid4())
        async with self._client.transaction() as cur:
            await cur.execute(
                """
                SELECT source_id
                FROM public.document_workspace_sources
                WHERE source_id = %s
                  AND project_id = %s
                  AND ownership_mode = 'managed_project_workspace'
                FOR UPDATE
                """,
                (source_id, self._project_id),
            )
            if await cur.fetchone() is None:
                raise FileNotFoundError("Managed workspace source not found")

            await cur.execute(
                """
                SELECT document_id
                FROM public.project_documents
                WHERE project_id = %s
                  AND source_id = %s
                  AND relative_path = %s
                FOR UPDATE
                """,
                (self._project_id, source_id, relative_path),
            )
            if await cur.fetchone() is not None:
                raise FileExistsError("Managed workspace file already exists")

            await cur.execute(
                """
                INSERT INTO public.project_documents (
                    document_id,
                    project_id,
                    session_id,
                    visibility_scope,
                    folder_root_id,
                    source_id,
                    source_kind,
                    original_name,
                    relative_path,
                    extension,
                    size_bytes,
                    content_hash,
                    status,
                    indexed_at,
                    error_message,
                    created_at,
                    updated_at
                )
                VALUES (
                    %s, %s, NULL, 'project', NULL, %s, 'workspace',
                    %s, %s, %s, %s, %s, 'queued', NULL, NULL, %s, %s
                )
                """,
                (
                    document_id,
                    self._project_id,
                    source_id,
                    original_name,
                    relative_path,
                    extension,
                    len(content),
                    content_hash,
                    updated_at,
                    updated_at,
                ),
            )
            await cur.execute(
                """
                INSERT INTO public.document_content (document_id, content)
                VALUES (%s, %s)
                """,
                (document_id, content),
            )
        return {
            "document_id": document_id,
            "project_id": self._project_id,
            "session_id": None,
            "visibility_scope": "project",
            "source_kind": "workspace",
            "original_name": original_name,
            "relative_path": relative_path,
            "extension": extension,
            "size_bytes": len(content),
            "content_hash": content_hash,
            "status": "queued",
            "indexed_at": None,
            "error_message": None,
            "created_at": updated_at,
            "updated_at": updated_at,
            "chunk_count": 0,
        }

    async def update_managed_workspace_file(
        self,
        *,
        relative_path: str,
        content: bytes,
        content_hash: str,
        expected_content_hash: str,
        updated_at: str,
    ) -> Dict:
        """Atomically replace a managed file after checking its content hash."""
        async with self._client.transaction() as cur:
            row = await self._lock_managed_workspace_file(cur, relative_path)
            if row is None:
                raise FileNotFoundError("Managed workspace file not found")
            if row["content_hash"] != expected_content_hash:
                raise WorkspaceConflictError("Managed workspace file changed")
            await self._replace_managed_workspace_file(
                cur,
                row,
                content=content,
                content_hash=content_hash,
                updated_at=updated_at,
            )
        return self._managed_file_result(
            row,
            content=content,
            content_hash=content_hash,
            updated_at=updated_at,
        )

    async def append_managed_workspace_file(
        self,
        *,
        relative_path: str,
        append_content: bytes,
        expected_content_hash: str,
        updated_at: str,
    ) -> Dict:
        """Atomically append to a managed file under optimistic concurrency."""
        async with self._client.transaction() as cur:
            row = await self._lock_managed_workspace_file(
                cur,
                relative_path,
                include_content=True,
            )
            if row is None:
                raise FileNotFoundError("Managed workspace file not found")
            if row["content_hash"] != expected_content_hash:
                raise WorkspaceConflictError("Managed workspace file changed")
            content = bytes(row["content"]) + append_content
            content_hash = hashlib.sha256(content).hexdigest()
            await self._replace_managed_workspace_file(
                cur,
                row,
                content=content,
                content_hash=content_hash,
                updated_at=updated_at,
            )
        return self._managed_file_result(
            row,
            content=content,
            content_hash=content_hash,
            updated_at=updated_at,
        )

    async def _lock_managed_workspace_file(
        self,
        cur,
        relative_path: str,
        *,
        include_content: bool = False,
    ):
        content_column = ", dc.content" if include_content else ""
        join = (
            " JOIN public.document_content AS dc "
            "ON dc.document_id = pd.document_id"
            if include_content
            else ""
        )
        await cur.execute(
            f"""
            SELECT
                pd.document_id,
                pd.project_id,
                pd.session_id,
                pd.visibility_scope,
                pd.source_kind,
                pd.original_name,
                pd.relative_path,
                pd.extension,
                pd.size_bytes,
                pd.content_hash,
                pd.status,
                pd.created_at,
                pd.updated_at,
                pd.indexed_at,
                pd.error_message{content_column}
            FROM public.project_documents AS pd
            JOIN public.document_workspace_sources AS ws
              ON ws.source_id = pd.source_id
             AND ws.project_id = pd.project_id
            {join}
            WHERE pd.project_id = %s
              AND ws.ownership_mode = 'managed_project_workspace'
              AND pd.relative_path = %s
            FOR UPDATE
            """,
            (self._project_id, relative_path),
        )
        return await cur.fetchone()

    async def _replace_managed_workspace_file(
        self,
        cur,
        row: Dict,
        *,
        content: bytes,
        content_hash: str,
        updated_at: str,
    ) -> None:
        await cur.execute(
            """
            UPDATE public.project_documents
            SET
                size_bytes = %s,
                content_hash = %s,
                status = 'queued',
                indexed_at = NULL,
                error_message = NULL,
                updated_at = %s
            WHERE document_id = %s
              AND project_id = %s
            """,
            (
                len(content),
                content_hash,
                updated_at,
                row["document_id"],
                self._project_id,
            ),
        )
        await cur.execute(
            """
            UPDATE public.document_content
            SET content = %s, extracted_text = NULL, extracted_content_hash = NULL
            WHERE document_id = %s
            """,
            (content, row["document_id"]),
        )
        await cur.execute(
            """
            DELETE FROM public.document_chunks
            WHERE document_id = %s
            """,
            (row["document_id"],),
        )

    def _managed_file_result(
        self,
        row: Dict,
        *,
        content: bytes,
        content_hash: str,
        updated_at: str,
    ) -> Dict:
        result = dict(row)
        result.pop("content", None)
        result.update(
            {
                "size_bytes": len(content),
                "content_hash": content_hash,
                "status": "queued",
                "indexed_at": None,
                "error_message": None,
                "updated_at": updated_at,
                "chunk_count": 0,
            }
        )
        return result

    async def sync_workspace_manifest(
        self,
        *,
        source_id: str,
        session_id: Optional[str],
        documents: List[Dict],
        candidate_count: int,
        included_count: int,
        excluded_count: int,
        excluded_reason_counts: Dict[str, int],
        updated_at: str,
    ) -> Dict[str, int]:
        """Atomically admit one complete workspace manifest.

        Unchanged ``relative_path``/``content_hash`` pairs are retained. New
        and changed files are queued for indexing; paths absent from the
        manifest are deleted together with their bytes and chunks.
        """
        async with self._client.transaction() as cur:
            await cur.execute(
                """
                SELECT source_id
                FROM public.document_workspace_sources
                WHERE source_id = %s
                  AND project_id = %s
                  AND ownership_mode = 'external_sync'
                  AND (
                      visibility_scope = 'project'
                      OR (
                          visibility_scope = 'session'
                          AND session_id = %s
                      )
                  )
                FOR UPDATE
                """,
                (source_id, self._project_id, session_id),
            )
            if await cur.fetchone() is None:
                raise FileNotFoundError("Workspace source not found")

            await cur.execute(
                """
                SELECT document_id, relative_path, content_hash
                FROM public.project_documents
                WHERE source_id = %s
                FOR UPDATE
                """,
                (source_id,),
            )
            existing_rows = await cur.fetchall()
            existing_by_path = {
                row["relative_path"]: row for row in existing_rows
            }
            manifest_paths = [document["relative_path"] for document in documents]
            unchanged = 0
            queued = 0

            for document in documents:
                existing = existing_by_path.get(document["relative_path"])
                if existing and existing["content_hash"] == document["content_hash"]:
                    unchanged += 1
                    continue

                document_id = (
                    str(existing["document_id"])
                    if existing is not None
                    else str(uuid.uuid4())
                )
                await cur.execute(
                    """
                    INSERT INTO public.project_documents (
                        document_id,
                        project_id,
                        session_id,
                        visibility_scope,
                        folder_root_id,
                        source_id,
                        source_kind,
                        original_name,
                        relative_path,
                        extension,
                        size_bytes,
                        content_hash,
                        status,
                        indexed_at,
                        error_message,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        %s, %s, %s, %s, NULL, %s, 'workspace',
                        %s, %s, %s, %s, %s, 'queued', NULL, NULL, %s, %s
                    )
                    ON CONFLICT (source_id, relative_path)
                    WHERE source_id IS NOT NULL
                    DO UPDATE SET
                        original_name = EXCLUDED.original_name,
                        extension = EXCLUDED.extension,
                        size_bytes = EXCLUDED.size_bytes,
                        content_hash = EXCLUDED.content_hash,
                        status = 'queued',
                        indexed_at = NULL,
                        error_message = NULL,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        document_id,
                        self._project_id,
                        session_id,
                        document["visibility_scope"],
                        source_id,
                        document["original_name"],
                        document["relative_path"],
                        document["extension"],
                        document["size_bytes"],
                        document["content_hash"],
                        updated_at,
                        updated_at,
                    ),
                )
                await cur.execute(
                    """
                    INSERT INTO public.document_content (document_id, content)
                    VALUES (%s, %s)
                    ON CONFLICT (document_id)
                    DO UPDATE SET
                        content = EXCLUDED.content,
                        extracted_text = NULL,
                        extracted_content_hash = NULL
                    """,
                    (document_id, document["content"]),
                )
                if existing is not None:
                    await cur.execute(
                        """
                        DELETE FROM public.document_chunks
                        WHERE document_id = %s
                        """,
                        (document_id,),
                    )
                queued += 1

            await cur.execute(
                """
                DELETE FROM public.project_documents
                WHERE source_id = %s
                  AND NOT (relative_path = ANY(%s))
                RETURNING document_id
                """,
                (source_id, manifest_paths),
            )
            removed = len(await cur.fetchall())
            await cur.execute(
                """
                UPDATE public.document_workspace_sources
                SET
                    last_synced_at = %s,
                    last_manifest_candidate_count = %s,
                    last_manifest_included_count = %s,
                    last_manifest_excluded_count = %s,
                    last_manifest_excluded_reason_counts = %s::jsonb,
                    updated_at = %s
                WHERE source_id = %s
                """,
                (
                    updated_at,
                    candidate_count,
                    included_count,
                    excluded_count,
                    json.dumps(excluded_reason_counts),
                    updated_at,
                    source_id,
                ),
            )
            return {
                "queued": queued,
                "unchanged": unchanged,
                "removed": removed,
            }

    async def sync_workspace_changes(
        self,
        *,
        source_id: str,
        session_id: Optional[str],
        documents: List[Dict],
        deleted_paths: List[str],
        updated_at: str,
    ) -> Dict[str, int]:
        """Atomically apply workspace upserts and deletions only.

        Unlike ``sync_workspace_manifest``, paths not named by this operation
        are retained. This makes it safe for a local filesystem watcher to
        submit a small change set after a file save.
        """
        changed_paths = [document["relative_path"] for document in documents]
        affected_paths = sorted(set(changed_paths) | set(deleted_paths))
        if not affected_paths:
            raise ValueError("workspace changes must affect at least one path")

        async with self._client.transaction() as cur:
            await cur.execute(
                """
                SELECT source_id
                FROM public.document_workspace_sources
                WHERE source_id = %s
                  AND project_id = %s
                  AND ownership_mode = 'external_sync'
                  AND (
                      visibility_scope = 'project'
                      OR (
                          visibility_scope = 'session'
                          AND session_id = %s
                      )
                  )
                FOR UPDATE
                """,
                (source_id, self._project_id, session_id),
            )
            if await cur.fetchone() is None:
                raise FileNotFoundError("Workspace source not found")

            await cur.execute(
                """
                SELECT document_id, relative_path, content_hash
                FROM public.project_documents
                WHERE source_id = %s
                  AND relative_path = ANY(%s)
                FOR UPDATE
                """,
                (source_id, affected_paths),
            )
            existing_by_path = {
                row["relative_path"]: row for row in await cur.fetchall()
            }
            unchanged = 0
            queued = 0

            for document in documents:
                existing = existing_by_path.get(document["relative_path"])
                if existing and existing["content_hash"] == document["content_hash"]:
                    unchanged += 1
                    continue

                document_id = (
                    str(existing["document_id"])
                    if existing is not None
                    else str(uuid.uuid4())
                )
                await cur.execute(
                    """
                    INSERT INTO public.project_documents (
                        document_id,
                        project_id,
                        session_id,
                        visibility_scope,
                        folder_root_id,
                        source_id,
                        source_kind,
                        original_name,
                        relative_path,
                        extension,
                        size_bytes,
                        content_hash,
                        status,
                        indexed_at,
                        error_message,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        %s, %s, %s, %s, NULL, %s, 'workspace',
                        %s, %s, %s, %s, %s, 'queued', NULL, NULL, %s, %s
                    )
                    ON CONFLICT (source_id, relative_path)
                    WHERE source_id IS NOT NULL
                    DO UPDATE SET
                        original_name = EXCLUDED.original_name,
                        extension = EXCLUDED.extension,
                        size_bytes = EXCLUDED.size_bytes,
                        content_hash = EXCLUDED.content_hash,
                        status = 'queued',
                        indexed_at = NULL,
                        error_message = NULL,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        document_id,
                        self._project_id,
                        session_id,
                        document["visibility_scope"],
                        source_id,
                        document["original_name"],
                        document["relative_path"],
                        document["extension"],
                        document["size_bytes"],
                        document["content_hash"],
                        updated_at,
                        updated_at,
                    ),
                )
                await cur.execute(
                    """
                    INSERT INTO public.document_content (document_id, content)
                    VALUES (%s, %s)
                    ON CONFLICT (document_id)
                    DO UPDATE SET
                        content = EXCLUDED.content,
                        extracted_text = NULL,
                        extracted_content_hash = NULL
                    """,
                    (document_id, document["content"]),
                )
                if existing is not None:
                    await cur.execute(
                        """
                        DELETE FROM public.document_chunks
                        WHERE document_id = %s
                        """,
                        (document_id,),
                    )
                queued += 1

            removed = 0
            if deleted_paths:
                await cur.execute(
                    """
                    DELETE FROM public.project_documents
                    WHERE source_id = %s
                      AND relative_path = ANY(%s)
                    RETURNING document_id
                    """,
                    (source_id, deleted_paths),
                )
                removed = len(await cur.fetchall())
            await cur.execute(
                """
                UPDATE public.document_workspace_sources
                SET
                    last_synced_at = %s,
                    updated_at = %s
                WHERE source_id = %s
                """,
                (updated_at, updated_at, source_id),
            )
            return {
                "queued": queued,
                "unchanged": unchanged,
                "removed": removed,
            }

    async def claim_workspace_documents(
        self,
        *,
        source_id: str,
        limit: int,
        updated_at: str,
    ) -> List[Dict]:
        """Claim a bounded FIFO batch of queued files for one workspace."""
        async with self._client.transaction() as cur:
            await cur.execute(
                """
                WITH candidates AS (
                    SELECT document_id
                    FROM public.project_documents
                    WHERE project_id = %s
                      AND source_id = %s
                      AND status = 'queued'
                    ORDER BY updated_at ASC, document_id ASC
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE public.project_documents AS pd
                SET
                    status = 'indexing',
                    indexed_at = NULL,
                    error_message = NULL,
                    updated_at = %s
                FROM candidates
                WHERE pd.document_id = candidates.document_id
                RETURNING
                    pd.document_id,
                    pd.session_id,
                    pd.relative_path,
                    pd.extension,
                    pd.content_hash
                """,
                (self._project_id, source_id, limit, updated_at),
            )
            return [dict(row) for row in await cur.fetchall()]

    async def persist_workspace_indexed_documents(
        self,
        *,
        documents: List[Dict],
        indexed_at: str,
    ) -> List[str]:
        """Persist a workspace batch with one transaction and one COPY stream."""
        if not documents:
            return []
        for document in documents:
            self._validate_chunk_embeddings(
                document["chunks"],
                document["embeddings"],
                "persist_workspace_indexed_documents",
            )
        expected_hashes = {
            str(document["document_id"]): document["content_hash"]
            for document in documents
        }
        document_ids = list(expected_hashes)
        async with self._client.transaction() as cur:
            await cur.execute(
                """
                SELECT document_id, content_hash
                FROM public.project_documents
                WHERE project_id = %s
                  AND document_id = ANY(%s)
                FOR UPDATE
                """,
                (self._project_id, document_ids),
            )
            current_hashes = {
                str(row["document_id"]): row["content_hash"]
                for row in await cur.fetchall()
            }
            current_documents = [
                document
                for document in documents
                if current_hashes.get(str(document["document_id"]))
                == document["content_hash"]
            ]
            if not current_documents:
                return []

            current_ids = [
                str(document["document_id"]) for document in current_documents
            ]
            await cur.execute(
                """
                DELETE FROM public.document_chunks
                WHERE document_id = ANY(%s)
                """,
                (current_ids,),
            )
            for document in current_documents:
                await cur.execute(
                    """
                    UPDATE public.document_content
                    SET
                        extracted_text = %s,
                        extracted_content_hash = %s
                    WHERE document_id = %s
                    """,
                    (
                        document["extracted_text"],
                        document["content_hash"],
                        document["document_id"],
                    ),
                )
            await self._copy_chunk_rows(
                cur,
                [
                    self._chunk_copy_row(
                        document_id=str(document["document_id"]),
                        relative_path=document["relative_path"],
                        chunk_index=chunk_index,
                        chunk=chunk,
                        embedding=embedding,
                    )
                    for document in current_documents
                    for chunk_index, (chunk, embedding) in enumerate(
                        zip(document["chunks"], document["embeddings"])
                    )
                ],
            )
            await cur.execute(
                """
                UPDATE public.project_documents
                SET
                    status = 'indexed',
                    indexed_at = %s,
                    error_message = NULL,
                    updated_at = %s
                WHERE document_id = ANY(%s)
                RETURNING document_id
                """,
                (indexed_at, indexed_at, current_ids),
            )
            return [str(row["document_id"]) for row in await cur.fetchall()]

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
                    %s, %s, %s, %s, %s, 'queued', %s, %s
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
                error_message,
                deleted_at
                """,
                (document_id, self._project_id, session_id),
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
                            INSERT INTO public.document_content (
                                document_id,
                                content,
                                extracted_text,
                                extracted_content_hash
                            )
                            VALUES (%s, %s, %s, %s)
                            """,
                    (
                        document["document_id"],
                        document["content"],
                        document["extracted_text"],
                        document["content_hash"],
                    ),
                )
                await self._copy_chunk_rows(
                    cur,
                    [
                        self._chunk_copy_row(
                            document_id=document["document_id"],
                            relative_path=document["relative_path"],
                            chunk_index=chunk_index,
                            chunk=chunk,
                            embedding=embedding,
                        )
                        for chunk_index, (chunk, embedding) in enumerate(
                            document["chunks"]
                        )
                    ],
                )

    async def persist_indexed_chunks(
        self,
        *,
        document_id: str,
        session_id: Optional[str],
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
            if (
                expected_content_hash is not None
                and locked["content_hash"] != expected_content_hash
            ):
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
