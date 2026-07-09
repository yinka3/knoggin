import asyncio
import hashlib
import json
import os
import uuid
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Dict, List, Optional

from loguru import logger

from common.schema.document import (
    FolderPreview,
    FolderScanSettings,
    FolderUploadEntry,
)
from common.utils.time_utils import get_now_iso
from infrastructure.postgres_client import PostgresClient
from core.knowledge.services.embedding_service import EmbeddingService

from .constants import (
    EXPECTED_EMBEDDING_DIMENSION,
    MAX_DOCUMENT_SIZE,
    MAX_ERROR_MESSAGE_LENGTH,
    MAX_READ_CHARACTERS,
    MAX_READ_LINES,
    VALID_VISIBILITY_SCOPES,
)

from .storage import (
    extract_text,
    iter_prepared_chunks,
    purge_quarantined_file,
    quarantine_stored_file,
    remove_stored_file,
    remove_tree,
    restore_quarantined_file,
    split_text,
    write_file_atomically,
    write_prepared_chunks,
)

from .scanning import (
    build_folder_preview,
    normalize_relative_path,
)


class DocumentService:
    """Project-scoped storage and retrieval boundary for documents."""

    def __init__(
        self,
        project_id: str,
        postgres_client: PostgresClient,
        storage_root: Path,
        embedding_service: EmbeddingService,
    ):
        self.project_id = project_id
        self._postgres = postgres_client
        self._storage_root = Path(storage_root).resolve()
        self._embedding = embedding_service

    async def preview_folder(
        self,
        *,
        folder_name: str,
        entries: List[FolderUploadEntry],
        settings: Optional[FolderScanSettings] = None,
        force_include_paths: Optional[List[str]] = None,
    ) -> FolderPreview:
        """Scan a virtual folder manifest without storing or indexing content."""
        if not isinstance(folder_name, str) or not folder_name.strip():
            raise ValueError("folder_name must not be empty")
        validated_entries = [
            entry
            if isinstance(entry, FolderUploadEntry)
            else FolderUploadEntry.model_validate(entry)
            for entry in entries
        ]
        if settings is None:
            scan_settings = await self.get_scan_settings()
        else:
            scan_settings = settings
            if not isinstance(scan_settings, FolderScanSettings):
                scan_settings = FolderScanSettings.model_validate(
                    scan_settings
                )
        return await asyncio.to_thread(
            build_folder_preview,
            folder_name.strip(),
            validated_entries,
            scan_settings,
            force_include_paths or [],
        )

    async def get_scan_settings(self) -> FolderScanSettings:
        """Return persisted project scan settings or validated defaults."""
        rows = await self._postgres.fetch_all(
            """
            SELECT settings
            FROM public.project_document_scan_settings
            WHERE project_id = %s
            """,
            (self.project_id,),
        )
        if not rows:
            return FolderScanSettings()
        return FolderScanSettings.model_validate(rows[0]["settings"])

    async def save_scan_settings(
        self,
        settings: FolderScanSettings,
    ) -> FolderScanSettings:
        """Validate and persist project scan settings."""
        validated = (
            settings
            if isinstance(settings, FolderScanSettings)
            else FolderScanSettings.model_validate(settings)
        )
        saved_at = get_now_iso()
        await self._postgres.execute(
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
            (
                self.project_id,
                json.dumps(validated.model_dump(mode="json")),
                saved_at,
                saved_at,
            ),
        )
        return validated

    async def reset_scan_settings(self) -> FolderScanSettings:
        """Remove saved project settings and return defaults."""
        await self._postgres.execute(
            """
            DELETE FROM public.project_document_scan_settings
            WHERE project_id = %s
            """,
            (self.project_id,),
        )
        return FolderScanSettings()

    def _resolve_storage_path(self, storage_key: str) -> Path:
        target = (self._storage_root / Path(storage_key)).resolve()
        if not target.is_relative_to(self._storage_root):
            raise ValueError("generated storage path escaped the storage root")
        return target

    @classmethod
    def _normalize_path_prefix(cls, path_prefix: Optional[str]) -> Optional[str]:
        if path_prefix is None:
            return None
        normalized = normalize_relative_path(path_prefix, path_prefix)
        return normalized.rstrip("/")

    @staticmethod
    def _validate_visibility(
        visibility_scope: str,
        session_id: Optional[str],
    ) -> None:
        if visibility_scope not in VALID_VISIBILITY_SCOPES:
            raise ValueError(
                "visibility_scope must be either 'project' or 'session'"
            )
        if visibility_scope == "session" and not session_id:
            raise ValueError("session-visible documents require session_id")

    def _resolve_document_storage_path(self, document_metadata: Dict) -> Path:
        expected_key = PurePosixPath(
            self.project_id,
            str(document_metadata["document_id"]),
            "content",
        ).as_posix()
        if document_metadata["storage_key"] != expected_key:
            raise ValueError("stored document has an invalid managed storage key")
        return self._resolve_storage_path(expected_key)

    @staticmethod
    def _public_metadata(row: Dict) -> Dict:
        metadata = dict(row)
        metadata.pop("storage_key", None)
        if metadata.get("document_id") is not None:
            metadata["document_id"] = str(metadata["document_id"])
        for key in ("created_at", "updated_at", "indexed_at"):
            value = metadata.get(key)
            if isinstance(value, datetime):
                metadata[key] = value.isoformat()
        metadata.setdefault("chunk_count", 0)
        return metadata

    @staticmethod
    def _public_folder_metadata(row: Dict) -> Dict:
        metadata = dict(row)
        if metadata.get("folder_root_id") is not None:
            metadata["folder_root_id"] = str(metadata["folder_root_id"])
        for key in ("created_at", "indexed_at"):
            value = metadata.get(key)
            if isinstance(value, datetime):
                metadata[key] = value.isoformat()
        return metadata

    @staticmethod
    def _escape_like(value: str) -> str:
        return (
            value.replace("\\", "\\\\")
            .replace("%", "\\%")
            .replace("_", "\\_")
        )

    async def _get_visible_folder_upload(
        self,
        *,
        folder_root_id: str,
        session_id: Optional[str],
    ) -> Optional[Dict]:
        rows = await self._postgres.fetch_all(
            """
            SELECT
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
            FROM public.document_folder_uploads
            WHERE folder_root_id = %s
              AND project_id = %s
              AND (
                  visibility_scope = 'project'
                  OR (
                      visibility_scope = 'session'
                      AND session_id = %s
                  )
              )
            """,
            (folder_root_id, self.project_id, session_id),
        )
        return rows[0] if rows else None

    async def _get_visible_document(
        self,
        *,
        document_id: str,
        session_id: Optional[str],
    ) -> Optional[Dict]:
        rows = await self._get_visible_documents_by_reference(
            document_id=document_id,
            relative_path=None,
            session_id=session_id,
        )
        return rows[0] if rows else None

    async def _get_visible_documents_by_reference(
        self,
        *,
        document_id: Optional[str],
        relative_path: Optional[str],
        session_id: Optional[str],
    ) -> List[Dict]:
        if (document_id is None) == (relative_path is None):
            raise ValueError(
                "provide exactly one of document_id or relative_path"
            )

        selector = (
            "pd.document_id = %s"
            if document_id is not None
            else "pd.relative_path = %s"
        )
        selector_value = (
            document_id if document_id is not None else relative_path
        )
        return await self._postgres.fetch_all(
            """
            SELECT
                pd.document_id,
                pd.project_id,
                pd.session_id,
                pd.visibility_scope,
                pd.folder_root_id,
                pd.source_kind,
                pd.original_name,
                pd.relative_path,
                pd.extension,
                pd.size_bytes,
                pd.content_hash,
                pd.storage_key,
                pd.status,
                pd.created_at,
                pd.updated_at,
                pd.indexed_at,
                pd.error_message,
                (
                    SELECT COUNT(*)::INTEGER
                    FROM public.document_chunks AS dc
                    WHERE dc.document_id = pd.document_id
                ) AS chunk_count
            FROM public.project_documents AS pd
            WHERE """
            + selector
            + """
              AND pd.project_id = %s
              AND (
                  pd.visibility_scope = 'project'
                  OR (
                      pd.visibility_scope = 'session'
                      AND pd.session_id = %s
                  )
              )
            ORDER BY pd.created_at DESC, pd.document_id DESC
            LIMIT 2
            """,
            (selector_value, self.project_id, session_id),
        )

    async def get_document_info(
        self,
        *,
        session_id: Optional[str] = None,
        document_id: Optional[str] = None,
        relative_path: Optional[str] = None,
    ) -> Dict:
        """Return metadata for one visible document without storage paths."""
        rows = await self._get_visible_documents_by_reference(
            document_id=document_id,
            relative_path=relative_path,
            session_id=session_id,
        )
        if not rows:
            raise FileNotFoundError("Document not found")
        if relative_path is not None and len(rows) > 1:
            raise ValueError(
                "Multiple visible uploads use this relative_path; "
                "use document_id"
            )
        return self._public_metadata(rows[0])

    async def read_document(
        self,
        *,
        session_id: Optional[str] = None,
        document_id: Optional[str] = None,
        relative_path: Optional[str] = None,
        start_line: int = 1,
        end_line: Optional[int] = None,
    ) -> Dict:
        """Read a bounded line range from one visible managed document."""
        if (
            not isinstance(start_line, int)
            or isinstance(start_line, bool)
            or start_line < 1
        ):
            raise ValueError("start_line must be a positive integer")
        if end_line is not None and (
            not isinstance(end_line, int)
            or isinstance(end_line, bool)
            or end_line < start_line
        ):
            raise ValueError("end_line must be an integer at least start_line")
        if end_line is not None and end_line - start_line + 1 > MAX_READ_LINES:
            raise ValueError(
                f"read_document is limited to {MAX_READ_LINES} lines"
            )

        rows = await self._get_visible_documents_by_reference(
            document_id=document_id,
            relative_path=relative_path,
            session_id=session_id,
        )
        if not rows:
            raise FileNotFoundError("Document not found")
        if relative_path is not None and len(rows) > 1:
            raise ValueError(
                "Multiple visible uploads use this relative_path; "
                "use document_id"
            )

        document_metadata = rows[0]
        stored_path = self._resolve_document_storage_path(document_metadata)
        text = await asyncio.to_thread(
            extract_text,
            stored_path,
            document_metadata["extension"],
        )
        lines = text.splitlines() or [text]
        total_lines = len(lines)
        if start_line > total_lines:
            raise ValueError(
                f"start_line {start_line} exceeds document length {total_lines}"
            )

        requested_end = end_line or min(
            total_lines,
            start_line + MAX_READ_LINES - 1,
        )
        requested_end = min(requested_end, total_lines)
        selected = lines[start_line - 1 : requested_end]
        numbered_lines = [
            f"{line_number}: {line}"
            for line_number, line in enumerate(selected, start=start_line)
        ]
        content = "\n".join(numbered_lines)
        character_truncated = len(content) > MAX_READ_CHARACTERS
        if character_truncated:
            content = content[:MAX_READ_CHARACTERS]

        result = self._public_metadata(document_metadata)
        result.update(
            {
                "document_name": result["original_name"],
                "chunk_index": f"lines:{start_line}-{requested_end}",
                "content": content,
                "start_line": start_line,
                "end_line": requested_end,
                "total_lines": total_lines,
                "truncated": character_truncated or requested_end < total_lines,
            }
        )
        return result

    async def delete_document(
        self,
        *,
        document_id: str,
        session_id: Optional[str] = None,
    ) -> Dict:
        """Permanently delete a visible document, chunks, and managed bytes."""
        if not isinstance(document_id, str) or not document_id.strip():
            raise ValueError("document_id must not be empty")

        pool = self._require_pool()
        quarantine = None
        stored_path = None
        deleted_metadata = None
        try:
            async with pool.connection() as conn:
                async with conn.transaction():
                    async with conn.cursor() as cur:
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
                                storage_key,
                                status,
                                created_at,
                                updated_at,
                                indexed_at,
                                error_message
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
                            (document_id.strip(), self.project_id, session_id),
                        )
                        document_metadata = await cur.fetchone()
                        if document_metadata is None:
                            raise FileNotFoundError("Document not found")

                        stored_path = self._resolve_document_storage_path(
                            document_metadata
                        )
                        quarantine = await asyncio.to_thread(
                            quarantine_stored_file,
                            stored_path,
                        )
                        await cur.execute(
                            """
                            DELETE FROM public.project_documents
                            WHERE document_id = %s
                              AND project_id = %s
                            RETURNING document_id
                            """,
                            (document_id.strip(), self.project_id),
                        )
                        deleted = await cur.fetchone()
                        if deleted is None:
                            raise RuntimeError(
                                "Document deletion did not remove a row"
                            )
                        deleted_metadata = self._public_metadata(
                            document_metadata
                        )
        except Exception:
            if quarantine is not None and stored_path is not None:
                try:
                    await asyncio.to_thread(
                        restore_quarantined_file,
                        quarantine,
                        stored_path,
                    )
                except Exception as restore_error:
                    logger.error(
                        "Failed to restore quarantined document bytes for {}: {}",
                        document_id,
                        restore_error,
                    )
            raise

        if quarantine is not None:
            try:
                await asyncio.to_thread(
                    purge_quarantined_file,
                    quarantine,
                )
            except Exception as purge_error:
                logger.error(
                    "Document metadata was deleted but storage purge failed for {}: {}",
                    document_id,
                    purge_error,
                )
                raise RuntimeError(
                    "Document metadata was deleted but stored-byte cleanup failed"
                ) from purge_error

        deleted_metadata["deleted"] = True
        return deleted_metadata

    def _require_pool(self):
        pool = self._postgres.async_pool
        if pool is None:
            raise RuntimeError("PostgresClient async_pool is not initialized")
        return pool

    async def _persist_indexed_chunks(
        self,
        *,
        document_id: str,
        session_id: Optional[str],
        chunks: List[str],
        embeddings: List[List[float]],
    ) -> Dict:
        pool = self._require_pool()
        async with pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        SELECT
                            pd.document_id,
                            pd.project_id,
                            pd.session_id,
                            pd.visibility_scope,
                            pd.folder_root_id,
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
                            pd.error_message,
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
                        (document_id, self.project_id, session_id),
                    )
                    locked_document = await cur.fetchone()
                    if locked_document is None:
                        raise FileNotFoundError("Document not found")
                    if locked_document["status"] == "indexed":
                        return self._public_metadata(locked_document)

                    await cur.execute(
                        """
                        DELETE FROM public.document_chunks
                        WHERE document_id = %s
                        """,
                        (document_id,),
                    )
                    for chunk_index, (content, embedding) in enumerate(
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
                                content,
                                json.dumps(embedding),
                            ),
                        )

                    indexed_at = get_now_iso()
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
                    indexed_document = await cur.fetchone()
                    if indexed_document is None:
                        raise RuntimeError(
                            "Indexed document status update failed"
                        )
                    indexed_document["chunk_count"] = len(chunks)
                    return self._public_metadata(indexed_document)

    async def _record_index_failure(
        self,
        *,
        document_id: str,
        session_id: Optional[str],
        error_message: str,
    ) -> None:
        pool = self._require_pool()
        async with pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
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
                        (document_id, self.project_id, session_id),
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
                    updated_at = get_now_iso()
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

    async def accept_folder(
        self,
        *,
        folder_name: str,
        entries: List[FolderUploadEntry],
        selected_paths: Optional[List[str]] = None,
        settings: Optional[FolderScanSettings] = None,
        force_include_paths: Optional[List[str]] = None,
        session_id: Optional[str] = None,
        visibility_scope: str = "project",
    ) -> Dict:
        """Rescan, synchronously index, and atomically persist a folder batch."""
        self._validate_visibility(visibility_scope, session_id)
        validated_entries = [
            entry
            if isinstance(entry, FolderUploadEntry)
            else FolderUploadEntry.model_validate(entry)
            for entry in entries
        ]
        preview = await self.preview_folder(
            folder_name=folder_name,
            entries=validated_entries,
            settings=settings,
            force_include_paths=force_include_paths,
        )
        if selected_paths is None:
            normalized_selected = [
                item.relative_path for item in preview.included
            ]
        else:
            if not selected_paths:
                raise ValueError(
                    "selected_paths must contain at least one path"
                )
            normalized_selected = [
                normalize_relative_path(path, path)
                for path in selected_paths
            ]
            if len(set(normalized_selected)) != len(normalized_selected):
                raise ValueError("selected_paths must not contain duplicates")
            normalized_selected.sort()
        if not normalized_selected:
            raise ValueError("folder preview contains no eligible documents")
        entry_content = {
            normalize_relative_path(
                entry.relative_path,
                entry.relative_path,
            ): entry.content
            for entry in validated_entries
        }
        included_by_path = {
            item.relative_path: item for item in preview.included
        }
        unknown = set(normalized_selected) - entry_content.keys()
        if unknown:
            raise ValueError(
                "selected_paths contain unknown entries: "
                + ", ".join(sorted(unknown))
            )
        unavailable = set(normalized_selected) - included_by_path.keys()
        if unavailable:
            raise ValueError(
                "selected_paths contain excluded entries: "
                + ", ".join(sorted(unavailable))
            )

        folder_root_id = str(uuid.uuid4())
        staging_root = self._resolve_storage_path(
            PurePosixPath(
                ".staging",
                f"folder-{folder_root_id}",
            ).as_posix()
        )
        originals_root = staging_root / "originals"
        prepared_root = staging_root / "prepared"
        prepared_documents = []
        moved_directories = []
        indexed_at = get_now_iso()

        try:
            for relative_path in normalized_selected:
                content = entry_content[relative_path]
                preview_entry = included_by_path[relative_path]
                document_id = str(uuid.uuid4())
                staged_directory = originals_root / document_id
                staged_content = staged_directory / "content"
                prepared_chunks = prepared_root / f"{document_id}.jsonl"
                await asyncio.to_thread(
                    write_file_atomically,
                    staged_content,
                    content,
                )

                text = await asyncio.to_thread(
                    extract_text,
                    staged_content,
                    preview_entry.extension,
                )
                chunks = await asyncio.to_thread(split_text, text)
                embeddings = await self._embedding.encode(chunks)
                if len(embeddings) != len(chunks):
                    raise ValueError(
                        "Embedding count does not match chunk count"
                    )
                if any(
                    len(embedding) != EXPECTED_EMBEDDING_DIMENSION
                    for embedding in embeddings
                ):
                    raise ValueError(
                        "Document chunk embeddings must have exactly "
                        f"{EXPECTED_EMBEDDING_DIMENSION} dimensions"
                    )
                await asyncio.to_thread(
                    write_prepared_chunks,
                    prepared_chunks,
                    chunks,
                    embeddings,
                )
                prepared_documents.append(
                    {
                        "document_id": document_id,
                        "relative_path": relative_path,
                        "original_name": preview_entry.original_name,
                        "extension": preview_entry.extension,
                        "size_bytes": preview_entry.size_bytes,
                        "content_hash": preview_entry.content_hash,
                        "storage_key": PurePosixPath(
                            self.project_id,
                            document_id,
                            "content",
                        ).as_posix(),
                        "staged_directory": staged_directory,
                        "prepared_chunks": prepared_chunks,
                        "chunk_count": len(chunks),
                    }
                )

            pool = self._require_pool()
            async with pool.connection() as conn:
                async with conn.transaction():
                    async with conn.cursor() as cur:
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
                                self.project_id,
                                session_id,
                                visibility_scope,
                                folder_name.strip(),
                                len(validated_entries),
                                sum(len(entry.content) for entry in validated_entries),
                                len(prepared_documents),
                                sum(
                                    document["size_bytes"]
                                    for document in prepared_documents
                                ),
                                preview.summary.excluded_count,
                                preview.summary.excluded_bytes,
                                preview.summary.excluded_directory_count,
                                json.dumps(preview.summary.reason_counts),
                                json.dumps(
                                    preview.settings.model_dump(mode="json")
                                ),
                                indexed_at,
                                indexed_at,
                            ),
                        )

                        for document in prepared_documents:
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
                                    storage_key,
                                    status,
                                    indexed_at,
                                    created_at,
                                    updated_at
                                )
                                VALUES (
                                    %s, %s, %s, %s, %s, 'folder_upload',
                                    %s, %s, %s, %s, %s, %s, 'indexed',
                                    %s, %s, %s
                                )
                                """,
                                (
                                    document["document_id"],
                                    self.project_id,
                                    session_id,
                                    visibility_scope,
                                    folder_root_id,
                                    document["original_name"],
                                    document["relative_path"],
                                    document["extension"],
                                    document["size_bytes"],
                                    document["content_hash"],
                                    document["storage_key"],
                                    indexed_at,
                                    indexed_at,
                                    indexed_at,
                                ),
                            )
                            for chunk in iter_prepared_chunks(
                                document["prepared_chunks"]
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
                                        chunk["chunk_index"],
                                        chunk["content"],
                                        json.dumps(chunk["embedding"]),
                                    ),
                                )

                            final_directory = self._resolve_storage_path(
                                PurePosixPath(
                                    self.project_id,
                                    document["document_id"],
                                ).as_posix()
                            )
                            final_directory.parent.mkdir(
                                parents=True,
                                exist_ok=True,
                            )
                            os.replace(
                                document["staged_directory"],
                                final_directory,
                            )
                            moved_directories.append(final_directory)
                        await asyncio.to_thread(
                            remove_tree,
                            staging_root,
                        )
        except Exception:
            for directory in reversed(moved_directories):
                try:
                    await asyncio.to_thread(remove_tree, directory)
                except Exception as cleanup_error:
                    logger.error(
                        "Failed to remove rolled-back folder document {}: {}",
                        directory,
                        cleanup_error,
                    )
            try:
                await asyncio.to_thread(remove_tree, staging_root)
            except Exception as cleanup_error:
                logger.error(
                    "Failed to remove folder staging directory {}: {}",
                    staging_root,
                    cleanup_error,
                )
            raise

        documents = [
            {
                "document_id": document["document_id"],
                "project_id": self.project_id,
                "session_id": session_id,
                "visibility_scope": visibility_scope,
                "folder_root_id": folder_root_id,
                "source_kind": "folder_upload",
                "original_name": document["original_name"],
                "relative_path": document["relative_path"],
                "extension": document["extension"],
                "size_bytes": document["size_bytes"],
                "content_hash": document["content_hash"],
                "status": "indexed",
                "indexed_at": indexed_at,
                "error_message": None,
                "created_at": indexed_at,
                "updated_at": indexed_at,
                "chunk_count": document["chunk_count"],
            }
            for document in prepared_documents
        ]
        return {
            "folder_root_id": folder_root_id,
            "project_id": self.project_id,
            "session_id": session_id,
            "visibility_scope": visibility_scope,
            "folder_name": folder_name.strip(),
            "candidate_count": len(validated_entries),
            "candidate_bytes": sum(
                len(entry.content) for entry in validated_entries
            ),
            "document_count": len(documents),
            "total_size_bytes": sum(
                document["size_bytes"] for document in prepared_documents
            ),
            "excluded_count": preview.summary.excluded_count,
            "excluded_bytes": preview.summary.excluded_bytes,
            "excluded_directory_count": (
                preview.summary.excluded_directory_count
            ),
            "excluded_reason_counts": preview.summary.reason_counts,
            "scan_settings": preview.settings.model_dump(mode="json"),
            "created_at": indexed_at,
            "indexed_at": indexed_at,
            "documents": documents,
        }

    async def add_document(
        self,
        *,
        content: bytes,
        original_name: str,
        relative_path: Optional[str] = None,
        session_id: Optional[str] = None,
        visibility_scope: str = "project",
    ) -> Dict:
        """Store original bytes and persist project document metadata."""
        if not isinstance(content, bytes):
            raise TypeError("content must be bytes")
        if not content:
            raise ValueError("document content must not be empty")
        if len(content) > MAX_DOCUMENT_SIZE:
            raise ValueError("document exceeds the 50 MB size limit")
        if not original_name or not original_name.strip():
            raise ValueError("original_name must not be empty")
        if "\x00" in original_name:
            raise ValueError("original_name contains an invalid null byte")
        if visibility_scope not in VALID_VISIBILITY_SCOPES:
            raise ValueError(
                "visibility_scope must be either 'project' or 'session'"
            )
        if visibility_scope == "session" and not session_id:
            raise ValueError("session-visible documents require session_id")

        normalized_path = normalize_relative_path(relative_path, original_name)
        document_id = str(uuid.uuid4())
        storage_key = PurePosixPath(
            self.project_id, document_id, "content"
        ).as_posix()
        stored_path = self._resolve_storage_path(storage_key)
        content_hash = hashlib.sha256(content).hexdigest()
        extension = Path(original_name).suffix.lower()
        created_at = get_now_iso()

        await asyncio.to_thread(write_file_atomically, stored_path, content)
        try:
            inserted = await self._postgres.execute(
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
                    storage_key,
                    status,
                    created_at,
                    updated_at
                )
                VALUES (
                    %s, %s, %s, %s, NULL, 'manual_upload',
                    %s, %s, %s, %s, %s, %s, 'uploaded', %s, %s
                )
                """,
                (
                    document_id,
                    self.project_id,
                    session_id,
                    visibility_scope,
                    original_name.strip(),
                    normalized_path,
                    extension,
                    len(content),
                    content_hash,
                    storage_key,
                    created_at,
                    created_at,
                ),
            )
            if inserted != 1:
                raise RuntimeError(
                    "project document metadata insert did not create a row"
                )
        except Exception:
            await asyncio.to_thread(remove_stored_file, stored_path)
            raise

        return {
            "document_id": document_id,
            "project_id": self.project_id,
            "session_id": session_id,
            "visibility_scope": visibility_scope,
            "folder_root_id": None,
            "source_kind": "manual_upload",
            "original_name": original_name.strip(),
            "relative_path": normalized_path,
            "extension": extension,
            "size_bytes": len(content),
            "content_hash": content_hash,
            "status": "uploaded",
            "indexed_at": None,
            "error_message": None,
            "created_at": created_at,
            "updated_at": created_at,
            "chunk_count": 0,
        }

    async def index_document(
        self,
        *,
        document_id: str,
        session_id: Optional[str] = None,
    ) -> Dict:
        """Extract, chunk, embed, and persist one visible project document."""
        document_metadata = await self._get_visible_document(
            document_id=document_id,
            session_id=session_id,
        )
        if document_metadata is None:
            raise FileNotFoundError("Document not found")
        if document_metadata["status"] == "indexed":
            return self._public_metadata(document_metadata)

        try:
            stored_path = self._resolve_document_storage_path(
                document_metadata
            )
            text = await asyncio.to_thread(
                extract_text,
                stored_path,
                document_metadata["extension"],
            )
            chunks = await asyncio.to_thread(split_text, text)
            embeddings = await self._embedding.encode(chunks)
            if len(embeddings) != len(chunks):
                raise ValueError("Embedding count does not match chunk count")
            if any(
                len(embedding) != EXPECTED_EMBEDDING_DIMENSION
                for embedding in embeddings
            ):
                raise ValueError(
                    "Document chunk embeddings must have exactly "
                    f"{EXPECTED_EMBEDDING_DIMENSION} dimensions"
                )
            return await self._persist_indexed_chunks(
                document_id=document_id,
                session_id=session_id,
                chunks=chunks,
                embeddings=embeddings,
            )
        except FileNotFoundError:
            raise
        except Exception as exc:
            error_message = str(exc).strip() or type(exc).__name__
            error_message = error_message[:MAX_ERROR_MESSAGE_LENGTH]
            try:
                await self._record_index_failure(
                    document_id=document_id,
                    session_id=session_id,
                    error_message=error_message,
                )
            except Exception as failure_error:
                logger.error(
                    "Failed to record document indexing failure for {}: {}",
                    document_id,
                    failure_error,
                )
            raise RuntimeError(
                f"Failed to index document: {error_message}"
            ) from exc

    async def list_folder_uploads(
        self,
        *,
        session_id: Optional[str] = None,
        visibility_scope: Optional[str] = None,
        limit: int = 25,
    ) -> List[Dict]:
        """List folder upload batches visible to the current session."""
        if (
            visibility_scope is not None
            and visibility_scope not in VALID_VISIBILITY_SCOPES
        ):
            raise ValueError(
                "visibility_scope must be either 'project' or 'session'"
            )
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 100
        ):
            raise ValueError("limit must be between 1 and 100")

        query = """
            SELECT
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
            FROM public.document_folder_uploads
            WHERE project_id = %s
              AND (
                  visibility_scope = 'project'
                  OR (
                      visibility_scope = 'session'
                      AND session_id = %s
                  )
              )
        """
        params: list = [self.project_id, session_id]
        if visibility_scope is not None:
            query += " AND visibility_scope = %s"
            params.append(visibility_scope)
        query += " ORDER BY created_at DESC, folder_root_id DESC LIMIT %s"
        params.append(limit)
        rows = await self._postgres.fetch_all(query, tuple(params))
        return [self._public_folder_metadata(row) for row in rows]

    async def get_folder_upload_summary(
        self,
        *,
        folder_root_id: str,
        session_id: Optional[str] = None,
        path_prefix: Optional[str] = None,
    ) -> Dict:
        """Return one visible folder batch and a shallow document tree."""
        if not isinstance(folder_root_id, str) or not folder_root_id.strip():
            raise ValueError("folder_root_id must not be empty")
        folder = await self._get_visible_folder_upload(
            folder_root_id=folder_root_id.strip(),
            session_id=session_id,
        )
        if folder is None:
            raise FileNotFoundError("Folder upload not found")
        result = self._public_folder_metadata(folder)
        result["tree"] = await self.list_folder_tree(
            folder_root_id=folder_root_id.strip(),
            session_id=session_id,
            path_prefix=path_prefix,
            max_depth=2,
        )
        return result

    async def resolve_focus_target(
        self,
        *,
        session_id: Optional[str] = None,
        document_id: Optional[str] = None,
        folder_root_id: Optional[str] = None,
        path_prefix: Optional[str] = None,
    ) -> Dict:
        """Validate and canonicalize one visible document-focus target."""
        if document_id is not None:
            if folder_root_id is not None or path_prefix is not None:
                raise ValueError(
                    "document focus cannot include folder filters"
                )
            document = await self.get_document_info(
                session_id=session_id,
                document_id=document_id,
            )
            return {
                "target_type": "document",
                "document_id": document["document_id"],
                "relative_path": document["relative_path"],
                "folder_root_id": document.get("folder_root_id"),
                "path_prefix": None,
            }

        if folder_root_id is None:
            raise ValueError(
                "document focus requires document_id or folder_root_id"
            )
        if not isinstance(folder_root_id, str) or not folder_root_id.strip():
            raise ValueError("folder_root_id must not be empty")
        normalized_prefix = self._normalize_path_prefix(path_prefix)
        folder = await self._get_visible_folder_upload(
            folder_root_id=folder_root_id.strip(),
            session_id=session_id,
        )
        if folder is None:
            raise FileNotFoundError("Document focus target not found")
        if normalized_prefix is not None:
            documents = await self.list_documents(
                session_id=session_id,
                folder_root_id=folder_root_id.strip(),
                path_prefix=normalized_prefix,
                limit=1,
            )
            if not documents:
                raise FileNotFoundError("Document focus target not found")
            return {
                "target_type": "subtree",
                "document_id": None,
                "relative_path": None,
                "folder_root_id": folder_root_id.strip(),
                "path_prefix": normalized_prefix,
            }
        return {
            "target_type": "folder_upload",
            "document_id": None,
            "relative_path": None,
            "folder_root_id": folder_root_id.strip(),
            "path_prefix": None,
        }

    async def _list_folder_documents(
        self,
        *,
        folder_root_id: str,
        session_id: Optional[str],
        path_prefix: Optional[str] = None,
    ) -> List[Dict]:
        query = """
            SELECT
                pd.document_id,
                pd.folder_root_id,
                pd.original_name,
                pd.relative_path,
                pd.extension,
                pd.size_bytes,
                pd.status,
                COUNT(dc.chunk_id)::INTEGER AS chunk_count
            FROM public.project_documents AS pd
            LEFT JOIN public.document_chunks AS dc
                ON dc.document_id = pd.document_id
            WHERE pd.project_id = %s
              AND pd.folder_root_id = %s
              AND (
                  pd.visibility_scope = 'project'
                  OR (
                      pd.visibility_scope = 'session'
                      AND pd.session_id = %s
                  )
              )
        """
        params: list = [self.project_id, folder_root_id, session_id]
        if path_prefix is not None:
            escaped = self._escape_like(path_prefix)
            query += (
                " AND (pd.relative_path = %s "
                "OR pd.relative_path LIKE %s ESCAPE '\\')"
            )
            params.extend([path_prefix, f"{escaped}/%"])
        query += """
            GROUP BY pd.document_id
            ORDER BY pd.relative_path, pd.document_id
        """
        return await self._postgres.fetch_all(query, tuple(params))

    @staticmethod
    def _build_folder_tree(rows: List[Dict], max_depth: int) -> List[Dict]:
        root = {"children": {}}
        for row in rows:
            parts = PurePosixPath(row["relative_path"]).parts
            current = root
            for depth, part in enumerate(parts[:-1], start=1):
                relative_path = PurePosixPath(*parts[:depth]).as_posix()
                children = current["children"]
                node = children.setdefault(
                    part,
                    {
                        "name": part,
                        "relative_path": relative_path,
                        "type": "directory",
                        "children": {},
                    },
                )
                if depth >= max_depth:
                    node["truncated"] = True
                    current = None
                    break
                current = node
            if current is None:
                continue
            name = parts[-1]
            current["children"][f"\0{name}:{row['document_id']}"] = {
                "name": name,
                "relative_path": row["relative_path"],
                "type": "document",
                "document_id": str(row["document_id"]),
                "status": row["status"],
                "size_bytes": row["size_bytes"],
                "chunk_count": row.get("chunk_count", 0),
            }

        def finalize(node):
            children = list(node.pop("children", {}).values())
            children.sort(
                key=lambda item: (
                    item["type"] == "document",
                    item["name"].lower(),
                    item["relative_path"],
                )
            )
            for child in children:
                if child["type"] == "directory":
                    finalize(child)
            node["children"] = children

        finalize(root)
        return root["children"]

    async def list_folder_tree(
        self,
        *,
        folder_root_id: str,
        session_id: Optional[str] = None,
        path_prefix: Optional[str] = None,
        max_depth: int = 3,
    ) -> List[Dict]:
        """Return a deterministic tree for one visible folder upload."""
        if not isinstance(folder_root_id, str) or not folder_root_id.strip():
            raise ValueError("folder_root_id must not be empty")
        if (
            not isinstance(max_depth, int)
            or isinstance(max_depth, bool)
            or not 1 <= max_depth <= 10
        ):
            raise ValueError("max_depth must be between 1 and 10")
        normalized_prefix = self._normalize_path_prefix(path_prefix)
        folder = await self._get_visible_folder_upload(
            folder_root_id=folder_root_id.strip(),
            session_id=session_id,
        )
        if folder is None:
            raise FileNotFoundError("Folder upload not found")
        rows = await self._list_folder_documents(
            folder_root_id=folder_root_id.strip(),
            session_id=session_id,
            path_prefix=normalized_prefix,
        )
        return self._build_folder_tree(rows, max_depth)

    async def list_documents(
        self,
        *,
        session_id: Optional[str] = None,
        folder_root_id: Optional[str] = None,
        path_prefix: Optional[str] = None,
        visibility_scope: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict]:
        """List documents visible to the current project/session context."""
        if (
            visibility_scope is not None
            and visibility_scope not in VALID_VISIBILITY_SCOPES
        ):
            raise ValueError(
                "visibility_scope must be either 'project' or 'session'"
            )
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 1000
        ):
            raise ValueError("limit must be between 1 and 1000")
        normalized_prefix = self._normalize_path_prefix(path_prefix)
        if folder_root_id is not None:
            if not isinstance(folder_root_id, str) or not folder_root_id.strip():
                raise ValueError("folder_root_id must not be empty")
            folder = await self._get_visible_folder_upload(
                folder_root_id=folder_root_id.strip(),
                session_id=session_id,
            )
            if folder is None:
                raise FileNotFoundError("Folder upload not found")

        query = """
            SELECT
                pd.document_id,
                pd.project_id,
                pd.session_id,
                pd.visibility_scope,
                pd.folder_root_id,
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
                pd.error_message,
                COUNT(dc.chunk_id)::INTEGER AS chunk_count
            FROM public.project_documents AS pd
            LEFT JOIN public.document_chunks AS dc
                ON dc.document_id = pd.document_id
            WHERE pd.project_id = %s
              AND (
                  pd.visibility_scope = 'project'
                  OR (
                      pd.visibility_scope = 'session'
                      AND pd.session_id = %s
                  )
              )
        """
        params: list = [self.project_id, session_id]
        if visibility_scope is not None:
            query += " AND pd.visibility_scope = %s"
            params.append(visibility_scope)
        if folder_root_id is not None:
            query += " AND pd.folder_root_id = %s"
            params.append(folder_root_id.strip())
        if normalized_prefix is not None:
            escaped = self._escape_like(normalized_prefix)
            query += (
                " AND (pd.relative_path = %s "
                "OR pd.relative_path LIKE %s ESCAPE '\\')"
            )
            params.extend([normalized_prefix, f"{escaped}/%"])
        query += """
            GROUP BY pd.document_id
            ORDER BY pd.created_at DESC, pd.document_id DESC
            LIMIT %s
        """
        params.append(limit)

        rows = await self._postgres.fetch_all(query, tuple(params))
        return [self._public_metadata(row) for row in rows]

    async def search(
        self,
        query: str,
        *,
        session_id: Optional[str] = None,
        n_results: int = 5,
        document_filter: Optional[str] = None,
        folder_root_id: Optional[str] = None,
        relative_path: Optional[str] = None,
        path_prefix: Optional[str] = None,
    ) -> List[Dict]:
        """Search indexed chunks visible to the current project/session context."""
        if not isinstance(query, str) or not query.strip():
            raise ValueError("query must not be empty")
        if (
            not isinstance(n_results, int)
            or isinstance(n_results, bool)
            or not 1 <= n_results <= 50
        ):
            raise ValueError("n_results must be between 1 and 50")
        if document_filter is not None and relative_path is not None:
            raise ValueError(
                "document_filter and relative_path are mutually exclusive"
            )
        if path_prefix is not None and (
            document_filter is not None or relative_path is not None
        ):
            raise ValueError(
                "path_prefix cannot be combined with an exact document selector"
            )
        normalized_relative_path = (
            normalize_relative_path(relative_path, relative_path)
            if relative_path is not None
            else None
        )
        normalized_prefix = self._normalize_path_prefix(path_prefix)
        if folder_root_id is not None:
            if not isinstance(folder_root_id, str) or not folder_root_id.strip():
                raise ValueError("folder_root_id must not be empty")
            folder = await self._get_visible_folder_upload(
                folder_root_id=folder_root_id.strip(),
                session_id=session_id,
            )
            if folder is None:
                raise FileNotFoundError("Folder upload not found")

        query_embedding = await self._embedding.encode_single(query.strip())
        if len(query_embedding) != EXPECTED_EMBEDDING_DIMENSION:
            raise ValueError(
                "Document search embeddings must have exactly "
                f"{EXPECTED_EMBEDDING_DIMENSION} dimensions"
            )

        embedding_json = json.dumps(query_embedding)
        sql = """
            SELECT
                dc.document_id,
                pd.folder_root_id,
                pd.original_name,
                pd.relative_path,
                dc.chunk_index,
                dc.content,
                1 - (dc.embedding <=> %s::vector) AS score
            FROM public.document_chunks AS dc
            JOIN public.project_documents AS pd
                ON pd.document_id = dc.document_id
            WHERE pd.project_id = %s
              AND pd.status = 'indexed'
              AND (
                  pd.visibility_scope = 'project'
                  OR (
                      pd.visibility_scope = 'session'
                      AND pd.session_id = %s
                  )
              )
        """
        params: list = [embedding_json, self.project_id, session_id]
        if document_filter is not None:
            sql += " AND pd.document_id = %s"
            params.append(document_filter)
        if folder_root_id is not None:
            sql += " AND pd.folder_root_id = %s"
            params.append(folder_root_id.strip())
        if normalized_relative_path is not None:
            sql += " AND pd.relative_path = %s"
            params.append(normalized_relative_path)
        if normalized_prefix is not None:
            escaped = self._escape_like(normalized_prefix)
            sql += (
                " AND (pd.relative_path = %s "
                "OR pd.relative_path LIKE %s ESCAPE '\\')"
            )
            params.extend([normalized_prefix, f"{escaped}/%"])
        sql += """
            ORDER BY
                dc.embedding <=> %s::vector,
                dc.document_id,
                dc.chunk_index
            LIMIT %s
        """
        params.extend([embedding_json, n_results])

        rows = await self._postgres.fetch_all(sql, tuple(params))
        results = []
        for row in rows:
            result = dict(row)
            if result.get("document_id") is not None:
                result["document_id"] = str(result["document_id"])
            result["document_name"] = result.get("original_name")
            if result.get("score") is not None:
                result["score"] = float(result["score"])
            results.append(result)
        return results
