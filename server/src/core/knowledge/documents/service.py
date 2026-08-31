import asyncio
import hashlib
import json
import uuid
from datetime import datetime
from pathlib import PurePosixPath
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional

from loguru import logger

from common.schema.document import (
    DocumentSelection,
    FolderPreview,
    FolderScanSettings,
    FolderUploadEntry,
    WorkspaceSyncChanges,
)
from common.schema.health import sanitize_health_details
from common.schema.source.locators import (
    CodeLineLocator,
    CsvRowLocator,
    DocxParagraphLocator,
    PdfPageLocator,
    TextLineLocator,
)
from common.utils.time_utils import get_now_iso
from core.knowledge.db.readers.document_reader import DocumentReader
from core.knowledge.db.writers.document_writer import DocumentWriter
from core.knowledge.services.embedding_service import EmbeddingService
from infrastructure.background_work import BackgroundWorkCoordinator
from infrastructure.postgres_client import PostgresClient

from .constants import (
    ACCEPTED_EXTENSIONS,
    DOCUMENT_RERANK_DEFAULT_CANDIDATES,
    EXPECTED_EMBEDDING_DIMENSION,
    HYBRID_SEARCH_CANDIDATE_MULTIPLIER,
    HYBRID_SEARCH_MAX_CANDIDATES,
    HYBRID_SEARCH_MIN_CANDIDATES,
    IMAGE_EXTENSIONS,
    INLINE_INDEX_MAX_BYTES,
    MAX_DOCUMENT_SIZE,
    MAX_READ_CHARACTERS,
    MAX_READ_LINES,
    VALID_VISIBILITY_SCOPES,
    WORKSPACE_PREPARE_CONCURRENCY,
    document_extension,
)
from .filesystem import ProjectFilesystem, ProjectFilesystemFactory
from .indexer import DocumentIndexer
from .policy import DocumentIndexPolicy
from .scanning import build_folder_preview, normalize_relative_path
from .storage import (
    csv_data_rows,
    docx_heading_path,
    extract_docx_paragraphs,
    extract_pdf_pages,
    extract_text,
    is_code_extension,
)

BlockingRunner = Callable[..., Awaitable[Any]]
_RECONCILIATION_MAX_FILES = 10_000


async def _run_in_worker(
    function: Callable[..., Any], *args: Any, **kwargs: Any
) -> Any:
    """Run CPU- or blocking-I/O work without blocking the event loop."""
    return await asyncio.to_thread(function, *args, **kwargs)


class DocumentService:
    """Project-scoped storage and retrieval boundary for documents."""

    def __init__(
        self,
        project_id: str,
        postgres_client: PostgresClient,
        embedding_service: EmbeddingService,
        background_work: Optional[BackgroundWorkCoordinator] = None,
        readable_project_ids: Optional[Iterable[str]] = None,
        reader: Optional[DocumentReader] = None,
        writer: Optional[DocumentWriter] = None,
        indexer: Optional[DocumentIndexer] = None,
        inline_index_max_bytes: int = INLINE_INDEX_MAX_BYTES,
        blocking_runner: BlockingRunner = _run_in_worker,
        document_rerank_enabled: bool = True,
        document_rerank_candidates: int = DOCUMENT_RERANK_DEFAULT_CANDIDATES,
        workspace_prepare_concurrency: int = WORKSPACE_PREPARE_CONCURRENCY,
        filesystem_factory: ProjectFilesystemFactory | None = None,
        reconciliation_interval_seconds: int = 60,
    ):
        self.project_id = project_id
        self._embedding = embedding_service
        self._reader = reader or DocumentReader(
            postgres_client,
            project_id,
            readable_project_ids=readable_project_ids,
        )
        self._writer = writer or DocumentWriter(postgres_client, project_id)
        self._run_blocking = blocking_runner
        self._filesystem_factory = filesystem_factory
        if indexer is None:
            indexing_policy = DocumentIndexPolicy.capture(
                inline_index_max_bytes=inline_index_max_bytes,
                workspace_prepare_concurrency=workspace_prepare_concurrency,
            )
            indexer = DocumentIndexer(
                project_id=project_id,
                reader=self._reader,
                writer=self._writer,
                embedding_service=embedding_service,
                policy=indexing_policy,
                blocking_runner=blocking_runner,
                background_work=background_work,
                filesystem=self._filesystem,
            )
        self._indexer = indexer
        if not isinstance(document_rerank_enabled, bool):
            raise ValueError("document_rerank_enabled must be a boolean")
        if (
            not isinstance(document_rerank_candidates, int)
            or isinstance(document_rerank_candidates, bool)
            or not 1 <= document_rerank_candidates <= 50
        ):
            raise ValueError("document_rerank_candidates must be between 1 and 50")
        self._document_rerank_enabled = document_rerank_enabled
        self._document_rerank_candidates = document_rerank_candidates
        if (
            not isinstance(reconciliation_interval_seconds, int)
            or isinstance(reconciliation_interval_seconds, bool)
            or reconciliation_interval_seconds < 10
        ):
            raise ValueError("reconciliation_interval_seconds must be at least 10")
        if self._filesystem_factory is not None:
            self._indexer.set_reconciliation_callback(
                self.reconcile_project_files,
                interval_seconds=reconciliation_interval_seconds,
            )

    @property
    def _filesystem(self) -> ProjectFilesystem | None:
        if self._filesystem_factory is None:
            return None
        return self._filesystem_factory.for_project(self.project_id)

    def _filesystem_for_document(self, document: Dict) -> ProjectFilesystem | None:
        if (
            self._filesystem_factory is None
            or document.get("source_kind") != "manual_upload"
            or document.get("visibility_scope") != "project"
        ):
            return None
        return self._filesystem_factory.for_project(document["project_id"])

    async def _read_source_bytes(
        self,
        document: Dict,
        *,
        session_id: Optional[str],
    ) -> bytes | None:
        filesystem = self._filesystem_for_document(document)
        if filesystem is not None:
            return await self._run_blocking(
                filesystem.read_bytes,
                document["relative_path"],
            )
        return await self._reader.fetch_document_content(
            document_id=str(document["document_id"]),
            session_id=session_id,
        )

    async def reconcile_project_files(self) -> Dict[str, int]:
        """Bring the manual-document catalog into line with the local project tree.

        Reconciliation deliberately uses the existing folder admission policy so
        ignored, generated, sensitive, binary, and oversized files do not enter
        the document catalog merely because an editor created them locally.
        """
        filesystem = self._filesystem
        if filesystem is None:
            return {"created": 0, "changed": 0, "deleted": 0, "excluded": 0}

        paths = await self._run_blocking(
            lambda: list(filesystem.iter_paths(limit=_RECONCILIATION_MAX_FILES + 1))
        )
        if len(paths) > _RECONCILIATION_MAX_FILES:
            raise RuntimeError(
                "project filesystem reconciliation exceeds the "
                f"{_RECONCILIATION_MAX_FILES}-file safety limit"
            )
        settings = await self.get_scan_settings()
        entries: list[FolderUploadEntry] = []
        for path in paths:
            if path.size_bytes > settings.max_document_size_bytes:
                continue
            content = await self._run_blocking(
                filesystem.read_bytes,
                path.relative_path,
                max_bytes=settings.max_document_size_bytes,
            )
            entries.append(
                FolderUploadEntry(relative_path=path.relative_path, content=content)
            )
        preview = await self.preview_folder(
            folder_name=self.project_id,
            entries=entries,
            settings=settings,
        )
        content_by_path = {entry.relative_path: entry.content for entry in entries}
        desired = {entry.relative_path: entry for entry in preview.included}
        current_rows = await self._reader.list_manual_documents_for_reconciliation(
            limit=_RECONCILIATION_MAX_FILES + 1,
        )
        current = {
            row["relative_path"]: row
            for row in current_rows
        }
        if len(current) > _RECONCILIATION_MAX_FILES:
            raise RuntimeError(
                "document catalog reconciliation exceeds the "
                f"{_RECONCILIATION_MAX_FILES}-file safety limit"
            )

        created = changed = deleted = 0
        now = get_now_iso()
        for relative_path, preview_entry in desired.items():
            content = content_by_path[relative_path]
            existing = current.pop(relative_path, None)
            if existing is not None and existing["content_hash"] == preview_entry.content_hash:
                continue
            if existing is not None:
                await self._writer.delete_document(
                    document_id=str(existing["document_id"]),
                    session_id=None,
                )
                changed += 1
            else:
                created += 1
            await self._writer.insert_document(
                document_id=str(uuid.uuid4()),
                session_id=None,
                visibility_scope="project",
                original_name=preview_entry.original_name,
                relative_path=relative_path,
                extension=preview_entry.extension,
                size_bytes=preview_entry.size_bytes,
                content_hash=preview_entry.content_hash,
                content=content,
                created_at=now,
            )
        for document in current.values():
            await self._writer.delete_document(
                document_id=str(document["document_id"]),
                session_id=None,
            )
            deleted += 1
        if created or changed or deleted:
            self._indexer.wake_pending_indexes()
        return {
            "created": created,
            "changed": changed,
            "deleted": deleted,
            "excluded": preview.summary.excluded_count,
        }

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
                scan_settings = FolderScanSettings.model_validate(scan_settings)
        return await self._run_blocking(
            build_folder_preview,
            folder_name.strip(),
            validated_entries,
            scan_settings,
            force_include_paths or [],
        )

    async def get_scan_settings(self) -> FolderScanSettings:
        """Return persisted project scan settings or validated defaults."""
        row = await self._reader.fetch_scan_settings()
        if row is None:
            return FolderScanSettings()
        return FolderScanSettings.model_validate(row["settings"])

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
        await self._writer.upsert_scan_settings(
            settings_json=json.dumps(validated.model_dump(mode="json")),
            saved_at=saved_at,
        )
        return validated

    async def reset_scan_settings(self) -> FolderScanSettings:
        """Remove saved project settings and return defaults."""
        await self._writer.delete_scan_settings()
        return FolderScanSettings()

    @staticmethod
    def _normalize_path_prefix(path_prefix: Optional[str]) -> Optional[str]:
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

    @staticmethod
    def _public_metadata(row: Dict) -> Dict:
        metadata = dict(row)
        if metadata.get("document_id") is not None:
            metadata["document_id"] = str(metadata["document_id"])
        for key in ("created_at", "updated_at", "indexed_at", "deleted_at"):
            value = metadata.get(key)
            if isinstance(value, datetime):
                metadata[key] = value.isoformat()
        metadata.setdefault("chunk_count", 0)
        return metadata

    async def _get_visible_document(
        self,
        *,
        session_id: Optional[str],
        document_id: Optional[str],
        relative_path: Optional[str],
    ) -> Dict:
        """Resolve exactly one non-deleted document visible to this session."""
        rows = await self._reader.fetch_documents_by_reference(
            document_id=document_id,
            relative_path=relative_path,
            session_id=session_id,
        )
        if not rows:
            raise FileNotFoundError("Document not found")
        if relative_path is not None and len(rows) > 1:
            raise ValueError(
                "Multiple visible uploads use this relative_path; use document_id"
            )
        return rows[0]

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
    def _public_workspace_source(row: Dict) -> Dict:
        source = dict(row)
        source.setdefault("ownership_mode", "external_sync")
        if source.get("source_id") is not None:
            source["source_id"] = str(source["source_id"])
        for key in ("created_at", "updated_at", "last_synced_at"):
            value = source.get(key)
            if isinstance(value, datetime):
                source[key] = value.isoformat()
        return source

    async def create_workspace_source(
        self,
        *,
        display_name: str,
        session_id: Optional[str] = None,
        visibility_scope: str = "project",
        ownership_mode: str = "external_sync",
    ) -> Dict:
        """Create a stable source identity for future workspace syncs.

        This does not upload, queue, or index files.  The returned ``source_id``
        is the durable handle a caller keeps when it later submits a manifest.
        """
        if ownership_mode not in {"external_sync", "managed_project_workspace"}:
            raise ValueError(
                "ownership_mode must be either 'external_sync' or "
                "'managed_project_workspace'"
            )
        self._validate_visibility(visibility_scope, session_id)
        if ownership_mode == "managed_project_workspace":
            if visibility_scope != "project" or session_id is not None:
                raise ValueError(
                    "managed_project_workspace sources must be project-visible"
                )
        if not isinstance(display_name, str) or not display_name.strip():
            raise ValueError("display_name must not be empty")

        source_id = str(uuid.uuid4())
        created_at = get_now_iso()
        normalized_name = display_name.strip()
        if ownership_mode == "managed_project_workspace":
            await self._writer.insert_managed_workspace_source(
                source_id=source_id,
                display_name=normalized_name,
                created_at=created_at,
            )
        else:
            await self._writer.insert_workspace_source(
                source_id=source_id,
                session_id=session_id,
                visibility_scope=visibility_scope,
                display_name=normalized_name,
                created_at=created_at,
            )
        return {
            "source_id": source_id,
            "project_id": self.project_id,
            "session_id": session_id,
            "visibility_scope": visibility_scope,
            "ownership_mode": ownership_mode,
            "display_name": normalized_name,
            "last_synced_at": None,
            "last_manifest_candidate_count": 0,
            "last_manifest_included_count": 0,
            "last_manifest_excluded_count": 0,
            "last_manifest_excluded_reason_counts": {},
            "created_at": created_at,
            "updated_at": created_at,
        }

    async def get_workspace_source(
        self,
        *,
        source_id: str,
        session_id: Optional[str] = None,
    ) -> Dict:
        """Return one visible workspace source by its durable identity."""
        if not isinstance(source_id, str) or not source_id.strip():
            raise ValueError("source_id must not be empty")
        source = await self._reader.fetch_workspace_source(
            source_id=source_id.strip(),
            session_id=session_id,
        )
        if source is None:
            raise FileNotFoundError("Workspace source not found")
        return self._public_workspace_source(source)

    async def sync_workspace_source(
        self,
        *,
        source_id: str,
        entries: List[FolderUploadEntry],
        settings: Optional[FolderScanSettings] = None,
        force_include_paths: Optional[List[str]] = None,
        session_id: Optional[str] = None,
    ) -> Dict:
        """Admit a complete workspace manifest without waiting for indexing.

        The manifest is filtered with the same safety rules as a folder preview.
        Files whose normalized path and hash are unchanged remain indexed; new
        or changed files become durable ``queued`` documents. Files omitted by
        the manifest (including newly excluded files) are removed.
        """
        source = await self.get_workspace_source(
            source_id=source_id,
            session_id=session_id,
        )
        if source.get("ownership_mode", "external_sync") != "external_sync":
            raise ValueError(
                "managed project workspace sources must use ProjectWorkspaceService"
            )
        validated_entries = [
            entry
            if isinstance(entry, FolderUploadEntry)
            else FolderUploadEntry.model_validate(entry)
            for entry in entries
        ]
        preview = await self.preview_folder(
            folder_name=source["display_name"],
            entries=validated_entries,
            settings=settings,
            force_include_paths=force_include_paths,
        )
        entry_content = {
            normalize_relative_path(
                entry.relative_path,
                entry.relative_path,
            ): entry.content
            for entry in validated_entries
        }
        documents = [
            {
                "original_name": item.original_name,
                "relative_path": item.relative_path,
                "extension": item.extension,
                "size_bytes": item.size_bytes,
                "content_hash": item.content_hash,
                "content": entry_content[item.relative_path],
                "visibility_scope": source["visibility_scope"],
            }
            for item in preview.included
        ]
        counts = await self._writer.sync_workspace_manifest(
            source_id=source["source_id"],
            session_id=session_id,
            documents=documents,
            candidate_count=len(validated_entries),
            included_count=preview.summary.included_count,
            excluded_count=preview.summary.excluded_count,
            excluded_reason_counts=preview.summary.reason_counts,
            updated_at=get_now_iso(),
        )
        if counts["queued"]:
            self._indexer.queue_workspace_source_indexing(
                source_id=source["source_id"],
                session_id=session_id,
            )
        return {
            "source": source,
            "candidate_count": len(validated_entries),
            "candidate_bytes": sum(
                len(entry.content) for entry in validated_entries
            ),
            "included_count": preview.summary.included_count,
            "excluded_count": preview.summary.excluded_count,
            "excluded_reason_counts": preview.summary.reason_counts,
            **counts,
        }

    async def sync_workspace_changes(
        self,
        *,
        source_id: str,
        changes: WorkspaceSyncChanges,
        settings: Optional[FolderScanSettings] = None,
        force_include_paths: Optional[List[str]] = None,
        session_id: Optional[str] = None,
    ) -> Dict:
        """Apply changed/new files and deletions without replacing a manifest.

        This is the server-side contract used by future local workspace
        watchers. It deliberately leaves paths absent from ``changes`` alone;
        callers use :meth:`sync_workspace_source` for a complete rescan.
        """
        source = await self.get_workspace_source(
            source_id=source_id,
            session_id=session_id,
        )
        if source.get("ownership_mode", "external_sync") != "external_sync":
            raise ValueError(
                "managed project workspace sources must use ProjectWorkspaceService"
            )
        validated_changes = (
            changes
            if isinstance(changes, WorkspaceSyncChanges)
            else WorkspaceSyncChanges.model_validate(changes)
        )
        validated_entries = list(validated_changes.upserts)
        normalized_upsert_paths = {
            normalize_relative_path(entry.relative_path, entry.relative_path)
            for entry in validated_entries
        }
        if len(normalized_upsert_paths) != len(validated_entries):
            raise ValueError(
                "upserts contain duplicate normalized relative_path values"
            )
        normalized_deleted_paths = {
            normalize_relative_path(path, path)
            for path in validated_changes.deleted_paths
        }
        if len(normalized_deleted_paths) != len(validated_changes.deleted_paths):
            raise ValueError(
                "deleted_paths contain duplicate normalized relative_path values"
            )
        overlap = normalized_upsert_paths & normalized_deleted_paths
        if overlap:
            raise ValueError(
                "a workspace path cannot be both upserted and deleted: "
                + ", ".join(sorted(overlap))
            )
        if not normalized_upsert_paths and not normalized_deleted_paths:
            raise ValueError("workspace changes must include an upsert or deletion")

        preview = await self.preview_folder(
            folder_name=source["display_name"],
            entries=validated_entries,
            settings=settings,
            force_include_paths=force_include_paths,
        )
        entry_content = {
            normalize_relative_path(
                entry.relative_path,
                entry.relative_path,
            ): entry.content
            for entry in validated_entries
        }
        documents = [
            {
                "original_name": item.original_name,
                "relative_path": item.relative_path,
                "extension": item.extension,
                "size_bytes": item.size_bytes,
                "content_hash": item.content_hash,
                "content": entry_content[item.relative_path],
                "visibility_scope": source["visibility_scope"],
            }
            for item in preview.included
        ]
        excluded_paths = {item.relative_path for item in preview.excluded}
        counts = await self._writer.sync_workspace_changes(
            source_id=source["source_id"],
            session_id=session_id,
            documents=documents,
            deleted_paths=sorted(normalized_deleted_paths | excluded_paths),
            updated_at=get_now_iso(),
        )
        if counts["queued"]:
            self._indexer.queue_workspace_source_indexing(
                source_id=source["source_id"],
                session_id=session_id,
            )
        return {
            "source": source,
            "upsert_count": len(validated_entries),
            "upsert_bytes": sum(len(entry.content) for entry in validated_entries),
            "included_count": preview.summary.included_count,
            "excluded_count": preview.summary.excluded_count,
            "excluded_reason_counts": preview.summary.reason_counts,
            "deleted_path_count": len(normalized_deleted_paths),
            **counts,
        }

    async def get_workspace_indexing_status(
        self,
        *,
        source_id: str,
        session_id: Optional[str] = None,
    ) -> Dict:
        """Return source lifecycle metadata and aggregate indexing progress."""
        if not isinstance(source_id, str) or not source_id.strip():
            raise ValueError("source_id must not be empty")
        status = await self._reader.fetch_workspace_indexing_status(
            source_id=source_id.strip(),
            session_id=session_id,
        )
        if status is None:
            raise FileNotFoundError("Workspace source not found")
        result = self._public_workspace_source(status)
        queued = int(result["queued_count"])
        indexing = int(result["indexing_count"])
        failed = int(result["failed_count"])
        indexed = int(result["indexed_count"])
        result["status"] = (
            "indexing"
            if queued or indexing
            else "failed"
            if failed and not indexed
            else "ready_with_failures"
            if failed
            else "ready"
        )
        return result

    def queue_workspace_source_indexing(
        self,
        *,
        source_id: str,
        session_id: Optional[str] = None,
    ) -> None:
        """Delegate durable workspace scheduling to the indexer."""
        self._indexer.queue_workspace_source_indexing(
            source_id=source_id,
            session_id=session_id,
        )

    async def get_document_info(
        self,
        *,
        session_id: Optional[str] = None,
        document_id: Optional[str] = None,
        relative_path: Optional[str] = None,
    ) -> Dict:
        """Return metadata for one visible document."""
        document = await self._get_visible_document(
            document_id=document_id,
            relative_path=relative_path,
            session_id=session_id,
        )
        return self._public_metadata(document)

    async def read_document(
        self,
        *,
        session_id: Optional[str] = None,
        document_id: Optional[str] = None,
        relative_path: Optional[str] = None,
        page_number: Optional[int] = None,
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
        if page_number is not None and (
            not isinstance(page_number, int)
            or isinstance(page_number, bool)
            or page_number < 1
        ):
            raise ValueError("page_number must be a positive integer")

        document_metadata = await self._get_visible_document(
            document_id=document_id,
            relative_path=relative_path,
            session_id=session_id,
        )
        extension = document_metadata["extension"].lower()
        selected_page = None
        docx_paragraphs = None
        if extension == ".pdf":
            raw_bytes = await self._read_source_bytes(
                document_metadata,
                session_id=session_id,
            )
            if raw_bytes is None:
                raise FileNotFoundError("Document content is missing")
            pages = await self._run_blocking(extract_pdf_pages, raw_bytes)
            selected_page = page_number or 1
            if selected_page > len(pages):
                raise ValueError(
                    f"page_number {selected_page} exceeds document page count "
                    f"{len(pages)}"
                )
            text = pages[selected_page - 1].text
        elif extension == ".docx":
            raw_bytes = await self._read_source_bytes(
                document_metadata,
                session_id=session_id,
            )
            if raw_bytes is None:
                raise FileNotFoundError("Document content is missing")
            docx_paragraphs = await self._run_blocking(
                extract_docx_paragraphs, raw_bytes
            )
            text = "\n".join(paragraph.text for paragraph in docx_paragraphs)
        else:
            text = await self._reader.fetch_extracted_text(
                document_id=str(document_metadata["document_id"]),
                content_hash=document_metadata["content_hash"],
                session_id=session_id,
            )
            if text is None:
                raw_bytes = await self._read_source_bytes(
                    document_metadata,
                    session_id=session_id,
                )
                if raw_bytes is None:
                    raise FileNotFoundError("Document content is missing")
                text = await self._run_blocking(
                    extract_text,
                    raw_bytes,
                    document_metadata["extension"],
                )
        if extension == ".csv":
            lines = csv_data_rows(text)
            locator = {
                "kind": "csv_rows",
                "start_row": start_line,
                "end_row": end_line,
            }
        elif extension == ".docx":
            lines = [paragraph.text for paragraph in docx_paragraphs]
            locator = {
                "kind": "docx_paragraphs",
                "start_paragraph": start_line,
                "end_paragraph": end_line,
            }
        else:
            lines = text.splitlines() or [text]
            locator = {
                "kind": "text_lines",
                "start_line": start_line,
                "end_line": end_line,
            }
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
        if extension == ".csv":
            locator["end_row"] = requested_end
        elif extension == ".docx":
            locator["end_paragraph"] = requested_end
            heading_path = docx_heading_path(docx_paragraphs, start_line)
            if heading_path is not None:
                locator["heading_path"] = list(heading_path)
        else:
            locator["end_line"] = requested_end
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
                "chunk_index": (
                    f"page:{selected_page}:lines:{start_line}-{requested_end}"
                    if selected_page is not None
                    else f"rows:{start_line}-{requested_end}"
                    if extension == ".csv"
                    else f"paragraphs:{start_line}-{requested_end}"
                    if extension == ".docx"
                    else f"lines:{start_line}-{requested_end}"
                ),
                "content": content,
                "start_line": start_line,
                "end_line": requested_end,
                "total_lines": total_lines,
                "truncated": character_truncated or requested_end < total_lines,
                "locator": (
                    {"kind": "pdf_page", "page": selected_page}
                    if selected_page is not None
                    else locator
                ),
            }
        )
        if selected_page is not None:
            result["page_number"] = selected_page
        return result

    async def resolve_document_selection(
        self,
        *,
        document_id: str,
        selection: DocumentSelection,
        session_id: Optional[str] = None,
    ) -> Dict:
        """Resolve a current, bounded passage selected from one document.

        The browser provides only a version hash and coordinate. This boundary
        checks both against the visible durable document and returns server-read
        content plus a canonical locator, never client-supplied display metadata.
        """
        document = await self._get_visible_document(
            document_id=document_id,
            relative_path=None,
            session_id=session_id,
        )
        if selection.content_hash != document["content_hash"]:
            raise ValueError(
                "Document selection is stale; refresh the document and select again"
            )

        extension = str(document["extension"]).lower()
        locator = selection.locator
        if isinstance(locator, PdfPageLocator):
            if extension != ".pdf":
                raise ValueError("PDF page selections require a PDF document")
            result = await self.read_document(
                document_id=str(document["document_id"]),
                session_id=session_id,
                page_number=locator.page,
            )
            if result["end_line"] != result["total_lines"]:
                raise ValueError("Selected PDF page exceeds the readable passage limit")
            canonical_locator = {"kind": "pdf_page", "page": locator.page}
        elif isinstance(locator, DocxParagraphLocator):
            if extension != ".docx":
                raise ValueError("DOCX paragraph selections require a DOCX document")
            result = await self.read_document(
                document_id=str(document["document_id"]),
                session_id=session_id,
                start_line=locator.start_paragraph,
                end_line=locator.end_paragraph,
            )
            self._require_exact_selection_range(
                result,
                start=locator.start_paragraph,
                end=locator.end_paragraph,
            )
            canonical_locator = dict(result["locator"])
        elif isinstance(locator, CsvRowLocator):
            if extension != ".csv":
                raise ValueError("CSV row selections require a CSV document")
            result = await self.read_document(
                document_id=str(document["document_id"]),
                session_id=session_id,
                start_line=locator.start_row,
                end_line=locator.end_row,
            )
            self._require_exact_selection_range(
                result,
                start=locator.start_row,
                end=locator.end_row,
            )
            canonical_locator = dict(result["locator"])
        elif isinstance(locator, CodeLineLocator):
            if not is_code_extension(extension):
                raise ValueError("Code line selections require a source-code document")
            result = await self.read_document(
                document_id=str(document["document_id"]),
                session_id=session_id,
                start_line=locator.start_line,
                end_line=locator.end_line,
            )
            self._require_exact_selection_range(
                result,
                start=locator.start_line,
                end=locator.end_line,
            )
            canonical_locator = {
                "kind": "code_lines",
                "start_line": result["start_line"],
                "end_line": result["end_line"],
            }
        elif isinstance(locator, TextLineLocator):
            if extension in {".pdf", ".docx", ".csv", ".ipynb", *IMAGE_EXTENSIONS}:
                raise ValueError(
                    "Text line selections are unsupported for this document format"
                )
            if is_code_extension(extension):
                raise ValueError("Source-code documents require a code line selection")
            result = await self.read_document(
                document_id=str(document["document_id"]),
                session_id=session_id,
                start_line=locator.start_line,
                end_line=locator.end_line,
            )
            self._require_exact_selection_range(
                result,
                start=locator.start_line,
                end=locator.end_line,
            )
            canonical_locator = dict(result["locator"])
        else:  # pragma: no cover - DocumentSelection validates the union.
            raise ValueError("Unsupported document selection locator")

        if len(result["content"]) >= MAX_READ_CHARACTERS:
            raise ValueError("Selected passage exceeds the readable character limit")
        result["locator"] = canonical_locator
        return result

    @staticmethod
    def _require_exact_selection_range(result: Dict, *, start: int, end: int) -> None:
        """Reject a requested range that had to be shortened during reading."""
        if result["start_line"] != start or result["end_line"] != end:
            raise ValueError("Selected passage is outside the current document range")

    async def delete_document(
        self,
        *,
        document_id: str,
        session_id: Optional[str] = None,
    ) -> Dict:
        """Purge document content while retaining a minimal provenance record."""
        if not isinstance(document_id, str) or not document_id.strip():
            raise ValueError("document_id must not be empty")
        document_id = document_id.strip()
        document = await self._get_visible_document(
            document_id=document_id,
            relative_path=None,
            session_id=session_id,
        )
        filesystem = self._filesystem_for_document(document)
        if filesystem is not None:
            await self._run_blocking(
                filesystem.delete_file,
                document["relative_path"],
                expected_content_hash=document["content_hash"],
            )
        row = await self._writer.delete_document(
            document_id=document_id,
            session_id=session_id,
        )
        if row is None:
            raise FileNotFoundError("Document not found")
        deleted_metadata = self._public_metadata(row)
        deleted_metadata["deleted"] = True
        return deleted_metadata

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
        """Durably admit a selected folder batch before indexing its files."""
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
        created_at = get_now_iso()
        candidate_bytes = sum(len(entry.content) for entry in validated_entries)
        prepared_documents = []

        for relative_path in normalized_selected:
            content = entry_content[relative_path]
            preview_entry = included_by_path[relative_path]
            prepared_documents.append(
                {
                    "document_id": str(uuid.uuid4()),
                    "relative_path": relative_path,
                    "original_name": preview_entry.original_name,
                    "extension": preview_entry.extension,
                    "size_bytes": preview_entry.size_bytes,
                    "content_hash": preview_entry.content_hash,
                    "content": content,
                }
            )

        await self._writer.insert_folder_batch(
            folder_root_id=folder_root_id,
            session_id=session_id,
            visibility_scope=visibility_scope,
            folder_name=folder_name.strip(),
            candidate_count=len(validated_entries),
            candidate_bytes=candidate_bytes,
            excluded_count=preview.summary.excluded_count,
            excluded_bytes=preview.summary.excluded_bytes,
            excluded_directory_count=preview.summary.excluded_directory_count,
            excluded_reason_counts=preview.summary.reason_counts,
            scan_settings=preview.settings.model_dump(mode="json"),
            documents=prepared_documents,
            created_at=created_at,
        )

        documents = [
            await self.schedule_document_index(
                document_id=document["document_id"],
                session_id=session_id,
            )
            for document in prepared_documents
        ]
        return {
            "folder_root_id": folder_root_id,
            "project_id": self.project_id,
            "session_id": session_id,
            "visibility_scope": visibility_scope,
            "folder_name": folder_name.strip(),
            "candidate_count": len(validated_entries),
            "candidate_bytes": candidate_bytes,
            "document_count": len(documents),
            "total_size_bytes": sum(
                document["size_bytes"] for document in prepared_documents
            ),
            "excluded_count": preview.summary.excluded_count,
            "excluded_bytes": preview.summary.excluded_bytes,
            "excluded_directory_count": preview.summary.excluded_directory_count,
            "excluded_reason_counts": preview.summary.reason_counts,
            "scan_settings": preview.settings.model_dump(mode="json"),
            "created_at": created_at,
            "indexed_at": None,
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
        """Store a manual upload as durable queued indexing work."""
        if not isinstance(content, bytes):
            raise TypeError("content must be bytes")
        if not content:
            raise ValueError("document content must not be empty")
        if len(content) > MAX_DOCUMENT_SIZE:
            raise ValueError("document exceeds the 50 MB size limit")
        original_name = original_name.strip() if original_name else original_name
        if not original_name:
            raise ValueError("original_name must not be empty")
        if "\x00" in original_name:
            raise ValueError("original_name contains an invalid null byte")
        extension = document_extension(original_name)
        if extension not in ACCEPTED_EXTENSIONS:
            raise ValueError(
                f"Unsupported file type '{extension}'. "
                f"Accepted types include PDF, DOCX, plain text, source code, and images."
            )
        self._validate_visibility(visibility_scope, session_id)
        normalized_path = normalize_relative_path(relative_path, original_name)
        document_id = str(uuid.uuid4())
        content_hash = hashlib.sha256(content).hexdigest()
        created_at = get_now_iso()
        filesystem = (
            self._filesystem if visibility_scope == "project" else None
        )
        if filesystem is not None:
            await self._run_blocking(
                filesystem.write_bytes,
                normalized_path,
                content,
            )
        try:
            await self._writer.insert_document(
                document_id=document_id,
                session_id=session_id,
                visibility_scope=visibility_scope,
                original_name=original_name,
                relative_path=normalized_path,
                extension=extension,
                size_bytes=len(content),
                content_hash=content_hash,
                content=content,
                created_at=created_at,
            )
        except Exception:
            if filesystem is not None:
                try:
                    await self._run_blocking(
                        filesystem.delete_file,
                        normalized_path,
                        expected_content_hash=content_hash,
                    )
                except Exception:
                    logger.exception(
                        "Could not roll back local document file after catalog failure"
                    )
            raise
        return {
            "document_id": document_id,
            "project_id": self.project_id,
            "session_id": session_id,
            "visibility_scope": visibility_scope,
            "folder_root_id": None,
            "source_kind": "manual_upload",
            "original_name": original_name,
            "relative_path": normalized_path,
            "extension": extension,
            "size_bytes": len(content),
            "content_hash": content_hash,
            "status": "queued",
            "deleted_at": None,
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
        policy: Optional[DocumentIndexPolicy] = None,
    ) -> Dict:
        """Delegate document derivation to this project's DocumentIndexer."""

        row = await self._indexer.index_document(
            document_id=document_id,
            session_id=session_id,
            policy=policy,
        )
        return self._public_metadata(row)

    @property
    def indexer(self) -> DocumentIndexer:
        """Expose the project-owned indexer for runtime lifecycle ownership."""

        return self._indexer

    async def submit_document(
        self,
        *,
        content: bytes,
        original_name: str,
        relative_path: Optional[str] = None,
        session_id: Optional[str] = None,
        visibility_scope: str = "project",
    ) -> Dict:
        """Persist a document, then index inline or admit durable background work."""
        document = await self.add_document(
            content=content,
            original_name=original_name,
            relative_path=relative_path,
            session_id=session_id,
            visibility_scope=visibility_scope,
        )
        return await self.schedule_document_index(
            document_id=document["document_id"],
            session_id=session_id,
        )

    async def schedule_document_index(
        self,
        *,
        document_id: str,
        session_id: Optional[str] = None,
    ) -> Dict:
        """Delegate durable index admission to the project-owned indexer."""
        return self._public_metadata(
            await self._indexer.schedule_document_index(
                document_id=document_id,
                session_id=session_id,
            )
        )

    async def shutdown(self) -> None:
        """Delegate indexer task shutdown while retaining this public façade."""
        await self._indexer.shutdown()

    async def recover_pending_indexes(self, limit: int = 16) -> int:
        """Delegate recovery to the project-owned indexer."""
        return await self._indexer.recover_pending_indexes(limit)

    async def pending_index_count(self) -> int:
        return await self._indexer.pending_index_count()

    def indexing_snapshot(self) -> Dict:
        """Expose the indexer's bounded health projection."""
        return self._indexer.indexing_snapshot()

    def indexing_snapshot_for_health(self) -> dict[str, object]:
        """Return a bounded public projection of indexing metrics."""
        return sanitize_health_details(self.indexing_snapshot())

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
        rows = await self._reader.list_folder_uploads(
            session_id=session_id,
            visibility_scope=visibility_scope,
            limit=limit,
        )
        return [self._public_folder_metadata(row) for row in rows]

    async def get_folder_upload_summary(
        self,
        *,
        folder_root_id: str,
        session_id: Optional[str] = None,
        path_prefix: Optional[str] = None,
    ) -> Dict:
        """Return one visible folder batch and a shallow document tree."""
        folder_root_id = folder_root_id.strip()
        if not folder_root_id:
            raise ValueError("folder_root_id must not be empty")
        folder = await self._reader.fetch_folder_upload(
            folder_root_id=folder_root_id,
            session_id=session_id,
        )
        if folder is None:
            raise FileNotFoundError("Folder upload not found")
        result = self._public_folder_metadata(folder)
        result["tree"] = await self.list_folder_tree(
            folder_root_id=folder_root_id,
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
            }

        if folder_root_id is None:
            raise ValueError(
                "document focus requires document_id or folder_root_id"
            )
        folder_root_id = folder_root_id.strip()
        if not folder_root_id:
            raise ValueError("folder_root_id must not be empty")
        normalized_prefix = self._normalize_path_prefix(path_prefix)
        folder = await self._reader.fetch_folder_upload(
            folder_root_id=folder_root_id,
            session_id=session_id,
        )
        if folder is None:
            raise FileNotFoundError("Document focus target not found")
        if normalized_prefix is not None:
            documents = await self.list_documents(
                session_id=session_id,
                folder_root_id=folder_root_id,
                path_prefix=normalized_prefix,
                limit=1,
            )
            if not documents:
                raise FileNotFoundError("Document focus target not found")
            return {
                "target_type": "subtree",
                "folder_root_id": folder_root_id,
                "path_prefix": normalized_prefix,
            }
        return {
            "target_type": "folder_upload",
            "folder_root_id": folder_root_id,
        }

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
        folder_root_id = folder_root_id.strip()
        if not folder_root_id:
            raise ValueError("folder_root_id must not be empty")
        if (
            not isinstance(max_depth, int)
            or isinstance(max_depth, bool)
            or not 1 <= max_depth <= 10
        ):
            raise ValueError("max_depth must be between 1 and 10")
        normalized_prefix = self._normalize_path_prefix(path_prefix)
        folder = await self._reader.fetch_folder_upload(
            folder_root_id=folder_root_id,
            session_id=session_id,
        )
        if folder is None:
            raise FileNotFoundError("Folder upload not found")
        rows = await self._reader.fetch_folder_documents(
            folder_root_id=folder_root_id,
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
            folder_root_id = folder_root_id.strip()
            if not folder_root_id:
                raise ValueError("folder_root_id must not be empty")
            folder = await self._reader.fetch_folder_upload(
                folder_root_id=folder_root_id,
                session_id=session_id,
            )
            if folder is None:
                raise FileNotFoundError("Folder upload not found")
        rows = await self._reader.list_documents(
            session_id=session_id,
            visibility_scope=visibility_scope,
            folder_root_id=folder_root_id,
            path_prefix=normalized_prefix,
            limit=limit,
        )
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
            folder_root_id = folder_root_id.strip()
            if not folder_root_id:
                raise ValueError("folder_root_id must not be empty")
            folder = await self._reader.fetch_folder_upload(
                folder_root_id=folder_root_id,
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

        retrieval_limit = (
            max(n_results, self._document_rerank_candidates)
            if self._document_rerank_enabled
            else n_results
        )
        rows = await self._reader.search_chunks(
            session_id=session_id,
            query_text=query.strip(),
            query_embedding=query_embedding,
            n_results=retrieval_limit,
            candidate_limit=min(
                HYBRID_SEARCH_MAX_CANDIDATES,
                max(
                    HYBRID_SEARCH_MIN_CANDIDATES,
                    retrieval_limit * HYBRID_SEARCH_CANDIDATE_MULTIPLIER,
                ),
            ),
            document_filter=document_filter,
            folder_root_id=folder_root_id,
            relative_path=normalized_relative_path,
            path_prefix=normalized_prefix,
        )
        if self._document_rerank_enabled and len(rows) > 1:
            try:
                rerank_inputs = [
                    "\n".join(
                        part
                        for part in (
                            f"File: {row['relative_path']}",
                            f"Symbol: {row['symbol_name']}"
                            if row.get("symbol_name")
                            else None,
                            row["content"],
                        )
                        if part
                    )
                    for row in rows
                ]
                rerank_scores = await self._embedding.rerank(
                    query.strip(), rerank_inputs
                )
                if len(rerank_scores) != len(rows):
                    raise ValueError(
                        "Document reranker returned an unexpected score count"
                    )
                rows = [
                    row
                    for row, _ in sorted(
                        zip(rows, rerank_scores),
                        key=lambda item: item[1],
                        reverse=True,
                    )
                ]
            except Exception as exc:
                logger.warning(f"Document reranking failed; using hybrid rank: {exc}")
        rows = rows[:n_results]
        results = []
        for row in rows:
            result = dict(row)
            if result.get("document_id") is not None:
                result["document_id"] = str(result["document_id"])
            result["document_name"] = result.get("original_name")
            if result.get("score") is not None:
                result["score"] = float(result["score"])
            if (
                result.get("extension", "").lower() == ".docx"
                and isinstance(result.get("start_paragraph"), int)
                and isinstance(result.get("end_paragraph"), int)
            ):
                locator = {
                    "kind": "docx_paragraphs",
                    "start_paragraph": result["start_paragraph"],
                    "end_paragraph": result["end_paragraph"],
                }
                if result.get("section_path"):
                    locator["heading_path"] = result["section_path"]
                result["locator"] = locator
            results.append(result)
        return results
