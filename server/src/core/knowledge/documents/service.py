import asyncio
import hashlib
import json
import uuid
from datetime import datetime
from pathlib import PurePosixPath
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional

from loguru import logger
from pydantic import TypeAdapter

from common.schema.document import (
    DocumentSelection,
    FolderPreview,
    FolderScanSettings,
    FolderUploadEntry,
    SavedWebLink,
    UserAttachedFile,
    UserAttachedSource,
    UserAttachedUrl,
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
from core.project.project_files import (
    CONTEXT_FILE_PATH,
    PROJECT_FILE_PATH,
    is_controlled_context_file,
)
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


class _UnsetSavedWebLinkField:
    """Distinguish an omitted bookmark update field from an explicit null."""


_UNSET_SAVED_WEB_LINK_FIELD = _UnsetSavedWebLinkField()
_USER_ATTACHED_SOURCE_ADAPTER = TypeAdapter(UserAttachedSource)


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
        if self._filesystem_factory is None:
            return None
        return self._filesystem_factory.for_project(document["project_id"])

    async def _read_source_bytes(
        self,
        document: Dict,
    ) -> bytes | None:
        filesystem = self._filesystem_for_document(document)
        if filesystem is None:
            raise RuntimeError("Document source filesystem is not configured")
        return await self._run_blocking(
            filesystem.read_bytes,
            document["relative_path"],
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
        reserved_paths = [path for path in paths if is_controlled_context_file(path.relative_path)]
        paths = [path for path in paths if not is_controlled_context_file(path.relative_path)]
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
        current_rows = await self._reader.list_documents_for_reconciliation(
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
                )
                changed += 1
            else:
                created += 1
            await self._writer.insert_document(
                document_id=str(uuid.uuid4()),
                original_name=preview_entry.original_name,
                relative_path=relative_path,
                extension=preview_entry.extension,
                size_bytes=preview_entry.size_bytes,
                content_hash=preview_entry.content_hash,
                created_at=now,
            )
        for document in current.values():
            await self._writer.delete_document(
                document_id=str(document["document_id"]),
            )
            deleted += 1
        if created or changed or deleted:
            self._indexer.wake_pending_indexes()
        return {
            "created": created,
            "changed": changed,
            "deleted": deleted,
            "excluded": preview.summary.excluded_count + len(reserved_paths),
        }

    async def list_project_files(
        self,
        *,
        path_prefix: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict]:
        """List bounded regular files in this project's canonical local tree."""
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        filesystem = self._filesystem
        if filesystem is None:
            return []
        normalized_prefix = None
        if path_prefix is not None and path_prefix.strip() not in {"", "."}:
            normalized_prefix = normalize_relative_path(path_prefix, path_prefix).rstrip("/")
        files = await self._run_blocking(lambda: list(filesystem.iter_files()))
        files = [
            file for file in files if not is_controlled_context_file(file.relative_path)
        ]
        if normalized_prefix is not None:
            files = [
                file
                for file in files
                if file.relative_path == normalized_prefix
                or file.relative_path.startswith(normalized_prefix + "/")
            ]
        return [
            {
                "relative_path": file.relative_path,
                "original_name": PurePosixPath(file.relative_path).name,
                "extension": document_extension(file.relative_path),
                "size_bytes": file.size_bytes,
                "content_hash": file.content_hash,
            }
            for file in files[:limit]
        ]

    async def read_project_file(
        self,
        path: str,
        *,
        start_line: int = 1,
        end_line: Optional[int] = None,
        max_characters: int = MAX_READ_CHARACTERS,
    ) -> Dict:
        """Read a bounded UTF-8 file slice from this project's local tree."""
        if not isinstance(start_line, int) or isinstance(start_line, bool) or start_line < 1:
            raise ValueError("start_line must be a positive integer")
        if end_line is not None and (
            not isinstance(end_line, int)
            or isinstance(end_line, bool)
            or end_line < start_line
            or end_line - start_line + 1 > MAX_READ_LINES
        ):
            raise ValueError(f"end_line must select at most {MAX_READ_LINES} lines")
        if (
            not isinstance(max_characters, int)
            or isinstance(max_characters, bool)
            or not 1 <= max_characters <= MAX_READ_CHARACTERS
        ):
            raise ValueError(
                f"max_characters must be between 1 and {MAX_READ_CHARACTERS}"
            )
        filesystem = self._filesystem
        if filesystem is None:
            raise RuntimeError("No local project filesystem is configured")
        normalized_path = normalize_relative_path(path, path)
        self._require_unreserved_context_path(normalized_path)
        content = await self._run_blocking(filesystem.read_bytes, normalized_path)
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("project file is not UTF-8 text") from exc
        lines = text.splitlines(keepends=True) or [text]
        if start_line > len(lines):
            raise ValueError(f"start_line {start_line} exceeds document length {len(lines)}")
        requested_end = min(
            end_line or start_line + MAX_READ_LINES - 1,
            len(lines),
        )
        selected = "".join(lines[start_line - 1 : requested_end])
        character_truncated = len(selected) > max_characters
        if character_truncated:
            selected = selected[:max_characters]
        return {
            "relative_path": normalized_path,
            "original_name": PurePosixPath(normalized_path).name,
            "content": selected,
            "content_hash": hashlib.sha256(content).hexdigest(),
            "start_line": start_line,
            "end_line": requested_end,
            "total_lines": len(lines),
            "truncated": character_truncated or requested_end < len(lines),
        }

    async def read_project_brief(self) -> Optional[str]:
        """Read the user-owned ``PROJECT.md`` brief without waiting for indexing.

        The engine-maintained ``CONTEXT.md`` projection is deliberately handled
        by the controlled Context component instead of this generic file API.
        """
        try:
            return (await self.read_project_file(PROJECT_FILE_PATH))["content"]
        except FileNotFoundError:
            return None

    async def create_project_file(self, path: str, content: str) -> Dict:
        """Create a text project file and reconcile it into the document catalog."""
        payload = self._validate_project_file_content(path, content)
        filesystem = self._require_filesystem()
        normalized_path = normalize_relative_path(path, path)
        self._require_unreserved_context_path(normalized_path)
        await self._run_blocking(filesystem.write_bytes, normalized_path, payload)
        await self.reconcile_project_files()
        return await self.read_project_file(normalized_path)

    async def update_project_file(
        self,
        path: str,
        content: str,
        *,
        expected_content_hash: str,
    ) -> Dict:
        """Replace a text project file only when its caller has the current hash."""
        payload = self._validate_project_file_content(path, content)
        filesystem = self._require_filesystem()
        normalized_path = normalize_relative_path(path, path)
        self._require_unreserved_context_path(normalized_path)
        await self._run_blocking(
            filesystem.write_bytes,
            normalized_path,
            payload,
            overwrite=True,
            expected_content_hash=expected_content_hash,
        )
        await self.reconcile_project_files()
        return await self.read_project_file(normalized_path)

    async def append_project_file(
        self,
        path: str,
        content: str,
        *,
        expected_content_hash: str,
    ) -> Dict:
        """Append text to a project file under the same stale-write guard."""
        if not isinstance(content, str) or not content:
            raise ValueError("content must not be empty")
        filesystem = self._require_filesystem()
        normalized_path = normalize_relative_path(path, path)
        self._require_unreserved_context_path(normalized_path)
        current = await self._run_blocking(filesystem.read_bytes, normalized_path)
        payload = current + content.encode("utf-8")
        self._validate_project_file_bytes(normalized_path, payload)
        await self._run_blocking(
            filesystem.write_bytes,
            normalized_path,
            payload,
            overwrite=True,
            expected_content_hash=expected_content_hash,
        )
        await self.reconcile_project_files()
        return await self.read_project_file(normalized_path)

    async def move_project_file(
        self,
        source_path: str,
        destination_path: str,
        *,
        expected_content_hash: str,
    ) -> Dict:
        """Move a project file to an unused path under optimistic concurrency."""
        filesystem = self._require_filesystem()
        source = normalize_relative_path(source_path, source_path)
        destination = normalize_relative_path(destination_path, destination_path)
        self._require_unreserved_context_path(source)
        self._require_unreserved_context_path(destination)
        self._validate_project_file_extension(destination)
        await self._run_blocking(
            filesystem.move_file,
            source,
            destination,
            expected_content_hash=expected_content_hash,
        )
        await self.reconcile_project_files()
        return await self.read_project_file(destination)

    async def delete_project_file(
        self,
        path: str,
        *,
        expected_content_hash: str,
    ) -> Dict:
        """Delete one project file and reconcile the durable catalog tombstone."""
        filesystem = self._require_filesystem()
        normalized_path = normalize_relative_path(path, path)
        self._require_unreserved_context_path(normalized_path)
        deleted = await self._run_blocking(
            filesystem.delete_file,
            normalized_path,
            expected_content_hash=expected_content_hash,
        )
        await self.reconcile_project_files()
        return {
            "relative_path": deleted.relative_path,
            "content_hash": deleted.content_hash,
            "deleted": True,
        }

    async def create_project_folder(self, path: str) -> Dict:
        """Create an empty project directory without introducing catalog state."""
        filesystem = self._require_filesystem()
        normalized_path = normalize_relative_path(path, path)
        return {"relative_path": await self._run_blocking(filesystem.create_folder, normalized_path)}

    def _require_filesystem(self) -> ProjectFilesystem:
        filesystem = self._filesystem
        if filesystem is None:
            raise RuntimeError("No local project filesystem is configured")
        return filesystem

    @staticmethod
    def _validate_project_file_extension(path: str) -> None:
        extension = document_extension(path)
        if extension not in ACCEPTED_EXTENSIONS - {".pdf", ".docx", *IMAGE_EXTENSIONS}:
            raise ValueError("agent project files must be text, code, or configuration")

    def _validate_project_file_content(self, path: str, content: str) -> bytes:
        if not isinstance(content, str) or not content:
            raise ValueError("content must not be empty")
        payload = content.encode("utf-8")
        self._validate_project_file_bytes(path, payload)
        return payload

    def _validate_project_file_bytes(self, path: str, content: bytes) -> None:
        self._require_unreserved_context_path(normalize_relative_path(path, path))
        self._validate_project_file_extension(path)
        if len(content) > MAX_DOCUMENT_SIZE:
            raise ValueError("document exceeds the 50 MB size limit")

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
        document_id: Optional[str],
        relative_path: Optional[str],
    ) -> Dict:
        """Resolve exactly one non-deleted document visible to this project."""
        rows = await self._reader.fetch_documents_by_reference(
            document_id=document_id,
            relative_path=relative_path,
        )
        if not rows:
            raise FileNotFoundError("Document not found")
        if relative_path is not None and len(rows) > 1:
            raise ValueError(
                "Multiple visible uploads use this relative_path; use document_id"
            )
        return rows[0]

    async def get_document_info(
        self,
        *,
        document_id: Optional[str] = None,
        relative_path: Optional[str] = None,
    ) -> Dict:
        """Return metadata for one visible document."""
        document = await self._get_visible_document(
            document_id=document_id,
            relative_path=relative_path,
        )
        return self._public_metadata(document)

    async def read_document(
        self,
        *,
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
        )
        extension = document_metadata["extension"].lower()
        selected_page = None
        docx_paragraphs = None
        if extension == ".pdf":
            raw_bytes = await self._read_source_bytes(
                document_metadata,
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
            )
            if text is None:
                raw_bytes = await self._read_source_bytes(
                    document_metadata,
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
    ) -> Dict:
        """Resolve a current, bounded passage selected from one document.

        The browser provides only a version hash and coordinate. This boundary
        checks both against the visible durable document and returns server-read
        content plus a canonical locator, never client-supplied display metadata.
        """
        document = await self._get_visible_document(
            document_id=document_id,
            relative_path=None,
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
    ) -> Dict:
        """Purge document content while retaining a minimal provenance record."""
        if not isinstance(document_id, str) or not document_id.strip():
            raise ValueError("document_id must not be empty")
        document_id = document_id.strip()
        document = await self._get_visible_document(
            document_id=document_id,
            relative_path=None,
        )
        filesystem = self._filesystem_for_document(document)
        if filesystem is None:
            raise RuntimeError("Document source filesystem is not configured")
        await self._run_blocking(
            filesystem.delete_file,
            document["relative_path"],
            expected_content_hash=document["content_hash"],
        )
        row = await self._writer.delete_document(
            document_id=document_id,
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
    ) -> Dict:
        """Copy selected folder entries into the canonical project tree.

        A folder upload is an admission event, not a durable object. Relative
        paths become the only lasting selectors once the files are written.
        """
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
        if any(is_controlled_context_file(path) for path in normalized_selected):
            raise PermissionError(
                f"{CONTEXT_FILE_PATH} is managed through the controlled Context importer"
            )

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

        candidate_bytes = sum(len(entry.content) for entry in validated_entries)
        filesystem = self._require_filesystem()
        written: list[tuple[str, str]] = []
        try:
            for relative_path in normalized_selected:
                content = entry_content[relative_path]
                file = await self._run_blocking(
                    filesystem.write_bytes,
                    relative_path,
                    content,
                )
                written.append((relative_path, file.content_hash))
        except Exception:
            for relative_path, content_hash in reversed(written):
                try:
                    await self._run_blocking(
                        filesystem.delete_file,
                        relative_path,
                        expected_content_hash=content_hash,
                    )
                except Exception:
                    logger.exception("Could not roll back folder import file {}", relative_path)
            raise
        try:
            await self.reconcile_project_files()
        except Exception:
            for relative_path, content_hash in reversed(written):
                try:
                    await self._run_blocking(
                        filesystem.delete_file,
                        relative_path,
                        expected_content_hash=content_hash,
                    )
                except Exception:
                    logger.exception(
                        "Could not roll back folder import file {}", relative_path
                    )
            raise
        return {
            "project_id": self.project_id,
            "path_prefix": self._common_path_prefix(normalized_selected),
            "candidate_count": len(validated_entries),
            "candidate_bytes": candidate_bytes,
            "document_count": len(written),
            "total_size_bytes": sum(len(entry_content[path]) for path in normalized_selected),
            "excluded_count": preview.summary.excluded_count,
            "excluded_bytes": preview.summary.excluded_bytes,
            "excluded_directory_count": preview.summary.excluded_directory_count,
            "excluded_reason_counts": preview.summary.reason_counts,
            "scan_settings": preview.settings.model_dump(mode="json"),
            "relative_paths": normalized_selected,
        }

    @staticmethod
    def _common_path_prefix(paths: List[str]) -> Optional[str]:
        """Return a stable shared directory when an imported tree has one."""
        parents = [PurePosixPath(path).parent.parts for path in paths]
        shared: list[str] = []
        for parts in zip(*parents):
            if len(set(parts)) != 1 or parts[0] == ".":
                break
            shared.append(parts[0])
        return PurePosixPath(*shared).as_posix() if shared else None

    async def add_document(
        self,
        *,
        content: bytes,
        original_name: str,
        relative_path: Optional[str] = None,
    ) -> Dict:
        """Store a project document as durable queued indexing work."""
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
        normalized_path = normalize_relative_path(relative_path, original_name)
        self._require_unreserved_context_path(normalized_path)
        document_id = str(uuid.uuid4())
        content_hash = hashlib.sha256(content).hexdigest()
        created_at = get_now_iso()
        filesystem = self._filesystem
        if filesystem is None:
            raise RuntimeError("Document source filesystem is not configured")
        await self._run_blocking(
            filesystem.write_bytes,
            normalized_path,
            content,
        )
        try:
            await self._writer.insert_document(
                document_id=document_id,
                original_name=original_name,
                relative_path=normalized_path,
                extension=extension,
                size_bytes=len(content),
                content_hash=content_hash,
                created_at=created_at,
            )
        except Exception:
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

    @staticmethod
    def _require_unreserved_context_path(path: str) -> None:
        if is_controlled_context_file(path):
            raise PermissionError(
                f"{CONTEXT_FILE_PATH} is managed through the controlled Context importer"
            )

    async def index_document(
        self,
        *,
        document_id: str,
        policy: Optional[DocumentIndexPolicy] = None,
    ) -> Dict:
        """Delegate document derivation to this project's DocumentIndexer."""

        row = await self._indexer.index_document(
            document_id=document_id,
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
    ) -> Dict:
        """Persist a document, then index inline or admit durable background work."""
        document = await self.add_document(
            content=content,
            original_name=original_name,
            relative_path=relative_path,
        )
        return await self.schedule_document_index(
            document_id=document["document_id"],
        )

    async def admit_user_source(
        self,
        source: UserAttachedSource | Dict[str, Any],
        *,
        durable: bool = True,
    ) -> Dict[str, Any]:
        """Admit one user-introduced source, durably by default.

        ``durable=False`` is an explicit per-request opt-out.  The transient
        descriptor is returned to the caller so it can remain run-local; no
        catalog row, bookmark, or index entry is created in that mode.
        """
        if not isinstance(durable, bool):
            raise ValueError("durable must be a boolean")
        validated = (
            source
            if isinstance(source, (UserAttachedFile, UserAttachedUrl))
            else _USER_ATTACHED_SOURCE_ADAPTER.validate_python(source)
        )
        if isinstance(validated, UserAttachedFile):
            content_hash = hashlib.sha256(validated.content).hexdigest()
            if not durable:
                return {
                    "source_type": "file",
                    "durable": False,
                    "original_name": validated.original_name,
                    "relative_path": validated.relative_path,
                    "content_hash": content_hash,
                    "size_bytes": len(validated.content),
                }
            return await self.submit_document(
                content=validated.content,
                original_name=validated.original_name,
                relative_path=validated.relative_path,
            )

        if not durable:
            return {
                "source_type": "url",
                "durable": False,
                "url": validated.url,
                "title": validated.title,
                "summary": validated.summary,
            }
        return await self.save_web_link(
            url=validated.url,
            title=validated.title,
            summary=validated.summary,
        )

    async def admit_user_sources(
        self,
        sources: Iterable[UserAttachedSource | Dict[str, Any]],
        *,
        durable: bool = True,
    ) -> list[Dict[str, Any]]:
        """Admit a bounded batch of user sources with one explicit policy."""
        values = list(sources)
        if len(values) > 100:
            raise ValueError("at most 100 user sources may be admitted at once")
        return [
            await self.admit_user_source(source, durable=durable)
            for source in values
        ]

    async def schedule_document_index(
        self,
        *,
        document_id: str,
    ) -> Dict:
        """Delegate durable index admission to the project-owned indexer."""
        return self._public_metadata(
            await self._indexer.schedule_document_index(
                document_id=document_id,
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

    async def resolve_focus_target(
        self,
        *,
        document_id: Optional[str] = None,
        path_prefix: Optional[str] = None,
    ) -> Dict:
        """Validate and canonicalize one visible document-focus target."""
        if document_id is not None:
            if path_prefix is not None:
                raise ValueError(
                    "document focus cannot include folder filters"
                )
            document = await self.get_document_info(
                document_id=document_id,
            )
            return {
                "target_type": "document",
                "document_id": document["document_id"],
                "relative_path": document["relative_path"],
            }

        normalized_prefix = self._normalize_path_prefix(path_prefix)
        if normalized_prefix is None:
            raise ValueError("document focus requires document_id or path_prefix")
        documents = await self.list_documents(
            path_prefix=normalized_prefix,
            limit=1,
        )
        if not documents:
            raise FileNotFoundError("Document focus target not found")
        return {
            "target_type": "subtree",
            "path_prefix": normalized_prefix,
        }

    async def save_web_link(
        self,
        *,
        url: str,
        title: str | None = None,
        summary: str | None = None,
    ) -> Dict:
        """Save one intentional project bookmark without indexing its content."""
        saved_at = get_now_iso()
        candidate = SavedWebLink(
            link_id=str(uuid.uuid4()),
            project_id=self.project_id,
            url=url,
            title=title,
            summary=summary,
            created_at=saved_at,
            updated_at=saved_at,
        )
        row = await self._writer.insert_saved_web_link(
            link_id=candidate.link_id,
            url=candidate.url,
            title=candidate.title,
            summary=candidate.summary,
            created_at=saved_at,
        )
        return self._public_saved_web_link(row)

    async def promote_source(
        self,
        source: Dict[str, Any] | Any,
        *,
        title: str | None = None,
        summary: str | None = None,
    ) -> Dict:
        """Explicitly promote an assistant-observed web source to a bookmark.

        Source provenance remains message-owned unless this method is called.
        Promotion stores only the URL bookmark; it never treats a transient
        search/read excerpt as durable document content.
        """
        from common.schema.source.references import SourceReferenceCandidate

        candidate = (
            source
            if isinstance(source, SourceReferenceCandidate)
            else SourceReferenceCandidate.model_validate(source)
        )
        if candidate.source_kind not in {
            "web_search_result",
            "news_search_result",
            "web_page",
            "web_pdf",
        }:
            raise ValueError("only assistant-observed web sources can be promoted")
        if not candidate.canonical_url:
            raise ValueError("assistant source is missing a canonical URL")
        if title is None:
            candidate_title = candidate.metadata.get("title")
            title = candidate_title if isinstance(candidate_title, str) else None
        return await self.save_web_link(
            url=candidate.canonical_url,
            title=title,
            summary=summary,
        )

    async def list_saved_web_links(self, *, limit: int = 50) -> List[Dict]:
        """List this project's durable bookmarks, newest update first."""
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 1000
        ):
            raise ValueError("limit must be between 1 and 1000")
        rows = await self._reader.list_saved_web_links(limit=limit)
        return [self._public_saved_web_link(row) for row in rows]

    async def update_saved_web_link(
        self,
        *,
        link_id: str,
        title: str | None | _UnsetSavedWebLinkField = _UNSET_SAVED_WEB_LINK_FIELD,
        summary: str | None | _UnsetSavedWebLinkField = _UNSET_SAVED_WEB_LINK_FIELD,
    ) -> Dict:
        """Update supplied bookmark presentation fields without replacing others."""
        if (
            title is _UNSET_SAVED_WEB_LINK_FIELD
            and summary is _UNSET_SAVED_WEB_LINK_FIELD
        ):
            raise ValueError("provide title or summary to update a saved web link")
        normalized_link_id = self._require_saved_web_link_id(link_id)
        existing = await self._reader.fetch_saved_web_link(link_id=normalized_link_id)
        if existing is None:
            raise FileNotFoundError("Saved web link not found")
        updated_at = get_now_iso()
        candidate_data = {**existing, "updated_at": updated_at}
        if title is not _UNSET_SAVED_WEB_LINK_FIELD:
            candidate_data["title"] = title
        if summary is not _UNSET_SAVED_WEB_LINK_FIELD:
            candidate_data["summary"] = summary
        candidate = SavedWebLink(
            **candidate_data,
        )
        row = await self._writer.update_saved_web_link(
            link_id=normalized_link_id,
            title=candidate.title,
            summary=candidate.summary,
            updated_at=updated_at,
        )
        if row is None:
            raise FileNotFoundError("Saved web link not found")
        return self._public_saved_web_link(row)

    async def delete_saved_web_link(self, *, link_id: str) -> Dict:
        """Delete a bookmark without changing prior source references."""
        normalized_link_id = self._require_saved_web_link_id(link_id)
        if not await self._writer.delete_saved_web_link(link_id=normalized_link_id):
            raise FileNotFoundError("Saved web link not found")
        return {"link_id": normalized_link_id, "deleted": True}

    @staticmethod
    def _require_saved_web_link_id(link_id: str) -> str:
        if not isinstance(link_id, str) or not (normalized := link_id.strip()):
            raise ValueError("link_id must not be empty")
        return normalized

    @staticmethod
    def _public_saved_web_link(row: Dict) -> Dict:
        """Normalize one database bookmark into the public service shape."""
        return SavedWebLink.model_validate(row).model_dump(mode="json")

    async def list_documents(
        self,
        *,
        path_prefix: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict]:
        """List documents visible to the current project context."""
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 1000
        ):
            raise ValueError("limit must be between 1 and 1000")
        normalized_prefix = self._normalize_path_prefix(path_prefix)
        rows = await self._reader.list_documents(
            path_prefix=normalized_prefix,
            limit=limit,
        )
        return [self._public_metadata(row) for row in rows]

    async def search(
        self,
        query: str,
        *,
        n_results: int = 5,
        document_filter: Optional[str] = None,
        relative_path: Optional[str] = None,
        path_prefix: Optional[str] = None,
    ) -> List[Dict]:
        """Search indexed chunks visible to the current project context."""
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
