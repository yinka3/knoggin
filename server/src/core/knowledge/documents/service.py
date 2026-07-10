import asyncio
import hashlib
import json
import uuid
from datetime import datetime
from pathlib import PurePosixPath
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
    ACCEPTED_EXTENSIONS,
    EXPECTED_EMBEDDING_DIMENSION,
    MAX_DOCUMENT_SIZE,
    MAX_ERROR_MESSAGE_LENGTH,
    MAX_READ_CHARACTERS,
    MAX_READ_LINES,
    VALID_VISIBILITY_SCOPES,
)
from .storage import extract_text, split_text
from .scanning import build_folder_preview, normalize_relative_path
from core.knowledge.db.readers.document_reader import DocumentReader
from core.knowledge.db.writers.document_writer import DocumentWriter


class DocumentService:
    """Project-scoped storage and retrieval boundary for documents."""

    def __init__(
        self,
        project_id: str,
        postgres_client: PostgresClient,
        embedding_service: EmbeddingService,
    ):
        self.project_id = project_id
        self._embedding = embedding_service
        self._reader = DocumentReader(postgres_client, project_id)
        self._writer = DocumentWriter(postgres_client, project_id)

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
        return await asyncio.to_thread(
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

    async def get_document_info(
        self,
        *,
        session_id: Optional[str] = None,
        document_id: Optional[str] = None,
        relative_path: Optional[str] = None,
    ) -> Dict:
        """Return metadata for one visible document."""
        rows = await self._reader.fetch_documents_by_reference(
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

        rows = await self._reader.fetch_documents_by_reference(
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
        raw_bytes = await self._reader.fetch_document_content(
            str(document_metadata["document_id"])
        )
        if raw_bytes is None:
            raise FileNotFoundError("Document content is missing")
        text = await asyncio.to_thread(
            extract_text,
            raw_bytes,
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
        """Permanently delete a visible document, chunks, and stored bytes."""
        if not isinstance(document_id, str) or not document_id.strip():
            raise ValueError("document_id must not be empty")
        row = await self._writer.delete_document(
            document_id=document_id.strip(),
            session_id=session_id,
        )
        if row is None:
            raise FileNotFoundError("Document not found")
        deleted_metadata = self._public_metadata(row)
        deleted_metadata["deleted"] = True
        return deleted_metadata

    @staticmethod
    def _validate_embeddings(
        embeddings: List[List[float]],
        chunks: List[str],
    ) -> None:
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

    async def _persist_indexed_chunks(
        self,
        *,
        document_id: str,
        session_id: Optional[str],
        chunks: List[str],
        embeddings: List[List[float]],
    ) -> Dict:
        indexed_at = get_now_iso()
        row = await self._writer.persist_indexed_chunks(
            document_id=document_id,
            session_id=session_id,
            chunks=chunks,
            embeddings=embeddings,
            indexed_at=indexed_at,
        )
        if row is None:
            raise FileNotFoundError("Document not found")
        return self._public_metadata(row)

    async def _record_index_failure(
        self,
        *,
        document_id: str,
        session_id: Optional[str],
        error_message: str,
    ) -> None:
        await self._writer.record_index_failure(
            document_id=document_id,
            session_id=session_id,
            error_message=error_message,
            updated_at=get_now_iso(),
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
        indexed_at = get_now_iso()
        candidate_bytes = sum(len(entry.content) for entry in validated_entries)
        prepared_documents = []

        for relative_path in normalized_selected:
            content = entry_content[relative_path]
            preview_entry = included_by_path[relative_path]
            document_id = str(uuid.uuid4())
            text = await asyncio.to_thread(
                extract_text,
                content,
                preview_entry.extension,
            )
            chunks = await asyncio.to_thread(split_text, text)
            embeddings = await self._embedding.encode(chunks)
            self._validate_embeddings(embeddings, chunks)
            prepared_documents.append(
                {
                    "document_id": document_id,
                    "relative_path": relative_path,
                    "original_name": preview_entry.original_name,
                    "extension": preview_entry.extension,
                    "size_bytes": preview_entry.size_bytes,
                    "content_hash": preview_entry.content_hash,
                    "content": content,
                    "chunks": list(zip(chunks, embeddings)),
                    "chunk_count": len(chunks),
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
            indexed_at=indexed_at,
        )

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
        original_name = original_name.strip() if original_name else original_name
        if not original_name:
            raise ValueError("original_name must not be empty")
        if "\x00" in original_name:
            raise ValueError("original_name contains an invalid null byte")
        extension = PurePosixPath(original_name).suffix.lower()
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
        rows = await self._reader.fetch_documents_by_reference(
            document_id=document_id,
            relative_path=None,
            session_id=session_id,
        )
        document_metadata = rows[0] if rows else None
        if document_metadata is None:
            raise FileNotFoundError("Document not found")
        if document_metadata["status"] == "indexed":
            return self._public_metadata(document_metadata)

        try:
            raw_bytes = await self._reader.fetch_document_content(
                str(document_metadata["document_id"])
            )
            if raw_bytes is None:
                raise FileNotFoundError("Document content is missing")
            text = await asyncio.to_thread(
                extract_text,
                raw_bytes,
                document_metadata["extension"],
            )
            chunks = await asyncio.to_thread(split_text, text)
            embeddings = await self._embedding.encode(chunks)
            self._validate_embeddings(embeddings, chunks)
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
                "folder_root_id": document.get("folder_root_id"),
                "path_prefix": None,
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
                "document_id": None,
                "relative_path": None,
                "folder_root_id": folder_root_id,
                "path_prefix": normalized_prefix,
            }
        return {
            "target_type": "folder_upload",
            "document_id": None,
            "relative_path": None,
            "folder_root_id": folder_root_id,
            "path_prefix": None,
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

        rows = await self._reader.search_chunks(
            session_id=session_id,
            query_embedding=query_embedding,
            n_results=n_results,
            document_filter=document_filter,
            folder_root_id=folder_root_id,
            relative_path=normalized_relative_path,
            path_prefix=normalized_prefix,
        )
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
