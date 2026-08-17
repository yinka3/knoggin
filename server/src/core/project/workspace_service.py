"""Project-owned virtual workspace files.

The managed workspace deliberately uses the document tables and indexing
pipeline instead of exposing a host filesystem.  Paths are normalized before
they reach storage and each mutation is a single transactional write with an
optimistic content-hash check where applicable.
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from common.utils.time_utils import get_now_iso
from core.knowledge.documents.constants import (
    ACCEPTED_EXTENSIONS,
    MAX_DOCUMENT_SIZE,
    MAX_READ_CHARACTERS,
    MAX_READ_LINES,
    document_extension,
)
from core.knowledge.documents.scanning import normalize_relative_path

if TYPE_CHECKING:
    from core.knowledge.documents.service import DocumentService


MANAGED_WORKSPACE_MODE = "managed_project_workspace"
MAX_LIST_FILES = 1000
PROJECT_FILE_PATH = "PROJECT.md"
PROJECT_CONTEXT_MAX_CHARACTERS = 12_000


def build_project_markdown(name: str, description: Optional[str] = None) -> str:
    """Build the small trusted seed for a newly-created project."""
    clean_name = " ".join(name.split())
    clean_description = description.strip() if description else ""
    sections = [f"# {clean_name}", ""]
    if clean_description:
        sections.extend([clean_description, ""])
    sections.extend(
        [
            "## Project Context",
            "",
            "Add project-specific context and instructions here.",
            "",
        ]
    )
    return "\n".join(sections)


class ProjectWorkspaceService:
    """Access the one managed workspace owned by a project."""

    def __init__(self, document_service: DocumentService) -> None:
        self._documents = document_service
        self.project_id = document_service.project_id
        self._reader = document_service._reader
        self._writer = document_service._writer

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
    def _public_source(row: Dict) -> Dict:
        source = dict(row)
        source.setdefault("ownership_mode", MANAGED_WORKSPACE_MODE)
        if source.get("source_id") is not None:
            source["source_id"] = str(source["source_id"])
        for key in ("created_at", "updated_at", "last_synced_at"):
            value = source.get(key)
            if isinstance(value, datetime):
                source[key] = value.isoformat()
        return source

    @staticmethod
    def _normalize_file_path(path: str) -> str:
        if not isinstance(path, str):
            raise TypeError("path must be a string")
        normalized = normalize_relative_path(path, path)
        extension = document_extension(normalized)
        if extension not in ACCEPTED_EXTENSIONS:
            raise ValueError(
                f"Unsupported file type '{extension}'. "
                "Managed workspace files must be text or supported documents."
            )
        return normalized

    @staticmethod
    def _normalize_prefix(path_prefix: Optional[str]) -> Optional[str]:
        if path_prefix is None or path_prefix.strip() in {"", "."}:
            return None
        return normalize_relative_path(path_prefix, path_prefix).rstrip("/")

    @staticmethod
    def _normalize_limit(limit: int) -> int:
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise ValueError("limit must be an integer")
        if not 1 <= limit <= MAX_LIST_FILES:
            raise ValueError(f"limit must be between 1 and {MAX_LIST_FILES}")
        return limit

    @staticmethod
    def _normalize_content(content: Union[str, bytes]) -> bytes:
        if isinstance(content, str):
            content = content.encode("utf-8")
        elif not isinstance(content, bytes):
            raise TypeError("content must be bytes or a string")
        if not content:
            raise ValueError("workspace file content must not be empty")
        if len(content) > MAX_DOCUMENT_SIZE:
            raise ValueError("document exceeds the 50 MB size limit")
        # Workspace reads are text-oriented.  Validate up front so a write
        # cannot create a file that the bounded reader cannot decode.
        content.decode("utf-8")
        return content

    async def get_source(self) -> Optional[Dict]:
        """Return the managed source metadata, without creating it."""
        source = await self._reader.fetch_managed_workspace_source()
        if source is None:
            return None
        return self._public_source(source)

    async def ensure_source(self, display_name: str = "Project Workspace") -> Dict:
        """Return or create the single project-owned workspace source."""
        source = await self.get_source()
        if source is not None:
            return source
        try:
            return await self._documents.create_workspace_source(
                display_name=display_name,
                visibility_scope="project",
                session_id=None,
                ownership_mode=MANAGED_WORKSPACE_MODE,
            )
        except Exception:
            # A concurrent creator may have won the unique project-source
            # race.  Re-read before surfacing a genuine storage failure.
            source = await self.get_source()
            if source is not None:
                return source
            raise

    async def list_files(
        self,
        *,
        path_prefix: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict]:
        """List file metadata under a virtual directory prefix."""
        normalized_prefix = self._normalize_prefix(path_prefix)
        rows = await self._reader.list_managed_workspace_documents(
            path_prefix=normalized_prefix,
            limit=self._normalize_limit(limit),
        )
        return [self._public_metadata(row) for row in rows]

    async def read_file(
        self,
        path: str,
        *,
        start_line: int = 1,
        max_lines: int = MAX_READ_LINES,
        end_line: Optional[int] = None,
        start_character: int = 0,
        max_characters: int = MAX_READ_CHARACTERS,
    ) -> Dict:
        """Read a bounded UTF-8 slice from one managed workspace file."""
        normalized_path = self._normalize_file_path(path)
        if isinstance(start_line, bool) or not isinstance(start_line, int) or start_line < 1:
            raise ValueError("start_line must be a positive integer")
        if isinstance(max_lines, bool) or not isinstance(max_lines, int) or not 1 <= max_lines <= MAX_READ_LINES:
            raise ValueError(f"max_lines must be between 1 and {MAX_READ_LINES}")
        if end_line is not None:
            if isinstance(end_line, bool) or not isinstance(end_line, int) or end_line < start_line:
                raise ValueError("end_line must be greater than or equal to start_line")
            requested_lines = end_line - start_line + 1
            if requested_lines > MAX_READ_LINES:
                raise ValueError(f"file range cannot exceed {MAX_READ_LINES} lines")
            max_lines = min(max_lines, requested_lines)
        if isinstance(start_character, bool) or not isinstance(start_character, int) or start_character < 0:
            raise ValueError("start_character must be a non-negative integer")
        if isinstance(max_characters, bool) or not isinstance(max_characters, int) or not 1 <= max_characters <= MAX_READ_CHARACTERS:
            raise ValueError(
                f"max_characters must be between 1 and {MAX_READ_CHARACTERS}"
            )

        row = await self._reader.fetch_managed_workspace_file(
            relative_path=normalized_path
        )
        if row is None:
            raise FileNotFoundError("Managed workspace file not found")
        raw_content = row.get("content", b"")
        if isinstance(raw_content, str):
            raw_content = raw_content.encode("utf-8")
        text = bytes(raw_content).decode("utf-8")
        lines = text.splitlines(keepends=True)
        if not lines:
            lines = [text]
        if start_line > len(lines):
            raise ValueError(f"start_line {start_line} exceeds document length {len(lines)}")
        selected = lines[start_line - 1 : start_line - 1 + max_lines]
        selected_end_line = start_line + len(selected) - 1
        content = "".join(selected)
        if start_character:
            content = content[start_character:]
        character_truncated = len(content) > max_characters
        if character_truncated:
            content = content[:max_characters]
        result = self._public_metadata(row)
        result.pop("content", None)
        result.update(
            {
                "content": content,
                "start_line": start_line,
                "end_line": selected_end_line,
                "total_lines": len(lines),
                "truncated": character_truncated
                or selected_end_line < len(lines),
            }
        )
        return result

    async def read_project_context(
        self,
        *,
        max_characters: int = PROJECT_CONTEXT_MAX_CHARACTERS,
    ) -> Optional[str]:
        """Read canonical ``PROJECT.md`` without waiting for indexing."""
        if (
            isinstance(max_characters, bool)
            or not isinstance(max_characters, int)
            or not 1 <= max_characters <= PROJECT_CONTEXT_MAX_CHARACTERS
        ):
            raise ValueError(
                "max_characters must be between 1 and "
                f"{PROJECT_CONTEXT_MAX_CHARACTERS}"
            )
        try:
            result = await self.read_file(
                PROJECT_FILE_PATH,
                max_lines=MAX_READ_LINES,
                max_characters=max_characters,
            )
        except FileNotFoundError:
            return None
        return result["content"]

    async def create_file(self, path: str, content: Union[str, bytes]) -> Dict:
        """Create one managed file and queue its indexing exactly once."""
        normalized_path = self._normalize_file_path(path)
        payload = self._normalize_content(content)
        source = await self.ensure_source()
        now = get_now_iso()
        result = await self._writer.insert_managed_workspace_file(
            source_id=source["source_id"],
            relative_path=normalized_path,
            original_name=PurePosixPath(normalized_path).name,
            extension=document_extension(normalized_path),
            content=payload,
            content_hash=hashlib.sha256(payload).hexdigest(),
            updated_at=now,
        )
        self._documents.queue_workspace_source_indexing(
            source_id=source["source_id"],
        )
        return self._public_metadata(result)

    async def update_file(
        self,
        path: str,
        content: Union[str, bytes],
        *,
        expected_content_hash: str,
    ) -> Dict:
        """Replace a managed file only if its caller's hash is current."""
        normalized_path = self._normalize_file_path(path)
        payload = self._normalize_content(content)
        if not isinstance(expected_content_hash, str) or not expected_content_hash:
            raise ValueError("expected_content_hash must not be empty")
        result = await self._writer.update_managed_workspace_file(
            relative_path=normalized_path,
            content=payload,
            content_hash=hashlib.sha256(payload).hexdigest(),
            expected_content_hash=expected_content_hash,
            updated_at=get_now_iso(),
        )
        source = await self.get_source()
        if source is not None:
            self._documents.queue_workspace_source_indexing(
                source_id=source["source_id"],
            )
        return self._public_metadata(result)

    async def append_file(
        self,
        path: str,
        content: Union[str, bytes],
        *,
        expected_content_hash: str,
    ) -> Dict:
        """Append to a managed file under the same optimistic-concurrency rule."""
        normalized_path = self._normalize_file_path(path)
        payload = self._normalize_content(content)
        if not isinstance(expected_content_hash, str) or not expected_content_hash:
            raise ValueError("expected_content_hash must not be empty")
        result = await self._writer.append_managed_workspace_file(
            relative_path=normalized_path,
            append_content=payload,
            expected_content_hash=expected_content_hash,
            updated_at=get_now_iso(),
        )
        source = await self.get_source()
        if source is not None:
            self._documents.queue_workspace_source_indexing(
                source_id=source["source_id"],
            )
        return self._public_metadata(result)
