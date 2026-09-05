"""Bounded agent tools for the current project's native file tree."""

from __future__ import annotations

from typing import Dict, List, Optional

from core.knowledge.documents.scanning import normalize_relative_path
from core.project.project_files import (
    CONTEXT_FILE_PATH,
    PROJECT_FILE_PATH,
    is_controlled_context_file,
)

PROJECT_FILE_TOOL_MAX_PATH_LENGTH = 512
PROJECT_FILE_TOOL_MAX_CONTENT_CHARACTERS = 20_000
PROJECT_FILE_TOOL_MAX_LIST_LIMIT = 100
PROJECT_FILE_TOOL_MAX_READ_CHARACTERS = 12_000
PROJECT_FILE_TOOL_MAX_READ_LINES = 200


def _normalize_tool_path(path: str) -> str:
    if not isinstance(path, str) or len(path) > PROJECT_FILE_TOOL_MAX_PATH_LENGTH:
        raise ValueError(
            "path must be a string no longer than "
            f"{PROJECT_FILE_TOOL_MAX_PATH_LENGTH} characters"
        )
    return normalize_relative_path(path, path)


def _editable_path(path: str) -> str:
    normalized = _normalize_tool_path(path)
    if normalized.casefold() == PROJECT_FILE_PATH.casefold():
        raise PermissionError(
            "PROJECT.md is user-owned and cannot be changed through ordinary "
            "project-file tools"
        )
    if is_controlled_context_file(normalized):
        raise PermissionError(
            f"{CONTEXT_FILE_PATH} is managed through the controlled Context importer"
        )
    return normalized


class ProjectFileTools:
    """Agent-facing local-file operations scoped by the injected DocumentService."""

    document_service = None

    def _files_unavailable(self, *, list_result: bool = False):
        message = "No project document service with local-file access is available"
        return [{"error": message}] if list_result else {"error": message}

    async def list_files(
        self,
        path_prefix: Optional[str] = None,
        limit: int = PROJECT_FILE_TOOL_MAX_LIST_LIMIT,
    ) -> List[Dict]:
        if self.document_service is None:
            return self._files_unavailable(list_result=True)
        if path_prefix is not None:
            if not isinstance(path_prefix, str) or len(path_prefix) > PROJECT_FILE_TOOL_MAX_PATH_LENGTH:
                raise ValueError("path_prefix must be a bounded string")
            path_prefix = (
                None
                if path_prefix.strip() in {"", "."}
                else normalize_relative_path(path_prefix, path_prefix)
            )
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= PROJECT_FILE_TOOL_MAX_LIST_LIMIT
        ):
            raise ValueError(
                f"limit must be between 1 and {PROJECT_FILE_TOOL_MAX_LIST_LIMIT}"
            )
        return await self.document_service.list_project_files(
            path_prefix=path_prefix,
            limit=limit,
        )

    async def read_file(
        self,
        path: str,
        start_line: int = 1,
        end_line: Optional[int] = None,
        max_characters: int = PROJECT_FILE_TOOL_MAX_READ_CHARACTERS,
    ) -> Dict:
        if self.document_service is None:
            return self._files_unavailable()
        normalized_path = _normalize_tool_path(path)
        if is_controlled_context_file(normalized_path):
            raise PermissionError(
                f"{CONTEXT_FILE_PATH} is managed through the controlled Context importer"
            )
        if not isinstance(start_line, int) or isinstance(start_line, bool) or start_line < 1:
            raise ValueError("start_line must be a positive integer")
        if end_line is not None and (
            not isinstance(end_line, int)
            or isinstance(end_line, bool)
            or end_line < start_line
            or end_line - start_line + 1 > PROJECT_FILE_TOOL_MAX_READ_LINES
        ):
            raise ValueError(
                "end_line must be at or after start_line and within the "
                f"{PROJECT_FILE_TOOL_MAX_READ_LINES}-line bound"
            )
        if (
            not isinstance(max_characters, int)
            or isinstance(max_characters, bool)
            or not 1 <= max_characters <= PROJECT_FILE_TOOL_MAX_READ_CHARACTERS
        ):
            raise ValueError(
                "max_characters must be between 1 and "
                f"{PROJECT_FILE_TOOL_MAX_READ_CHARACTERS}"
            )
        return await self.document_service.read_project_file(
            normalized_path,
            start_line=start_line,
            end_line=end_line,
            max_characters=max_characters,
        )

    async def create_file(self, path: str, content: str) -> Dict:
        if self.document_service is None:
            return self._files_unavailable()
        return await self.document_service.create_project_file(
            _editable_path(path),
            _validate_content(content),
        )

    async def update_file(
        self,
        path: str,
        content: str,
        expected_content_hash: str,
    ) -> Dict:
        if self.document_service is None:
            return self._files_unavailable()
        return await self.document_service.update_project_file(
            _editable_path(path),
            _validate_content(content),
            expected_content_hash=_validate_content_hash(expected_content_hash),
        )

    async def append_file(
        self,
        path: str,
        content: str,
        expected_content_hash: str,
    ) -> Dict:
        if self.document_service is None:
            return self._files_unavailable()
        return await self.document_service.append_project_file(
            _editable_path(path),
            _validate_content(content),
            expected_content_hash=_validate_content_hash(expected_content_hash),
        )

    async def move_file(
        self,
        source_path: str,
        destination_path: str,
        expected_content_hash: str,
    ) -> Dict:
        if self.document_service is None:
            return self._files_unavailable()
        return await self.document_service.move_project_file(
            _editable_path(source_path),
            _editable_path(destination_path),
            expected_content_hash=_validate_content_hash(expected_content_hash),
        )

    async def delete_file(self, path: str, expected_content_hash: str) -> Dict:
        if self.document_service is None:
            return self._files_unavailable()
        return await self.document_service.delete_project_file(
            _editable_path(path),
            expected_content_hash=_validate_content_hash(expected_content_hash),
        )

    async def create_folder(self, path: str) -> Dict:
        if self.document_service is None:
            return self._files_unavailable()
        return await self.document_service.create_project_folder(_editable_path(path))


def _validate_content(content: str) -> str:
    if (
        not isinstance(content, str)
        or not content
        or len(content) > PROJECT_FILE_TOOL_MAX_CONTENT_CHARACTERS
    ):
        raise ValueError(
            "content must be non-empty and no longer than "
            f"{PROJECT_FILE_TOOL_MAX_CONTENT_CHARACTERS} characters"
        )
    return content


def _validate_content_hash(content_hash: str) -> str:
    if (
        not isinstance(content_hash, str)
        or len(content_hash) != 64
        or any(character not in "0123456789abcdefABCDEF" for character in content_hash)
    ):
        raise ValueError("expected_content_hash must be a SHA-256 hexadecimal hash")
    return content_hash
