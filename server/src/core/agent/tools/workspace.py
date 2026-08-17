"""Bounded agent tools for the current project's managed workspace."""

from __future__ import annotations

from typing import Dict, List, Optional

from core.knowledge.documents.scanning import normalize_relative_path
from core.project.workspace_service import PROJECT_FILE_PATH

WORKSPACE_TOOL_MAX_PATH_LENGTH = 512
WORKSPACE_TOOL_MAX_CONTENT_CHARACTERS = 20_000
WORKSPACE_TOOL_MAX_LIST_LIMIT = 100
WORKSPACE_TOOL_MAX_READ_CHARACTERS = 12_000
WORKSPACE_TOOL_MAX_READ_LINES = 200


def _normalize_tool_path(path: str) -> str:
    """Normalize a workspace path before applying the protected-file guard."""

    if not isinstance(path, str) or len(path) > WORKSPACE_TOOL_MAX_PATH_LENGTH:
        raise ValueError(
            "path must be a string no longer than "
            f"{WORKSPACE_TOOL_MAX_PATH_LENGTH} characters"
        )
    return normalize_relative_path(path, path)


def _editable_path(path: str) -> str:
    normalized = _normalize_tool_path(path)
    if normalized.casefold() == PROJECT_FILE_PATH.casefold():
        raise PermissionError(
            "PROJECT.md is user-owned and cannot be changed through ordinary "
            "workspace tools"
        )
    return normalized


class WorkspaceTools:
    """Agent-facing operations over the project-owned virtual workspace.

    The service injected on ``Tools.workspace_service`` is already scoped to
    the current project.  No filesystem path or project selector is accepted
    here, which keeps the agent boundary project-local by construction.
    """

    workspace_service = None

    def _workspace_unavailable(self, *, list_result: bool = False):
        message = "No managed project workspace service available"
        return [{"error": message}] if list_result else {"error": message}

    async def list_workspace_files(
        self,
        path_prefix: Optional[str] = None,
        limit: int = WORKSPACE_TOOL_MAX_LIST_LIMIT,
    ) -> List[Dict]:
        """List bounded metadata for files in the current managed workspace."""

        if self.workspace_service is None:
            return self._workspace_unavailable(list_result=True)
        if path_prefix is not None:
            if not isinstance(path_prefix, str):
                raise ValueError("path_prefix must be a string")
            if len(path_prefix) > WORKSPACE_TOOL_MAX_PATH_LENGTH:
                raise ValueError(
                    "path_prefix must be no longer than "
                    f"{WORKSPACE_TOOL_MAX_PATH_LENGTH} characters"
                )
            if path_prefix.strip() in {"", "."}:
                path_prefix = None
            else:
                path_prefix = normalize_relative_path(path_prefix, path_prefix)
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= WORKSPACE_TOOL_MAX_LIST_LIMIT
        ):
            raise ValueError(
                "limit must be between 1 and "
                f"{WORKSPACE_TOOL_MAX_LIST_LIMIT}"
            )
        return await self.workspace_service.list_files(
            path_prefix=path_prefix,
            limit=limit,
        )

    async def read_workspace_file(
        self,
        path: str,
        start_line: int = 1,
        end_line: Optional[int] = None,
        max_characters: int = WORKSPACE_TOOL_MAX_READ_CHARACTERS,
    ) -> Dict:
        """Read a bounded text slice from one managed workspace file."""

        if self.workspace_service is None:
            return self._workspace_unavailable()
        normalized_path = _normalize_tool_path(path)
        if (
            isinstance(start_line, bool)
            or not isinstance(start_line, int)
            or start_line < 1
        ):
            raise ValueError("start_line must be a positive integer")
        if end_line is not None and (
            isinstance(end_line, bool)
            or not isinstance(end_line, int)
            or end_line < start_line
            or end_line - start_line + 1 > WORKSPACE_TOOL_MAX_READ_LINES
        ):
            raise ValueError(
                "end_line must be at or after start_line and within the "
                f"{WORKSPACE_TOOL_MAX_READ_LINES}-line bound"
            )
        if (
            isinstance(max_characters, bool)
            or not isinstance(max_characters, int)
            or not 1 <= max_characters <= WORKSPACE_TOOL_MAX_READ_CHARACTERS
        ):
            raise ValueError(
                "max_characters must be between 1 and "
                f"{WORKSPACE_TOOL_MAX_READ_CHARACTERS}"
            )
        return await self.workspace_service.read_file(
            normalized_path,
            start_line=start_line,
            end_line=end_line,
            max_lines=WORKSPACE_TOOL_MAX_READ_LINES,
            max_characters=max_characters,
        )

    async def create_workspace_file(self, path: str, content: str) -> Dict:
        """Create one bounded workspace artifact, excluding ``PROJECT.md``."""

        normalized_path = _editable_path(path)
        _validate_content(content)
        if self.workspace_service is None:
            return self._workspace_unavailable()
        return await self.workspace_service.create_file(normalized_path, content)

    async def update_workspace_file(
        self,
        path: str,
        content: str,
        expected_content_hash: str,
    ) -> Dict:
        """Replace an artifact only when its expected hash is still current."""

        normalized_path = _editable_path(path)
        _validate_content(content)
        _validate_content_hash(expected_content_hash)
        if self.workspace_service is None:
            return self._workspace_unavailable()
        return await self.workspace_service.update_file(
            normalized_path,
            content,
            expected_content_hash=expected_content_hash,
        )

    async def append_workspace_file(
        self,
        path: str,
        content: str,
        expected_content_hash: str,
    ) -> Dict:
        """Append an artifact only when its expected hash is still current."""

        normalized_path = _editable_path(path)
        _validate_content(content)
        _validate_content_hash(expected_content_hash)
        if self.workspace_service is None:
            return self._workspace_unavailable()
        return await self.workspace_service.append_file(
            normalized_path,
            content,
            expected_content_hash=expected_content_hash,
        )


def _validate_content(content: str) -> None:
    if (
        not isinstance(content, str)
        or not content
        or len(content) > WORKSPACE_TOOL_MAX_CONTENT_CHARACTERS
    ):
        raise ValueError(
            "content must be non-empty and no longer than "
            f"{WORKSPACE_TOOL_MAX_CONTENT_CHARACTERS} characters"
        )


def _validate_content_hash(content_hash: str) -> None:
    if (
        not isinstance(content_hash, str)
        or len(content_hash) != 64
        or any(character not in "0123456789abcdefABCDEF" for character in content_hash)
    ):
        raise ValueError("expected_content_hash must be a SHA-256 hexadecimal hash")
