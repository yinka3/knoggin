import asyncio
import hashlib
import os
import uuid
from datetime import datetime
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Dict, List, Optional

from common.utils.time_utils import get_now_iso
from infrastructure.postgres_client import PostgresClient

MAX_FILE_SIZE = 50 * 1024 * 1024
VALID_VISIBILITY_SCOPES = {"project", "session"}


class FileRAGService:
    """Project-scoped storage and retrieval boundary for uploaded files."""

    def __init__(
        self,
        project_id: str,
        postgres_client: PostgresClient,
        storage_root: Path,
    ):
        self.project_id = project_id
        self._postgres = postgres_client
        self._storage_root = Path(storage_root).resolve()

    @staticmethod
    def _normalize_relative_path(
        relative_path: Optional[str],
        original_name: str,
    ) -> str:
        raw_path = relative_path if relative_path is not None else original_name
        if not raw_path or not raw_path.strip():
            raise ValueError("relative_path must not be empty")
        if "\x00" in raw_path:
            raise ValueError("relative_path contains an invalid null byte")

        slash_path = raw_path.strip().replace("\\", "/")
        windows_path = PureWindowsPath(raw_path)
        posix_path = PurePosixPath(slash_path)
        if windows_path.is_absolute() or windows_path.drive or posix_path.is_absolute():
            raise ValueError("relative_path must be relative")
        if any(part == ".." for part in posix_path.parts):
            raise ValueError("relative_path must not escape the project root")

        normalized = posix_path.as_posix()
        if normalized in {"", "."}:
            raise ValueError("relative_path must identify a file")
        return normalized

    def _resolve_storage_path(self, storage_key: str) -> Path:
        target = (self._storage_root / Path(storage_key)).resolve()
        if not target.is_relative_to(self._storage_root):
            raise ValueError("generated storage path escaped the storage root")
        return target

    @staticmethod
    def _write_file_atomically(target: Path, content: bytes) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
        try:
            with temporary.open("xb") as file_handle:
                file_handle.write(content)
                file_handle.flush()
                os.fsync(file_handle.fileno())
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)

    @staticmethod
    def _remove_stored_file(target: Path) -> None:
        target.unlink(missing_ok=True)
        try:
            target.parent.rmdir()
        except OSError:
            pass

    @staticmethod
    def _public_metadata(row: Dict) -> Dict:
        metadata = dict(row)
        metadata.pop("storage_key", None)
        if metadata.get("file_id") is not None:
            metadata["file_id"] = str(metadata["file_id"])
        for key in ("created_at", "updated_at"):
            value = metadata.get(key)
            if isinstance(value, datetime):
                metadata[key] = value.isoformat()
        metadata.setdefault("chunk_count", 0)
        return metadata

    async def add_file(
        self,
        *,
        content: bytes,
        original_name: str,
        relative_path: Optional[str] = None,
        session_id: Optional[str] = None,
        visibility_scope: str = "project",
    ) -> Dict:
        """Store original bytes and persist project-scoped file metadata."""
        if not isinstance(content, bytes):
            raise TypeError("content must be bytes")
        if not content:
            raise ValueError("file content must not be empty")
        if len(content) > MAX_FILE_SIZE:
            raise ValueError("file exceeds the 50 MB size limit")
        if not original_name or not original_name.strip():
            raise ValueError("original_name must not be empty")
        if "\x00" in original_name:
            raise ValueError("original_name contains an invalid null byte")
        if visibility_scope not in VALID_VISIBILITY_SCOPES:
            raise ValueError(
                "visibility_scope must be either 'project' or 'session'"
            )
        if visibility_scope == "session" and not session_id:
            raise ValueError("session-visible files require session_id")

        normalized_path = self._normalize_relative_path(relative_path, original_name)
        file_id = str(uuid.uuid4())
        storage_key = PurePosixPath(self.project_id, file_id, "content").as_posix()
        stored_path = self._resolve_storage_path(storage_key)
        content_hash = hashlib.sha256(content).hexdigest()
        extension = Path(original_name).suffix.lower()
        created_at = get_now_iso()

        await asyncio.to_thread(self._write_file_atomically, stored_path, content)
        try:
            inserted = await self._postgres.execute_write(
                """
                INSERT INTO public.project_files (
                    file_id,
                    project_id,
                    session_id,
                    visibility_scope,
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
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    'uploaded', %s, %s
                )
                """,
                (
                    file_id,
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
                raise RuntimeError("project file metadata insert did not create a row")
        except Exception:
            await asyncio.to_thread(self._remove_stored_file, stored_path)
            raise

        return {
            "file_id": file_id,
            "project_id": self.project_id,
            "session_id": session_id,
            "visibility_scope": visibility_scope,
            "original_name": original_name.strip(),
            "relative_path": normalized_path,
            "extension": extension,
            "size_bytes": len(content),
            "content_hash": content_hash,
            "status": "uploaded",
            "created_at": created_at,
            "updated_at": created_at,
            "chunk_count": 0,
        }

    async def list_files(
        self,
        *,
        session_id: Optional[str] = None,
        visibility_scope: Optional[str] = None,
    ) -> List[Dict]:
        """List files visible to the current project/session context."""
        if (
            visibility_scope is not None
            and visibility_scope not in VALID_VISIBILITY_SCOPES
        ):
            raise ValueError(
                "visibility_scope must be either 'project' or 'session'"
            )

        query = """
            SELECT
                pf.file_id,
                pf.project_id,
                pf.session_id,
                pf.visibility_scope,
                pf.original_name,
                pf.relative_path,
                pf.extension,
                pf.size_bytes,
                pf.content_hash,
                pf.status,
                pf.created_at,
                pf.updated_at,
                COUNT(fc.chunk_id)::INTEGER AS chunk_count
            FROM public.project_files AS pf
            LEFT JOIN public.file_chunks AS fc ON fc.file_id = pf.file_id
            WHERE pf.project_id = %s
              AND (
                  pf.visibility_scope = 'project'
                  OR (
                      pf.visibility_scope = 'session'
                      AND pf.session_id = %s
                  )
              )
        """
        params: list = [self.project_id, session_id]
        if visibility_scope is not None:
            query += " AND pf.visibility_scope = %s"
            params.append(visibility_scope)
        query += """
            GROUP BY pf.file_id
            ORDER BY pf.created_at DESC, pf.file_id DESC
        """

        rows = await self._postgres.execute_read(query, tuple(params))
        return [self._public_metadata(row) for row in rows]

    async def search(
        self,
        query: str,
        n_results: int = 5,
        file_filter: Optional[str] = None,
    ) -> List[Dict]:
        """Return matching chunks once project file indexing is implemented."""
        return []
