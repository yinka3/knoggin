"""Temporary, bounded folder uploads owned by the API process."""

from __future__ import annotations

import asyncio
import hashlib
import shutil
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from uuid import uuid4

import anyio

from common.document_limits import MAX_DOCUMENT_SIZE
from core.knowledge.documents.scanning import FolderScanPathEntry, normalize_relative_path

MAX_STAGED_FOLDER_BYTES = 2 * 1024 * 1024 * 1024
MAX_ACTIVE_INTAKES = 2
INTAKE_IDLE_SECONDS = 2 * 60 * 60


class IntakeConflict(Exception):
    pass


class IntakeTooLarge(Exception):
    pass


@dataclass
class _File:
    file_id: str
    relative_path: str
    size_bytes: int
    disk_path: Path
    status: str = "pending_upload"
    content_hash: str | None = None
    result: dict | None = None
    error: dict | None = None

    def public(self) -> dict:
        return {
            "file_id": self.file_id,
            "relative_path": self.relative_path,
            "size_bytes": self.size_bytes,
            "status": self.status,
            "content_hash": self.content_hash,
            "result": self.result,
            "error": self.error,
        }


@dataclass
class _Intake:
    intake_id: str
    project_id: str
    folder_name: str
    force_include_paths: list[str]
    files: dict[str, _File]
    directory: Path
    status: str = "uploading"
    preview: object | None = None
    scan_settings: object | None = None
    last_activity: float = field(default_factory=time.monotonic)
    session_id: str | None = None
    announced_document_ids: set[str] = field(default_factory=set)

    def public(self) -> dict:
        return {
            "intake_id": self.intake_id,
            "folder_name": self.folder_name,
            "status": self.status,
            "files": [item.public() for item in self.files.values()],
        }


class FolderStaging:
    """One-process staging; retries are per file and restarts abandon the intake."""

    def __init__(self):
        self._temporary = tempfile.TemporaryDirectory(prefix="knoggin-folder-intake-")
        self._root = Path(self._temporary.name)
        self._intakes: dict[str, _Intake] = {}
        self._lock = asyncio.Lock()

    async def close(self) -> None:
        async with self._lock:
            self._intakes.clear()
            await asyncio.to_thread(self._temporary.cleanup)

    async def _expire(self) -> None:
        now = time.monotonic()
        for intake_id, intake in tuple(self._intakes.items()):
            if now - intake.last_activity > INTAKE_IDLE_SECONDS:
                self._intakes.pop(intake_id)
                await asyncio.to_thread(shutil.rmtree, intake.directory, ignore_errors=True)

    def _get(self, project_id: str, intake_id: str) -> _Intake:
        intake = self._intakes.get(intake_id)
        if intake is None or intake.project_id != project_id:
            raise KeyError(intake_id)
        intake.last_activity = time.monotonic()
        return intake

    async def create(self, project_id: str, payload) -> dict:
        async with self._lock:
            await self._expire()
            if not payload.folder_name.strip():
                raise ValueError("Folder name must not be blank")
            active = sum(
                any(item.status not in {"accepted", "excluded"} for item in intake.files.values())
                for intake in self._intakes.values()
            )
            if active >= MAX_ACTIVE_INTAKES:
                raise IntakeConflict("Too many active folder intakes")
            paths = []
            seen_paths = set()
            total = 0
            for item in payload.files:
                path = normalize_relative_path(item.relative_path, item.relative_path)
                if path in seen_paths:
                    raise ValueError("Duplicate normalized folder path")
                if item.size_bytes > MAX_DOCUMENT_SIZE:
                    raise IntakeTooLarge("An individual file exceeds the upload limit")
                total += item.size_bytes
                if total > MAX_STAGED_FOLDER_BYTES:
                    raise IntakeTooLarge("Folder exceeds the staged upload limit")
                paths.append(path)
                seen_paths.add(path)
            force_include_paths = [
                normalize_relative_path(path, path)
                for path in payload.force_include_paths
            ]
            if any(path not in seen_paths for path in force_include_paths):
                raise ValueError("Force-included path is not in the folder manifest")
            intake_id = str(uuid4())
            directory = self._root / intake_id
            directory.mkdir(mode=0o700)
            files = {}
            for index, (path, item) in enumerate(zip(paths, payload.files, strict=True)):
                file_id = str(index)
                files[file_id] = _File(
                    file_id=file_id, relative_path=path,
                    size_bytes=item.size_bytes, disk_path=directory / file_id,
                )
            intake = _Intake(
                intake_id=intake_id, project_id=project_id,
                folder_name=payload.folder_name.strip(),
                force_include_paths=force_include_paths,
                files=files, directory=directory, session_id=payload.session_id,
            )
            self._intakes[intake_id] = intake
            return intake.public()

    async def get(self, project_id: str, intake_id: str) -> dict:
        async with self._lock:
            await self._expire()
            return self._get(project_id, intake_id).public()

    async def delete(self, project_id: str, intake_id: str) -> None:
        async with self._lock:
            await self._expire()
            intake = self._get(project_id, intake_id)
            self._intakes.pop(intake_id)
            await asyncio.to_thread(shutil.rmtree, intake.directory, ignore_errors=True)

    async def upload(self, project_id: str, intake_id: str, file_id: str, stream) -> dict:
        async with self._lock:
            await self._expire()
            intake = self._get(project_id, intake_id)
            if intake.status != "uploading":
                raise IntakeConflict("Folder has already been previewed")
            item = intake.files.get(file_id)
            if item is None:
                raise KeyError(file_id)
            digest = hashlib.sha256()
            size = 0
            temporary_path = item.disk_path.with_suffix(".part")
            try:
                async with await anyio.open_file(temporary_path, "wb") as output:
                    async for chunk in stream:
                        size += len(chunk)
                        if size > item.size_bytes:
                            raise IntakeTooLarge("Uploaded file exceeds its declared size")
                        digest.update(chunk)
                        await output.write(chunk)
                if size != item.size_bytes:
                    raise ValueError("Uploaded file size does not match the manifest")
                content_hash = digest.hexdigest()
                if item.status == "uploaded":
                    if item.content_hash != content_hash:
                        raise IntakeConflict("Uploaded file differs from the first upload")
                else:
                    temporary_path.replace(item.disk_path)
                    item.content_hash = content_hash
                    item.status = "uploaded"
                return item.public()
            finally:
                temporary_path.unlink(missing_ok=True)

    async def preview(self, project_id: str, intake_id: str, service):
        async with self._lock:
            await self._expire()
            intake = self._get(project_id, intake_id)
            if intake.preview is not None:
                return intake.preview
            if intake.status != "uploading":
                raise IntakeConflict("Folder has already been previewed")
            if any(item.status != "uploaded" for item in intake.files.values()):
                raise IntakeConflict("Upload every file before previewing")
            settings = await service.get_scan_settings()
            by_path = {item.relative_path: item for item in intake.files.values()}

            def load(path: str, size: int) -> bytes:
                item = by_path[path]
                with item.disk_path.open("rb") as handle:
                    content = handle.read(MAX_DOCUMENT_SIZE + 1)
                if len(content) != size or len(content) > MAX_DOCUMENT_SIZE:
                    raise ValueError("Staged file changed during preview")
                return content

            preview = await service.preview_folder(
                folder_name=intake.folder_name,
                entries=[FolderScanPathEntry(item.relative_path, item.size_bytes)
                         for item in intake.files.values()],
                settings=settings,
                force_include_paths=intake.force_include_paths,
                content_loader=load,
            )
            included = {entry.relative_path for entry in preview.included}
            for item in intake.files.values():
                item.status = "ready" if item.relative_path in included else "excluded"
            intake.preview = preview
            intake.scan_settings = settings
            intake.status = "previewed"
            return preview

    async def take_finished_batch(
        self, project_id: str, intake_id: str
    ) -> tuple[str, str, list[str]] | None:
        """Once the intake is complete, its accepted documents not yet handed
        out, with the chat to tell. Commit returns "complete" again on later
        calls, and a retry can accept more files after that, so each document
        is handed out once."""

        async with self._lock:
            intake = self._intakes.get(intake_id)
            if (
                intake is None or intake.project_id != project_id
                or intake.status != "complete" or intake.session_id is None
            ):
                return None
            document_ids = [
                str(item.result["document_id"]) for item in intake.files.values()
                if item.status == "accepted" and item.result
                and str(item.result["document_id"]) not in intake.announced_document_ids
            ]
            if not document_ids:
                return None
            intake.announced_document_ids.update(document_ids)
            return intake.session_id, intake.folder_name, document_ids

    async def commit(self, project_id: str, intake_id: str, service, *, retry_failed: bool):
        async with self._lock:
            await self._expire()
            intake = self._get(project_id, intake_id)
            if intake.status not in {"previewed", "committing", "complete"}:
                raise IntakeConflict("Preview the complete folder before committing")
            if await service.get_scan_settings() != intake.scan_settings:
                raise IntakeConflict("Folder scan settings changed; start a new intake")
            item = next((item for item in intake.files.values() if item.status == "ready"), None)
            if item is None and retry_failed:
                item = next((item for item in intake.files.values() if item.status == "failed"), None)
            if item is None:
                if all(value.status in {"accepted", "excluded"} for value in intake.files.values()):
                    intake.status = "complete"
                    await asyncio.to_thread(shutil.rmtree, intake.directory, ignore_errors=True)
                return intake.public()
            intake.status = "committing"
            try:
                content = await asyncio.to_thread(item.disk_path.read_bytes)
                if len(content) != item.size_bytes or hashlib.sha256(content).hexdigest() != item.content_hash:
                    raise ValueError("Staged file changed before commit")
                row = await service.submit_document(
                    content=content, original_name=PurePosixPath(item.relative_path).name,
                    relative_path=item.relative_path,
                )
                item.result = row
                item.error = None
                item.status = "accepted"
            except FileExistsError:
                item.status = "failed"
                item.error = {
                    "code": "document_exists",
                    "message": "A document already exists at this path",
                }
            except ValueError:
                item.status = "failed"
                item.error = {
                    "code": "invalid_document",
                    "message": "Could not add this document",
                }
            except Exception:
                item.status = "failed"
                item.error = {
                    "code": "document_intake_failed",
                    "message": "Could not add this document",
                }
            if item.status == "accepted":
                try:
                    await asyncio.to_thread(item.disk_path.unlink, missing_ok=True)
                except OSError:
                    # The durable document is already admitted; keep its success state.
                    pass
            if all(value.status in {"accepted", "excluded", "failed"} for value in intake.files.values()):
                intake.status = "complete"
                if all(value.status in {"accepted", "excluded"} for value in intake.files.values()):
                    await asyncio.to_thread(shutil.rmtree, intake.directory, ignore_errors=True)
            return intake.public()
