"""Own the sandbox availability check and, later, project containers."""

import asyncio
import hashlib
import json
import os
import re
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING, Callable
from uuid import uuid4

from loguru import logger

from common.schema.settings import ContainerRuntimeSettings, WorkspaceCommandSettings
from infrastructure.container_runtime import (
    ContainerRuntime,
    ContainerRuntimeError,
    discover,
)

if TYPE_CHECKING:
    from core.project.project_manager import ProjectManager

ROOT_RUNTIME_WARNING = (
    "The container runtime runs as root; anything that can use its socket has root on this machine."
)


class SandboxManager:
    def __init__(
        self,
        *,
        user_name: str,
        runtime_settings: ContainerRuntimeSettings,
        command_settings: WorkspaceCommandSettings,
        project_manager: "ProjectManager",
        library_root: Path,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.user_name = user_name
        self.runtime_settings = runtime_settings
        self.command_settings = command_settings
        self.project_manager = project_manager
        self.library_root = library_root.resolve()
        self._clock = clock
        self._runtime: ContainerRuntime | None = None
        self._container_lock = asyncio.Lock()
        self._last_used: dict[str, float] = {}
        self._idle_task: asyncio.Task | None = None
        self._status = self._result(False, "not_checked", "Sandbox has not been checked")

    def status(self) -> dict:
        return dict(self._status, warnings=list(self._status["warnings"]))

    def _result(self, available: bool, stage: str, message: str) -> dict:
        runtime = self._runtime
        return {
            "available": available,
            "stage": stage,
            "message": message,
            "runtime": ("podman" if runtime.is_podman else "docker") if runtime else None,
            "rootless": runtime.rootless if runtime else None,
            "warnings": [ROOT_RUNTIME_WARNING] if runtime and not runtime.rootless else [],
        }

    async def start(self) -> None:
        await self.check()
        if self._runtime is not None:
            try:
                async with self._container_lock:
                    for container in await self._runtime.list_containers(
                        {"knoggin.owner": self.user_name}
                    ):
                        if (
                            container.get("Labels", {}).get("knoggin.owner") == self.user_name
                            and container.get("State") == "running"
                        ):
                            await self._runtime.stop(container["Id"])
            except Exception as exc:
                if self._status["available"]:
                    self._status = self._result(False, "container_failed", str(exc))
        if self._status["available"] and self._idle_task is None:
            self._idle_task = asyncio.create_task(self._idle_loop())

    async def check(self) -> dict:
        try:
            return await self._check()
        except Exception as exc:
            self._status = self._result(False, "container_failed", str(exc))
            return self.status()

    async def _check(self) -> dict:
        if not self.command_settings.enabled:
            self._status = self._result(False, "disabled", "Workspace commands are disabled")
            return self.status()

        if self._runtime is None:
            socket = self.runtime_settings.socket
            try:
                self._runtime = await discover(socket or None)
            except Exception as exc:
                self._status = self._result(False, "runtime_unreachable", str(exc))
                return self.status()
            if self._runtime is None:
                candidates = [Path(socket)] if socket else [Path("/var/run/docker.sock")]
                runtime_dir = os.environ.get("XDG_RUNTIME_DIR")
                if not socket and runtime_dir:
                    candidates.extend(
                        [
                            Path(runtime_dir) / "podman" / "podman.sock",
                            Path(runtime_dir) / "docker.sock",
                        ]
                    )
                stage = "runtime_unreachable" if any(path.exists() for path in candidates) else "no_runtime"
                self._status = self._result(False, stage, "No reachable container runtime")
                return self.status()

        runtime = self._runtime
        image = self.command_settings.default_image
        try:
            if not await runtime.image_exists(image):
                await asyncio.wait_for(
                    runtime.pull(image), timeout=self.command_settings.check_timeout_seconds
                )
        except asyncio.TimeoutError:
            self._status = self._result(False, "image_unavailable", "Image pull timed out")
            return self.status()
        except ContainerRuntimeError as exc:
            self._status = self._result(False, "image_unavailable", str(exc))
            return self.status()

        with tempfile.TemporaryDirectory(prefix="knoggin-sandbox-check-") as temp_dir:
            marker = uuid4().hex
            name = f"knoggin-check-{marker}"
            config = {
                "Image": image,
                "Cmd": ["sh", "-c", f"printf {marker} > /check/probe"],
                "HostConfig": {
                    "NetworkMode": "none",
                    "Binds": [f"{temp_dir}:/check:rw"],
                    "CapDrop": ["ALL"],
                    "SecurityOpt": ["no-new-privileges"],
                    "Privileged": False,
                },
                "Labels": {
                    "knoggin.owner": self.user_name,
                    "knoggin.project": name,
                },
            }
            if runtime.is_podman:
                config["HostConfig"]["UsernsMode"] = "keep-id"
            else:
                config["User"] = f"{os.getuid()}:{os.getgid()}"
            container_id = None
            try:
                container_id = await runtime.create_container(name, config)
                await runtime.start(container_id)
                result = await asyncio.wait_for(
                    runtime.wait(container_id),
                    timeout=self.command_settings.check_timeout_seconds,
                )
                if result.get("StatusCode") != 0 or (Path(temp_dir) / "probe").read_text() != marker:
                    self._status = self._result(False, "mount_failed", "Sandbox mount write failed")
                else:
                    self._status = self._result(True, "ok", "Sandbox check passed")
            except asyncio.TimeoutError:
                self._status = self._result(False, "container_failed", "Sandbox check timed out")
            except OSError:
                self._status = self._result(False, "mount_failed", "Sandbox did not write to the mount")
            except ContainerRuntimeError as exc:
                self._status = self._result(False, "container_failed", str(exc))
            finally:
                if container_id is not None:
                    try:
                        await runtime.remove(container_id, force=True)
                    except ContainerRuntimeError as exc:
                        self._status = self._result(False, "container_failed", f"Check cleanup failed: {exc}")
        return self.status()

    async def shutdown(self) -> None:
        if self._idle_task is not None:
            self._idle_task.cancel()
            try:
                await self._idle_task
            except asyncio.CancelledError:
                pass
            self._idle_task = None
        if self._runtime is not None:
            await self._runtime.close()
            self._runtime = None

    async def _idle_loop(self) -> None:
        while True:
            await asyncio.sleep(60)
            try:
                await self._stop_idle()
            except Exception:
                # A transient runtime failure should not end later idle checks.
                logger.exception("Sandbox idle stop failed")

    async def _stop_idle(self) -> None:
        async with self._container_lock:
            idle_seconds = self.command_settings.idle_minutes * 60
            for project_id, last_used in list(self._last_used.items()):
                if self._clock() - last_used < idle_seconds:
                    continue
                name = self._container_name(project_id)
                existing = await self._owned_container(name, project_id)
                if existing and existing.get("State", {}).get("Running", False):
                    await self._runtime.stop(existing["Id"])
                self._last_used.pop(project_id, None)

    @staticmethod
    def _safe_id(project_id: str) -> str:
        if not project_id or project_id in {".", ".."} or any(
            char in project_id for char in "/\\\x00"
        ):
            raise ValueError("project_id must be one path component")
        return re.sub(r"[^A-Za-z0-9_.-]", "-", project_id)

    def _container_name(self, project_id: str) -> str:
        return f"knoggin-{self._safe_id(project_id)}"

    async def _owned_container(self, name: str, project_id: str) -> dict | None:
        try:
            existing = await self._runtime.inspect(name)
        except ContainerRuntimeError as exc:
            if exc.status_code == 404:
                return None
            raise
        labels = existing.get("Config", {}).get("Labels") or {}
        if labels.get("knoggin.owner") != self.user_name or labels.get("knoggin.project") != project_id:
            raise RuntimeError(f"Container name already belongs to another owner: {name}")
        return existing

    async def reset(self, project_id: str) -> None:
        if self._runtime is None:
            return
        async with self._container_lock:
            existing = await self._owned_container(self._container_name(project_id), project_id)
            if existing is not None:
                if existing.get("State", {}).get("Running", False):
                    await self._runtime.stop(existing["Id"])
                await self._runtime.remove(existing["Id"])
            self._last_used.pop(project_id, None)

    async def remove_project(self, project_id: str) -> None:
        if self._runtime is None:
            return
        async with self._container_lock:
            existing = await self._owned_container(self._container_name(project_id), project_id)
            if existing is not None:
                if existing.get("State", {}).get("Running", False):
                    await self._runtime.stop(existing["Id"])
                await self._runtime.remove(existing["Id"])
            volume_name = f"knoggin-home-{self._safe_id(project_id)}"
            volume = await self._runtime.inspect_volume(volume_name)
            if volume is not None:
                labels = volume.get("Labels") or {}
                if labels.get("knoggin.owner") != self.user_name or labels.get("knoggin.project") != project_id:
                    raise RuntimeError(f"Volume already belongs to another owner: {volume_name}")
                await self._runtime.remove_volume(volume_name)
            self._last_used.pop(project_id, None)

    async def ensure_running(self, project_id: str) -> str:
        if not self._status["available"] or self._runtime is None:
            raise RuntimeError(f"Sandbox unavailable: {self._status['message']}")
        safe_id = self._safe_id(project_id)

        async with self._container_lock:
            project = await self.project_manager.get_project(project_id)
            if project is None or project.get("status") == "deleted":
                raise ValueError(f"Project not found: {project_id}")
            runtime = self._runtime
            name = f"knoggin-{safe_id}"
            volume = f"knoggin-home-{safe_id}"
            labels = {"knoggin.owner": self.user_name, "knoggin.project": project_id}
            config = self._project_config(project_id, project["workspaces"], volume, labels)
            config_hash = hashlib.sha256(
                json.dumps(config, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            config["Labels"]["knoggin.config"] = config_hash

            existing = await self._owned_container(name, project_id)
            if existing is not None:
                stored_labels = existing.get("Config", {}).get("Labels") or {}
                if stored_labels.get("knoggin.config") == config_hash:
                    if not existing.get("State", {}).get("Running", False):
                        await runtime.start(existing["Id"])
                    self._last_used[project_id] = self._clock()
                    return existing["Id"]
                if existing.get("State", {}).get("Running", False):
                    await runtime.stop(existing["Id"])
                await runtime.remove(existing["Id"])

            await self._ensure_home_volume(volume, labels)
            container_id = await runtime.create_container(name, config)
            await runtime.start(container_id)
            self._last_used[project_id] = self._clock()
            return container_id

    def _project_config(
        self, project_id: str, workspaces: dict, volume: str, labels: dict[str, str]
    ) -> dict:
        project_root = self.library_root / project_id
        if project_root.is_symlink():
            raise ValueError("Project root must not be a symlink")
        binds = []
        host_root = (
            Path(self.runtime_settings.host_library_root).expanduser()
            if self.runtime_settings.host_library_root
            else self.library_root
        )
        if not host_root.is_absolute():
            raise ValueError("host_library_root must be absolute")
        for name, settings in sorted(workspaces.items()):
            if not name or name.startswith(".") or len(name) > 100 or any(
                char in name for char in "/\\\x00"
            ):
                raise ValueError(f"Invalid workspace name: {name}")
            source = project_root / name
            if source.is_symlink():
                raise ValueError(f"Workspace must not be a symlink: {name}")
            source.mkdir(parents=True, exist_ok=True)
            mode = "ro" if settings["read_only"] else "rw"
            binds.append(f"{host_root / project_id / name}:/workspaces/{name}:{mode}")

        home_bind = f"{volume}:/home/sandbox:rw"
        if self._runtime.is_podman:
            home_bind += ",U"
        binds.append(home_bind)
        commands = self.command_settings
        host_config = {
            "Binds": binds,
            "NetworkMode": "none",
            "CapDrop": ["ALL"],
            "SecurityOpt": ["no-new-privileges"],
            "Privileged": False,
            "Memory": commands.memory_mb * 1024 * 1024,
            "NanoCpus": int(commands.cpus * 1_000_000_000),
            "PidsLimit": commands.pids,
            "ReadonlyRootfs": False,
        }
        config = {
            "Image": commands.default_image,
            "Cmd": ["sleep", "infinity"],
            "WorkingDir": "/workspaces",
            "Env": ["HOME=/home/sandbox"],
            "HostConfig": host_config,
            "Labels": dict(labels),
        }
        if self._runtime.is_podman:
            host_config["UsernsMode"] = "keep-id"
        else:
            config["User"] = f"{os.getuid()}:{os.getgid()}"
        return config

    async def _ensure_home_volume(self, volume: str, labels: dict[str, str]) -> None:
        runtime = self._runtime
        existing = await runtime.inspect_volume(volume)
        if existing is not None:
            if any((existing.get("Labels") or {}).get(key) != value for key, value in labels.items()):
                raise RuntimeError(f"Volume already belongs to another owner: {volume}")
            return
        await runtime.create_volume(volume, labels)
        if runtime.is_podman:
            return

        helper_name = f"knoggin-chown-{uuid4().hex}"
        helper_id = None
        failed = False
        try:
            helper_id = await runtime.create_container(
                helper_name,
                {
                    "Image": self.command_settings.default_image,
                    "User": "0:0",
                    "Cmd": ["chown", f"{os.getuid()}:{os.getgid()}", "/home/sandbox"],
                    "HostConfig": {
                        "Binds": [f"{volume}:/home/sandbox:rw"],
                        "NetworkMode": "none",
                    },
                    "Labels": dict(labels),
                },
            )
            await runtime.start(helper_id)
            result = await runtime.wait(helper_id)
            if result.get("StatusCode") != 0:
                raise RuntimeError("Home volume ownership setup failed")
        except BaseException:
            failed = True
            raise
        finally:
            try:
                if helper_id is not None:
                    await runtime.remove(helper_id, force=True)
            finally:
                if failed:
                    await runtime.remove_volume(volume)
