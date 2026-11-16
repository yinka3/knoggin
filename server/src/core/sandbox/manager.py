"""Own the sandbox availability check and, later, project containers."""

import asyncio
import hashlib
import json
import os
import re
import shutil
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
                        if (
                            container.get("Labels", {}).get("knoggin.owner") == self.user_name
                            and (
                                container.get("Labels", {}).get("knoggin.install") == "true"
                                or container.get("Labels", {}).get("knoggin.migrate") == "true"
                            )
                        ):
                            await self._runtime.remove(container["Id"], force=True)
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

        self.library_root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(
            prefix="knoggin-sandbox-check-", dir=self.library_root
        ) as temp_dir:
            marker = uuid4().hex
            name = f"knoggin-check-{marker}"
            host_temp_dir = self._host_library_root() / Path(temp_dir).name
            config = {
                "Image": image,
                "Cmd": ["sh", "-c", f"printf {marker} > /check/probe"],
                "HostConfig": {
                    "NetworkMode": "none",
                    "Binds": [f"{host_temp_dir}:/check:rw"],
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
            async with self._container_lock:
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
        async with self._container_lock:
            return await self._ensure_running_locked(project_id)

    async def _ensure_running_locked(self, project_id: str) -> str:
        safe_id = self._safe_id(project_id)
        project = await self.project_manager.get_project(project_id)
        if project is None or project.get("status") == "deleted":
            raise ValueError(f"Project not found: {project_id}")
        runtime = self._runtime
        name = f"knoggin-{safe_id}"
        labels = {"knoggin.owner": self.user_name, "knoggin.project": project_id}
        await self._prepare_project_storage(project_id, labels)
        config = self._project_config(project_id, project["workspaces"], labels)
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

        container_id = await runtime.create_container(name, config)
        await runtime.start(container_id)
        self._last_used[project_id] = self._clock()
        return container_id

    async def run_command(
        self, project_id: str, workspace: str, command: str, *,
        cwd: str = ".", timeout_seconds: int = 300,
        on_event: Callable[[str, dict], None] | None = None,
    ) -> dict:
        """Run one bounded command while holding the container lifecycle lock."""
        if not isinstance(command, str) or not command.strip() or len(command) > 20_000:
            raise ValueError("command must be non-empty and at most 20,000 characters")
        if not isinstance(timeout_seconds, int) or isinstance(timeout_seconds, bool) or not 1 <= timeout_seconds <= 900:
            raise ValueError("timeout_seconds must be between 1 and 900")
        if not isinstance(workspace, str) or not workspace or workspace.startswith(".") or any(
            char in workspace for char in "/\\\x00"
        ):
            raise ValueError("workspace must name one configured workspace")
        if not isinstance(cwd, str) or len(cwd) > 512 or not cwd or "\x00" in cwd:
            raise ValueError("cwd must be a bounded path inside the workspace")
        relative_cwd = Path(cwd)
        if relative_cwd.is_absolute() or ".." in relative_cwd.parts:
            raise ValueError("cwd must stay inside the workspace")
        if not self._status["available"] or self._runtime is None:
            raise RuntimeError(f"Sandbox unavailable: {self._status['message']}")

        async with self._container_lock:
            project = await self.project_manager.get_project(project_id)
            if project is None or workspace not in project["workspaces"]:
                raise ValueError(f"Workspace not found: {workspace}")
            container_id = await self._ensure_running_locked(project_id)
            workspace_root = (self.library_root / project_id / workspace).resolve()
            real_cwd = (workspace_root / relative_cwd).resolve()
            if not real_cwd.is_relative_to(workspace_root) or not real_cwd.is_dir():
                raise ValueError("cwd must be an existing directory inside the workspace")
            container_cwd = f"/workspaces/{workspace}"
            if real_cwd != workspace_root:
                container_cwd += "/" + real_cwd.relative_to(workspace_root).as_posix()
            try:
                result = await self._exec_locked(
                    container_id,
                    ["sh", "-c", command],
                    workdir=container_cwd,
                    timeout_seconds=timeout_seconds,
                    on_event=on_event,
                )
            finally:
                await self._clear_temporary_storage(project_id)
            self._last_used[project_id] = self._clock()
            return {
                **result,
                "workspace": workspace,
                "cwd": container_cwd,
                "read_only": bool(project["workspaces"][workspace]["read_only"]),
            }

    async def _exec_locked(
        self, container_id: str, command: list[str], *, workdir: str,
        timeout_seconds: int, on_event: Callable[[str, dict], None] | None,
    ) -> dict:
        """Run one exec while the caller owns the container lifecycle lock."""
        runtime = self._runtime
        min_free_mb = self.command_settings.min_free_disk_mb
        min_free_bytes = min_free_mb * 1024**2
        if shutil.disk_usage(self.library_root).free < min_free_bytes:
            raise RuntimeError(
                f"Sandbox command refused: less than {min_free_mb} MiB of host disk space remains"
            )
        exec_id = await runtime.exec_create(
            container_id,
            ["timeout", "--kill-after=5", str(timeout_seconds), *command],
            workdir,
        )
        output = {"stdout": bytearray(), "stderr": bytearray()}
        output_limit = 16 * 1024
        truncated = False
        disk_low = False

        async def watch_disk():
            nonlocal disk_low
            while True:
                await asyncio.sleep(self.command_settings.disk_check_interval_seconds)
                if shutil.disk_usage(self.library_root).free < min_free_bytes:
                    disk_low = True
                    await runtime.stop(container_id, timeout=1)
                    return

        async def status_notice():
            await asyncio.sleep(min(30, max(1, timeout_seconds // 2)))
            if on_event is not None:
                on_event("command_status", {"message": "Command is still running", "timeout_seconds": timeout_seconds})

        notice_task = asyncio.create_task(status_notice())
        disk_task = asyncio.create_task(watch_disk())
        try:
            async for stream, payload in runtime.exec_stream(exec_id):
                remaining = max(0, output_limit - len(output[stream]))
                kept = payload[:remaining]
                output[stream].extend(kept)
                if len(kept) != len(payload):
                    truncated = True
                if kept and on_event is not None:
                    on_event("command_output", {"stream": stream, "text": kept.decode("utf-8", errors="replace")})
            result = await runtime.exec_inspect(exec_id)
            if disk_low:
                raise RuntimeError(
                    f"Sandbox command stopped: less than {min_free_mb} MiB of host disk space remains"
                )
            if result.get("Running") or result.get("ExitCode") is None:
                raise ContainerRuntimeError("Exec stream ended before the command exited")
            exit_code = result["ExitCode"]
            timed_out = exit_code in (124, 137)
            return {
                "stdout": output["stdout"].decode("utf-8", errors="replace"),
                "stderr": output["stderr"].decode("utf-8", errors="replace"),
                "exit_code": exit_code,
                "timed_out": timed_out,
                "output_truncated": truncated,
            }
        finally:
            notice_task.cancel()
            disk_task.cancel()
            await asyncio.gather(notice_task, disk_task, return_exceptions=True)
            # The shell may have detached children even when it exits normally.
            # Stop the container after every exec so no work survives a call.
            await runtime.stop(container_id, timeout=1)

    async def list_workspaces(self, project_id: str) -> list[dict]:
        project = await self.project_manager.get_project(project_id)
        if project is None or project.get("status") == "deleted":
            raise ValueError(f"Project not found: {project_id}")
        return [
            {"name": name, "read_only": bool(settings["read_only"])}
            for name, settings in sorted(project["workspaces"].items())
        ]

    async def install_python_package(
        self, project_id: str, package: str, *, timeout_seconds: int = 300,
        on_event: Callable[[str, dict], None] | None = None,
    ) -> dict:
        """Install one pinned package into the durable project home, with temporary network."""
        self.validate_python_package(package, timeout_seconds)
        if not self._status["available"] or self._runtime is None:
            raise RuntimeError(f"Sandbox unavailable: {self._status['message']}")

        async with self._container_lock:
            project = await self.project_manager.get_project(project_id)
            if project is None or project.get("status") == "deleted":
                raise ValueError(f"Project not found: {project_id}")
            labels = {
                "knoggin.owner": self.user_name,
                "knoggin.project": project_id,
                "knoggin.install": "true",
            }
            await self._prepare_project_storage(
                project_id, {"knoggin.owner": self.user_name, "knoggin.project": project_id}
            )
            config = self._project_config(project_id, project["workspaces"], labels)
            config["HostConfig"]["Binds"] = [
                bind for bind in config["HostConfig"]["Binds"]
                if ":/home/sandbox:" in bind or ":/tmp:" in bind
            ]
            config["HostConfig"].pop("NetworkMode")  # runtime default, only for this helper
            config["WorkingDir"] = "/"
            runtime = self._runtime
            container_id = await runtime.create_container(f"knoggin-install-{uuid4().hex}", config)
            try:
                await runtime.start(container_id)
                result = await self._exec_locked(
                    container_id,
                    [
                        "sh", "-c",
                        "python -m pip --isolated install --no-input "
                        "--disable-pip-version-check --no-cache-dir "
                        "--index-url https://pypi.org/simple "
                        "--target /tmp/knoggin-python-install \"$1\" "
                        "&& mkdir -p /home/sandbox/python "
                        "&& cp -a /tmp/knoggin-python-install/. /home/sandbox/python/",
                        "install", package,
                    ],
                    workdir="/",
                    timeout_seconds=timeout_seconds,
                    on_event=on_event,
                )
                return {
                    **result,
                    "package": package,
                    "installed": result["exit_code"] == 0,
                    "location": "/home/sandbox/python",
                    "persists_after_rebuild": result["exit_code"] == 0,
                }
            finally:
                try:
                    await runtime.remove(container_id, force=True)
                finally:
                    await self._clear_temporary_storage(project_id)

    @staticmethod
    def validate_python_package(package: str, timeout_seconds: int) -> None:
        if not isinstance(package, str) or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}==[A-Za-z0-9][A-Za-z0-9._+!-]{0,127}",
            package,
        ):
            raise ValueError("package must be a name and exact version, such as colorama==0.4.6")
        if not isinstance(timeout_seconds, int) or isinstance(timeout_seconds, bool) or not 1 <= timeout_seconds <= 900:
            raise ValueError("timeout_seconds must be between 1 and 900")

    def _project_config(
        self, project_id: str, workspaces: dict, labels: dict[str, str]
    ) -> dict:
        project_root = self.library_root / project_id
        if project_root.is_symlink():
            raise ValueError("Project root must not be a symlink")
        binds = []
        host_root = self._host_library_root()
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

        binds.extend([
            f"{host_root / project_id / '.sandbox' / 'home'}:/home/sandbox:rw",
            f"{host_root / project_id / '.sandbox' / 'tmp'}:/tmp:rw",
        ])
        commands = self.command_settings
        host_config = {
            "Binds": binds,
            "Init": True,
            "NetworkMode": "none",
            "CapDrop": ["ALL"],
            "SecurityOpt": ["no-new-privileges"],
            "Privileged": False,
            "Memory": commands.memory_mb * 1024 * 1024,
            "NanoCpus": int(commands.cpus * 1_000_000_000),
            "PidsLimit": commands.pids,
            "ReadonlyRootfs": True,
        }
        config = {
            "Image": commands.default_image,
            "Cmd": ["sleep", "infinity"],
            "WorkingDir": "/workspaces",
            "Env": ["HOME=/home/sandbox", "PYTHONPATH=/home/sandbox/python"],
            "HostConfig": host_config,
            "Labels": dict(labels),
        }
        if self._runtime.is_podman:
            host_config["UsernsMode"] = "keep-id"
        else:
            config["User"] = f"{os.getuid()}:{os.getgid()}"
        return config

    def _host_library_root(self) -> Path:
        host_root = (
            Path(self.runtime_settings.host_library_root).expanduser()
            if self.runtime_settings.host_library_root
            else self.library_root
        )
        if not host_root.is_absolute():
            raise ValueError("host_library_root must be absolute")
        return host_root

    async def _clear_temporary_storage(self, project_id: str) -> None:
        def clear() -> None:
            temporary = self.library_root / project_id / ".sandbox" / "tmp"
            if temporary.is_symlink():
                raise ValueError("Sandbox temporary storage must not be a symlink")
            shutil.rmtree(temporary)
            temporary.mkdir()

        task = asyncio.create_task(asyncio.to_thread(clear))
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    async def _prepare_project_storage(self, project_id: str, labels: dict[str, str]) -> None:
        project_root = self.library_root / project_id
        sandbox_root = project_root / ".sandbox"
        home = sandbox_root / "home"
        temporary = sandbox_root / "tmp"
        for path in (project_root, sandbox_root, home, temporary):
            if path.is_symlink():
                raise ValueError(f"Sandbox storage must not be a symlink: {path}")
            path.mkdir(parents=True, exist_ok=True)

        volume = f"knoggin-home-{self._safe_id(project_id)}"
        runtime = self._runtime
        existing = await runtime.inspect_volume(volume)
        if existing is None:
            return
        if any((existing.get("Labels") or {}).get(key) != value for key, value in labels.items()):
            raise RuntimeError(f"Volume already belongs to another owner: {volume}")
        old_container = await self._owned_container(self._container_name(project_id), project_id)
        if old_container is not None and any(
            bind.startswith(f"{volume}:/home/sandbox:")
            for bind in (old_container.get("HostConfig", {}).get("Binds") or [])
        ):
            if old_container.get("State", {}).get("Running", False):
                await runtime.stop(old_container["Id"])
            await runtime.remove(old_container["Id"])
        migrated_marker = sandbox_root / "home-volume-migrated"
        if migrated_marker.exists():
            await runtime.remove_volume(volume)
            return
        if any(home.iterdir()):
            raise RuntimeError("Cannot migrate the old sandbox home into a non-empty home directory")

        staging = sandbox_root / f"home-migrating-{uuid4().hex}"
        staging.mkdir()
        host_home = self._host_library_root() / project_id / ".sandbox" / staging.name
        config = {
            "Image": self.command_settings.default_image,
            "Cmd": ["cp", "-a", "/old/.", "/new/"],
            "WorkingDir": "/",
            "HostConfig": {
                "Binds": [f"{volume}:/old:ro", f"{host_home}:/new:rw"],
                "NetworkMode": "none",
                "ReadonlyRootfs": True,
                "CapDrop": ["ALL"],
                "SecurityOpt": ["no-new-privileges"],
                "Privileged": False,
            },
            "Labels": {**labels, "knoggin.migrate": "true"},
        }
        if runtime.is_podman:
            config["HostConfig"]["UsernsMode"] = "keep-id"
        else:
            config["User"] = f"{os.getuid()}:{os.getgid()}"
        try:
            helper_id = await runtime.create_container(f"knoggin-migrate-{uuid4().hex}", config)
            try:
                await runtime.start(helper_id)
                result = await runtime.wait(helper_id)
                if result.get("StatusCode") != 0:
                    raise RuntimeError("Sandbox home migration failed; old volume was kept")
            finally:
                await runtime.remove(helper_id, force=True)
            home.rmdir()
            staging.rename(home)
            migrated_marker.touch()
        except BaseException:
            if staging.exists():
                shutil.rmtree(staging)
            raise
        await runtime.remove_volume(volume)
