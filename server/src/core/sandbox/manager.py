"""Own the sandbox availability check and, later, project containers."""

import os
import tempfile
from pathlib import Path
from uuid import uuid4

from common.schema.settings import ContainerRuntimeSettings, WorkspaceCommandSettings
from infrastructure.container_runtime import (
    ContainerRuntime,
    ContainerRuntimeError,
    discover,
)

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
    ) -> None:
        self.user_name = user_name
        self.runtime_settings = runtime_settings
        self.command_settings = command_settings
        self._runtime: ContainerRuntime | None = None
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
                await runtime.pull(image)
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
                result = await runtime.wait(container_id)
                if result.get("StatusCode") != 0 or (Path(temp_dir) / "probe").read_text() != marker:
                    self._status = self._result(False, "mount_failed", "Sandbox mount write failed")
                else:
                    self._status = self._result(True, "ok", "Sandbox check passed")
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
        if self._runtime is not None:
            await self._runtime.close()
            self._runtime = None
