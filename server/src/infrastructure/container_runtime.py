"""Small async client for the Docker-compatible container API."""

import json
import os
from collections.abc import Mapping
from typing import Any
from urllib.parse import quote

import httpx


class ContainerRuntimeError(RuntimeError):
    def __init__(self, message: str, status_code: int | None = None):
        self.status_code = status_code
        super().__init__(message)


class ContainerRuntime:
    def __init__(self, socket_path: str, transport: httpx.AsyncBaseTransport | None = None):
        self.socket_path = socket_path
        self.is_podman = False
        self.rootless = False
        self._client = httpx.AsyncClient(
            base_url="http://container-runtime",
            transport=transport or httpx.AsyncHTTPTransport(uds=socket_path),
            timeout=30.0,
        )

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        try:
            response = await self._client.request(method, path, **kwargs)
        except httpx.HTTPError as exc:
            raise ContainerRuntimeError(str(exc)) from exc
        if response.is_error:
            try:
                message = response.json().get("message", response.text)
            except (ValueError, AttributeError):
                message = response.text
            raise ContainerRuntimeError(str(message), response.status_code)
        return response

    async def ping(self) -> bool:
        response = await self._request("GET", "/_ping")
        return response.text.strip() == "OK"

    async def identify(self) -> None:
        version = (await self._request("GET", "/version")).json()
        info = (await self._request("GET", "/info")).json()
        components = version.get("Components", [])
        names = [str(component.get("Name", "")) for component in components]
        names.append(str(version.get("Platform", {}).get("Name", "")))
        self.is_podman = any("podman" in name.lower() for name in names)
        if self.is_podman:
            host = info.get("host", info.get("Host", {}))
            security = host.get("security", host.get("Security", {}))
            self.rootless = bool(
                security.get("rootless", security.get("Rootless", False))
                or "name=rootless" in info.get("SecurityOptions", [])
            )
        else:
            self.rootless = "name=rootless" in info.get("SecurityOptions", [])

    async def image_exists(self, ref: str) -> bool:
        try:
            await self._request("GET", f"/images/{quote(ref, safe='')}/json")
        except ContainerRuntimeError as exc:
            if exc.status_code == 404:
                return False
            raise
        return True

    async def pull(self, ref: str) -> None:
        response = await self._request(
            "POST", "/images/create", params={"fromImage": ref}, timeout=None
        )
        # The image endpoint can report a pull failure inside a 200 NDJSON body.
        for line in response.text.splitlines():
            if line:
                event = json.loads(line)
                if "error" in event:
                    raise ContainerRuntimeError(str(event["error"]))

    async def create_container(self, name: str, config: Mapping[str, Any]) -> str:
        response = await self._request(
            "POST", "/containers/create", params={"name": name}, json=dict(config)
        )
        return str(response.json()["Id"])

    async def start(self, container_id: str) -> None:
        await self._request("POST", f"/containers/{quote(container_id, safe='')}/start")

    async def stop(self, container_id: str, timeout: int = 10) -> None:
        await self._request(
            "POST", f"/containers/{quote(container_id, safe='')}/stop", params={"t": timeout}
        )

    async def remove(self, container_id: str, force: bool = True) -> None:
        await self._request(
            "DELETE", f"/containers/{quote(container_id, safe='')}", params={"force": force}
        )

    async def inspect(self, container_id: str) -> dict[str, Any]:
        response = await self._request("GET", f"/containers/{quote(container_id, safe='')}/json")
        return response.json()

    async def wait(self, container_id: str) -> dict[str, Any]:
        response = await self._request(
            "POST", f"/containers/{quote(container_id, safe='')}/wait", timeout=None
        )
        return response.json()

    async def exec_create(self, container_id: str, command: list[str], workdir: str) -> str:
        response = await self._request(
            "POST", f"/containers/{quote(container_id, safe='')}/exec",
            json={
                "Cmd": command, "WorkingDir": workdir,
                "AttachStdout": True, "AttachStderr": True,
                "AttachStdin": False, "Tty": False,
            },
        )
        return str(response.json()["Id"])

    async def exec_stream(self, exec_id: str):
        """Yield Docker's separate stdout/stderr frames without buffering the command."""
        try:
            async with self._client.stream(
                "POST", f"/exec/{quote(exec_id, safe='')}/start",
                json={"Detach": False, "Tty": False}, timeout=None,
            ) as response:
                if response.is_error:
                    await response.aread()
                    raise ContainerRuntimeError(response.text, response.status_code)
                pending = bytearray()
                async for chunk in response.aiter_raw():
                    pending.extend(chunk)
                    while len(pending) >= 8:
                        size = int.from_bytes(pending[4:8], "big")
                        if size > 1024 * 1024:
                            raise ContainerRuntimeError("Exec output frame exceeds 1 MiB")
                        if len(pending) < 8 + size:
                            break
                        stream = pending[0]
                        payload = bytes(pending[8:8 + size])
                        del pending[:8 + size]
                        if stream not in (1, 2):
                            raise ContainerRuntimeError("Invalid exec output stream")
                        yield ("stdout" if stream == 1 else "stderr"), payload
                if pending:
                    raise ContainerRuntimeError("Incomplete exec output frame")
        except httpx.HTTPError as exc:
            raise ContainerRuntimeError(str(exc)) from exc

    async def exec_inspect(self, exec_id: str) -> dict[str, Any]:
        response = await self._request("GET", f"/exec/{quote(exec_id, safe='')}/json")
        return response.json()

    async def list_containers(self, labels: Mapping[str, str]) -> list[dict[str, Any]]:
        filters = {"label": [f"{key}={value}" for key, value in labels.items()]}
        response = await self._request(
            "GET", "/containers/json", params={"all": True, "filters": json.dumps(filters)}
        )
        return response.json()

    async def create_volume(self, name: str, labels: Mapping[str, str]) -> dict[str, Any]:
        response = await self._request(
            "POST", "/volumes/create", json={"Name": name, "Labels": dict(labels)}
        )
        return response.json()

    async def inspect_volume(self, name: str) -> dict[str, Any] | None:
        try:
            response = await self._request("GET", f"/volumes/{quote(name, safe='')}")
        except ContainerRuntimeError as exc:
            if exc.status_code == 404:
                return None
            raise
        return response.json()

    async def remove_volume(self, name: str) -> None:
        await self._request("DELETE", f"/volumes/{quote(name, safe='')}")

    async def close(self) -> None:
        await self._client.aclose()


async def discover(configured_socket: str | None = None) -> ContainerRuntime | None:
    if configured_socket:
        candidates = [configured_socket]
    else:
        candidates = []
        runtime_dir = os.environ.get("XDG_RUNTIME_DIR")
        if runtime_dir:
            candidates.extend(
                [
                    os.path.join(runtime_dir, "podman", "podman.sock"),
                    os.path.join(runtime_dir, "docker.sock"),
                ]
            )
        candidates.append("/var/run/docker.sock")
    for socket_path in candidates:
        runtime = ContainerRuntime(socket_path)
        try:
            if await runtime.ping():
                await runtime.identify()
                return runtime
        except ContainerRuntimeError:
            pass
        await runtime.close()
    return None
