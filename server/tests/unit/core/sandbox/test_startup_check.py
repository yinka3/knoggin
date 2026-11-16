import asyncio
import json
from pathlib import Path

import httpx
import pytest

from common.schema.settings import ContainerRuntimeSettings, WorkspaceCommandSettings
from core.sandbox.manager import ROOT_RUNTIME_WARNING, SandboxManager
from infrastructure.container_runtime import ContainerRuntime


def make_manager(*, enabled=True):
    return SandboxManager(
        user_name="alice",
        runtime_settings=ContainerRuntimeSettings(),
        command_settings=WorkspaceCommandSettings(enabled=enabled),
        project_manager=None,
        library_root=Path("/tmp"),
    )


@pytest.mark.parametrize(
    ("failure", "stage"),
    [
        ("image", "image_unavailable"),
        ("create", "container_failed"),
        ("start", "container_failed"),
        ("wait", "container_failed"),
        ("exit", "mount_failed"),
        ("missing_write", "mount_failed"),
    ],
)
async def test_check_failure_stages_and_cleanup(monkeypatch, failure, stage):
    calls = []
    check_dir = None

    def respond(request):
        nonlocal check_dir
        calls.append((request.method, request.url.path))
        path = request.url.path
        if path == "/containers/json":
            return httpx.Response(200, json=[])
        if path.startswith("/images/"):
            if failure == "image":
                return httpx.Response(404, json={"message": "missing"})
            return httpx.Response(200, json={})
        if path == "/images/create":
            return httpx.Response(200, text='{"error":"pull denied"}\n')
        if path == "/containers/create":
            config = json.loads(request.content)
            check_dir = Path(config["HostConfig"]["Binds"][0].split(":")[0])
            if failure == "create":
                return httpx.Response(500, json={"message": "create failed"})
            return httpx.Response(201, json={"Id": "check-1"})
        if path.endswith("/start"):
            if failure == "start":
                return httpx.Response(500, json={"message": "start failed"})
            return httpx.Response(204)
        if path.endswith("/wait"):
            if failure == "wait":
                return httpx.Response(500, json={"message": "wait failed"})
            return httpx.Response(200, json={"StatusCode": 1 if failure == "exit" else 0})
        return httpx.Response(204)

    runtime = ContainerRuntime("/fake.sock", httpx.MockTransport(respond))
    monkeypatch.setattr("core.sandbox.manager.discover", _discover(runtime))
    manager = make_manager()
    await manager.start()
    assert manager.status()["stage"] == stage
    assert manager.status()["available"] is False
    assert check_dir is None or not check_dir.exists()
    assert (("DELETE", "/containers/check-1") in calls) is (failure not in {"image", "create"})
    await manager.shutdown()


def _discover(runtime):
    async def fake_discover(_socket):
        return runtime

    return fake_discover


async def test_success_writes_mount_and_cleans_up(monkeypatch):
    calls = []
    source = None
    marker = None

    def respond(request):
        nonlocal source, marker
        calls.append((request.method, request.url.path))
        path = request.url.path
        if path == "/containers/json":
            return httpx.Response(200, json=[])
        if path.startswith("/images/"):
            return httpx.Response(200, json={})
        if path == "/containers/create":
            config = json.loads(request.content)
            source = Path(config["HostConfig"]["Binds"][0].split(":")[0])
            marker = config["Cmd"][2].split()[1]
            assert config["User"]
            assert config["HostConfig"]["NetworkMode"] == "none"
            assert config["Labels"]["knoggin.owner"] == "alice"
            return httpx.Response(201, json={"Id": "check-1"})
        if path.endswith("/start"):
            (source / "probe").write_text(marker)
            return httpx.Response(204)
        if path.endswith("/wait"):
            return httpx.Response(200, json={"StatusCode": 0})
        return httpx.Response(204)

    runtime = ContainerRuntime("/fake.sock", httpx.MockTransport(respond))
    monkeypatch.setattr("core.sandbox.manager.discover", _discover(runtime))
    manager = make_manager()
    await manager.start()
    assert manager.status()["stage"] == "ok"
    assert manager.status()["available"] is True
    assert manager.status()["warnings"] == [ROOT_RUNTIME_WARNING]
    assert source is not None and not source.exists()
    assert ("DELETE", "/containers/check-1") in calls
    await manager.shutdown()


async def test_disabled_manager_does_not_discover(monkeypatch):
    async def unexpected(_socket):
        raise AssertionError("disabled manager contacted runtime")

    monkeypatch.setattr("core.sandbox.manager.discover", unexpected)
    manager = make_manager(enabled=False)
    await manager.start()
    assert manager.status()["stage"] == "disabled"


@pytest.mark.parametrize("socket_exists", [False, True])
async def test_missing_or_unreachable_runtime(monkeypatch, tmp_path, socket_exists):
    socket = tmp_path / "runtime.sock"
    if socket_exists:
        socket.touch()

    async def unavailable(_socket):
        return None

    monkeypatch.setattr("core.sandbox.manager.discover", unavailable)
    manager = SandboxManager(
        user_name="alice",
        runtime_settings=ContainerRuntimeSettings(socket=str(socket)),
        command_settings=WorkspaceCommandSettings(),
        project_manager=None,
        library_root=tmp_path,
    )
    await manager.start()
    assert manager.status()["stage"] == (
        "runtime_unreachable" if socket_exists else "no_runtime"
    )


async def test_rootless_runtime_has_no_warning(monkeypatch):
    def respond(request):
        if request.url.path.startswith("/images/"):
            return httpx.Response(404, json={"message": "missing"})
        return httpx.Response(200, text='{"error":"unavailable"}\n')

    runtime = ContainerRuntime(
        "/fake.sock", httpx.MockTransport(respond)
    )
    runtime.is_podman = True
    runtime.rootless = True
    monkeypatch.setattr("core.sandbox.manager.discover", _discover(runtime))
    manager = make_manager()
    await manager.start()
    assert manager.status()["runtime"] == "podman"
    assert manager.status()["rootless"] is True
    assert manager.status()["warnings"] == []
    await manager.shutdown()


@pytest.mark.parametrize(
    ("slow_operation", "stage"),
    [("pull", "image_unavailable"), ("wait", "container_failed")],
)
async def test_check_timeout_cleans_up_container(monkeypatch, tmp_path, slow_operation, stage):
    requests = []
    check_dir = None

    async def respond(request):
        nonlocal check_dir
        requests.append((request.method, request.url.path))
        path = request.url.path
        if path.startswith("/images/"):
            return httpx.Response(
                404 if slow_operation == "pull" else 200,
                json={"message": "missing"} if slow_operation == "pull" else {},
            )
        if path == "/images/create":
            await asyncio.sleep(1)
            return httpx.Response(200, text='{"status":"done"}\n')
        if path == "/containers/create":
            config = json.loads(request.content)
            check_dir = Path(config["HostConfig"]["Binds"][0].split(":")[0])
            return httpx.Response(201, json={"Id": "check-1"})
        if path.endswith("/wait"):
            await asyncio.sleep(1)
            return httpx.Response(200, json={"StatusCode": 0})
        if path == "/containers/json":
            return httpx.Response(200, json=[])
        return httpx.Response(204)

    runtime = ContainerRuntime("/fake.sock", httpx.MockTransport(respond))
    monkeypatch.setattr("core.sandbox.manager.discover", _discover(runtime))
    manager = SandboxManager(
        user_name="alice",
        runtime_settings=ContainerRuntimeSettings(),
        command_settings=WorkspaceCommandSettings(check_timeout_seconds=0.01),
        project_manager=None,
        library_root=tmp_path,
    )
    await manager.start()
    assert manager.status()["stage"] == stage
    assert check_dir is None or not check_dir.exists()
    if slow_operation == "wait":
        assert ("DELETE", "/containers/check-1") in requests
    await manager.shutdown()
