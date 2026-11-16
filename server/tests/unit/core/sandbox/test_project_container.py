import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from common.schema.settings import ContainerRuntimeSettings, WorkspaceCommandSettings
from core.sandbox.manager import SandboxManager
from infrastructure.container_runtime import ContainerRuntime


class FakeProjects:
    def __init__(self):
        self.workspaces = {"api": {"read_only": False}, "docs": {"read_only": True}}

    async def get_project(self, project_id):
        return {"id": project_id, "status": "active", "workspaces": self.workspaces}


class FakeEngine:
    def __init__(self, *, podman=False):
        self.podman = podman
        self.containers = {}
        self.volumes = {}
        self.events = []
        self.next_id = 0
        self.host_library_root = ""
        self.local_library_root = None

    def respond(self, request):
        method, path = request.method, request.url.path
        self.events.append((method, path))
        if path.startswith("/images/"):
            return httpx.Response(200, json={})
        if method == "GET" and path == "/containers/json":
            return httpx.Response(200, json=[
                {"Id": item["Id"], "State": "running" if item["State"]["Running"] else "exited",
                 "Labels": item["Config"].get("Labels", {})}
                for item in self.containers.values()
            ])
        if method == "POST" and path == "/containers/create":
            self.next_id += 1
            container_id = f"id-{self.next_id}"
            name = request.url.params["name"]
            self.containers[name] = {
                "Id": container_id,
                "Config": json.loads(request.content),
                "State": {"Running": False},
            }
            return httpx.Response(201, json={"Id": container_id})
        if path.startswith("/containers/"):
            target, operation = path.removeprefix("/containers/").split("/", 1) if "/" in path.removeprefix("/containers/") else (path.removeprefix("/containers/"), "")
            container = next(
                (item for name, item in self.containers.items() if name == target or item["Id"] == target),
                None,
            )
            if container is None:
                return httpx.Response(404, json={"message": "not found"})
            if method == "GET" and operation == "json":
                return httpx.Response(200, json=container)
            if method == "POST" and operation == "start":
                container["State"]["Running"] = True
                config = container["Config"]
                if config["Cmd"][:2] == ["sh", "-c"]:
                    source = Path(config["HostConfig"]["Binds"][0].split(":")[0])
                    if self.host_library_root and source.is_relative_to(self.host_library_root):
                        source = self.local_library_root / source.relative_to(self.host_library_root)
                    (source / "probe").write_text(config["Cmd"][2].split()[1])
                return httpx.Response(204)
            if method == "POST" and operation == "stop":
                container["State"]["Running"] = False
                return httpx.Response(204)
            if method == "POST" and operation == "wait":
                return httpx.Response(200, json={"StatusCode": 0})
            if method == "DELETE":
                name = next(name for name, item in self.containers.items() if item is container)
                del self.containers[name]
                return httpx.Response(204)
        if path == "/volumes/create":
            body = json.loads(request.content)
            self.volumes[body["Name"]] = body
            return httpx.Response(201, json=body)
        if path.startswith("/volumes/"):
            name = path.removeprefix("/volumes/")
            if method == "GET":
                if name not in self.volumes:
                    return httpx.Response(404, json={"message": "not found"})
                return httpx.Response(200, json=self.volumes[name])
            del self.volumes[name]
            return httpx.Response(204)
        raise AssertionError(f"Unexpected request: {method} {path}")


async def make_manager(monkeypatch, tmp_path, engine, projects, *, host_root="", clock=None):
    engine.host_library_root = host_root
    engine.local_library_root = tmp_path
    runtime = ContainerRuntime("/fake.sock", httpx.MockTransport(engine.respond))
    runtime.is_podman = engine.podman
    runtime.rootless = engine.podman

    async def discover(_socket):
        return runtime

    monkeypatch.setattr("core.sandbox.manager.discover", discover)
    manager = SandboxManager(
        user_name="alice",
        runtime_settings=ContainerRuntimeSettings(host_library_root=host_root),
        command_settings=WorkspaceCommandSettings(),
        project_manager=projects,
        library_root=tmp_path,
        **({"clock": clock} if clock is not None else {}),
    )
    await manager.start()
    assert manager.status()["available"]
    return manager


async def test_docker_project_config_volume_and_reuse(monkeypatch, tmp_path):
    engine = FakeEngine()
    projects = FakeProjects()
    manager = await make_manager(monkeypatch, tmp_path, engine, projects)
    try:
        assert await manager.list_workspaces("project-1") == [
            {"name": "api", "read_only": False},
            {"name": "docs", "read_only": True},
        ]
        first = await manager.ensure_running("project-1")
        container = engine.containers["knoggin-project-1"]
        config = container["Config"]
        host = config["HostConfig"]
        assert host["Binds"] == [
            f"{tmp_path}/project-1/api:/workspaces/api:rw",
            f"{tmp_path}/project-1/docs:/workspaces/docs:ro",
            f"{tmp_path}/project-1/.sandbox/home:/home/sandbox:rw",
            f"{tmp_path}/project-1/.sandbox/tmp:/tmp:rw",
        ]
        assert str(tmp_path / "project-1") not in [bind.split(":")[0] for bind in host["Binds"]]
        assert config["Image"] == manager.command_settings.default_image
        assert config["User"]
        assert config["Cmd"] == ["sleep", "infinity"]
        assert config["WorkingDir"] == "/workspaces"
        assert config["Env"] == ["HOME=/home/sandbox", "PYTHONPATH=/home/sandbox/python"]
        assert host["NetworkMode"] == "none"
        assert host["CapDrop"] == ["ALL"]
        assert host["SecurityOpt"] == ["no-new-privileges"]
        assert host["Privileged"] is False
        assert host["Memory"] == 4096 * 1024 * 1024
        assert host["NanoCpus"] == 2_000_000_000
        assert host["PidsLimit"] == 512
        assert host["ReadonlyRootfs"] is True
        assert config["Labels"]["knoggin.owner"] == "alice"
        assert config["Labels"]["knoggin.project"] == "project-1"
        assert len(config["Labels"]["knoggin.config"]) == 64
        assert (tmp_path / "project-1" / ".sandbox" / "home").is_dir()
        assert (tmp_path / "project-1" / ".sandbox" / "tmp").is_dir()
        assert first == await manager.ensure_running("project-1")
        assert len([event for event in engine.events if event == ("POST", "/containers/create")]) == 2
        assert not any(event[0] == "DELETE" and event[1].startswith("/volumes/") for event in engine.events)
    finally:
        await manager.shutdown()


async def test_workspace_command_refuses_low_host_disk(monkeypatch, tmp_path):
    engine = FakeEngine()
    manager = await make_manager(monkeypatch, tmp_path, engine, FakeProjects())
    try:
        manager.command_settings.min_free_disk_mb = 4096
        monkeypatch.setattr(
            "core.sandbox.manager.shutil.disk_usage",
            lambda _path: SimpleNamespace(free=1024),
        )
        with pytest.raises(RuntimeError, match="less than 4096 MiB"):
            await manager.run_command("project-1", "api", "true")
        assert not any(path.endswith("/exec") for _, path in engine.events)
    finally:
        await manager.shutdown()


async def test_config_changes_recreate_but_keep_home(monkeypatch, tmp_path):
    engine = FakeEngine()
    projects = FakeProjects()
    manager = await make_manager(monkeypatch, tmp_path, engine, projects)
    try:
        first = await manager.ensure_running("project-1")
        projects.workspaces["docs"]["read_only"] = False
        second = await manager.ensure_running("project-1")
        assert second != first
        projects.workspaces["extra"] = {"read_only": False}
        third = await manager.ensure_running("project-1")
        assert third != second
        manager.command_settings.default_image = "ubuntu:other"
        fourth = await manager.ensure_running("project-1")
        assert fourth != third
        assert (tmp_path / "project-1" / ".sandbox" / "home").is_dir()
        assert not any(event[0] == "DELETE" and event[1].startswith("/volumes/") for event in engine.events)
    finally:
        await manager.shutdown()


async def test_host_root_and_podman_user_mapping(monkeypatch, tmp_path):
    engine = FakeEngine(podman=True)
    projects = FakeProjects()
    manager = await make_manager(
        monkeypatch, tmp_path, engine, projects, host_root="/host/library"
    )
    try:
        await manager.ensure_running("project-1")
        config = engine.containers["knoggin-project-1"]["Config"]
        assert config["HostConfig"]["Binds"] == [
            "/host/library/project-1/api:/workspaces/api:rw",
            "/host/library/project-1/docs:/workspaces/docs:ro",
            "/host/library/project-1/.sandbox/home:/home/sandbox:rw",
            "/host/library/project-1/.sandbox/tmp:/tmp:rw",
        ]
        assert config["HostConfig"]["UsernsMode"] == "keep-id"
        assert "User" not in config
        assert not any(name.startswith("knoggin-chown-") for name in engine.containers)
        assert len([event for event in engine.events if event == ("POST", "/containers/create")]) == 2
    finally:
        await manager.shutdown()


async def test_foreign_named_container_is_not_changed(monkeypatch, tmp_path):
    engine = FakeEngine()
    engine.containers["knoggin-project-1"] = {
        "Id": "foreign-id",
        "Config": {"Labels": {"knoggin.owner": "someone-else"}},
        "State": {"Running": True},
    }
    manager = await make_manager(monkeypatch, tmp_path, engine, FakeProjects())
    try:
        with pytest.raises(RuntimeError, match="another owner"):
            await manager.ensure_running("project-1")
        assert engine.containers["knoggin-project-1"]["Id"] == "foreign-id"
        assert ("POST", "/containers/foreign-id/stop") not in engine.events
        assert ("DELETE", "/containers/foreign-id") not in engine.events
    finally:
        await manager.shutdown()


async def test_symlinked_sandbox_storage_is_rejected(monkeypatch, tmp_path):
    engine = FakeEngine()
    manager = await make_manager(monkeypatch, tmp_path, engine, FakeProjects())
    try:
        project_root = tmp_path / "project-1"
        project_root.mkdir()
        (project_root / ".sandbox").symlink_to(tmp_path, target_is_directory=True)
        with pytest.raises(ValueError, match="must not be a symlink"):
            await manager.ensure_running("project-1")
        assert "knoggin-project-1" not in engine.containers
    finally:
        await manager.shutdown()


async def test_start_stops_only_owned_running_leftovers(monkeypatch, tmp_path):
    engine = FakeEngine()
    engine.containers["knoggin-owned"] = {
        "Id": "owned-id",
        "Config": {"Labels": {"knoggin.owner": "alice", "knoggin.project": "owned"}},
        "State": {"Running": True},
    }
    engine.containers["knoggin-foreign"] = {
        "Id": "foreign-id",
        "Config": {"Labels": {"knoggin.owner": "bob", "knoggin.project": "foreign"}},
        "State": {"Running": True},
    }
    engine.containers["knoggin-migrate-leftover"] = {
        "Id": "migrate-id",
        "Config": {"Labels": {
            "knoggin.owner": "alice", "knoggin.project": "owned",
            "knoggin.migrate": "true",
        }},
        "State": {"Running": True},
    }
    manager = await make_manager(monkeypatch, tmp_path, engine, FakeProjects())
    try:
        assert engine.containers["knoggin-owned"]["State"]["Running"] is False
        assert engine.containers["knoggin-foreign"]["State"]["Running"] is True
        assert ("POST", "/containers/owned-id/stop") in engine.events
        assert ("POST", "/containers/foreign-id/stop") not in engine.events
        assert "knoggin-migrate-leftover" not in engine.containers
    finally:
        await manager.shutdown()


async def test_idle_stop_reset_and_project_removal(monkeypatch, tmp_path):
    now = [100.0]
    engine = FakeEngine()
    manager = await make_manager(
        monkeypatch, tmp_path, engine, FakeProjects(), clock=lambda: now[0]
    )
    try:
        first = await manager.ensure_running("project-1")
        now[0] = 100.0 + 29 * 60
        await manager._stop_idle()
        assert engine.containers["knoggin-project-1"]["State"]["Running"] is True
        now[0] = 100.0 + 30 * 60
        await manager._stop_idle()
        assert engine.containers["knoggin-project-1"]["State"]["Running"] is False
        assert await manager.ensure_running("project-1") == first
        await manager.reset("project-1")
        assert "knoggin-project-1" not in engine.containers
        assert (tmp_path / "project-1" / ".sandbox" / "home").is_dir()
        await manager.ensure_running("project-1")
        await manager.remove_project("project-1")
        assert "knoggin-project-1" not in engine.containers
        assert "knoggin-home-project-1" not in engine.volumes
    finally:
        await manager.shutdown()
