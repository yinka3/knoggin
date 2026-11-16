import asyncio
import os
from types import SimpleNamespace
from uuid import uuid4

import pytest

from common.schema.settings import ContainerRuntimeSettings, WorkspaceCommandSettings
from core.sandbox.manager import SandboxManager
from infrastructure.container_runtime import discover

pytestmark = pytest.mark.requires_container


class TemporaryProject:
    def __init__(self):
        self.workspaces = {"api": {"read_only": False}, "docs": {"read_only": True}}

    async def get_project(self, project_id):
        return {"id": project_id, "status": "active", "workspaces": self.workspaces}


async def test_real_project_mounts_recreation_and_writable_home(tmp_path):
    runtime = await discover()
    if runtime is None:
        pytest.skip("No reachable Podman or Docker socket")
    await runtime.close()

    owner = f"knoggin-project-test-{uuid4().hex}"
    project_id = f"test-{uuid4().hex}"
    project_root = tmp_path / project_id
    (project_root / "api").mkdir(parents=True)
    (project_root / "docs").mkdir()
    project = TemporaryProject()
    manager = SandboxManager(
        user_name=owner,
        runtime_settings=ContainerRuntimeSettings(),
        command_settings=WorkspaceCommandSettings(),
        project_manager=project,
        library_root=tmp_path,
    )
    try:
        await manager.start()
        assert manager.status()["stage"] == "ok", manager.status()
        first = await manager.ensure_running(project_id)
        probe = await discover()
        assert probe is not None
        try:
            inspect = await probe.inspect(first)
            assert inspect["State"]["Running"]
            assert inspect["HostConfig"]["NetworkMode"] == "none"
            mounts = {mount["Destination"]: mount for mount in inspect["Mounts"]}
            assert mounts["/workspaces/api"]["Source"] == str(project_root / "api")
            assert mounts["/workspaces/api"]["RW"] is True
            assert mounts["/workspaces/docs"]["Source"] == str(project_root / "docs")
            assert mounts["/workspaces/docs"]["RW"] is False
            assert mounts["/home/sandbox"]["Source"] == str(project_root / ".sandbox" / "home")
            assert mounts["/tmp"]["Source"] == str(project_root / ".sandbox" / "tmp")
            assert inspect["HostConfig"]["ReadonlyRootfs"] is True
            written = await manager.run_command(
                project_id, "api", f"printf {uuid4().hex} > /home/sandbox/probe"
            )
            assert written["exit_code"] == 0
            marker = (project_root / ".sandbox" / "home" / "probe").read_text()
            assert await manager.ensure_running(project_id) == first
            project.workspaces["docs"]["read_only"] = False
            second = await manager.ensure_running(project_id)
            assert second != first
            assert (await probe.inspect(second))["State"]["Running"]

            verified = await manager.run_command(
                project_id, "api", f"test \"$(cat /home/sandbox/probe)\" = {marker}"
            )
            assert verified["exit_code"] == 0
        finally:
            await probe.close()
    finally:
        cleanup = await discover()
        if cleanup is not None:
            try:
                for container in await cleanup.list_containers({"knoggin.owner": owner}):
                    await cleanup.remove(container["Id"], force=True)
            finally:
                await cleanup.close()
        await manager.shutdown()


async def test_old_home_volume_migrates_into_project_storage(tmp_path):
    runtime = await discover()
    if runtime is None:
        pytest.skip("No reachable Podman or Docker socket")
    if runtime.is_podman:
        await runtime.close()
        pytest.skip("Old-volume migration requires a separate Podman host check")

    owner = f"knoggin-migration-test-{uuid4().hex}"
    project_id = f"test-{uuid4().hex}"
    volume = f"knoggin-home-{project_id}"
    labels = {"knoggin.owner": owner, "knoggin.project": project_id}
    (tmp_path / project_id / "api").mkdir(parents=True)
    manager = SandboxManager(
        user_name=owner,
        runtime_settings=ContainerRuntimeSettings(),
        command_settings=WorkspaceCommandSettings(),
        project_manager=TemporaryProject(),
        library_root=tmp_path,
    )
    try:
        await runtime.create_volume(volume, labels)
        setup = await runtime.create_container(
            f"knoggin-migration-setup-{uuid4().hex}",
            {
                "Image": manager.command_settings.default_image,
                "Cmd": ["sh", "-c", f"printf {project_id} > /old/probe && chown -R {os.getuid()}:{os.getgid()} /old"],
                "HostConfig": {"Binds": [f"{volume}:/old:rw"], "NetworkMode": "none"},
                "Labels": labels,
            },
        )
        await runtime.start(setup)
        assert (await runtime.wait(setup))["StatusCode"] == 0
        await runtime.remove(setup)
        old_container = await runtime.create_container(
            f"knoggin-{project_id}",
            {
                "Image": manager.command_settings.default_image,
                "Cmd": ["sleep", "infinity"],
                "HostConfig": {
                    "Binds": [f"{volume}:/home/sandbox:rw"],
                    "NetworkMode": "none",
                },
                "Labels": labels,
            },
        )

        await manager.start()
        assert manager.status()["stage"] == "ok", manager.status()
        result = await manager.run_command(
            project_id, "api", f"test \"$(cat /home/sandbox/probe)\" = {project_id}"
        )
        assert result["exit_code"] == 0
        assert (tmp_path / project_id / ".sandbox" / "home" / "probe").read_text() == project_id
        assert await runtime.inspect_volume(volume) is None
        assert (await runtime.inspect(f"knoggin-{project_id}"))["Id"] != old_container
        await manager.reset(project_id)
        repeated = await manager.run_command(project_id, "api", "test -f /home/sandbox/probe")
        assert repeated["exit_code"] == 0
    finally:
        for container in await runtime.list_containers({"knoggin.owner": owner}):
            await runtime.remove(container["Id"], force=True)
        if await runtime.inspect_volume(volume) is not None:
            await runtime.remove_volume(volume)
        await runtime.close()
        await manager.shutdown()


async def test_real_workspace_exec_output_timeout_and_cancellation(tmp_path):
    runtime = await discover()
    if runtime is None:
        pytest.skip("No reachable Podman or Docker socket")
    await runtime.close()

    owner = f"knoggin-exec-test-{uuid4().hex}"
    project_id = f"test-{uuid4().hex}"
    (tmp_path / project_id / "api").mkdir(parents=True)
    (tmp_path / project_id / "api" / "src").mkdir()
    manager = SandboxManager(
        user_name=owner,
        runtime_settings=ContainerRuntimeSettings(),
        command_settings=WorkspaceCommandSettings(),
        project_manager=TemporaryProject(),
        library_root=tmp_path,
    )
    events = []
    try:
        await manager.start()
        assert manager.status()["stage"] == "ok", manager.status()
        result = await manager.run_command(
            project_id, "api", "printf OUT; printf ERR >&2; exit 7",
            on_event=lambda kind, data: events.append((kind, data)),
        )
        assert (result["stdout"], result["stderr"], result["exit_code"]) == ("OUT", "ERR", 7)
        assert not result["output_truncated"]
        assert {kind for kind, _ in events} == {"command_output"}

        nested = await manager.run_command(project_id, "api", "pwd", cwd="src")
        assert nested["stdout"].strip() == "/workspaces/api/src"
        temporary = await manager.run_command(project_id, "api", "printf temporary > /tmp/probe")
        assert temporary["exit_code"] == 0
        assert not (tmp_path / project_id / ".sandbox" / "tmp" / "probe").exists()
        detached = await manager.run_command(
            project_id, "api", "setsid sleep 30 >/dev/null 2>&1 &",
        )
        assert detached["exit_code"] == 0
        runtime = await discover()
        try:
            assert runtime is not None
            container = await runtime.inspect(f"knoggin-{project_id}")
            assert not container["State"]["Running"]
        finally:
            if runtime is not None:
                await runtime.close()
        with pytest.raises(ValueError, match="cwd must stay"):
            await manager.run_command(project_id, "api", "pwd", cwd="../docs")

        result = await manager.run_command(
            project_id, "api", "sleep 5", timeout_seconds=2,
            on_event=lambda kind, data: events.append((kind, data)),
        )
        assert result["timed_out"] and result["exit_code"] == 124
        assert any(kind == "command_status" for kind, _ in events)
        runtime = await discover()
        try:
            assert runtime is not None
            container = await runtime.inspect(f"knoggin-{project_id}")
            assert not container["State"]["Running"]
        finally:
            if runtime is not None:
                await runtime.close()

        # Start a worker in a separate process session, then time out its shell.
        result = await manager.run_command(
            project_id, "api", "setsid sleep 30 >/dev/null 2>&1 & sleep 5", timeout_seconds=1,
        )
        assert result["timed_out"]
        runtime = await discover()
        try:
            assert runtime is not None
            container = await runtime.inspect(f"knoggin-{project_id}")
            assert not container["State"]["Running"]
        finally:
            if runtime is not None:
                await runtime.close()

        task = asyncio.create_task(manager.run_command(project_id, "api", "echo READY; sleep 30"))
        await asyncio.sleep(1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        runtime = await discover()
        try:
            assert runtime is not None
            container = await runtime.inspect(f"knoggin-{project_id}")
            assert not container["State"]["Running"]
        finally:
            if runtime is not None:
                await runtime.close()
    finally:
        cleanup = await discover()
        if cleanup is not None:
            try:
                for container in await cleanup.list_containers({"knoggin.owner": owner}):
                    await cleanup.remove(container["Id"], force=True)
                volume = f"knoggin-home-{project_id}"
                if await cleanup.inspect_volume(volume) is not None:
                    await cleanup.remove_volume(volume)
            finally:
                await cleanup.close()
        await manager.shutdown()


async def test_running_command_stops_when_host_disk_is_low(tmp_path, monkeypatch):
    runtime = await discover()
    if runtime is None:
        pytest.skip("No reachable Podman or Docker socket")
    await runtime.close()

    owner = f"knoggin-disk-test-{uuid4().hex}"
    project_id = f"test-{uuid4().hex}"
    (tmp_path / project_id / "api").mkdir(parents=True)
    manager = SandboxManager(
        user_name=owner,
        runtime_settings=ContainerRuntimeSettings(),
        command_settings=WorkspaceCommandSettings(),
        project_manager=TemporaryProject(),
        library_root=tmp_path,
    )
    try:
        await manager.start()
        assert manager.status()["stage"] == "ok", manager.status()
        reads = 0

        def disk_usage(_path):
            nonlocal reads
            reads += 1
            return SimpleNamespace(free=3 * 1024**3 if reads == 1 else 1024)

        monkeypatch.setattr("core.sandbox.manager.shutil.disk_usage", disk_usage)
        with pytest.raises(RuntimeError, match="less than 2048 MiB"):
            await manager.run_command(project_id, "api", "sleep 30")
        probe = await discover()
        try:
            assert probe is not None
            container = await probe.inspect(f"knoggin-{project_id}")
            assert not container["State"]["Running"]
        finally:
            if probe is not None:
                await probe.close()
    finally:
        cleanup = await discover()
        if cleanup is not None:
            try:
                for container in await cleanup.list_containers({"knoggin.owner": owner}):
                    await cleanup.remove(container["Id"], force=True)
                volume = f"knoggin-home-{project_id}"
                if await cleanup.inspect_volume(volume) is not None:
                    await cleanup.remove_volume(volume)
            finally:
                await cleanup.close()
        await manager.shutdown()


async def test_real_approved_style_python_install_is_persistent_and_offline(tmp_path):
    runtime = await discover()
    if runtime is None:
        pytest.skip("No reachable Podman or Docker socket")
    await runtime.close()

    owner = f"knoggin-install-test-{uuid4().hex}"
    project_id = f"test-{uuid4().hex}"
    (tmp_path / project_id / "api").mkdir(parents=True)
    manager = SandboxManager(
        user_name=owner,
        runtime_settings=ContainerRuntimeSettings(),
        command_settings=WorkspaceCommandSettings(),
        project_manager=TemporaryProject(),
        library_root=tmp_path,
    )
    try:
        await manager.start()
        assert manager.status()["stage"] == "ok", manager.status()
        install_configs = []
        original_create = manager._runtime.create_container

        async def capture_install(name, config):
            if name.startswith("knoggin-install-"):
                install_configs.append(config)
            return await original_create(name, config)

        manager._runtime.create_container = capture_install
        missing = await manager.install_python_package(
            project_id, f"knoggin-no-such-package-{uuid4().hex}==0.0.1",
            timeout_seconds=30,
        )
        assert not missing["installed"] and not missing["persists_after_rebuild"]
        check = await manager.run_command(project_id, "api", "test ! -e /home/sandbox/python")
        assert check["exit_code"] == 0
        install = await manager.install_python_package(
            project_id, "colorama==0.4.6", timeout_seconds=90,
        )
        assert install["installed"], install["stderr"]
        assert install["persists_after_rebuild"]
        assert not any((tmp_path / project_id / ".sandbox" / "tmp").iterdir())
        assert len(install_configs) == 2
        for config in install_configs:
            assert config["HostConfig"].get("NetworkMode") is None
            assert config["HostConfig"]["Binds"] == [
                f"{tmp_path}/{project_id}/.sandbox/home:/home/sandbox:rw",
                f"{tmp_path}/{project_id}/.sandbox/tmp:/tmp:rw",
            ]
            assert config["HostConfig"]["ReadonlyRootfs"] is True
        result = await manager.run_command(
            project_id, "api", "python -c 'import colorama; print(colorama.__version__)'",
        )
        assert result["exit_code"] == 0 and result["stdout"].strip() == "0.4.6"
        runtime = await discover()
        try:
            assert runtime is not None
            container = await runtime.inspect(f"knoggin-{project_id}")
            assert container["HostConfig"]["NetworkMode"] == "none"
            assert not any(
                item.get("Labels", {}).get("knoggin.install") == "true"
                for item in await runtime.list_containers({"knoggin.owner": owner})
            )
        finally:
            if runtime is not None:
                await runtime.close()
        await manager.reset(project_id)
        result = await manager.run_command(
            project_id, "api", "python -c 'import colorama; print(colorama.__version__)'",
        )
        assert result["exit_code"] == 0 and result["stdout"].strip() == "0.4.6"
    finally:
        cleanup = await discover()
        if cleanup is not None:
            try:
                for container in await cleanup.list_containers({"knoggin.owner": owner}):
                    await cleanup.remove(container["Id"], force=True)
                volume = f"knoggin-home-{project_id}"
                if await cleanup.inspect_volume(volume) is not None:
                    await cleanup.remove_volume(volume)
            finally:
                await cleanup.close()
        await manager.shutdown()
