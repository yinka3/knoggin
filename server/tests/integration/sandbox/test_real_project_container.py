import os
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
    volume = f"knoggin-home-{project_id}"
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
            assert mounts["/home/sandbox"]["Name"] == volume
            assert await manager.ensure_running(project_id) == first
            project.workspaces["docs"]["read_only"] = False
            second = await manager.ensure_running(project_id)
            assert second != first
            assert (await probe.inspect(second))["State"]["Running"]

            marker = uuid4().hex
            labels = {"knoggin.owner": owner, "knoggin.project": project_id}
            for command in (
                ["sh", "-c", f"printf {marker} > /home/sandbox/probe"],
                ["sh", "-c", f"test \"$(cat /home/sandbox/probe)\" = {marker}"],
            ):
                helper_host = {
                    "Binds": [f"{volume}:/home/sandbox:rw{',U' if probe.is_podman else ''}"],
                    "NetworkMode": "none",
                }
                helper_config = {
                    "Image": manager.command_settings.default_image,
                    "Cmd": command,
                    "HostConfig": helper_host,
                    "Labels": labels,
                }
                if probe.is_podman:
                    helper_host["UsernsMode"] = "keep-id"
                else:
                    helper_config["User"] = f"{os.getuid()}:{os.getgid()}"
                helper_id = await probe.create_container(
                    f"knoggin-test-helper-{uuid4().hex}",
                    helper_config,
                )
                await probe.start(helper_id)
                assert (await probe.wait(helper_id))["StatusCode"] == 0
                await probe.remove(helper_id)
        finally:
            await probe.close()
    finally:
        cleanup = await discover()
        if cleanup is not None:
            try:
                for container in await cleanup.list_containers({"knoggin.owner": owner}):
                    await cleanup.remove(container["Id"], force=True)
                if await cleanup.inspect_volume(volume) is not None:
                    await cleanup.remove_volume(volume)
            finally:
                await cleanup.close()
        await manager.shutdown()
