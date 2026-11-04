from uuid import uuid4

import pytest

from common.schema.settings import ContainerRuntimeSettings, WorkspaceCommandSettings
from core.sandbox.manager import SandboxManager
from infrastructure.container_runtime import discover

pytestmark = pytest.mark.requires_container


async def test_real_startup_check_cleans_up_its_container(tmp_path):
    runtime = await discover()
    if runtime is None:
        pytest.skip("No reachable Podman or Docker socket")
    await runtime.close()

    owner = f"knoggin-check-test-{uuid4().hex}"
    manager = SandboxManager(
        user_name=owner,
        runtime_settings=ContainerRuntimeSettings(),
        command_settings=WorkspaceCommandSettings(),
        project_manager=None,
        library_root=tmp_path,
    )
    try:
        result = await manager.check()
        assert result["stage"] == "ok", result
        probe = await discover()
        assert probe is not None
        try:
            assert await probe.list_containers({"knoggin.owner": owner}) == []
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
