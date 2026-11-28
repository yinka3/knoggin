from pathlib import Path

import pytest

from common.schema.settings import ContainerRuntimeSettings, WorkspaceCommandSettings
from core.sandbox.manager import SandboxManager


class FakeExecRuntime:
    """Exec-only runtime whose command 'runs' for a set number of seconds."""

    def __init__(self, now, *, exit_code, runs_for):
        self.now = now
        self.exit_code = exit_code
        self.runs_for = runs_for
        self.stopped = []

    async def exec_create(self, container_id, command, workdir, env=None):
        self.env = env
        return "exec-1"

    async def exec_stream(self, exec_id):
        self.now[0] += self.runs_for
        yield "stderr", b"bye\n"

    async def exec_inspect(self, exec_id):
        return {"Running": False, "ExitCode": self.exit_code}

    async def stop(self, container_id, timeout=None):
        self.stopped.append(container_id)


def make_manager(tmp_path: Path, runtime, now):
    manager = SandboxManager(
        user_name="alice",
        runtime_settings=ContainerRuntimeSettings(),
        command_settings=WorkspaceCommandSettings(min_free_disk_mb=0),
        project_manager=None,
        library_root=tmp_path,
        clock=lambda: now[0],
    )
    manager._runtime = runtime
    return manager


@pytest.mark.parametrize(
    ("exit_code", "runs_for", "timed_out", "killed"),
    [
        (124, 10, True, False),   # timeout's polite stop at the limit
        (137, 15, True, False),   # timeout's --kill-after after the limit
        (137, 2, False, True),    # SIGKILL well before the limit: memory
        (0, 2, False, False),
        (1, 2, False, False),
    ],
)
async def test_exit_codes_separate_timeouts_from_early_kills(
    tmp_path, exit_code, runs_for, timed_out, killed
):
    now = [100.0]
    runtime = FakeExecRuntime(now, exit_code=exit_code, runs_for=runs_for)
    manager = make_manager(tmp_path, runtime, now)

    result = await manager._exec_locked(
        "container-1", ["sh", "-c", "work"], workdir="/workspaces/api",
        timeout_seconds=10, on_event=None,
    )

    assert result["exit_code"] == exit_code
    assert result["timed_out"] is timed_out
    assert result["killed"] is killed
    assert ("kill_reason" in result) is killed
    if killed:
        assert f"{manager.command_settings.memory_mb} MiB" in result["kill_reason"]
    assert runtime.stopped == ["container-1"]


async def test_platform_without_uids_reports_unsupported(monkeypatch, tmp_path):
    monkeypatch.delattr("core.sandbox.manager.os.getuid", raising=False)

    async def discover(_socket):
        raise AssertionError("discovery must not run on an unsupported platform")

    monkeypatch.setattr("core.sandbox.manager.discover", discover)
    manager = SandboxManager(
        user_name="alice",
        runtime_settings=ContainerRuntimeSettings(),
        command_settings=WorkspaceCommandSettings(),
        project_manager=None,
        library_root=tmp_path,
    )

    status = await manager.check()

    assert status["available"] is False
    assert status["stage"] == "unsupported_platform"
    assert "Windows" in status["message"]



async def test_approved_commands_run_in_the_networked_container(tmp_path, monkeypatch):
    manager = SandboxManager(
        user_name="alice",
        runtime_settings=ContainerRuntimeSettings(),
        command_settings=WorkspaceCommandSettings(),
        project_manager=None,
        library_root=tmp_path,
    )
    calls = []

    async def networked(project_id, argv, **kwargs):
        calls.append((project_id, argv, kwargs["timeout_seconds"]))
        return {"exit_code": 0}

    monkeypatch.setattr(manager, "_run_networked", networked)

    await manager.run_approved("p1", ["true"], timeout_seconds=30)
    with pytest.raises(ValueError, match="non-empty list"):
        await manager.run_approved("p1", "rm -rf /")

    assert calls == [("p1", ["true"], 30)]


class ConnectedProjects:
    def __init__(self, connections):
        self.connections = connections

    async def get_project(self, project_id):
        return {"id": project_id, "status": "active", "workspaces": {},
                "connections": self.connections}


@pytest.mark.parametrize(
    ("allowed", "saved", "required", "expected"),
    [
        (["huggingface"], {"HF_TOKEN": "hf"}, False, {"HF_TOKEN": "hf"}),
        ([], {"HF_TOKEN": "hf"}, False, {}),  # saved but not allowed here
        (["huggingface"], {}, False, {}),  # public files still work
        ([], {"HF_TOKEN": "hf"}, True, "allow Hugging Face in this project"),
        (["huggingface"], {}, True, "Save one in your profile."),
    ],
)
async def test_a_login_reaches_a_command_only_when_the_project_allows_it(
    tmp_path, monkeypatch, allowed, saved, required, expected,
):
    monkeypatch.setattr(
        "core.sandbox.manager.credential_store.values", lambda service: dict(saved),
    )
    manager = SandboxManager(
        user_name="alice",
        runtime_settings=ContainerRuntimeSettings(),
        command_settings=WorkspaceCommandSettings(),
        project_manager=ConnectedProjects(allowed),
        library_root=tmp_path,
    )

    if isinstance(expected, str):
        with pytest.raises(ValueError, match=expected):
            await manager.connection_env("p1", "huggingface", required=required)
    else:
        assert await manager.connection_env("p1", "huggingface", required=required) == expected
