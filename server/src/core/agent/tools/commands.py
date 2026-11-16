"""Agent commands in configured project workspaces."""

from common.exceptions import ToolExecutionError
from core.agent.tool_events import ask_user, emit_tool_event


class WorkspaceCommandTools:
    sandbox = None

    async def list_command_workspaces(self) -> dict:
        if self.sandbox is None:
            raise ToolExecutionError("list_command_workspaces", "Workspace commands are unavailable")
        return {"workspaces": await self.sandbox.list_workspaces(self.project_id)}

    async def run_workspace_command(
        self, workspace: str, command: str, cwd: str = ".", timeout_seconds: int = 300,
    ) -> dict:
        if self.sandbox is None:
            raise ToolExecutionError("run_workspace_command", "Workspace commands are unavailable")
        return await self.sandbox.run_command(
            self.project_id, workspace, command, cwd=cwd,
            timeout_seconds=timeout_seconds, on_event=emit_tool_event,
        )

    async def install_python_package(
        self, package: str, timeout_seconds: int = 300,
    ) -> dict:
        if self.sandbox is None:
            raise ToolExecutionError("install_python_package", "Workspace commands are unavailable")
        # Validate before asking the user so the choice names exactly what will run.
        self.sandbox.validate_python_package(package, timeout_seconds)
        choice = await ask_user(
            f"Allow a temporary networked container to install {package} from PyPI into this "
            "project's persistent /home/sandbox/python folder? It cannot read "
            "project workspaces. The normal command container stays offline.",
            ("approve", "deny"), timeout_seconds=120, default="deny",
        )
        if choice != "approve":
            return {"package": package, "approved": False, "installed": False}
        result = await self.sandbox.install_python_package(
            self.project_id, package,
            timeout_seconds=timeout_seconds, on_event=emit_tool_event,
        )
        return {**result, "approved": True}
