"""Agent commands in configured project workspaces."""

from common import credential_store
from common.exceptions import ToolExecutionError
from core.agent.tool_events import ask_user, emit_tool_event, finish_or_hand_off
from core.sandbox.approved_commands import get_approved_command

# Shares of the background report (capped at 4,000 characters in total).
_REPORT_STDOUT_CHARS = 2_000
_REPORT_STDERR_CHARS = 1_200


def _tail(text: str, limit: int) -> str:
    text = (text or "").strip()
    return text if len(text) <= limit else "..." + text[-limit:]


def describe_command_result(result: dict) -> str:
    """The text a background command reports to its chat: how it ended and
    the end of its output, which is where results and errors usually are."""

    lines = [f"Exit code: {result.get('exit_code')}"]
    if result.get("timed_out"):
        lines.append("The command hit its time limit.")
    if result.get("killed"):
        lines.append(result.get("kill_reason") or "The command was killed.")
    if result.get("output_truncated"):
        lines.append("Output was longer than the sandbox keeps; only its start was captured.")
    stdout = _tail(result.get("stdout", ""), _REPORT_STDOUT_CHARS)
    stderr = _tail(result.get("stderr", ""), _REPORT_STDERR_CHARS)
    if stdout:
        lines.append(f"stdout (end):\n{stdout}")
    if stderr:
        lines.append(f"stderr (end):\n{stderr}")
    return "\n".join(lines)


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
        # If the run stops because nobody is watching, the command keeps
        # going in the background and reports to the chat when it ends.
        return await finish_or_hand_off(
            self.sandbox.run_command(
                self.project_id, workspace, command, cwd=cwd,
                timeout_seconds=timeout_seconds, on_event=emit_tool_event,
            ),
            tool="run_workspace_command",
            label=" ".join(command.split())[:80],
            describe=describe_command_result,
        )

    async def run_approved_command(
        self, name: str, args: dict | None = None, timeout_seconds: int = 300,
    ) -> dict:
        """Run one network command template after the user approves the exact
        filled-in command. It runs where no workspace is visible."""

        if self.sandbox is None:
            raise ToolExecutionError("run_approved_command", "Workspace commands are unavailable")
        template = get_approved_command(name)
        # Validate before asking, so the question names exactly what will run.
        argv, checked = template.build(args)
        if not isinstance(timeout_seconds, int) or isinstance(timeout_seconds, bool) or not 1 <= timeout_seconds <= 900:
            raise ValueError("timeout_seconds must be between 1 and 900")
        env = {}
        if template.connection is not None:
            env = await self.sandbox.connection_env(
                self.project_id, template.connection,
                required=template.connection_required,
            )
        using = (
            f", using your {credential_store.SERVICES[template.connection].label} login"
            if env else ""
        )
        choice = await ask_user(
            f"Allow the agent to run `{template.describe(checked)}` in a temporary "
            f"container WITH network access that cannot read your workspaces{using}?",
            ("approve", "deny"), timeout_seconds=120, default="deny",
        )
        if choice != "approve":
            return {"command": name, "approved": False}
        # Only the command moves to the background; the approval needs the run.
        result = await finish_or_hand_off(
            self.sandbox.run_approved(
                self.project_id, argv, timeout_seconds=timeout_seconds,
                on_event=emit_tool_event, env=env,
            ),
            tool="run_approved_command",
            label=template.describe(checked)[:80],
            describe=describe_command_result,
        )
        result = {**result, "command": name, "approved": True}
        if template.needs and result.get("exit_code") == 127:
            result["note"] = (
                f"{template.needs} is not installed in the sandbox image "
                "(developer_settings.workspace_commands.default_image)."
            )
        return result
