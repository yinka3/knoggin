import asyncio
from types import SimpleNamespace

import pytest

from common.schema.settings import RootConfig
from core.agent.tool_events import bind_handoffs, unbind_handoffs
from core.agent.tools.commands import WorkspaceCommandTools, describe_command_result
from core.session.tool_handoffs import ToolHandoffs


class FakeSandbox:
    def __init__(self):
        self.release = asyncio.Event()
        self.cancelled = False

    async def run_command(self, project_id, workspace, command, *, cwd, timeout_seconds, on_event):
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        return {
            "stdout": "collected 12 items\n12 passed", "stderr": "", "exit_code": 0,
            "timed_out": False, "killed": False, "output_truncated": False,
        }


class Tools(WorkspaceCommandTools):
    def __init__(self, sandbox):
        self.sandbox = sandbox
        self.project_id = "p1"


class Chat:
    def __init__(self):
        self.reports = []

    async def queue_report(self, text, *, origin, metadata):
        self.reports.append(text)
        return {"queued": True}


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize("may_hand_off", [True, False])
async def test_a_command_outlives_an_unwatched_run_but_not_a_user_stop(may_hand_off):
    chat = Chat()

    async def get_or_resume_session(_session_id):
        return chat

    handoffs = ToolHandoffs(
        SimpleNamespace(user_name="ada", get_or_resume_session=get_or_resume_session),
        SimpleNamespace(config=RootConfig()),
    )
    sandbox = FakeSandbox()
    token = bind_handoffs(
        handoffs, project_id="p1", session_id="s1", may_hand_off=lambda: may_hand_off,
    )
    try:
        call = asyncio.create_task(Tools(sandbox).run_workspace_command("api", "pytest  -q"))
    finally:
        unbind_handoffs(token)
    await asyncio.sleep(0)

    call.cancel()  # the run stopped
    await asyncio.gather(call, return_exceptions=True)

    if may_hand_off:
        assert not sandbox.cancelled
        [item] = handoffs.list(session_id="s1")
        assert item["state"] == "running" and item["label"] == "pytest -q"
        sandbox.release.set()
        await asyncio.gather(*list(handoffs._tasks.values()), return_exceptions=True)
        [report] = chat.reports
        assert "Exit code: 0" in report and "12 passed" in report
    else:
        assert sandbox.cancelled
        assert handoffs.list(session_id="s1") == []


@pytest.mark.unit
@pytest.mark.no_network
async def test_an_uninterrupted_command_returns_its_full_result():
    sandbox = FakeSandbox()
    sandbox.release.set()

    result = await Tools(sandbox).run_workspace_command("api", "pytest")

    assert result["stdout"] == "collected 12 items\n12 passed"


@pytest.mark.unit
def test_the_report_keeps_how_it_ended_and_the_end_of_the_output():
    text = describe_command_result({
        "stdout": "x" * 5_000 + "TAIL", "stderr": "boom", "exit_code": 137,
        "timed_out": False, "killed": True, "kill_reason": "Exceeded the memory limit.",
        "output_truncated": False,
    })

    assert "Exit code: 137" in text and "Exceeded the memory limit." in text
    assert text.count("x") <= 2_000 and "TAIL" in text and "boom" in text
