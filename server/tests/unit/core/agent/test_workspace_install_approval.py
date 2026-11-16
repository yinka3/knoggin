import pytest

from core.agent.tool_events import (
    RESULTS,
    DecisionBroker,
    bind_decision_broker,
    stream_tool_calls,
    unbind_decision_broker,
)
from core.agent.tools.commands import WorkspaceCommandTools
from core.sandbox.manager import SandboxManager


class RecordingSandbox:
    def __init__(self):
        self.calls = []

    validate_python_package = staticmethod(SandboxManager.validate_python_package)

    async def install_python_package(self, project_id, package, **kwargs):
        self.calls.append((project_id, package, kwargs["timeout_seconds"]))
        return {"package": package, "installed": True}


@pytest.mark.no_network
async def test_install_requires_approval_of_exact_pinned_package():
    sandbox = RecordingSandbox()
    tools = WorkspaceCommandTools()
    tools.project_id = "project-1"
    tools.sandbox = sandbox

    assert await tools.install_python_package("colorama==0.4.6") == {
        "package": "colorama==0.4.6", "approved": False, "installed": False,
    }
    assert sandbox.calls == []
    with pytest.raises(ValueError, match="exact version"):
        await tools.install_python_package("colorama")

    broker = DecisionBroker()
    token = bind_decision_broker(broker)
    try:
        stream = stream_tool_calls([
            ("call-1", "install_python_package", lambda: tools.install_python_package("colorama==0.4.6"))
        ])
        asked = await anext(stream)
        assert asked["event"] == "decision_required"
        assert "colorama==0.4.6" in asked["data"]["question"]
        assert asked["data"]["default"] == "deny"
        assert sandbox.calls == []
        assert broker.answer(asked["data"]["decision_id"], "approve")
        events = [event async for event in stream]
        assert events[-1]["event"] == RESULTS
        assert events[-1]["data"][0]["approved"] is True
        assert sandbox.calls == [("project-1", "colorama==0.4.6", 300)]
    finally:
        unbind_decision_broker(token)
