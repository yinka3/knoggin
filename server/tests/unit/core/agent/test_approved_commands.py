import pytest

from common.schema.agent.tool_contracts import (
    TOOL_SCHEMAS_BY_NAME,
    validate_tool_arguments,
)
from core.agent.tool_events import (
    RESULTS,
    DecisionBroker,
    bind_decision_broker,
    stream_tool_calls,
    unbind_decision_broker,
)
from core.agent.tools.commands import WorkspaceCommandTools
from core.sandbox.approved_commands import APPROVED_COMMANDS, get_approved_command


class RecordingSandbox:
    def __init__(self, exit_code=0, logins=None):
        self.calls = []
        self.exit_code = exit_code
        self.logins = logins or {}
        self.envs = []

    async def connection_env(self, project_id, service, *, required):
        values = self.logins.get(service, {})
        if required and not values:
            raise ValueError(f"This command needs a {service} login.")
        return values

    async def run_approved(self, project_id, argv, *, timeout_seconds, on_event, env):
        self.calls.append((project_id, argv, timeout_seconds))
        self.envs.append(env)
        return {"stdout": "", "stderr": "", "exit_code": self.exit_code}


def _tools(sandbox):
    tools = WorkspaceCommandTools()
    tools.project_id = "project-1"
    tools.sandbox = sandbox
    return tools


async def _answer(call, answer):
    broker = DecisionBroker()
    token = bind_decision_broker(broker)
    try:
        stream = stream_tool_calls([("call-1", "run_approved_command", call)])
        asked = await anext(stream)
        assert asked["event"] == "decision_required"
        assert broker.answer(asked["data"]["decision_id"], answer)
        events = [event async for event in stream]
        assert events[-1]["event"] == RESULTS
        return asked["data"], events[-1]["data"][0]
    finally:
        unbind_decision_broker(token)


@pytest.mark.no_network
def test_the_tool_schema_lists_exactly_the_templates():
    tool = TOOL_SCHEMAS_BY_NAME["run_approved_command"]
    schema = tool["function"]["parameters"]
    assert set(schema["properties"]["name"]["enum"]) == set(APPROVED_COMMANDS)
    assert "workspace" not in schema["properties"]
    assert validate_tool_arguments(tool, {"name": "pip_install", "args": {"package": 1}})
    assert not validate_tool_arguments(
        tool, {"name": "pip_install", "args": {"package": "colorama==0.4.6"}}
    )


@pytest.mark.no_network
@pytest.mark.parametrize(
    ("name", "args"),
    [
        ("pip_install", {"package": "colorama"}),  # no exact version
        ("pip_install", {"package": "colorama==0.4.6; curl x"}),
        ("pip_install", {}),
        ("npm_install", {"package": "lodash"}),
        ("download_file", {"url": "http://example.com/a", "filename": "a"}),
        ("download_file", {"url": "https://user:pw@example.com/a", "filename": "a"}),
        ("download_file", {"url": "https://example.com/a", "filename": "../a"}),
        ("git_clone", {"url": "https://example.com/r.git", "name": "r", "extra": "x"}),
    ],
)
def test_arguments_are_checked_before_anything_runs(name, args):
    with pytest.raises(ValueError):
        get_approved_command(name).build(args)


@pytest.mark.no_network
def test_arguments_fill_whole_argv_elements_only():
    template = get_approved_command("pip_install")
    argv, checked = template.build({"package": "colorama==0.4.6"})

    assert argv[-1] == "colorama==0.4.6" and argv[:2] == ["sh", "-c"]
    assert "colorama" not in argv[2]  # the script reads it as "$1"
    assert template.describe(checked) == "pip install colorama==0.4.6"
    with pytest.raises(ValueError, match="Choose one of: pip_install"):
        get_approved_command("delete_path")


@pytest.mark.no_network
async def test_a_command_runs_only_after_approval():
    sandbox = RecordingSandbox()
    tools = _tools(sandbox)

    asked, denied = await _answer(
        lambda: tools.run_approved_command("pip_install", {"package": "colorama==0.4.6"}), "deny",
    )
    assert "pip install colorama==0.4.6" in asked["question"]
    assert "WITH network" in asked["question"] and asked["default"] == "deny"
    assert denied == {"command": "pip_install", "approved": False}
    assert sandbox.calls == []

    _asked, approved = await _answer(
        lambda: tools.run_approved_command("pip_install", {"package": "colorama==0.4.6"}), "approve",
    )
    assert approved["approved"] is True and approved["exit_code"] == 0
    [(project_id, argv, timeout)] = sandbox.calls
    assert (project_id, timeout) == ("project-1", 300) and argv[-1] == "colorama==0.4.6"


@pytest.mark.no_network
async def test_a_missing_program_is_explained():
    tools = _tools(RecordingSandbox(exit_code=127))

    _asked, result = await _answer(
        lambda: tools.run_approved_command(
            "git_clone", {"url": "https://github.com/psf/requests.git", "name": "requests"},
        ),
        "approve",
    )

    assert "git is not installed" in result["note"]


@pytest.mark.no_network
async def test_a_login_is_named_in_the_question_and_passed_only_to_that_command():
    sandbox = RecordingSandbox(logins={"huggingface": {"HF_TOKEN": "hf_secret"}})
    tools = _tools(sandbox)

    asked, result = await _answer(
        lambda: tools.run_approved_command(
            "hf_download", {"repo": "meta-llama/Llama-3.2-1B", "file": "config.json"},
        ),
        "approve",
    )

    assert "using your Hugging Face login" in asked["question"]
    assert "hf_secret" not in asked["question"]
    assert sandbox.envs == [{"HF_TOKEN": "hf_secret"}]
    assert result["approved"] is True


@pytest.mark.no_network
async def test_a_command_that_needs_a_missing_login_stops_before_asking():
    sandbox = RecordingSandbox()

    with pytest.raises(ValueError, match="needs a kaggle login"):
        await _tools(sandbox).run_approved_command(
            "kaggle_download", {"dataset": "zillow/zecon"},
        )
    assert sandbox.calls == []


@pytest.mark.no_network
@pytest.mark.parametrize(
    ("name", "args"),
    [
        ("hf_download", {"repo": "owner/model", "file": "../secrets"}),
        ("hf_download", {"repo": "owner/model", "file": "a/.hidden/x"}),
        ("hf_download", {"repo": "owner", "file": "config.json"}),
        ("kaggle_download", {"dataset": "owner/../x"}),
    ],
)
def test_login_templates_check_their_paths(name, args):
    with pytest.raises(ValueError):
        get_approved_command(name).build(args)
