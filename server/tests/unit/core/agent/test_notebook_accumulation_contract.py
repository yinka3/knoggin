from types import SimpleNamespace

import pytest

from common.schema.agent.identity import AgentConfig
from core.agent.executor import AgentExecutor
from core.agent.executor import _ToolCall as ToolCall
from core.agent.run import AgentIdentity, AgentRun, AgentRunLimits


def make_run():
    return AgentRun.open(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        user_query="What changed?",
        run_id="run-1",
        agent=AgentIdentity(
            config=AgentConfig(
                id="agent-1",
                name="Researcher",
                persona={
                    "attention_bias": "evidence",
                    "reasoning_style": "methodical",
                    "social_temperament": "calm",
                    "communication_signature": "clear",
                    "productive_flaw": "overexplains",
                },
            ),
            name="Researcher",
            persona="Careful and evidence-led",
        ),
        limits=AgentRunLimits(max_calls=2),
    )


@pytest.mark.no_network
async def test_executor_accumulates_raw_result_before_model_localization(monkeypatch):
    run = make_run()
    executor = AgentExecutor(run, SimpleNamespace(), SimpleNamespace())

    raw_result = {
        "data": {
            "resolution": "recent",
            "results": [
                {"episodes": [{"episode_id": "real-episode-id", "summary": "A change"}]}
            ],
        }
    }

    async def fake_execute(_tools, _name, _args):
        return raw_result

    def fake_localize(_ctx, _name, _result):
        return {
            "data": {
                "resolution": "recent",
                "results": [
                    {"episodes": [{"episode_id": "ep_1", "summary": "A change"}]}
                ],
            }
        }

    monkeypatch.setattr("core.agent.executor.execute_tool", fake_execute)
    monkeypatch.setattr("core.agent.executor.localize_agent_tool_result", fake_localize)

    results = []
    events = [
        event
        async for event in executor._execute_tools(
            [ToolCall("read_recent_episodes", {}, call_id="recent-1")],
            results,
        )
    ]

    assert not [event for event in events if event["event"] == "tool_error"]
    assert run.notebook.section_items("episodes")[0]["episode_id"] == (
        "real-episode-id"
    )
    assert results[0]["result"]["data"]["results"][0]["episodes"][0]["episode_id"] == "ep_1"
    assert run.notebook.last_applied_references == ("episode:real-episode-id",)
