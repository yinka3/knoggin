import asyncio

import pytest

from core.agent.tool_events import RESULTS, emit_tool_event, stream_tool_calls


@pytest.mark.no_network
async def test_events_arrive_while_a_tool_runs_and_each_call_keeps_its_own():
    release = asyncio.Event()
    seen = []

    async def slow():
        emit_tool_event("command_output", {"text": "line 1"})
        await release.wait()
        emit_tool_event("command_output", {"text": "line 2"})
        return "slow done"

    async def failing():
        emit_tool_event("note", {"text": "about to fail"})
        raise ValueError("broken")

    stream = stream_tool_calls([("c1", "run", slow), ("c2", "other", failing)])
    async for item in stream:
        seen.append(item)
        if item["event"] == RESULTS:
            break
        if item["data"].get("text") == "line 1":
            # The first event reached us before the tool finished.
            assert not release.is_set()
            release.set()

    events = [(item["data"]["call_id"], item["data"]["text"]) for item in seen[:-1]]
    assert sorted(events) == [("c1", "line 1"), ("c1", "line 2"), ("c2", "about to fail")]
    assert seen[0]["data"]["tool"] in {"run", "other"}
    results = seen[-1]["data"]
    assert results[0] == "slow done" and isinstance(results[1], ValueError)


@pytest.mark.no_network
async def test_closing_the_stream_cancels_running_calls():
    cancelled = asyncio.Event()

    async def forever():
        emit_tool_event("started", {})
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    stream = stream_tool_calls([("c1", "run", forever)])
    assert (await anext(stream))["event"] == "started"
    await stream.aclose()
    assert cancelled.is_set()


@pytest.mark.no_network
def test_emitting_outside_a_run_does_nothing():
    emit_tool_event("command_output", {"text": "ignored"})
