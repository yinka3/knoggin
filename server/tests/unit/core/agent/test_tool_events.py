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


@pytest.mark.no_network
async def test_a_tool_can_ask_the_user_and_wait_for_the_answer():
    from core.agent.tool_events import DecisionBroker, ask_user, bind_decision_broker

    broker = DecisionBroker()
    bind_decision_broker(broker)

    async def tool():
        return await ask_user("Run with network?", ("approve", "deny"),
                              timeout_seconds=5, default="deny")

    stream = stream_tool_calls([("c1", "run_command", tool)])
    asked = await anext(stream)
    assert asked["event"] == "decision_required"
    assert asked["data"]["options"] == ["approve", "deny"] and asked["data"]["default"] == "deny"
    decision_id = asked["data"]["decision_id"]

    with pytest.raises(ValueError):
        broker.answer(decision_id, "maybe")  # not an offered choice
    assert broker.answer("unknown", "approve") is False
    assert broker.answer(decision_id, "approve") is True

    rest = [item async for item in stream]
    assert rest[0] == {"event": "decision_resolved", "data": {
        "decision_id": decision_id, "choice": "approve", "call_id": "c1", "tool": "run_command",
    }}
    assert rest[-1]["data"] == ["approve"]
    assert broker.answer(decision_id, "deny") is False  # already answered


@pytest.mark.no_network
async def test_an_unanswered_or_abandoned_question_gets_the_default():
    from core.agent.tool_events import DecisionBroker, ask_user, bind_decision_broker

    assert await ask_user("No run here", ("a", "b"), timeout_seconds=1, default="b") == "b"

    bind_decision_broker(DecisionBroker())
    assert await ask_user("Too slow", ("a", "b"), timeout_seconds=0.01, default="a") == "a"

    broker = DecisionBroker()
    bind_decision_broker(broker)
    waiting = asyncio.create_task(ask_user("Run ends", ("a", "b"), timeout_seconds=5, default="b"))
    await asyncio.sleep(0)
    broker.close()  # the run finished first
    assert await waiting == "b"


@pytest.mark.no_network
async def test_cancelling_the_stream_mid_wait_leaves_no_pending_waiter():
    import asyncio as _asyncio

    from core.agent.tool_events import stream_tool_calls

    started = _asyncio.Event()

    async def slow():
        started.set()
        await _asyncio.Event().wait()

    async def consume():
        async for _event in stream_tool_calls([("c1", "slow_tool", slow)]):
            pass

    before = set(_asyncio.all_tasks())
    consumer = _asyncio.create_task(consume())
    await started.wait()
    await _asyncio.sleep(0)
    consumer.cancel()
    await _asyncio.gather(consumer, return_exceptions=True)
    await _asyncio.sleep(0)

    leftover = [task for task in _asyncio.all_tasks() - before if not task.done()]
    assert leftover == []
