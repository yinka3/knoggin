import asyncio
from types import SimpleNamespace

import pytest

from common.schema.settings import RootConfig
from core.agent.tool_events import bind_handoffs, finish_or_hand_off, unbind_handoffs
from core.session.tool_handoffs import HandoffLimitReached, ToolHandoffs


class Chat:
    def __init__(self):
        self.reports = []

    async def queue_report(self, text, *, origin, metadata):
        self.reports.append((origin, metadata, text))
        return {"queued": True}


def _handoffs():
    chat = Chat()

    async def get_or_resume_session(_session_id):
        return chat

    sessions = SimpleNamespace(user_name="ada", get_or_resume_session=get_or_resume_session)
    return ToolHandoffs(sessions, SimpleNamespace(config=RootConfig())), chat


async def _settle(handoffs):
    await asyncio.gather(*list(handoffs._tasks.values()), return_exceptions=True)


@pytest.mark.unit
@pytest.mark.no_network
async def test_handed_off_work_reports_when_done_and_the_chat_limit_holds():
    handoffs, chat = _handoffs()
    release = asyncio.Event()

    async def build():
        await release.wait()
        return "12 passed"

    item = handoffs.adopt(asyncio.create_task(build()), project_id="p1", session_id="s1",
                          tool="run_command", label="pytest")
    with pytest.raises(HandoffLimitReached):  # the default is one per chat
        handoffs.adopt(asyncio.create_task(build()), project_id="p1", session_id="s1",
                       tool="run_command", label="second")
    assert [entry["state"] for entry in handoffs.list(session_id="s1")] == ["running"]

    release.set()
    await _settle(handoffs)
    finished = handoffs.list(session_id="s1")[0]
    assert finished["item_id"] == item["item_id"] and finished["state"] == "completed"
    assert finished["result"] == "12 passed"
    origin, metadata, text = chat.reports[0]
    assert origin == "tool_handoff_report" and metadata == {"item_id": item["item_id"]}
    assert "12 passed" in text


@pytest.mark.unit
@pytest.mark.no_network
async def test_the_user_can_stop_handed_off_work():
    handoffs, chat = _handoffs()
    cancelled = asyncio.Event()

    async def forever():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    item = handoffs.adopt(asyncio.create_task(forever()), project_id="p1", session_id="s1",
                          tool="run_command", label="server")
    await asyncio.sleep(0)
    assert await handoffs.stop(item["item_id"], session_id="other") is None
    stopped = await handoffs.stop(item["item_id"], session_id="s1")
    assert stopped["state"] == "stopped" and cancelled.is_set()
    assert "stopped it" in chat.reports[0][2]


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize("user_pressed_stop", [False, True])
async def test_a_stopped_run_keeps_its_work_unless_the_user_asked_to_stop(user_pressed_stop):
    handoffs, chat = _handoffs()
    release = asyncio.Event()
    work_cancelled = asyncio.Event()

    async def work():
        try:
            await release.wait()
            return "done"
        except asyncio.CancelledError:
            work_cancelled.set()
            raise

    async def tool():
        token = bind_handoffs(handoffs, project_id="p1", session_id="s1",
                              may_hand_off=lambda: not user_pressed_stop)
        try:
            return await finish_or_hand_off(work(), tool="run_command", label="build")
        finally:
            unbind_handoffs(token)

    running = asyncio.create_task(tool())
    await asyncio.sleep(0)
    running.cancel()  # the run stops
    with pytest.raises(asyncio.CancelledError):
        await running

    if user_pressed_stop:
        await asyncio.sleep(0)
        assert work_cancelled.is_set() and handoffs.list(session_id="s1") == []
    else:
        release.set()
        await _settle(handoffs)
        assert not work_cancelled.is_set()
        assert handoffs.list(session_id="s1")[0]["state"] == "completed"
        assert "done" in chat.reports[0][2]


@pytest.mark.unit
@pytest.mark.no_network
async def test_work_stopped_before_its_follower_starts_is_still_cancelled():
    handoffs, _chat = _handoffs()
    work = asyncio.create_task(asyncio.Event().wait())

    item = handoffs.adopt(work, project_id="p1", session_id="s1",
                          tool="run_command", label="server")
    # No yield: the follower has not taken its first step yet.
    stopped = await handoffs.stop(item["item_id"], session_id="s1")
    await asyncio.gather(work, return_exceptions=True)

    assert work.cancelled()
    assert stopped["state"] == "stopped"
    assert handoffs._tasks == {}
