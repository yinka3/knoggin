import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from core.community.aac_store import AACStore
from core.community.runtime import AACRuntime
from core.community.token_budget import AACTokenBudget


def runtime(store):
    return AACRuntime(user_name="ada", resources=SimpleNamespace(),
                      agent_manager=SimpleNamespace(), read_context=SimpleNamespace(),
                      store=store, seeder=object())


async def test_failed_terminal_write_survives_shutdown_for_retry():
    store = AsyncMock()
    store.finish_discussion.side_effect = [RuntimeError("write"), RuntimeError("retry"), None]
    owner = runtime(store)
    await owner._run_discussion(discussion_id="d", topic="topic", participants=[], budget=AACTokenBudget(0))
    assert owner._pending_finalizations["d"]["end_reason"] == "token_budget"
    with pytest.raises(RuntimeError, match="shutdown cleanup"):
        await owner.shutdown()
    assert "d" in owner._pending_finalizations
    await owner.shutdown()
    assert not owner._pending_finalizations
    assert store.finish_discussion.await_count == 3


async def test_failed_unsubscribe_is_retained_and_retried():
    owner = runtime(AsyncMock())
    unsubscribe = Mock(side_effect=[RuntimeError("unsubscribe"), None])
    owner._config_unsubscribe = unsubscribe
    with pytest.raises(RuntimeError, match="shutdown cleanup"):
        await owner.shutdown()
    assert owner._config_unsubscribe is unsubscribe
    await owner.shutdown()
    assert owner._config_unsubscribe is None
    assert unsubscribe.call_count == 2


async def test_stop_write_failure_reuses_event_identity_on_retry():
    store = AsyncMock()
    store.append_timeline.side_effect = [RuntimeError("write"), "saved"]
    owner = runtime(store)
    owner._discussion_id = "d"
    owner._discussion_history = []
    owner._discussion_task = asyncio.create_task(asyncio.Event().wait())
    try:
        with pytest.raises(RuntimeError, match="write"):
            await owner.request_stop()
        assert owner._discussion_stop_event.is_set()
        assert await owner.request_stop()
        calls = store.append_timeline.await_args_list
        assert calls[0].kwargs == calls[1].kwargs
        assert not owner._pending_stop_events
        assert len(owner._discussion_history) == 1
    finally:
        owner._discussion_task.cancel()
        await asyncio.gather(owner._discussion_task, return_exceptions=True)


async def test_timeline_replay_requires_same_scoped_content():
    client = AsyncMock()
    client.execute.return_value = 0
    client.fetch_all.return_value = [{"kind": "system_event", "agent_id": None, "content": "stop"}]
    store = AACStore(client)
    args = dict(discussion_id="d", user_name="ada", kind="system_event", content="stop", timeline_id="event")
    assert await store.append_timeline(**args) == "event"
    with pytest.raises(ValueError, match="conflicts"):
        await store.append_timeline(**{**args, "content": "different"})


@pytest.mark.parametrize("status", ["completed", "failed"])
async def test_terminal_retry_accepts_only_matching_saved_outcome(status):
    client = AsyncMock()
    client.execute.return_value = 0
    client.fetch_all.return_value = [{"status": status, "end_reason": "token_budget", "tokens_used": 5}]
    args = dict(discussion_id="d", user_name="ada", status="completed", end_reason="token_budget", tokens_used=5)
    if status == "completed":
        await AACStore(client).finish_discussion(**args)
    else:
        with pytest.raises(ValueError, match="conflicts"):
            await AACStore(client).finish_discussion(**args)
