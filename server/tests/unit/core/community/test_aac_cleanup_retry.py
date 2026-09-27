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
