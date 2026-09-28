from unittest.mock import Mock

import pytest

from common.conf.manager import ConfigManager


@pytest.fixture
def manager(tmp_path):
    ConfigManager._instance = None
    owner = ConfigManager.initialize(tmp_path)
    yield owner
    ConfigManager._instance = None


@pytest.mark.parametrize("path", ["", "llmm", "llm.typo", "llm.model_dump", "llm..api_key", " llm", 3])
def test_invalid_paths_do_not_register_or_call(manager, path):
    callback = Mock()
    with pytest.raises(ValueError, match="subscription path"):
        manager.subscribe(callback, path)
    callback.assert_not_called()
    assert not manager.subscribers


def test_null_settings_are_delivered_initially_and_on_update(manager):
    received = []
    manager.subscribe(received.append, "llm.base_url")
    assert received == [None]
    assert manager.update_settings({"llm": {"base_url": "https://example.test"}})
    assert manager.update_settings({"llm": {"base_url": None}})
    assert received == [None, "https://example.test", None]


async def test_async_subscriber_is_rejected_even_for_null_setting(manager):
    async def callback(value):
        pass

    class AsyncCallable:
        async def __call__(self, value):
            pass

    for subscriber in (callback, AsyncCallable()):
        with pytest.raises(TypeError, match="synchronous"):
            manager.subscribe(subscriber, "llm.base_url")
    assert not manager.subscribers


def test_returned_coroutine_is_closed_and_failed_registration_removed(manager):
    async def apply():
        pass

    coroutine = apply()
    with pytest.raises(RuntimeError, match="Initial"):
        manager.subscribe(lambda value: coroutine)
    assert coroutine.cr_frame is None
    assert not manager.subscribers


def test_callback_signature_must_accept_one_value(manager):
    with pytest.raises(TypeError, match="one settings value"):
        manager.subscribe(lambda first, second: None)
    assert not manager.subscribers


def test_duplicate_registration_handles_are_independent(manager):
    received = []
    first = manager.subscribe(received.append, "user_aliases")
    second = manager.subscribe(received.append, "user_aliases")
    first()
    first()
    assert manager.update_settings({"user_aliases": ["Ada"]})
    assert received == [[], [], ["Ada"]]
    second()
    second()
    assert not manager.subscribers


def test_callback_removing_another_subscription_skips_queued_delivery(manager):
    enabled = False
    received = []

    def first(value):
        if enabled:
            unsubscribe()

    manager.subscribe(first, "user_aliases")
    unsubscribe = manager.subscribe(received.append, "user_aliases")
    enabled = True
    assert manager.update_settings({"user_aliases": ["Ada"]})
    assert received == [[]]
    assert manager.last_apply_status.fully_applied


def test_failed_initial_apply_leaves_no_registration_or_retry(manager):
    def fail(value):
        raise RuntimeError("secret")

    with pytest.raises(RuntimeError, match="Initial") as error:
        manager.subscribe(fail)
    assert "secret" not in str(error.value)
    assert not manager.subscribers
    assert manager.retry_failed_applies().failed_subscriptions == ()


def test_reentrant_initial_failure_clears_stale_apply_diagnostics(manager):
    first = True

    def callback(value):
        nonlocal first
        if first:
            first = False
            assert manager.update_settings({"user_aliases": ["Ada"]})
        raise RuntimeError("apply")

    with pytest.raises(RuntimeError, match="Initial"):
        manager.subscribe(callback, "user_aliases")
    assert manager.config.user_aliases == ["Ada"]
    assert not manager.subscribers
    assert manager.last_apply_status.failed_subscriptions == ()
    assert manager.last_apply_status.pending_subscriptions == ()
