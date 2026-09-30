from unittest.mock import Mock

import pytest

from common.conf.manager import ConfigManager


@pytest.fixture
def manager(tmp_path):
    ConfigManager._instance = None
    owner = ConfigManager.initialize(tmp_path)
    yield owner
    ConfigManager._instance = None


def test_reload_publishes_changed_subtrees_without_rewriting_source(manager):
    aliases, models = [], []
    manager.subscribe(aliases.append, "user_aliases")
    manager.subscribe(models.append, "llm.agent_model")
    source = "user_aliases: [Ada]\n"
    manager.config_file.write_text(source, encoding="utf-8")
    assert manager.load()
    assert aliases == [[], ["Ada"]]
    assert len(models) == 1
    assert manager.config_file.read_text(encoding="utf-8") == source
    assert manager.last_apply_status.fully_applied


def test_failed_subscriber_does_not_skip_others_and_retries_latest_state(manager):
    received, attempts = [], []
    fail = False

    def subscriber(value):
        attempts.append(value)
        if fail:
            raise RuntimeError("SECRET-SHOULD-NOT-BE-LOGGED")

    unsubscribe = manager.subscribe(subscriber, "user_aliases")
    manager.subscribe(received.append, "user_aliases")
    fail = True
    assert manager.update_settings({"user_aliases": ["Ada"]})
    status = manager.last_apply_status
    assert status.persisted and status.activated and not status.fully_applied
    assert len(status.failed_subscriptions) == 1
    assert received[-1] == ["Ada"]
    assert manager.update_settings({"user_aliases": ["Grace"]})
    assert attempts[-1] == ["Grace"]
    source = manager.config_file.read_bytes()
    fail = False
    status = manager.retry_failed_applies()
    assert status.fully_applied
    assert attempts[-1] == ["Grace"]
    assert manager.config_file.read_bytes() == source
    unsubscribe()
    unsubscribe()
    assert manager.retry_failed_applies().fully_applied


def test_failed_subscriber_logs_only_registration_id(manager, monkeypatch):
    from common.conf import manager as module

    errors = Mock()
    monkeypatch.setattr(module.logger, "error", errors)
    fail = False

    def subscriber(value):
        if fail:
            raise ValueError("private-token")

    manager.subscribe(subscriber, "user_aliases")
    fail = True
    assert manager.update_settings({"user_aliases": ["private-value"]})
    assert "private-token" not in str(errors.call_args_list)
    assert "private-value" not in str(errors.call_args_list)
    assert "private" not in repr(manager.last_apply_status)


def test_nested_update_coalesces_pending_callbacks_to_latest_generation(manager):
    observed = []
    enabled = False

    def reenter(value):
        if enabled and value == ["first"]:
            assert manager.update_settings({"user_aliases": ["latest"]})
            assert manager.last_apply_status.pending_subscriptions
            assert not manager.last_apply_status.fully_applied

    manager.subscribe(reenter, "user_aliases")
    manager.subscribe(observed.append, "user_aliases")
    enabled = True
    assert manager.update_settings({"user_aliases": ["first"]})
    assert observed == [[], ["latest"]]
    assert manager.config.user_aliases == ["latest"]
    assert manager.last_apply_status.fully_applied


def test_persistence_failure_does_not_publish_or_notify(manager, monkeypatch):
    received = []
    manager.subscribe(received.append, "user_aliases")
    previous_generation = manager.last_apply_status.generation
    monkeypatch.setattr(manager, "_persist_config", lambda config: False)
    assert manager.update_settings({"user_aliases": ["Ada"]}) is False
    assert received == [[]]
    assert manager.config.user_aliases == []
    status = manager.last_apply_status
    assert status.generation == previous_generation
    assert not status.persisted and not status.activated


def test_failed_reload_preserves_state_and_reports_no_activation(manager):
    previous = manager.config
    manager.config_file.write_text("false", encoding="utf-8")
    assert not manager.load()
    assert manager.config == previous
    assert not manager.last_apply_status.activated


def test_apply_retry_does_not_claim_failed_reload_was_persisted(manager):
    manager.config_file.write_text("false", encoding="utf-8")
    assert not manager.load()
    status = manager.retry_failed_applies()
    assert status.fully_applied
    assert not status.persisted
    assert manager.config_file.read_text(encoding="utf-8") == "false"


def test_reload_accepts_config_but_reports_failed_service_apply(manager):
    fail = False

    def subscriber(value):
        if fail:
            raise RuntimeError("apply")

    manager.subscribe(subscriber, "user_aliases")
    fail = True
    manager.config_file.write_text("user_aliases: [Ada]", encoding="utf-8")
    assert manager.load()
    assert manager.config.user_aliases == ["Ada"]
    assert manager.last_apply_status.persisted
    assert not manager.last_apply_status.fully_applied
    fail = False
    assert manager.retry_failed_applies().fully_applied
