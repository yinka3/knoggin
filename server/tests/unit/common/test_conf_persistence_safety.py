import traceback
from unittest.mock import Mock

import pytest

from common.conf import manager as module
from common.conf.manager import ConfigManager, ConfigurationLoadError


@pytest.fixture
def manager(tmp_path):
    ConfigManager._instance = None
    owner = ConfigManager.initialize(tmp_path)
    yield owner
    ConfigManager._instance = None


def test_unicode_round_trip_and_save_does_not_change_umask(manager, monkeypatch):
    def forbidden(*args):
        raise AssertionError("process umask must not change")

    monkeypatch.setattr(module.os, "umask", forbidden)
    aliases = ["Adéyì", "日本語", "🧠"]
    assert manager.update_settings({"user_aliases": aliases})
    assert "日本語" in manager.config_file.read_text(encoding="utf-8")
    assert manager.load()
    assert manager.config.user_aliases == aliases


@pytest.mark.parametrize("source", [
    "llm:\n  api_key: [private-token",
    "llm:\n  api_key: [private-token]\n",
    "private-token: 1\n",
])
def test_load_diagnostics_do_not_expose_values_or_unknown_keys(manager, monkeypatch, source):
    errors = Mock()
    monkeypatch.setattr(module.logger, "error", errors)
    manager.config_file.write_text(source, encoding="utf-8")
    assert not manager.load()
    assert "private-token" not in str(errors.call_args_list)
    with pytest.raises(ConfigurationLoadError) as error:
        manager.load(require_valid=True)
    assert "private-token" not in "".join(traceback.format_exception(error.value))


def test_update_and_write_failures_do_not_expose_inputs_or_exception_text(manager, monkeypatch):
    errors = Mock()
    monkeypatch.setattr(module.logger, "error", errors)
    assert not manager.update_settings({"llm": {"api_key": ["private-token"]}})
    assert not manager.update_settings({"private-token": "private-value"})

    def fail_replace(*args):
        raise OSError("private-token")

    before = manager.config_file.read_bytes()
    files = set(manager.config_dir.iterdir())
    monkeypatch.setattr(type(manager.config_file), "replace", fail_replace)
    assert not manager.update_settings({"llm": {"api_key": "private-token"}})
    assert manager.config_file.read_bytes() == before
    assert set(manager.config_dir.iterdir()) == files
    assert "private-token" not in str(errors.call_args_list)
    assert "private-value" not in str(errors.call_args_list)


def test_runtime_validator_error_is_not_exposed_in_startup_trace(tmp_path, monkeypatch):
    ConfigManager._instance = None

    def fail(config):
        raise ValueError("private-token")

    monkeypatch.setattr(ConfigManager, "_validate_runtime_config", staticmethod(fail))
    with pytest.raises(ConfigurationLoadError) as error:
        ConfigManager.initialize(tmp_path)
    assert "private-token" not in "".join(traceback.format_exception(error.value))
    assert ConfigManager._instance is None


def test_failed_stream_creation_closes_descriptor_and_removes_temp(manager, monkeypatch):
    descriptors = []
    mkstemp = module.tempfile.mkstemp

    def track(**kwargs):
        descriptor, path = mkstemp(**kwargs)
        descriptors.append(descriptor)
        return descriptor, path

    def fail(*args, **kwargs):
        raise OSError("stream creation")

    files = set(manager.config_dir.iterdir())
    monkeypatch.setattr(module.tempfile, "mkstemp", track)
    monkeypatch.setattr(module.os, "fdopen", fail)
    assert not manager.save()
    assert set(manager.config_dir.iterdir()) == files
    with pytest.raises(OSError):
        module.os.fstat(descriptors[0])
