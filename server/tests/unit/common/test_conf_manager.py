import os
import json
import yaml
import pytest
from unittest.mock import patch, MagicMock

from common.conf.manager import ConfigManager, deep_merge
from common.schema.settings import RootConfig


@pytest.fixture
def mock_config_paths(tmp_path):
    yaml_path = tmp_path / "knoggin.yml"
    with patch("common.conf.manager.CONFIG_DIR", tmp_path), \
         patch("common.conf.manager.CONFIG_FILE_YAML", yaml_path):
        yield {"yaml": yaml_path, "dir": tmp_path}


@pytest.fixture
def reset_config_manager():
    """Ensure ConfigManager is reset before and after test."""
    ConfigManager._instance = None
    yield
    ConfigManager._instance = None


@pytest.mark.unit
@pytest.mark.no_network
def test_deep_merge():
    source = {
        "a": 1,
        "b": {"c": 2, "d": 3},
        "e": 5
    }
    updates = {
        "b": {"c": 9, "new_key": 4},
        "f": 6
    }
    
    result = deep_merge(source, updates)
    assert result == {
        "a": 1,
        "b": {"c": 9, "d": 3, "new_key": 4},
        "e": 5,
        "f": 6
    }


@pytest.mark.unit
@pytest.mark.no_network
def test_config_manager_loads_defaults_when_files_missing(mock_config_paths, reset_config_manager):
    mgr = ConfigManager.get()
    assert isinstance(mgr.config, RootConfig)
    
    # It should have saved the default config to YAML
    assert mock_config_paths["yaml"].exists()


@pytest.mark.unit
@pytest.mark.no_network
def test_config_manager_subscription_and_update(mock_config_paths, reset_config_manager):
    mgr = ConfigManager.get()
    
    received_updates = []
    def callback(val):
        received_updates.append(val)
        
    unsubscribe = mgr.subscribe(callback, path="developer_settings.jobs.cleaner.stale_junk_days")
    
    # Subscription fires immediately with current value
    assert len(received_updates) == 1
    initial_val = received_updates[0]
    
    # Update unrelated setting (should not fire)
    mgr.update_settings({"developer_settings": {"jobs": {"dlq": {"batch_size": 100}}}})
    assert len(received_updates) == 1
    
    # Update related setting (should fire)
    mgr.update_settings({"developer_settings": {"jobs": {"cleaner": {"stale_junk_days": 42}}}})
    assert len(received_updates) == 2
    assert received_updates[-1] == 42
    
    # Unsubscribe
    unsubscribe()
    mgr.update_settings({"developer_settings": {"jobs": {"cleaner": {"stale_junk_days": 100}}}})
    assert len(received_updates) == 2  # Did not fire again
