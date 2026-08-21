from unittest.mock import patch

import pytest

from common.conf.manager import ConfigManager, deep_merge
from common.schema.agent.settings import AgentLimitSettings
from common.schema.settings import LLMSettings, RootConfig


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

    # Update unrelated setting (should not fire)
    mgr.update_settings(
        {"developer_settings": {"jobs": {"episode": {"max_narrative_chars": 5000}}}}
    )
    assert len(received_updates) == 1

    # Update related setting (should fire)
    mgr.update_settings({"developer_settings": {"jobs": {"cleaner": {"stale_junk_days": 42}}}})
    assert len(received_updates) == 2
    assert received_updates[-1] == 42

    # Unsubscribe
    unsubscribe()
    mgr.update_settings({"developer_settings": {"jobs": {"cleaner": {"stale_junk_days": 100}}}})
    assert len(received_updates) == 2  # Did not fire again


@pytest.mark.unit
@pytest.mark.no_network
def test_root_config_rejects_unknown_top_level_and_nested_keys():
    with pytest.raises(ValueError, match="llmm"):
        RootConfig.model_validate({"llmm": {}})

    with pytest.raises(ValueError, match="agent_modell"):
        RootConfig.model_validate({"llm": {"agent_modell": "invalid"}})


@pytest.mark.unit
@pytest.mark.no_network
def test_failed_config_reload_keeps_the_previous_valid_config(
    mock_config_paths,
    reset_config_manager,
):
    mgr = ConfigManager.get()
    mgr.config = RootConfig(llm=LLMSettings(agent_model="known-good"))
    invalid_source = "llm:\n  agent_modell: typo\n"
    mock_config_paths["yaml"].write_text(invalid_source, encoding="utf-8")

    mgr.load()

    assert mgr.config.llm.agent_model == "known-good"
    assert mock_config_paths["yaml"].read_text(encoding="utf-8") == invalid_source


@pytest.mark.unit
@pytest.mark.no_network
def test_agent_limits_reject_boolean_and_duplicate_normalized_overrides():
    with pytest.raises(ValueError):
        AgentLimitSettings(tool_limit_overrides={"search_entity": True})

    with pytest.raises(ValueError, match="duplicate"):
        AgentLimitSettings(
            tool_limit_overrides={" search_entity ": 2, "search_entity": 3}
        )


@pytest.mark.unit
@pytest.mark.no_network
def test_unknown_tool_limit_does_not_activate(mock_config_paths, reset_config_manager):
    mgr = ConfigManager.get()
    previous = mgr.config

    assert mgr.update_settings(
        {
            "developer_settings": {
                "limits": {"tool_limit_overrides": {"not_a_tool": 2}}
            }
        }
    ) is False
    assert mgr.config == previous


@pytest.mark.unit
@pytest.mark.no_network
def test_registered_tool_limit_without_a_default_limit_is_allowed(
    mock_config_paths,
    reset_config_manager,
):
    mgr = ConfigManager.get()

    assert mgr.update_settings(
        {
            "developer_settings": {
                "limits": {"tool_limit_overrides": {"check_graph_health": 2}}
            }
        }
    ) is True
    assert (
        mgr.config.developer_settings.limits.tool_limit_overrides[
            "check_graph_health"
        ]
        == 2
    )
