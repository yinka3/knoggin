import pytest

from common.conf.manager import (
    ConfigManager,
    ConfigurationLoadError,
    ConfigurationPersistenceError,
    deep_merge,
)
from common.schema.agent.settings import AgentLimitSettings
from common.schema.settings import LLMSettings, RootConfig


@pytest.fixture
def reset_config_manager():
    """Ensure ConfigManager is reset before and after test."""
    ConfigManager._instance = None
    yield
    ConfigManager._instance = None


@pytest.fixture
def mock_config_paths(tmp_path, reset_config_manager):
    manager = ConfigManager.initialize(tmp_path)
    return {"yaml": tmp_path / "knoggin.yml", "dir": tmp_path, "manager": manager}


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
def test_config_manager_reuses_only_the_same_explicit_directory(
    mock_config_paths, tmp_path
):
    manager = mock_config_paths["manager"]

    assert ConfigManager.initialize(mock_config_paths["dir"] / ".") is manager
    with pytest.raises(RuntimeError, match="already initialized"):
        ConfigManager.initialize(tmp_path / "different")


@pytest.mark.unit
@pytest.mark.no_network
def test_config_manager_requires_initialization(reset_config_manager):
    with pytest.raises(RuntimeError, match="has not been initialized"):
        ConfigManager.get()


@pytest.mark.unit
@pytest.mark.no_network
def test_config_writes_do_not_follow_the_process_working_directory(
    mock_config_paths, tmp_path, monkeypatch
):
    manager = mock_config_paths["manager"]
    other_working_directory = tmp_path / "elsewhere"
    other_working_directory.mkdir()
    monkeypatch.chdir(other_working_directory)

    assert manager.update_settings({"user_aliases": ["Ada"]}) is True

    assert mock_config_paths["yaml"].exists()
    assert not (other_working_directory / "config" / "knoggin.yml").exists()
    assert manager.resolve_path("data/projects") == (
        mock_config_paths["dir"] / "data" / "projects"
    ).resolve()


@pytest.mark.unit
@pytest.mark.no_network
def test_config_manager_subscription_and_update(mock_config_paths, reset_config_manager):
    mgr = ConfigManager.get()

    received_updates = []
    def callback(val):
        received_updates.append(val)

    unsubscribe = mgr.subscribe(
        callback, path="developer_settings.jobs.conflict_discovery.interval_hours"
    )

    # Subscription fires immediately with current value
    assert len(received_updates) == 1

    # Update unrelated setting (should not fire)
    mgr.update_settings(
        {"developer_settings": {"jobs": {"episode": {"max_narrative_chars": 5000}}}}
    )
    assert len(received_updates) == 1

    # Update related setting (should fire)
    mgr.update_settings(
        {"developer_settings": {"jobs": {"conflict_discovery": {"interval_hours": 8}}}}
    )
    assert len(received_updates) == 2
    assert received_updates[-1] == 8

    # Unsubscribe
    unsubscribe()
    mgr.update_settings(
        {"developer_settings": {"jobs": {"conflict_discovery": {"interval_hours": 12}}}}
    )
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
def test_root_config_excludes_retired_identity_metadata():
    serialized = RootConfig().model_dump()

    assert "user_name" not in serialized
    assert "configured_at" not in serialized
    with pytest.raises(ValueError, match="user_name"):
        RootConfig.model_validate({"user_name": "stale"})


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

    assert mgr.load() is False

    assert mgr.config.llm.agent_model == "known-good"
    assert mock_config_paths["yaml"].read_text(encoding="utf-8") == invalid_source


@pytest.mark.unit
@pytest.mark.no_network
def test_invalid_existing_config_prevents_initialization(tmp_path, reset_config_manager):
    config_file = tmp_path / "knoggin.yml"
    config_file.write_text("llm:\n  agent_modell: typo\n", encoding="utf-8")

    with pytest.raises(ConfigurationLoadError, match="agent_modell"):
        ConfigManager.initialize(tmp_path)

    assert ConfigManager._instance is None


@pytest.mark.unit
@pytest.mark.no_network
def test_initial_config_write_failure_prevents_initialization(
    tmp_path, reset_config_manager, monkeypatch
):
    monkeypatch.setattr(ConfigManager, "save", lambda self, config=None: False)

    with pytest.raises(ConfigurationPersistenceError, match="initial configuration"):
        ConfigManager.initialize(tmp_path)

    assert ConfigManager._instance is None


@pytest.mark.unit
@pytest.mark.no_network
def test_failed_update_write_preserves_active_config_and_subscribers(
    mock_config_paths, monkeypatch
):
    manager = mock_config_paths["manager"]
    received = []
    manager.subscribe(received.append, "user_aliases")
    previous = manager.config
    monkeypatch.setattr(manager, "save", lambda config=None: False)

    assert manager.update_settings({"user_aliases": ["Ada"]}) is False
    assert manager.config is previous
    assert received == [[]]


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
def test_agent_limits_default_to_adaptive_project_briefing_and_validate_modes():
    assert AgentLimitSettings().project_briefing_mode == "adaptive"
    assert AgentLimitSettings(project_briefing_mode="always").project_briefing_mode == (
        "always"
    )
    with pytest.raises(ValueError):
        AgentLimitSettings(project_briefing_mode="never")


@pytest.mark.unit
@pytest.mark.no_network
def test_project_briefing_mode_is_persisted_by_config_updates(
    mock_config_paths,
    reset_config_manager,
):
    mgr = ConfigManager.get()

    assert mgr.update_settings(
        {
            "developer_settings": {
                "limits": {"project_briefing_mode": "always"}
            }
        }
    )
    assert mgr.config.developer_settings.limits.project_briefing_mode == "always"
    assert "project_briefing_mode: always" in mock_config_paths["yaml"].read_text(
        encoding="utf-8"
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
