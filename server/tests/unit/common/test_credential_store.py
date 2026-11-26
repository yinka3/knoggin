import json
import os
import stat

import pytest

from common import api_keys, credential_store


@pytest.fixture
def store(tmp_path, monkeypatch):
    path = tmp_path / "credentials.json"
    monkeypatch.setattr(credential_store, "_path", lambda: path)
    monkeypatch.setattr(credential_store, "_cache", None)
    for service in credential_store.SERVICES.values():
        for field in service.fields:
            monkeypatch.delenv(field, raising=False)
    return path


@pytest.mark.unit
@pytest.mark.no_network
def test_saved_logins_are_used_and_the_environment_wins(store, monkeypatch):
    credential_store.save("openrouter", {"OPENROUTER_API_KEY": " sk-saved "})

    assert api_keys.llm_api_key() == "sk-saved"
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-env")
    assert api_keys.llm_api_key() == "sk-env"

    [openrouter] = [row for row in credential_store.status() if row["service"] == "openrouter"]
    assert openrouter["configured"] is True and openrouter["source"] == "environment"
    assert "sk-saved" not in json.dumps(credential_store.status())


@pytest.mark.unit
@pytest.mark.no_network
def test_a_service_with_several_fields_is_saved_whole_or_not_at_all(store):
    with pytest.raises(ValueError, match="KAGGLE_USERNAME, KAGGLE_KEY"):
        credential_store.save("kaggle", {"KAGGLE_KEY": "k"})
    with pytest.raises(ValueError, match="single-line"):
        credential_store.save("kaggle", {"KAGGLE_USERNAME": "ada", "KAGGLE_KEY": "a\nb"})
    assert credential_store.values("kaggle") == {}

    credential_store.save("kaggle", {"KAGGLE_USERNAME": "ada", "KAGGLE_KEY": "k"})
    assert credential_store.values("kaggle") == {"KAGGLE_USERNAME": "ada", "KAGGLE_KEY": "k"}

    assert credential_store.remove("kaggle") is True
    assert credential_store.remove("kaggle") is False
    assert credential_store.values("kaggle") == {}
    with pytest.raises(ValueError, match="Unknown service"):
        credential_store.save("dropbox", {})


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.skipif(os.name != "posix", reason="POSIX file modes")
def test_the_file_is_readable_by_its_owner_only(store):
    credential_store.save("github", {"GITHUB_TOKEN": "ghp_x"})

    assert stat.S_IMODE(store.stat().st_mode) == 0o600


@pytest.mark.unit
@pytest.mark.no_network
def test_without_a_config_folder_only_the_environment_counts(monkeypatch):
    monkeypatch.setattr(credential_store, "_path", lambda: None)
    monkeypatch.setenv("HF_TOKEN", "hf_env")

    assert credential_store.values("huggingface") == {"HF_TOKEN": "hf_env"}
    with pytest.raises(RuntimeError, match="config folder"):
        credential_store.save("huggingface", {"HF_TOKEN": "x"})
