from huggingface_hub import constants

from common.model_cache import cached_model_path


def test_a_cached_model_resolves_to_its_local_folder_without_the_hub(tmp_path, monkeypatch):
    monkeypatch.setattr(constants, "HF_HUB_OFFLINE", True)  # any Hub call would fail
    monkeypatch.setattr(constants, "HF_HUB_CACHE", str(tmp_path))
    repo = tmp_path / "models--org--model"
    (repo / "refs").mkdir(parents=True)
    (repo / "refs" / "main").write_text("abc123")
    snapshot = repo / "snapshots" / "abc123"
    snapshot.mkdir(parents=True)
    (snapshot / "config.json").write_text("{}")

    assert cached_model_path("org/model") == str(snapshot)
    assert cached_model_path("org/other") is None
    assert cached_model_path(str(snapshot)) == str(snapshot)
