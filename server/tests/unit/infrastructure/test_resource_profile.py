import pytest

from infrastructure.resource_profile import ResourceProfile

_PROFILE_VARIABLES = (
    "RESOURCE_PROFILE",
    "THREAD_WORKERS",
    "EMBEDDING_BATCH_SIZE",
    "BACKGROUND_JOB_WORKERS",
    "FOREGROUND_MODEL_WORKERS",
    "BACKGROUND_MODEL_WORKERS",
)


def _clear_profile_variables(monkeypatch):
    for variable in _PROFILE_VARIABLES:
        monkeypatch.delenv(variable, raising=False)


@pytest.mark.no_network
def test_resource_profile_defaults_to_existing_balanced_limits(monkeypatch):
    _clear_profile_variables(monkeypatch)

    profile = ResourceProfile.from_environment()

    assert profile == ResourceProfile(
        name="balanced",
        thread_workers=4,
        embedding_batch_size=32,
        background_job_workers=1,
        foreground_model_workers=1,
        background_model_workers=1,
    )


@pytest.mark.no_network
def test_resource_profile_is_explicit_and_allows_manual_overrides(monkeypatch):
    _clear_profile_variables(monkeypatch)
    monkeypatch.setenv("RESOURCE_PROFILE", "conservative")
    monkeypatch.setenv("EMBEDDING_BATCH_SIZE", "12")

    profile = ResourceProfile.from_environment()

    assert profile.name == "conservative"
    assert profile.thread_workers == 2
    assert profile.embedding_batch_size == 12
    assert profile.background_job_workers == 1


@pytest.mark.no_network
@pytest.mark.parametrize(
    ("variable", "value", "error"),
    [
        ("RESOURCE_PROFILE", "turbo", "must be one of"),
        ("THREAD_WORKERS", "0", "positive integer"),
        ("EMBEDDING_BATCH_SIZE", "many", "positive integer"),
    ],
)
def test_resource_profile_rejects_invalid_settings(
    monkeypatch, variable, value, error
):
    _clear_profile_variables(monkeypatch)
    monkeypatch.setenv(variable, value)

    with pytest.raises(ValueError, match=error):
        ResourceProfile.from_environment()
