import pytest


@pytest.fixture(autouse=True)
def _allow_the_test_host(monkeypatch):
    # httpx's test client sends Host: test.
    monkeypatch.setenv("KNOGGIN_ALLOWED_HOSTS", "test")
