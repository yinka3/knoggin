from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from knoggin_app_api import main


@pytest.mark.parametrize("run_failure", [RuntimeError("runs"), None])
async def test_lifespan_always_attempts_application_cleanup(monkeypatch, run_failure):
    client, runs = AsyncMock(), AsyncMock()
    runs.close.side_effect = run_failure
    client.close.side_effect = RuntimeError("application")
    monkeypatch.setattr(main.Knoggin, "start", AsyncMock(return_value=client))
    monkeypatch.setattr(main, "RunManager", lambda _: runs)
    expected = ExceptionGroup if run_failure else RuntimeError
    with pytest.raises(expected):
        async with main.lifespan(FastAPI()):
            pass
    runs.close.assert_awaited_once()
    client.close.assert_awaited_once()


async def test_failed_run_manager_construction_closes_application(monkeypatch):
    client = AsyncMock()
    monkeypatch.setattr(main.Knoggin, "start", AsyncMock(return_value=client))

    def fail(_):
        raise RuntimeError("construction")

    monkeypatch.setattr(main, "RunManager", fail)
    with pytest.raises(RuntimeError, match="construction"):
        async with main.lifespan(FastAPI()):
            pass
    client.close.assert_awaited_once()
