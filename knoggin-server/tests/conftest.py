import asyncio
import sys
from pathlib import Path

import pytest

SERVER_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = SERVER_ROOT / "src"

for path in (SERVER_ROOT, SRC_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


@pytest.fixture(autouse=True)
def reset_async_redis_singleton(monkeypatch):
    from infrastructure.redis_client import AsyncRedisClient

    monkeypatch.setattr(AsyncRedisClient, "_instance", None)
    yield
    monkeypatch.setattr(AsyncRedisClient, "_instance", None)
