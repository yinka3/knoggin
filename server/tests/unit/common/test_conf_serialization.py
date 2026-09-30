import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
import yaml

from common.conf.manager import ConfigManager


@pytest.fixture
def manager(tmp_path):
    ConfigManager._instance = None
    owner = ConfigManager.initialize(tmp_path)
    yield owner
    ConfigManager._instance = None


def test_concurrent_updates_merge_without_losing_a_change(manager):
    barrier = threading.Barrier(2)

    def update(values):
        barrier.wait(timeout=5)
        return manager.update_settings(values)

    with ThreadPoolExecutor(max_workers=2) as workers:
        first = workers.submit(update, {"user_aliases": ["Ada"]})
        second = workers.submit(update, {"llm": {"agent_model": "new-model"}})
        assert first.result(timeout=5) and second.result(timeout=5)
    assert manager.config.user_aliases == ["Ada"]
    assert manager.config.llm.agent_model == "new-model"
    assert yaml.safe_load(manager.config_file.read_text(encoding="utf-8")) == manager.config.model_dump(mode="json")


def test_save_and_update_are_ordered(manager, monkeypatch):
    entered, release, update_started = threading.Event(), threading.Event(), threading.Event()
    persist = manager._persist_config

    def blocked(config):
        entered.set()
        assert release.wait(timeout=5)
        return persist(config)

    monkeypatch.setattr(manager, "_persist_config", blocked)

    def update():
        update_started.set()
        return manager.update_settings({"user_aliases": ["Ada"]})

    with ThreadPoolExecutor(max_workers=2) as workers:
        saved = workers.submit(manager.save)
        assert entered.wait(timeout=5)
        changed = workers.submit(update)
        assert update_started.wait(timeout=5)
        release.set()
        assert saved.result(timeout=5) and changed.result(timeout=5)
    assert yaml.safe_load(manager.config_file.read_text(encoding="utf-8"))["user_aliases"] == ["Ada"]


def test_snapshots_and_callback_payloads_cannot_mutate_active_state(manager):
    snapshot = manager.config
    snapshot.user_aliases.append("outside")
    snapshot.llm.agent_model = "outside"
    received = []
    manager.subscribe(lambda value: value.append("subscriber"), "user_aliases")
    manager.subscribe(received.append, "user_aliases")
    assert manager.update_settings({"user_aliases": ["Ada"]})
    assert manager.config.user_aliases == ["Ada"]
    assert received == [[], ["Ada"]]
    assert manager.config.llm.agent_model != "outside"
    with pytest.raises(AttributeError):
        manager.config = snapshot


def test_cross_thread_publication_rejected_before_persistence(manager):
    manager.subscribe(lambda value: None, "user_aliases")
    with ThreadPoolExecutor(max_workers=1) as workers:
        result = workers.submit(manager.update_settings, {"user_aliases": ["wrong-thread"]})
        with pytest.raises(RuntimeError, match="subscriber thread"):
            result.result(timeout=5)
    assert manager.config.user_aliases == []


async def test_cancelled_async_save_settles_worker_before_return(manager, monkeypatch):
    loop = asyncio.get_running_loop()
    entered, release, finished = asyncio.Event(), threading.Event(), threading.Event()
    save = manager.save

    def blocked():
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(timeout=5)
        try:
            return save()
        finally:
            finished.set()

    monkeypatch.setattr(manager, "save", blocked)
    task = asyncio.create_task(manager.async_save())
    await asyncio.wait_for(entered.wait(), timeout=5)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert finished.is_set()
