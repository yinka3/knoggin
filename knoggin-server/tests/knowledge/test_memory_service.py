import json

import pytest

from infrastructure.redis_client import RedisKeys
from knoggin_server.knowledge.services.memory_service import MemoryManager
from tests.fixtures.factories import make_topic_config
from tests.fixtures.fakes import FakeRedis


@pytest.fixture
def memory_manager():
    events = []
    redis = FakeRedis()
    manager = MemoryManager(
        redis=redis,
        user_name="ada",
        session_id="session-1",
        agent_id="agent-1",
        topic_config=make_topic_config(),
        on_event=lambda source, event, data: events.append((source, event, data)),
    )
    return manager, redis, events


@pytest.mark.storage
@pytest.mark.no_network
async def test_memory_manager_saves_lists_and_forgets_session_memory(memory_manager):
    manager, redis, events = memory_manager

    saved = await manager.save_memory(" remember this ", "General")

    assert saved.success is True
    key = RedisKeys.agent_memory("ada", "session-1", "General")
    assert saved.memory_id in redis.hashes[key]
    payload = json.loads(redis.hashes[key][saved.memory_id])
    assert payload["content"] == "remember this"

    listed = await manager.get_memory_blocks(["General"])
    assert listed.total == 1
    assert listed.blocks["General"][0].content == "remember this"

    forgotten = await manager.forget_memory(saved.memory_id)
    assert forgotten.success is True
    assert forgotten.topic == "General"
    assert redis.hashes[key] == {}
    assert [event[1] for event in events] == ["memory_saved", "memory_forgotten"]


@pytest.mark.storage
@pytest.mark.no_network
async def test_memory_manager_rejects_empty_inactive_and_oversized_memory(
    memory_manager,
):
    manager, _, _ = memory_manager
    manager.topic_config.toggle_active("Identity", False)

    empty = await manager.save_memory("   ", "General")
    inactive = await manager.save_memory("hello", "Identity")
    oversized = await manager.save_memory("x" * 201, "General")

    assert empty.success is False
    assert empty.error == "Empty memory content"
    assert inactive.success is False
    assert "not active" in inactive.error
    assert oversized.success is False
    assert "Memory too long" in oversized.error


@pytest.mark.storage
@pytest.mark.no_network
async def test_memory_manager_working_memory_lifecycle(memory_manager):
    manager, redis, events = memory_manager

    added = await manager.add_working_memory("rules", "Be concise")
    listed = await manager.list_working_memory("rules")
    removed = await manager.remove_working_memory("rules", added.memory_id)

    assert added.success is True
    assert listed.blocks["rules"][0].content == "Be concise"
    assert removed.success is True
    assert redis.hashes[RedisKeys.agent_working_memory("agent-1", "rules")] == {}
    assert [event[1] for event in events] == [
        "working_memory_added",
        "working_memory_removed",
    ]


@pytest.mark.storage
@pytest.mark.no_network
async def test_memory_manager_clear_working_memory_counts_removed_items(
    memory_manager,
):
    manager, _, _ = memory_manager

    await manager.add_working_memory("preferences", "Prefer detail")
    await manager.add_working_memory("preferences", "Prefer examples")
    cleared = await manager.clear_working_memory("preferences")

    assert cleared.success is True
    assert cleared.cleared == 2
