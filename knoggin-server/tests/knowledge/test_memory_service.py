import json

import pytest

from common.schema.settings import TopicSchema
from common.utils.time_utils import frozen_time
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
async def test_memory_manager_prompt_strings_include_only_requested_active_topics(
    memory_manager, monkeypatch
):
    manager, redis, _ = memory_manager
    manager.topic_config.add_topic(
        "Work",
        TopicSchema(active=True, labels=["project"], hierarchy={}, aliases=["job"]),
    )
    manager.topic_config.toggle_active("Identity", False)

    with frozen_time("2026-01-01T10:00:00+00:00"):
        general = await manager.save_memory("General note", "General")
    with frozen_time("2026-01-01T10:05:00+00:00"):
        work = await manager.save_memory("Work note", "job")

    identity_key = RedisKeys.agent_memory("ada", "session-1", "Identity")
    await redis.hset(
        identity_key,
        "mem_identity",
        json.dumps(
            {
                "content": "inactive identity note",
                "topic": "Identity",
                "created_at": "2026-01-01T10:10:00+00:00",
            }
        ),
    )
    await redis.hset(
        RedisKeys.agent_memory("ada", "session-1", "General"),
        "mem_corrupt",
        "{not-json",
    )

    await manager.add_directive("require", "Stay grounded")
    await manager.add_directive("prefer", "Prefer direct evidence")
    await manager.add_directive("avoid", "Avoid vague claims")

    prompt = await manager.load_prompt_strings(["job", "Identity", "unknown"])

    assert prompt.memory_ctx == f"[Work]\n  - ({work.memory_id}) Work note"
    assert prompt.directives == (
        "Required:\n"
        "- Stay grounded\n\n"
        "Preferred:\n"
        "- Prefer direct evidence\n\n"
        "Avoid:\n"
        "- Avoid vague claims"
    )
    assert general.memory_id not in prompt.memory_ctx
    assert "General note" not in prompt.memory_ctx
    assert "inactive identity note" not in prompt.memory_ctx
    assert "mem_corrupt" not in prompt.memory_ctx


@pytest.mark.storage
@pytest.mark.no_network
async def test_memory_manager_can_load_general_when_explicitly_requested(
    memory_manager,
):
    manager, _, _ = memory_manager
    saved = await manager.save_memory("General note", "General")

    blocks = await manager.get_memory_blocks(["General"])

    assert blocks.total == 1
    assert blocks.blocks["General"][0].id == saved.memory_id
    assert blocks.blocks["General"][0].content == "General note"


@pytest.mark.storage
@pytest.mark.no_network
async def test_memory_manager_session_and_user_scopes_do_not_leak():
    redis = FakeRedis()
    manager = MemoryManager(
        redis=redis,
        user_name="ada",
        session_id="session-1",
        agent_id="agent-1",
        topic_config=make_topic_config(),
    )
    same_user_other_session = MemoryManager(
        redis=redis,
        user_name="ada",
        session_id="session-2",
        agent_id="agent-1",
        topic_config=make_topic_config(),
    )
    other_user_same_session = MemoryManager(
        redis=redis,
        user_name="grace",
        session_id="session-1",
        agent_id="agent-1",
        topic_config=make_topic_config(),
    )

    saved = await manager.save_memory("Scoped note", "General")

    assert (await same_user_other_session.get_memory_blocks(["General"])).total == 0
    assert (await other_user_same_session.get_memory_blocks(["General"])).total == 0

    miss = await same_user_other_session.forget_memory(saved.memory_id)
    assert miss.success is False
    assert saved.memory_id in redis.hashes[
        RedisKeys.agent_memory("ada", "session-1", "General")
    ]

    removed = await manager.forget_memory(saved.memory_id)
    assert removed.success is True
    assert saved.memory_id not in redis.hashes[
        RedisKeys.agent_memory("ada", "session-1", "General")
    ]


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
async def test_memory_manager_directive_lifecycle(memory_manager):
    manager, redis, events = memory_manager

    added = await manager.add_directive("require", "Be concise")
    listed = await manager.list_directives("require")
    removed = await manager.remove_directive(added.directive_id)

    assert added.success is True
    assert added.mode == "require"
    assert listed.directives[0].content == "Be concise"
    assert listed.directives[0].mode == "require"
    assert removed.success is True
    assert redis.hashes[RedisKeys.agent_working_memory("agent-1", "directives")] == {}
    assert [event[1] for event in events] == [
        "directive_added",
        "directive_removed",
    ]


@pytest.mark.storage
@pytest.mark.no_network
async def test_memory_manager_clear_directives_counts_removed_items(
    memory_manager,
):
    manager, _, _ = memory_manager

    await manager.add_directive("prefer", "Prefer detail")
    await manager.add_directive("prefer", "Prefer examples")
    await manager.add_directive("avoid", "Avoid vague claims")
    cleared = await manager.clear_directives("prefer")
    remaining = await manager.list_directives()

    assert cleared.success is True
    assert cleared.cleared == 2
    assert [directive.mode for directive in remaining.directives] == ["avoid"]


@pytest.mark.storage
@pytest.mark.no_network
async def test_memory_manager_rejects_invalid_directive_mode(memory_manager):
    manager, _, _ = memory_manager

    invalid = await manager.add_directive("rule", "Stay grounded")
    empty = await manager.add_directive("prefer", "   ")

    assert invalid.success is False
    assert "Invalid directive mode" in invalid.error
    assert empty.success is False
    assert empty.error == "Empty directive content"
