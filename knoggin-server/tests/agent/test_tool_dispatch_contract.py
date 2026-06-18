import pytest

from common.exceptions import ToolExecutionError
from knoggin_server.agent.internals import execute_tool
from knoggin_server.agent.tools.memory import MemoryTools


class DispatchTools:
    def __init__(self):
        self.calls = []

    async def search_messages(self, query, limit=None):
        self.calls.append(("search_messages", query, limit))
        return [{"id": "msg_1"}]

    async def search_entity(self, query, limit=None):
        self.calls.append(("search_entity", query, limit))
        return [{"id": 1, "query": query}]

    async def get_recent_activity(self, entity_name, hours=None):
        self.calls.append(("get_recent_activity", entity_name, hours))
        return [{"entity": entity_name}]

    async def fact_check(self, entity_name, query):
        self.calls.append(("fact_check", entity_name, query))
        return {"resolution": "exact"}

    async def broken(self):
        raise RuntimeError("method exploded")


@pytest.mark.no_network
async def test_execute_tool_dispatches_known_tools_and_coerces_schema_types():
    tools = DispatchTools()

    result = await execute_tool(
        tools,
        "search_messages",
        {"query": 1234, "limit": "5", "ignored": "drop me"},
    )
    activity = await execute_tool(
        tools,
        "get_recent_activity",
        {"entity_name": "Knoggin", "hours": "48"},
    )
    entity = await execute_tool(
        tools,
        "search_entity",
        {"query": 99, "limit": "2"},
    )

    assert result == {"data": [{"id": "msg_1"}]}
    assert activity == {"data": [{"entity": "Knoggin"}]}
    assert entity == {"data": [{"id": 1, "query": "99"}]}
    assert tools.calls == [
        ("search_messages", "1234", 5),
        ("get_recent_activity", "Knoggin", 48),
        ("search_entity", "99", 2),
    ]


@pytest.mark.no_network
async def test_execute_tool_special_request_tools_do_not_touch_methods():
    tools = DispatchTools()

    clarification = await execute_tool(
        tools,
        "request_clarification",
        {"question": "Which project?"},
    )
    replanning = await execute_tool(
        tools,
        "request_replanning",
        {"reason": "Search results were empty"},
    )

    assert clarification == {"clarification": "Which project?"}
    assert replanning == {"replanning": "Search results were empty"}
    assert tools.calls == []


@pytest.mark.no_network
async def test_execute_tool_raises_for_unknown_or_missing_methods():
    tools = DispatchTools()

    with pytest.raises(ToolExecutionError) as unknown:
        await execute_tool(tools, "not_a_tool", {})

    assert unknown.value.details["tool"] == "not_a_tool"
    assert "Unknown tool" in unknown.value.message

    with pytest.raises(ToolExecutionError) as missing:
        await execute_tool(tools, "forget_memory", {"memory_id": "mem_1"})

    assert missing.value.details["tool"] == "forget_memory"
    assert "Tool method not found" in missing.value.message


@pytest.mark.no_network
async def test_execute_tool_wraps_tool_method_exceptions(monkeypatch):
    tools = DispatchTools()

    monkeypatch.setattr(
        "knoggin_server.agent.tools.registry.TOOL_DISPATCH",
        {"broken_tool": ("broken", [])},
    )
    monkeypatch.setattr(
        "knoggin_server.agent.internals.TOOL_DISPATCH",
        {"broken_tool": ("broken", [])},
    )

    with pytest.raises(ToolExecutionError) as exc:
        await execute_tool(tools, "broken_tool", {})

    assert exc.value.details["tool"] == "broken_tool"
    assert "method exploded" in exc.value.message


class RecordingMemory:
    def __init__(self):
        self.calls = []

    async def save_memory_dict(self, content, topic):
        self.calls.append(("save", content, topic))
        return {"saved": True, "memory_id": "mem_1"}

    async def forget_memory_dict(self, memory_id):
        self.calls.append(("forget", memory_id))
        return {"forgotten": True, "memory_id": memory_id}

    async def get_memory_blocks_dict(self, hot_topics):
        self.calls.append(("blocks", hot_topics))
        return {"Identity": [{"id": "mem_1"}]}


class MemoryToolHarness(MemoryTools):
    def __init__(self, memory=None):
        self.memory = memory


@pytest.mark.no_network
async def test_memory_tools_delegate_to_configured_memory_manager():
    memory = RecordingMemory()
    tools = MemoryToolHarness(memory)

    saved = await tools.save_memory("Ada prefers scoped tests", topic="Identity")
    forgotten = await tools.forget_memory("mem_1")
    blocks = await tools.get_memory_blocks(["Identity"])

    assert saved == {"saved": True, "memory_id": "mem_1"}
    assert forgotten == {"forgotten": True, "memory_id": "mem_1"}
    assert blocks == {"Identity": [{"id": "mem_1"}]}
    assert memory.calls == [
        ("save", "Ada prefers scoped tests", "Identity"),
        ("forget", "mem_1"),
        ("blocks", ["Identity"]),
    ]


@pytest.mark.no_network
async def test_memory_tools_return_clean_defaults_without_memory_manager():
    tools = MemoryToolHarness(memory=None)

    assert await tools.save_memory("note") == {"error": "No memory manager configured"}
    assert await tools.forget_memory("mem_1") == {
        "error": "No memory manager configured"
    }
    assert await tools.get_memory_blocks(["Identity"]) == {}


@pytest.mark.no_network
async def test_normal_memory_tools_keep_community_only_tools_unavailable():
    tools = MemoryToolHarness(memory=RecordingMemory())

    assert await tools.save_insight("community insight") == {
        "error": "save_insight is only available in community discussions."
    }
    assert await tools.spawn_specialist("Expert", "Persona") == {
        "error": "spawn_specialist is only available in community discussions."
    }
