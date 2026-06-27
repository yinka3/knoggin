from types import SimpleNamespace

import pytest

from common.schema.aac_schema import AAC_DEFAULT_ENABLED_TOOLS
from knoggin_server.agent.tools.community_tools import (
    MAX_SPAWNED_SPECIALISTS,
    CommunityTools,
)
from tests.fixtures.fakes import FakeRedis


PERSONA = {
    "attention_bias": "Weak or missing evidence",
    "reasoning_style": "Trace claims back to primary observations",
    "social_temperament": "Calm and constructively skeptical",
    "communication_signature": "Brief, sourced, and explicit about uncertainty",
    "productive_flaw": "Can spend too long validating small details",
}


class RecordingCommunityStore:
    def __init__(self):
        self.messages = []
        self.spawns = []

    async def add_message(self, discussion_id, agent_id, content, role="agent"):
        self.messages.append(
            {
                "discussion_id": discussion_id,
                "agent_id": agent_id,
                "content": content,
                "role": role,
            }
        )

    async def register_agent_spawn(self, parent_id, child_id, detail=""):
        self.spawns.append(
            {
                "parent_id": parent_id,
                "child_id": child_id,
                "detail": detail,
            }
        )


class RecordingPostgres:
    def __init__(self, *, spawned_count=0, write_count=1):
        self.spawned_count = spawned_count
        self.write_count = write_count
        self.calls = []

    async def fetch_all(self, query, params=None):
        self.calls.append(("fetch_all", query, params))
        return [{"count": self.spawned_count}]

    async def execute(self, query, params=None):
        self.calls.append(("execute", query, params))
        return self.write_count


def make_tool(*, postgres=None, participants=None):
    redis = FakeRedis()
    postgres = postgres or RecordingPostgres()
    store = RecordingCommunityStore()
    participants = participants if participants is not None else ["agent-1"]
    entities = SimpleNamespace(
        embedding_service=object(),
        project_id="project-1",
        readable_project_ids=["project-1"],
    )
    base_tools = SimpleNamespace(
        entities=entities,
        session_id="session-1",
        topic_config=SimpleNamespace(active_topics=["General"]),
        search_cfg={},
        document_service=None,
        document_focus=None,
        knowledge_store=SimpleNamespace(),
        postgres=postgres,
        redis=redis,
        readable_project_ids=["project-1"],
    )
    tool = CommunityTools(
        user_name="ada",
        base_tools=base_tools,
        community_store=store,
        discussion_id="disc-1",
        agent_id="agent-1",
        participants=participants,
    )
    return tool, store, postgres, participants


def patch_config_and_events(monkeypatch):
    events = []
    root = SimpleNamespace(
        config=SimpleNamespace(llm=SimpleNamespace(agent_model="test-agent-model"))
    )
    monkeypatch.setattr(
        "knoggin_server.agent.tools.community_tools.ConfigManager.get",
        staticmethod(lambda: root),
    )

    async def fake_emit_community(*args):
        events.append(args)

    monkeypatch.setattr(
        "knoggin_server.agent.tools.community_tools.emit_community",
        fake_emit_community,
    )
    return events


@pytest.mark.no_network
async def test_save_insight_writes_only_to_discussion_store():
    tool, store, _, _ = make_tool()

    result = await tool.save_insight("Prefer direct evidence")

    assert result == {"saved": True, "type": "insight"}
    assert store.messages == [
        {
            "discussion_id": "disc-1",
            "agent_id": "system",
            "content": "INSIGHT: Prefer direct evidence",
            "role": "insight",
        }
    ]


@pytest.mark.no_network
async def test_spawn_specialist_persists_agent_and_initial_brain(monkeypatch):
    events = patch_config_and_events(monkeypatch)
    participants = ["agent-1"]
    tool, store, postgres, participants = make_tool(participants=participants)

    result = await tool.spawn_specialist(
        name="Evidence Steward",
        persona=PERSONA,
        initial_directives=[
            {"mode": "require", "content": "Never invent source messages."},
            {"mode": "avoid", "content": "Unscoped profile claims."},
        ],
    )

    assert result["id"].startswith("spawned_")
    assert result["persona"] == PERSONA
    assert result["seeded_directives"] == 2
    assert participants == ["agent-1", result["id"]]

    write = next(call for call in postgres.calls if call[0] == "execute")
    params = write[2]
    assert "INSERT INTO public.agents" in write[1]
    assert "INSERT INTO public.agent_brain_revisions" in write[1]
    assert params["project_id"] == "project-1"
    assert params["model"] == "test-agent-model"
    assert params["enabled_tools"]
    assert params["spawned_by"] == "agent-1"
    assert "Never invent source messages." in params["instructions"]
    assert AAC_DEFAULT_ENABLED_TOOLS

    assert store.spawns[0]["child_id"] == result["id"]
    assert store.spawns[0]["detail"] == result["persona_markdown"]
    assert events[0][2] == "agent_spawned"


@pytest.mark.no_network
async def test_spawn_specialist_blocks_at_postgres_backed_limit(monkeypatch):
    patch_config_and_events(monkeypatch)
    postgres = RecordingPostgres(spawned_count=MAX_SPAWNED_SPECIALISTS)
    participants = [f"spawned-{index}" for index in range(MAX_SPAWNED_SPECIALISTS)]
    tool, store, postgres, participants = make_tool(
        postgres=postgres,
        participants=participants,
    )

    result = await tool.spawn_specialist("Extra", PERSONA)

    assert result == {
        "error": "Spawn limit reached. Max 10 sub-agents per discussion."
    }
    assert not any(call[0] == "execute" for call in postgres.calls)
    assert store.spawns == []


@pytest.mark.no_network
async def test_spawn_specialist_rejects_incomplete_persona_without_db_access():
    tool, _, postgres, _ = make_tool()

    result = await tool.spawn_specialist(
        "Incomplete",
        {"attention_bias": "Only one field"},
    )

    assert "error" in result
    assert postgres.calls == []
