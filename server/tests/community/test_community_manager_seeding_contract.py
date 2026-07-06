from types import SimpleNamespace

import pytest

from common.schema.agent_contracts import AgentConfig
from common.schema.settings import CommunitySettings, DeveloperSettings, RootConfig
from core.community.community_manager import CommunityManager
from tests.fixtures.fakes import FakePostgresClient, FakeRedis


class RecordingCommunityGraph:
    def __init__(self):
        self.recent_discussions = [
            {
                "topic": "Old profile drift discussion",
                "status": "closed",
                "message_count": 3,
            }
        ]
        self.insights = [
            {
                "content": "INSIGHT: Ada likes precise test plans",
                "discussion_topic": "Testing",
            }
        ]

    async def get_recent_discussions(self, limit=5):
        assert limit == 5
        return self.recent_discussions

    async def get_discussion_insights(self, limit=5):
        assert limit == 5
        return self.insights


class RecordingKnowledgeStore:
    def __init__(self):
        self.community = RecordingCommunityGraph()
        self.raise_context = False

    async def get_graph_stats(self, *, visible_project_ids):
        assert visible_project_ids == ["__identity__", "project-1"]
        if self.raise_context:
            raise RuntimeError("graph unavailable")
        return {"entities": 4, "facts": 7, "relationships": 2}

    async def get_notable_entities(self, *, visible_project_ids, limit):
        assert visible_project_ids == ["__identity__", "project-1"]
        assert limit == 8
        return [
            {
                "name": "Knoggin",
                "type": "project",
                "topic": "Build",
                "connection_count": 5,
                "fact_count": 9,
            }
        ]

    async def get_recently_active_entities(
        self, *, visible_project_ids, days, limit
    ):
        assert visible_project_ids == ["__identity__", "project-1"]
        assert (days, limit) == (7, 5)
        return [{"name": "Profile Refinement", "type": "concept", "recent_facts": 4}]

    async def get_recent_facts(self, *, visible_project_ids, days, limit):
        assert visible_project_ids == ["__identity__", "project-1"]
        assert (days, limit) == (7, 10)
        return [
            {
                "entity_name": "Ada",
                "content": (
                    "Ada wants stable profiles, not over-updating from weak evidence."
                ),
            }
        ]


class RecordingLLM:
    def __init__(self, response):
        self.response = response
        self.calls = []

    async def generate_text(self, system, user, model, temperature):
        self.calls.append(
            {
                "system": system,
                "user": user,
                "model": model,
                "temperature": temperature,
            }
        )
        return self.response


def root_config(**community_overrides):
    root = RootConfig(developer_settings=DeveloperSettings())
    root.llm.agent_model = "fallback-model"
    root.developer_settings.community = CommunitySettings(**community_overrides)
    return root


def make_resources(*, redis=None, llm_response="{}"):
    return SimpleNamespace(
        redis=redis or FakeRedis(),
        postgres=FakePostgresClient(),
        knowledge_store=RecordingKnowledgeStore(),
        llm_service=RecordingLLM(llm_response),
    )


def make_project_state():
    return SimpleNamespace(project_id="project-1", topic_config=SimpleNamespace())


async def save_agent(postgres, agent_id, *, name=None, persona=None, **overrides):
    agent = AgentConfig(
        id=agent_id,
        name=name or agent_id.title(),
        persona=persona or f"{agent_id} persona",
        model=overrides.pop("model", "agent-model"),
        **overrides,
    )
    postgres.upsert_agent(agent)
    return agent


async def save_project(postgres, project_id, status="active"):
    postgres.upsert_project(project_id, status=status)


def patch_manager_config(monkeypatch, root):
    monkeypatch.setattr(
        "core.community.community_manager.ConfigManager.get",
        staticmethod(lambda: SimpleNamespace(config=root)),
    )


def patch_events(monkeypatch):
    events = []

    async def fake_emit_community(*args):
        events.append(args)

    monkeypatch.setattr(
        "core.community.community_manager.emit_community",
        fake_emit_community,
    )
    return events


@pytest.mark.no_network
async def test_build_agent_pool_context_returns_default_without_agents(monkeypatch):
    patch_manager_config(monkeypatch, root_config())
    resources = make_resources()
    manager = CommunityManager(make_project_state(), "ada", resources)

    agent_ids, description = await manager._build_agent_pool_context()

    assert len(agent_ids) == 1
    assert "- STELLA (id:" in description


@pytest.mark.no_network
async def test_build_agent_pool_context_filters_pool_and_marks_spawned(monkeypatch):
    resources = make_resources()
    await save_agent(
        resources.postgres,
        "agent-1",
        name="Planner",
        persona="Plans carefully",
    )
    await save_agent(
        resources.postgres,
        "spawned-1",
        name="Evidence",
        persona="Tracks evidence",
        is_spawned=True,
    )
    await save_agent(
        resources.postgres,
        "filtered-out",
        name="Hidden",
        persona="Not in pool",
    )
    patch_manager_config(
        monkeypatch,
        root_config(agent_pool_ids=["agent-1", "spawned-1", "bad-json"]),
    )
    manager = CommunityManager(make_project_state(), "ada", resources)

    agent_ids, description = await manager._build_agent_pool_context()

    assert agent_ids == ["agent-1", "spawned-1"]
    assert "- Planner (id: agent-1):" in description
    assert "Plans carefully" in description
    assert "- Evidence [spawned] (id: spawned-1):" in description
    assert "Tracks evidence" in description
    assert "filtered-out" not in description


@pytest.mark.no_network
async def test_build_seeding_context_includes_graph_and_community_sections():
    resources = make_resources()
    await save_project(resources.postgres, "project-1")
    manager = CommunityManager(make_project_state(), "ada", resources)

    context = await manager._build_seeding_context()

    assert "=== GRAPH OVERVIEW ===" in context
    assert "Entities: 4" in context
    assert "=== NOTABLE ENTITIES ===" in context
    assert "- Knoggin (project, Build): 5 connections, 9 facts" in context
    assert "=== RECENTLY ACTIVE (last 7 days) ===" in context
    assert "- Profile Refinement (concept): 4 new facts" in context
    assert "=== RECENT FACTS ===" in context
    assert "[Ada] Ada wants stable profiles" in context
    assert "=== PREVIOUS DISCUSSIONS ===" in context
    assert '"Old profile drift discussion" (closed, 3 messages)' in context
    assert "=== INSIGHTS FROM PAST DISCUSSIONS ===" in context
    assert "- Ada likes precise test plans" in context


@pytest.mark.no_network
async def test_build_seeding_context_falls_back_when_graph_collection_fails():
    resources = make_resources()
    await save_project(resources.postgres, "project-1")
    resources.knowledge_store.raise_context = True
    manager = CommunityManager(make_project_state(), "ada", resources)

    assert (
        await manager._build_seeding_context()
        == "Knowledge graph is available for exploration."
    )


@pytest.mark.no_network
async def test_community_scope_defaults_to_active_and_archived_projects(monkeypatch):
    resources = make_resources()
    await save_project(resources.postgres, "active-project", "active")
    await save_project(resources.postgres, "archived-project", "archived")
    await save_project(resources.postgres, "deleted-project", "deleted")
    patch_manager_config(monkeypatch, root_config())
    manager = CommunityManager(
        make_project_state(),
        "ada",
        resources,
    )

    assert await manager._resolve_project_scope() == [
        "__identity__",
        "active-project",
        "archived-project",
    ]


@pytest.mark.no_network
async def test_community_scope_configuration_is_exact_and_filters_invalid_ids(
    monkeypatch,
):
    resources = make_resources()
    await save_project(resources.postgres, "project-1", "active")
    await save_project(resources.postgres, "project-2", "archived")
    await save_project(resources.postgres, "deleted-project", "deleted")
    patch_manager_config(
        monkeypatch,
        root_config(
            project_ids=["project-2", "missing", "deleted-project"]
        ),
    )
    manager = CommunityManager(
        make_project_state(),
        "ada",
        resources,
    )

    assert await manager._resolve_project_scope() == [
        "__identity__",
        "project-2",
    ]


@pytest.mark.no_network
async def test_community_seeding_skips_storage_when_scope_is_empty(monkeypatch):
    patch_manager_config(monkeypatch, root_config())
    resources = make_resources()
    manager = CommunityManager(make_project_state(), "ada", resources)

    assert (
        await manager._build_seeding_context()
        == "No community project scope is configured."
    )


@pytest.mark.no_network
async def test_seed_discussion_parses_fenced_json_and_filters_unknown_agents(
    monkeypatch,
):
    response = """
    ```json
    {
        "topic": "Profile refinement stability",
        "objective": "Find weak evidence update risks",
        "discussion_type": "investigation",
        "reasoning": "Recent facts touch profile behavior",
        "agent_ids": ["agent-1", "missing-agent"]
    }
    ```
    """
    resources = make_resources(llm_response=response)
    await save_project(resources.postgres, "project-1")
    await save_agent(
        resources.postgres,
        "seed-agent",
        name="Seeder",
        persona="Starts discussions",
        is_default=True,
    )
    await save_agent(
        resources.postgres,
        "agent-1",
        name="Analyst",
        persona="Analyzes evidence",
    )
    patch_manager_config(monkeypatch, root_config())
    events = patch_events(monkeypatch)
    manager = CommunityManager(make_project_state(), "ada", resources)

    async def get_directives(_agent_id):
        return (
            "Required:\n"
            "- Stay grounded\n\n"
            "Preferred:\n"
            "- Prefer direct evidence\n\n"
            "Avoid:\n"
            "- Vague claims"
        )

    manager._get_agent_directives = get_directives

    seed = await manager._seed_discussion()

    assert seed["topic"] == "Profile refinement stability"
    assert seed["agent_ids"] == ["agent-1"]
    assert resources.llm_service.calls[0]["model"] == "agent-model"
    assert "Stay grounded" in resources.llm_service.calls[0]["system"]
    assert "=== AVAILABLE AGENTS ===" in resources.llm_service.calls[0]["user"]
    assert [event[2] for event in events] == [
        "seeding_started",
        "discussion_seeded",
    ]
    assert events[-1][3]["agent_ids"] == ["agent-1"]


@pytest.mark.no_network
async def test_seed_discussion_falls_back_to_seeding_agent_on_invalid_response(
    monkeypatch,
):
    resources = make_resources(llm_response="not json")
    await save_project(resources.postgres, "project-1")
    await save_agent(
        resources.postgres,
        "seed-agent",
        name="Seeder",
        persona="Starts discussions",
        is_default=True,
    )
    patch_manager_config(monkeypatch, root_config())
    patch_events(monkeypatch)
    manager = CommunityManager(make_project_state(), "ada", resources)

    async def get_directives(_agent_id):
        return ""

    manager._get_agent_directives = get_directives

    seed = await manager._seed_discussion()

    assert seed == {
        "topic": "Knowledge graph exploration and insight discovery",
        "objective": "Find interesting patterns or connections in the user's knowledge",
        "discussion_type": "brainstorm",
        "reasoning": "Fallback due to seeding failure",
        "agent_ids": ["seed-agent"],
    }
