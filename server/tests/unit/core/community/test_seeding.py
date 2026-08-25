from __future__ import annotations

from types import SimpleNamespace

import pytest

from core.agent.services.agent_manager import AgentManager
from core.community.aac_store import AACStore
from core.community.read_context import AACReadContext
from core.community.seeding import AACSeeder, SeedDecision
from tests.fixtures.fakes import FakeResources


def _config_provider():
    return SimpleNamespace(
        get=staticmethod(
            lambda: SimpleNamespace(
                config=SimpleNamespace(
                    developer_settings=SimpleNamespace(
                        community=SimpleNamespace(
                            seeding_agent_id=None,
                            token_budget=50_000,
                        )
                    )
                )
            )
        )
    )


@pytest.mark.runtime
@pytest.mark.no_network
def test_seed_decision_accepts_only_start_topic_or_skip():
    assert SeedDecision.parse("START(Investigate conflicting dates)") == SeedDecision(
        "START", "Investigate conflicting dates"
    )
    assert SeedDecision.parse("SKIP") == SeedDecision("SKIP")
    assert SeedDecision.parse('{"topic": "not the contract"}') == SeedDecision("SKIP")


@pytest.mark.runtime
@pytest.mark.no_network
async def test_seeder_uses_normal_agent_run_and_skips_unusable_output(monkeypatch):
    resources = FakeResources()
    manager = AgentManager(resources, user_name="ada")
    await manager.ensure_default_agent()
    context = await AACReadContext.create(
        user_name="ada",
        postgres=resources.postgres,
        knowledge_store=resources.knowledge_store,
        embedding_service=resources.embedding,
        redis=resources.redis,
    )

    class FakeExecutor:
        def __init__(self, run, llm, tools, **kwargs):
            assert run.project_id == "__aac__"
            assert run.tool_runtime.permissions.audit_project_id is None
            assert kwargs["aac_budget"] is not None
            self.tools = tools

        async def execute(self):
            yield {"event": "response", "data": {"content": "not valid"}}

    monkeypatch.setattr("core.community.seeding.AgentExecutor", FakeExecutor)
    seeder = AACSeeder(
        user_name="ada",
        resources=resources,
        read_context=context,
        agent_manager=manager,
        store=AACStore(resources.postgres),
        config_provider=_config_provider(),
    )

    decision = await seeder.decide()

    assert decision == SeedDecision("SKIP")
