from __future__ import annotations

from types import SimpleNamespace

import pytest

from common.schema.agent.community_tools import AAC_SPECIFIC_SCHEMAS
from core.agent.services.agent_manager import AgentManager
from core.community.aac_store import AACStore
from core.community.tools import AACTools
from tests.fixtures.fakes import FakeResources


def _persona():
    return {
        "attention_bias": "evidence",
        "reasoning_style": "careful",
        "social_temperament": "curious",
        "communication_signature": "concise",
        "productive_flaw": "overchecks",
    }


def _base_tools(resources):
    return SimpleNamespace(
        entities=SimpleNamespace(
            project_id="__identity__",
            readable_project_ids=["__identity__", "project-1"],
            embedding_service=resources.embedding,
        ),
        session_id="aac:discussion-1",
        compiled_domain=None,
        search_cfg={},
        document_service=None,
        document_focus=None,
        knowledge_retrieval=object(),
        knowledge_store=resources.knowledge_store,
        postgres=resources.postgres,
        entity_maintenance_service=object(),
    )


@pytest.mark.runtime
@pytest.mark.no_network
async def test_agent_manager_creates_user_owned_specialist_without_participant_side_effects():
    resources = FakeResources()
    manager = AgentManager(resources, user_name="ada")
    parent = await manager.create_agent("Parent", "Careful")

    specialist = await manager.create_specialist(
        parent_id=parent.id,
        name="Evidence checker",
        persona=_persona(),
        brain="Check dates before conclusions.",
    )

    assert specialist.is_spawned is True
    assert specialist.spawned_by == parent.id
    assert specialist.aac_enabled is False
    assert resources.postgres.agents[specialist.id]["spawned_by"] == parent.id
    assert "project_id" not in resources.postgres.agents[specialist.id]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_aac_tools_persist_local_insights_and_only_consult_owned_specialists():
    resources = FakeResources()
    manager = AgentManager(resources, user_name="ada")
    parent = await manager.create_agent("Parent", "Careful")
    specialist = await manager.create_specialist(
        parent_id=parent.id,
        name="Evidence checker",
        persona=_persona(),
    )
    store = AACStore(resources.postgres)
    responses = []

    async def runner(config, question):
        responses.append((config.id, question))
        return "I checked the dates."

    tools = AACTools(
        user_name="ada",
        base_tools=_base_tools(resources),
        store=store,
        agent_manager=manager,
        discussion_id="discussion-1",
        agent_id=parent.id,
        specialist_runner=runner,
    )

    insight = await tools.save_insight("The dates conflict.", visibility="private")
    consultation = await tools.consult_specialist(specialist.id, "Check dates")

    assert insight["saved"] is True
    assert insight["visibility"] == "private"
    assert consultation["result"] == "I checked the dates."
    assert responses == [(specialist.id, "Check dates")]

    outsider = await manager.create_agent("Outsider", "Careful")
    with pytest.raises(ValueError, match="own specialists"):
        await tools.consult_specialist(outsider.id, "No")
    await tools.close()


def test_save_insight_contract_exposes_shared_and_private_visibility():
    schema = next(
        item for item in AAC_SPECIFIC_SCHEMAS if item["function"]["name"] == "save_insight"
    )
    visibility = schema["function"]["parameters"]["properties"]["visibility"]

    assert visibility["enum"] == ["shared", "private"]
    assert visibility["default"] == "shared"
