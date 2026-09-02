from types import SimpleNamespace

import pytest

import core.agent.tools.maintenance as maintenance_module
from common.schema.agent.tool_contracts import (
    REVERSIBLE_WRITE_CAPABILITY,
    TOOL_SCHEMAS_BY_NAME,
    get_schema_capability,
)
from core.agent.tools.maintenance import MaintenanceTools


class RecordingKnowledgeStore:
    def __init__(self) -> None:
        self.calls = []

    async def record_conflict_detection(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(
            group=SimpleNamespace(conflict_id="conflict-1", status="open"),
            created=True,
            evidence_added=2,
        )


class MaintenanceHarness(MaintenanceTools):
    def __init__(self) -> None:
        self.user_name = "ada"
        self.project_id = "project-1"
        self.knowledge_store = RecordingKnowledgeStore()
        self.postgres = object()


@pytest.mark.no_network
async def test_agent_conflict_report_keeps_evidence_immutable_and_opens_review_workflow(
    monkeypatch,
):
    tools = MaintenanceHarness()

    class RecordingConflictService:
        def __init__(self, _writer):
            self.calls = []

        async def record_detection(self, **kwargs):
            self.calls.append(kwargs)
            return SimpleNamespace(
                group=SimpleNamespace(conflict_id="conflict-1", status="open"),
                created=True,
                evidence_added=2,
            )

    service = RecordingConflictService(None)
    monkeypatch.setattr(maintenance_module, "ConflictService", lambda _writer: service)

    result = await tools.report_relationship_conflict(
        evidence_observation_ids=[101, 104],
        kind="possible_contradiction",
        reasoning="The employment observations conflict for the same period.",
        confidence=0.8,
    )

    assert result["conflict_id"] == "conflict-1"
    assert result["created"] is True
    assert service.calls == [
        {
            "user_name": "ada",
            "project_id": "project-1",
            "origin": "agent_discovery",
            "kind": "possible_contradiction",
            "rationale": "The employment observations conflict for the same period.",
            "confidence": 0.8,
            "evidence_ids": [101, 104],
            "metadata": {"reported_by": "agent"},
        }
    ]


def test_agent_conflict_report_is_a_reversible_write_with_grounded_evidence_contract():
    schema = TOOL_SCHEMAS_BY_NAME["report_relationship_conflict"]

    assert get_schema_capability(schema) == REVERSIBLE_WRITE_CAPABILITY
    assert schema["function"]["parameters"]["required"] == [
        "evidence_observation_ids",
        "kind",
        "reasoning",
        "confidence",
    ]
