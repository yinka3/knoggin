import pytest

from core.knowledge.entity.maintenance_service import EntityMaintenanceService
from core.knowledge.maintenance_reviews import (
    EntityContextMergeChoice,
    EntityMergePlan,
    EntityMergeRollbackPlan,
    validate_plan,
)
from tests.fixtures.fakes import RecordingPostgresClient


@pytest.mark.storage
@pytest.mark.no_network
async def test_completed_semantic_windows_define_the_stable_frontier():
    client = RecordingPostgresClient(
        fetch_one_results=[
            {
                "pending_count": 0,
                "frontier_message_id": 42,
                "frontier_timestamp_ms": 1234,
            }
        ]
    )

    frontier = await EntityMaintenanceService(client, "ada").capture_frontier(
        ["project-1"]
    )

    assert frontier["project-1"] == {
        "project_id": "project-1",
        "message_id": 42,
        "timestamp_ms": 1234,
        "token": EntityMaintenanceService._frontier_token(42, 1234),
    }
    assert "project_semantic_windows" in client.calls[0][1]


@pytest.mark.storage
@pytest.mark.no_network
async def test_uncompleted_semantic_work_blocks_stable_frontier():
    client = RecordingPostgresClient(
        fetch_one_results=[
            {
                "pending_count": 1,
                "frontier_message_id": 42,
                "frontier_timestamp_ms": 1234,
            }
        ]
    )

    with pytest.raises(RuntimeError, match="pending semantic completion"):
        await EntityMaintenanceService(client, "ada").capture_frontier(["project-1"])


@pytest.mark.storage
@pytest.mark.no_network
def test_global_merge_and_rollback_plans_are_typed():
    merge = EntityMergePlan(
        survivor_entity_id=2,
        retired_entity_id=3,
        context_choices=[
            EntityContextMergeChoice(
                project_id="project-2", entity_type="person", topic="People"
            )
        ],
        frontier_tokens={"project-1": "frontier"},
        definition_versions={"project-1": 4},
        expected_state_hash="state",
    )
    rollback = validate_plan(
        {
            "kind": "entity_merge_rollback",
            "merge_id": "merge-1",
            "safe_mutation_ids": [1],
            "conflicting_mutation_ids": [2],
            "required_decisions": ["choose a context"],
        }
    )

    assert merge.model_dump(mode="json")["frontier_tokens"] == {
        "project-1": "frontier"
    }
    assert isinstance(rollback, EntityMergeRollbackPlan)
