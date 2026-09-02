import pytest

from core.knowledge.db.writers.message_lifecycle_writer import (
    MessageLifecycleWriter,
)
from core.knowledge.maintenance_reviews import (
    EntityContextMergeChoice,
    EntityMergePlan,
    EntityMergeRollbackPlan,
    validate_plan,
)
from tests.fixtures.fakes import RecordingPostgresClient


class _MessageWriter:
    pass


@pytest.mark.storage
@pytest.mark.no_network
async def test_terminal_ingestion_failure_is_included_in_stable_frontier():
    client = RecordingPostgresClient(
        fetch_one_results=[
            {
                "pending_count": 0,
                "frontier_message_id": 42,
                "frontier_timestamp_ms": 1234,
            }
        ]
    )

    frontier = await MessageLifecycleWriter(
        client, _MessageWriter()
    ).get_stable_ingestion_frontier(user_name="ada", project_id="project-1")

    assert frontier is not None
    assert frontier.message_id == 42
    assert frontier.timestamp_ms == 1234
    assert frontier.token


@pytest.mark.storage
@pytest.mark.no_network
async def test_live_ingestion_work_blocks_stable_frontier():
    client = RecordingPostgresClient(
        fetch_one_results=[
            {
                "pending_count": 1,
                "frontier_message_id": 42,
                "frontier_timestamp_ms": 1234,
            }
        ]
    )

    assert (
        await MessageLifecycleWriter(
            client, _MessageWriter()
        ).get_stable_ingestion_frontier(user_name="ada", project_id="project-1")
        is None
    )


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
