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


@pytest.mark.no_network
async def test_merge_preview_includes_context_block_associations_in_scope_and_hash():
    class Writer:
        async def snapshot(self, *_args, **_kwargs):
            return {
                "entities": [
                    {"entity_id": 2, "status": "active"},
                    {"entity_id": 3, "status": "active"},
                ],
                "aliases": [],
                "contexts": [],
                "context_block_entities": [
                    {
                        "block_id": "block-1",
                        "project_id": "project-1",
                        "entity_id": 3,
                        "mention_text": "Augusta",
                    },
                    {
                        "block_id": "block-2",
                        "project_id": "project-2",
                        "entity_id": 3,
                        "mention_text": "Ada",
                    },
                    {
                        "block_id": "block-2",
                        "project_id": "project-2",
                        "entity_id": 2,
                        "mention_text": "Ada Lovelace",
                    },
                ],
                "message_refs": [],
                "episode_entities": [],
                "relationships": [],
                "relationship_observations": [],
                "episode_relationships": [],
            }

    service = EntityMaintenanceService(RecordingPostgresClient(), "ada")
    service.writer = Writer()

    async def frontiers(projects, **_kwargs):
        return {project_id: {"token": f"frontier:{project_id}"} for project_id in projects}

    async def versions(_actor, projects, **_kwargs):
        return {project_id: 1 for project_id in projects}

    service.capture_frontier = frontiers
    service._definition_versions = versions

    preview = await service.preview_merge(
        survivor_entity_id=2,
        retired_entity_id=3,
    )

    assert preview["affected_project_ids"] == ["project-1", "project-2"]
    assert preview["plan"].context_block_association_counts == {
        "project-1": 1,
        "project-2": 1,
    }
    changed_snapshot = {
        **preview["snapshot"],
        "context_block_entities": [
            {
                **preview["snapshot"]["context_block_entities"][0],
                "mention_text": "Changed mention",
            },
            *preview["snapshot"]["context_block_entities"][1:],
        ],
    }
    assert EntityMaintenanceService.state_hash(changed_snapshot) != preview["state_hash"]
    changed_survivor_snapshot = {
        **preview["snapshot"],
        "context_block_entities": [
            *preview["snapshot"]["context_block_entities"][:2],
            {
                **preview["snapshot"]["context_block_entities"][2],
                "mention_text": "Changed survivor mention",
            },
        ],
    }
    assert (
        EntityMaintenanceService.state_hash(changed_survivor_snapshot)
        != preview["state_hash"]
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
