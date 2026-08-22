from types import SimpleNamespace

import pytest

from core.project.entity_cleanup import EntityCleanupWorkflow


class _Store:
    def __init__(self) -> None:
        self.preview_calls = []
        self.delete_calls = []

    async def preview_project_entity_cleanup(self, **kwargs):
        self.preview_calls.append(kwargs)
        return [{"entity_id": 7, "canonical_name": "Roadmap"}]

    async def delete_selected_project_entities(self, entity_ids, **kwargs):
        self.delete_calls.append((entity_ids, kwargs))
        return entity_ids


@pytest.mark.unit
@pytest.mark.no_network
async def test_entity_cleanup_workflow_previews_project_owned_evidence():
    store = _Store()
    workflow = EntityCleanupWorkflow(store)

    preview = await workflow.preview(user_name="ada", project_id="project-1", limit=25)

    assert preview == {
        "project_id": "project-1",
        "candidates": [{"entity_id": 7, "canonical_name": "Roadmap"}],
    }
    assert store.preview_calls == [
        {"user_name": "ada", "project_id": "project-1", "limit": 25}
    ]


@pytest.mark.unit
@pytest.mark.no_network
async def test_entity_cleanup_workflow_applies_only_explicit_non_identity_ids():
    store = _Store()
    workflow = EntityCleanupWorkflow(store)

    result = await workflow.apply(
        user_name="ada",
        project_id="project-1",
        entity_ids=[9, 4, 9],
    )

    assert result == {"project_id": "project-1", "deleted_entity_ids": [4, 9]}
    assert store.delete_calls == [
        ([4, 9], {"user_name": "ada", "project_id": "project-1"})
    ]


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize("entity_ids", [[], [1], [0], [True], ["7"]])
async def test_entity_cleanup_workflow_rejects_invalid_or_reserved_selections(entity_ids):
    workflow = EntityCleanupWorkflow(SimpleNamespace())

    with pytest.raises((TypeError, ValueError)):
        await workflow.apply(
            user_name="ada",
            project_id="project-1",
            entity_ids=entity_ids,
        )
