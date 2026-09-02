from types import SimpleNamespace

import pytest

from common.conf.domain_config import DomainConfig
from core.knowledge.db.writers.relationship_interpretation_writer import (
    RelationshipInterpretationResult,
    RelationshipInterpretationWriter,
)
from core.knowledge.entity.maintenance_service import EntityMaintenanceService
from core.knowledge.maintenance_reviews import (
    ConflictResolutionPlan,
    EntityMergePlan,
    MaintenanceReview,
    RelationshipInterpretationChange,
    RelationshipInterpretationPlan,
)
from core.project.maintenance_service import ProjectMaintenanceService
from core.project.project_manager import ProjectManager
from tests.fixtures.fakes import RecordingPostgresClient


def _domain() -> DomainConfig:
    return DomainConfig.from_mapping(
        {
            "version": 1,
            "topics": {"General": {}},
            "entity_types": {
                "Concept": {"topic": "General", "labels": ["concept"]}
            },
            "relationships": {
                "RELATED_TO": {
                    "source_types": ["Concept"],
                    "target_types": ["Concept"],
                }
            },
        }
    )


def _relationship_review(*, status: str = "open") -> MaintenanceReview:
    return MaintenanceReview(
        review_id="review-1",
        user_name="ada",
        scope="project",
        project_id="project-1",
        kind="relationship_interpretation",
        reasoning="The observed relationship should use the project definition.",
        proposed_plan=RelationshipInterpretationPlan(
            changes=[
                RelationshipInterpretationChange(
                    observation_id=10,
                    expected_relationship_id="old",
                    target_relationship_type="RELATED_TO",
                    interpretation_source="review",
                )
            ]
        ),
        expected_state={"domain_version": 1},
        status=status,
    )


class _ReviewStore:
    def __init__(self, review: MaintenanceReview):
        self.review = review
        self.transitions = []

    async def get(self, review_id, **kwargs):
        assert review_id == self.review.review_id
        return self.review

    async def transition(self, review_id, **kwargs):
        self.transitions.append((review_id, kwargs))
        self.review = self.review.model_copy(update={"status": kwargs["status"]})
        return self.review


class _DomainStore:
    async def load(self, user_name, project_id):
        assert (user_name, project_id) == ("ada", "project-1")
        return _domain()


class _InterpretationWriter:
    def __init__(self, reviews: _ReviewStore):
        self.reviews = reviews
        self.calls = []

    async def apply_plan(self, **kwargs):
        self.calls.append(kwargs)
        await self.reviews.transition(
            kwargs["review_id"],
            status="applied",
        )
        return RelationshipInterpretationResult(1, 0, 0, 0, "audit-1")


async def _active_project(_project_id):
    return {"project_id": "project-1", "status": "active"}


def _project_service(review: MaintenanceReview):
    service = ProjectMaintenanceService(
        resources=SimpleNamespace(postgres=object(), knowledge_store=object()),
        user_name="ada",
        project_lookup=_active_project,
        active_projects={},
        project_leases={},
    )
    reviews = _ReviewStore(review)
    service._maintenance_reviews = reviews
    service._domain_store = _DomainStore()
    service._relationship_interpretation_writer = _InterpretationWriter(reviews)
    return service, reviews


@pytest.mark.no_network
async def test_applying_relationship_review_executes_plan_before_applied_status():
    service, reviews = _project_service(_relationship_review())

    result = await service.transition_maintenance_review(
        "project-1",
        "review-1",
        status="applied",
        expected_state={"domain_version": 1},
    )

    assert result.status == "applied"
    assert service._relationship_interpretation_writer.calls[0]["review_id"] == (
        "review-1"
    )
    assert reviews.transitions[-1][1]["status"] == "applied"


@pytest.mark.no_network
async def test_generic_applied_transition_rejects_plan_with_dedicated_workflow():
    review = _relationship_review().model_copy(
        update={
            "kind": "relationship_conflict",
            "proposed_plan": ConflictResolutionPlan(
                conflict_kind="possible_contradiction"
            ),
        }
    )
    service, reviews = _project_service(review)

    with pytest.raises(ValueError, match="dedicated maintenance operation"):
        await service.transition_maintenance_review(
            "project-1",
            "review-1",
            status="applied",
        )

    assert reviews.transitions == []


@pytest.mark.no_network
async def test_dismissal_remains_a_status_only_transition():
    service, reviews = _project_service(_relationship_review())

    result = await service.transition_maintenance_review(
        "project-1",
        "review-1",
        status="dismissed",
        reason="Not applicable",
    )

    assert result.status == "dismissed"
    assert service._relationship_interpretation_writer.calls == []
    assert reviews.transitions[-1][1]["status"] == "dismissed"


@pytest.mark.no_network
async def test_reviewed_relationship_plan_rejects_all_changes_before_mutation():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [
                {
                    "observation_id": 10,
                    "relationship_id": "changed",
                    "project_id": "project-1",
                    "source_entity_id": 2,
                    "target_entity_id": 3,
                    "relationship_type": "RELATED_TO",
                    "symmetric": False,
                    "source_type": "Concept",
                    "target_type": "Concept",
                }
            ]
        ]
    )
    writer = RelationshipInterpretationWriter(client)

    with pytest.raises(ValueError, match="review is stale"):
        await writer.apply_plan(
            user_name="ada",
            project_id="project-1",
            plan=_relationship_review().proposed_plan,
            review_id="review-1",
        )

    assert not any(
        "UPDATE public.relationship_observations" in query
        for kind, query, _params in client.calls
        if kind == "execute"
    )


class _MergeReviewStore:
    def __init__(self, review):
        self.review = review

    async def get(self, review_id, **_kwargs):
        return self.review if review_id == self.review.review_id else None


@pytest.mark.no_network
async def test_global_merge_review_uses_stored_plan_and_expected_state(monkeypatch):
    service = EntityMaintenanceService(
        postgres=RecordingPostgresClient(),
        user_name="ada",
    )
    plan = EntityMergePlan(
        survivor_entity_id=2,
        retired_entity_id=3,
        expected_state_hash="hash-1",
    )
    review = MaintenanceReview(
        review_id="review-merge",
        user_name="ada",
        scope="user-global",
        project_id=None,
        kind="entity_merge",
        reasoning="The evidence identifies one entity.",
        proposed_plan=plan,
        expected_state={"state_hash": "hash-1"},
    )
    service.review_writer = _MergeReviewStore(review)
    calls = []

    async def merge(received_plan, **kwargs):
        calls.append((received_plan, kwargs))
        return {
            "merge_id": "merge-1",
            "affected_project_ids": ["project-1"],
            "survivor_entity_id": 2,
            "retired_entity_id": 3,
        }

    monkeypatch.setattr(service, "merge", merge)

    result = await service.apply_merge_review(
        "review-merge",
        expected_state={"state_hash": "hash-1"},
    )

    assert result["review_id"] == "review-merge"
    assert calls == [
        (
            plan,
            {
                "user_name": None,
                "expected_state_hash": "hash-1",
                "review_id": "review-merge",
                "review_expected_state": {"state_hash": "hash-1"},
            },
        )
    ]


class _EntityCache:
    def __init__(self):
        self.removed = []

    def remove_entities(self, entity_ids):
        self.removed.append(entity_ids)
        return len(entity_ids)


class _EntityService:
    async def apply_merge_review(self, review_id, **_kwargs):
        assert review_id == "review-merge"
        return {
            "merge_id": "merge-1",
            "affected_project_ids": ["project-1"],
            "survivor_entity_id": 2,
            "retired_entity_id": 3,
        }

    async def rollback(self, merge_id, **_kwargs):
        assert merge_id == "merge-1"
        return {
            "merge_id": merge_id,
            "affected_project_ids": ["project-1"],
            "survivor_entity_id": 2,
            "retired_entity_id": 3,
            "applied_mutation_ids": [1],
        }


@pytest.mark.no_network
async def test_project_manager_invalidates_affected_live_entity_cache():
    manager = ProjectManager.__new__(ProjectManager)
    manager.entity_maintenance_service = _EntityService()
    manager.maintenance_service = SimpleNamespace(lock=__import__("asyncio").Lock())
    cache = _EntityCache()
    manager.active_projects = {"project-1": SimpleNamespace(entities=cache)}

    result = await manager.apply_global_entity_merge_review("review-merge")

    assert cache.removed == [[2, 3]]
    assert result["runtime_cache_invalidations"] == {"project-1": 2}


@pytest.mark.no_network
async def test_project_manager_invalidates_cache_after_rollback():
    manager = ProjectManager.__new__(ProjectManager)
    manager.entity_maintenance_service = _EntityService()
    manager.maintenance_service = SimpleNamespace(lock=__import__("asyncio").Lock())
    cache = _EntityCache()
    manager.active_projects = {"project-1": SimpleNamespace(entities=cache)}

    result = await manager.rollback_global_entity_merge("merge-1")

    assert cache.removed == [[2, 3]]
    assert result["runtime_cache_invalidations"] == {"project-1": 2}


class _RollbackWriter:
    async def plan_rollback(self, **_kwargs):
        return {
            "merge_id": "merge-1",
            "survivor_entity_id": 2,
            "retired_entity_id": 3,
            "affected_project_ids": ["project-1", "project-2"],
            "safe_mutation_ids": [1],
            "conflicting_mutations": [],
            "already_applied_mutation_ids": [],
            "mutations": [{"mutation_id": 1}],
        }

    async def rollback_safe(self, **_kwargs):
        return {
            "merge_id": "merge-1",
            "survivor_entity_id": 2,
            "retired_entity_id": 3,
            "affected_project_ids": ["project-1", "project-2"],
            "applied_mutation_ids": [1],
            "rolled_back": True,
            "concurrent_conflicts": [],
        }


class _ProjectionBuilder:
    def __init__(self):
        self.calls = []

    async def rebuild_project_projection(self, project_id, user_name):
        self.calls.append((project_id, user_name))
        return {"entities": 1, "relationships": 0}


@pytest.mark.no_network
async def test_rollback_rebuilds_every_affected_projection():
    service = EntityMaintenanceService(postgres=object(), user_name="ada")
    service.writer = _RollbackWriter()
    service.projection_rebuilder = _ProjectionBuilder()

    result = await service.rollback("merge-1")

    assert result["rolled_back"] is True
    assert result["projection_errors"] == []
    assert service.projection_rebuilder.calls == [
        ("project-1", "ada"),
        ("project-2", "ada"),
    ]


class _AtomicMergeWriter:
    def __init__(self):
        self.cursor = None

    async def merge(self, **kwargs):
        self.cursor = kwargs["cur"]
        return {
            "merge_id": "merge-1",
            "affected_project_ids": ["project-1"],
            "survivor_entity_id": 2,
            "retired_entity_id": 3,
        }


class _AtomicReviewWriter:
    def __init__(self):
        self.cursor = None

    async def transition(self, _review_id, **kwargs):
        self.cursor = kwargs["cur"]


@pytest.mark.no_network
async def test_merge_and_review_transition_share_one_transaction(monkeypatch):
    client = RecordingPostgresClient()
    service = EntityMaintenanceService(postgres=client, user_name="ada")
    service.writer = _AtomicMergeWriter()
    service.review_writer = _AtomicReviewWriter()
    plan = EntityMergePlan(
        survivor_entity_id=2,
        retired_entity_id=3,
        frontier_tokens={"project-1": "frontier"},
        definition_versions={"project-1": 1},
        expected_state_hash="hash-1",
    )

    async def preview_merge(**_kwargs):
        return {
            "context_conflicts": [],
            "state_hash": "hash-1",
            "affected_project_ids": ["project-1"],
            "frontiers": {"project-1": {"token": ""}},
        }

    async def true_frontier(*_args, **_kwargs):
        return True

    async def versions(*_args, **_kwargs):
        return {"project-1": 1}

    async def no_projection_errors(*_args, **_kwargs):
        return []

    monkeypatch.setattr(service, "preview_merge", preview_merge)
    monkeypatch.setattr(service, "revalidate_frontier", true_frontier)
    monkeypatch.setattr(service, "_definition_versions", versions)
    monkeypatch.setattr(service, "_rebuild_projections", no_projection_errors)

    await service.merge(
        plan,
        review_id="review-1",
        review_expected_state={"state_hash": "hash-1"},
    )

    assert service.writer.cursor is service.review_writer.cursor
    assert client.transaction_enters == 1
