import pytest

from core.knowledge.documents.service import DocumentService
from infrastructure.background_work import BackgroundWorkCoordinator
from infrastructure.model_work import ModelWorkCoordinator


@pytest.mark.unit
@pytest.mark.no_network
def test_model_snapshot_for_health_is_bounded_and_does_not_mutate_source() -> None:
    coordinator = object.__new__(ModelWorkCoordinator)
    raw = {
        "queued": 2,
        "work_by_name": {"embedding": {"queued": 2}},
        "exception": RuntimeError("private"),
        "document_id": "doc-1",
    }
    coordinator.snapshot = lambda: raw

    safe = coordinator.snapshot_for_health()

    assert safe == {"queued": 2, "work_by_name": {"embedding": {"queued": 2}}}
    assert raw["document_id"] == "doc-1"
    assert isinstance(raw["exception"], RuntimeError)


@pytest.mark.unit
@pytest.mark.no_network
def test_background_snapshot_for_health_projects_one_project_only() -> None:
    coordinator = object.__new__(BackgroundWorkCoordinator)
    raw = {
        "queued": 3,
        "queued_by_project": {"project-a": 2, "project-b": 1},
        "queued_categories_by_project": {
            "project-a": ["profile", "aac"],
            "project-b": ["document-index"],
        },
        "active_by_project": {"project-b": ["document-index"]},
    }
    coordinator.snapshot = lambda: raw

    safe = coordinator.snapshot_for_health(project_id="project-a")

    assert safe == {
        "queued": 3,
        "queued_for_project": 2,
        "queued_operation_categories": ["profile", "aac"],
        "active_operation_categories": [],
        "active_for_project": False,
    }
    assert raw["queued_by_project"] == {"project-a": 2, "project-b": 1}


@pytest.mark.unit
@pytest.mark.no_network
def test_background_snapshot_for_health_without_scope_drops_project_state() -> None:
    coordinator = object.__new__(BackgroundWorkCoordinator)
    coordinator.snapshot = lambda: {
        "queued": 1,
        "queued_by_project": {"project-a": 1},
        "ready_projects": ["project-a"],
        "active_projects": [],
    }

    assert coordinator.snapshot_for_health() == {"queued": 1}


@pytest.mark.unit
@pytest.mark.no_network
def test_document_indexing_snapshot_for_health_is_public_and_json_safe() -> None:
    service = object.__new__(DocumentService)
    raw = {
        "policy_version": "v1",
        "recovered_count": 2,
        "document_id": "doc-1",
        "message": "document content",
    }
    service.indexing_snapshot = lambda: raw

    safe = service.indexing_snapshot_for_health()

    assert safe == {"policy_version": "v1", "recovered_count": 2}
    assert raw["document_id"] == "doc-1"
