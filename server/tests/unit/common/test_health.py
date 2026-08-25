import json
from datetime import UTC, datetime

import pytest

from common.schema.health import (
    HealthActivity,
    HealthSnapshot,
    HealthStatus,
    sanitize_health_details,
)


@pytest.mark.unit
@pytest.mark.no_network
def test_health_snapshot_is_typed_json_safe_and_bounded() -> None:
    details = {
        "queued": 3,
        "pool_size": 2,
        "dsn": "postgresql://user:password@example.invalid/db",
        "message": "document text must not leave the component boundary",
        "exception": RuntimeError("private failure"),
        "nested": {"checked_at": datetime(2026, 1, 1, tzinfo=UTC)},
        "items": list(range(200)),
    }

    snapshot = HealthSnapshot(
        status=HealthStatus.DEGRADED,
        activity=HealthActivity.BUSY,
        summary="  work is progressing  ",
        details=details,
        warnings=[" delayed ", "", "queue is growing"],
    )
    payload = snapshot.model_dump(mode="json")

    json.dumps(payload)
    assert payload["status"] == "degraded"
    assert payload["activity"] == "busy"
    assert payload["summary"] == "work is progressing"
    assert payload["details"]["queued"] == 3
    assert "dsn" not in payload["details"]
    assert "message" not in payload["details"]
    assert "exception" not in payload["details"]
    assert len(payload["details"]["items"]) == 100
    assert payload["warnings"] == ["delayed", "queue is growing"]


@pytest.mark.unit
@pytest.mark.no_network
def test_sanitize_health_details_removes_scope_identifiers_without_mutating_input() -> None:
    details = {
        "queued": 1,
        "queued_by_project": {"project-a": 1, "project-b": 4},
        "active_projects": ["project-a"],
        "current_project_id": "project-a",
        "work_by_name": {"embedding": {"queued": 1}},
    }
    original = details.copy()

    sanitized = sanitize_health_details(details)

    assert details == original
    assert sanitized == {"queued": 1, "work_by_name": {"embedding": {"queued": 1}}}


@pytest.mark.unit
@pytest.mark.no_network
def test_health_sanitization_redacts_secret_strings_in_values_and_warnings() -> None:
    snapshot = HealthSnapshot(
        summary="degraded",
        details={
            "connection": "postgresql://user:password@db.internal/knoggin",
            "note": "worker is delayed",
        },
        warnings=["postgresql://user:password@example.invalid/db", "worker is delayed"],
    )

    payload = snapshot.model_dump(mode="json")
    assert payload["details"]["connection"] == "[redacted]"
    assert payload["details"]["note"] == "worker is delayed"
    assert payload["warnings"] == ["[redacted]", "worker is delayed"]


@pytest.mark.unit
@pytest.mark.no_network
def test_health_snapshot_rejects_non_mapping_details() -> None:
    with pytest.raises(ValueError, match="details must be a mapping"):
        HealthSnapshot(summary="invalid", details=["not", "a", "mapping"])
