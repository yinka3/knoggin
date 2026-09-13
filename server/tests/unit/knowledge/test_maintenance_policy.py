import pytest
from pydantic import ValidationError

from common.schema.settings import ConflictDiscoverySettings
from core.knowledge.maintenance_policy import MaintenanceTrustPolicy


@pytest.mark.unit
@pytest.mark.no_network
def test_trust_policy_only_authorizes_explicit_non_destructive_classifications():
    policy = MaintenanceTrustPolicy.capture(
        ConflictDiscoverySettings(
            mode="trusted",
            trusted_actions=["resolve_conflict:not_a_conflict"],
        )
    )

    assert policy.scheduler_enabled is True
    assert policy.allows_automated_conflict_resolution("not_a_conflict") is True
    assert policy.allows_automated_conflict_resolution("confirmed_conflict") is False
    assert policy.health_snapshot() == {
        "mode": "trusted",
        "scheduler_enabled": True,
        "trusted_action_count": 1,
    }
    with pytest.raises(PermissionError, match="does not authorize"):
        policy.require_automated_conflict_resolution("confirmed_conflict")
    disabled = MaintenanceTrustPolicy.capture(
        ConflictDiscoverySettings(
            enabled=False,
            mode="trusted",
            trusted_actions=["resolve_conflict:not_a_conflict"],
        )
    )
    assert disabled.allows_automated_conflict_resolution("not_a_conflict") is False


@pytest.mark.unit
@pytest.mark.no_network
def test_manual_and_assisted_modes_cannot_activate_a_trusted_allowlist():
    assert (
        MaintenanceTrustPolicy.capture(
            ConflictDiscoverySettings(mode="manual")
        ).scheduler_enabled
        is False
    )
    with pytest.raises(
        ValidationError, match="require conflict discovery mode='trusted'"
    ):
        ConflictDiscoverySettings(
            mode="assisted",
            trusted_actions=["resolve_conflict:not_a_conflict"],
        )
    with pytest.raises(ValidationError):
        ConflictDiscoverySettings(
            mode="trusted",
            trusted_actions=["entity_merge"],
        )
