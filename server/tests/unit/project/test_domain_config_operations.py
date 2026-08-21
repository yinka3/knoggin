import pytest

from common.conf.domain_config import DomainConfig
from core.project.domain_config_operations import (
    parse_candidate,
    preview_domain_config,
    validate_domain_config,
)


def make_domain(version=1):
    return DomainConfig.from_mapping(
        {
            "version": version,
            "topics": {
                "Software Development": {"description": "Build software"},
                "Operations": {"active": False},
            },
            "entity_types": {
                "Project": {
                    "topic": "Software Development",
                    "description": "An organized software effort.",
                    "labels": ["project"],
                },
                "Platform": {
                    "topic": "Operations",
                    "labels": ["platform"],
                },
            },
            "relationships": {
                "USES": {
                    "source_types": ["Project"],
                    "target_types": ["Platform"],
                }
            },
        }
    )

@pytest.mark.unit
@pytest.mark.no_network
def test_validation_returns_canonical_candidate_without_side_effects():
    result = validate_domain_config(
        {
            "version": 99,
            "topics": {"Software Development": {}},
            "entity_types": {
                "Project": {
                    "topic": "software development",
                    "labels": ["Project"],
                }
            },
        }
    )

    assert result.valid
    assert result.errors == ()
    assert any(
        "Entity type 'Project' has no description" in warning
        for warning in result.warnings
    )
    assert result.config.entity_types[0].labels == ("project",)
    assert result.to_dict()["config"]["version"] == 99
    assert parse_candidate(result.config.to_dict()) == result.config
    assert parse_candidate(result.config.to_dict()) == result.config


@pytest.mark.unit
@pytest.mark.no_network
def test_validation_collects_invalid_candidate_as_user_facing_error():
    result = validate_domain_config(
        {
            "topics": {"Software Development": {}},
            "entity_types": {"Project": {"topic": "Missing Topic", "labels": []}},
        }
    )

    assert not result.valid
    assert result.config is None
    assert "unknown topic" in result.errors[0].lower()


@pytest.mark.unit
@pytest.mark.no_network
def test_validation_rejects_duplicate_relationship_meanings():
    payload = make_domain().to_dict()
    payload["relationships"][" USES "] = payload["relationships"]["USES"].copy()

    result = validate_domain_config(payload)

    assert not result.valid
    assert "duplicate relationship name" in result.errors[0].lower()


@pytest.mark.unit
@pytest.mark.no_network
def test_preview_reports_future_effects_and_no_historical_rewrite():
    current = make_domain(version=4)
    candidate = DomainConfig.from_mapping(
        {
            "version": 0,
            "topics": {
                "Software Development": {"description": "Build software"},
                "Operations": {},
            },
            "entity_types": {
                "Project": {
                    "topic": "Software Development",
                    "description": "An organized software effort.",
                    "labels": ["project"],
                },
                "Platform": {"topic": "Operations", "labels": ["platform"]},
            },
            "relationships": {
                "USES": {
                    "source_types": ["Project"],
                    "target_types": ["Platform"],
                },
                "DEPLOYS_TO": {
                    "source_types": ["Project"],
                    "target_types": ["Platform"],
                },
            },
        }
    )

    preview = preview_domain_config(current, candidate)

    assert preview.current_version == 4
    assert preview.next_version == 5
    assert preview.topics_activated == ("Operations",)
    assert preview.relationships_added == ("DEPLOYS_TO",)
    assert any(
        "not normalized automatically" in effect for effect in preview.future_effects
    )
    assert preview.has_changes
    assert preview.to_dict()["next_version"] == 5


@pytest.mark.unit
@pytest.mark.no_network
def test_preview_is_pure_and_empty_candidate_has_no_changes():
    current = make_domain(version=2)
    preview = preview_domain_config(current, current.to_dict())

    assert not preview.has_changes
    assert preview.future_effects == (
        "No future ingestion behavior changes were detected.",
    )
    assert current.version == 2


@pytest.mark.unit
@pytest.mark.no_network
def test_preview_describes_changes_to_existing_definitions():
    current = make_domain(version=2)
    candidate = make_domain(version=0).to_dict()
    candidate["topics"]["Software Development"]["active"] = False
    candidate["entity_types"]["Project"]["description"] = "A changed project"
    candidate["relationships"]["USES"]["symmetric"] = False

    preview = preview_domain_config(current, candidate)

    assert preview.topics_deactivated == ("Software Development",)
    assert preview.entity_types_changed == ("Project",)
    assert preview.relationships_changed == ()
    assert any(
        "stop assigning new entities" in effect for effect in preview.future_effects
    )
    assert any(
        "updated topic, labels, or description" in effect
        for effect in preview.future_effects
    )
