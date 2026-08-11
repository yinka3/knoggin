import pytest

from common.conf.domain_config import DomainConfig
from common.schema.contracts import relationship_identity
from core.knowledge.relationship_reclassification import (
    plan_relationship_reclassification,
)


def compiled_domain():
    return DomainConfig.from_mapping(
        {
            "version": 11,
            "topics": {
                "Software": {"active": True},
                "Identity": {"active": True},
            },
            "entity_types": {
                "Project": {"topic": "Software", "labels": ["project"]},
                "Technology": {"topic": "Software", "labels": ["technology"]},
                "Person": {"topic": "Identity", "labels": ["person"]},
            },
            "relationships": {
                "DEPLOYS_TO": {
                    "source_types": ["Project"],
                    "target_types": ["Technology"],
                },
                "KNOWS": {
                    "source_types": ["Person"],
                    "target_types": ["Person"],
                    "symmetric": True,
                },
            },
        }
    ).compile()


def relationship_row(**overrides):
    row = {
        "relationship_id": "demo:10:20:deploys to",
        "project_id": "demo",
        "entity_a_id": 10,
        "entity_b_id": 20,
        "relationship_type": "deploys to",
        "canonical_relationship_type": None,
        "observed_relationship_label": "deploys to",
        "domain_status": "unrecognized",
        "symmetric": False,
        "source_type": "Project",
        "target_type": "Technology",
    }
    row.update(overrides)
    return row


@pytest.mark.unit
@pytest.mark.no_network
def test_relationship_reclassification_maps_exact_label_and_endpoint_types():
    plan = plan_relationship_reclassification([relationship_row()], compiled_domain())

    assert plan.domain_version == 11
    assert plan.changed == 1
    assert plan.unmapped == 0
    assert plan.incompatible == 0
    assert plan.changes[0].new_relationship_id == relationship_identity(
        "demo", 10, 20, "DEPLOYS_TO", symmetric=False
    )
    assert plan.changes[0].new_canonical_relationship_type == "DEPLOYS_TO"


@pytest.mark.unit
@pytest.mark.no_network
def test_relationship_reclassification_leaves_unknown_and_incompatible_rows():
    plan = plan_relationship_reclassification(
        [
            relationship_row(observed_relationship_label="invented relation"),
            relationship_row(
                relationship_id="demo:10:30:deploys to",
                entity_b_id=30,
                source_type="Technology",
                target_type="Project",
            ),
        ],
        compiled_domain(),
    )

    assert plan.changed == 0
    assert plan.unmapped == 1
    assert plan.incompatible == 1


@pytest.mark.unit
@pytest.mark.no_network
def test_relationship_reclassification_uses_symmetric_identity():
    row = relationship_row(
        relationship_id="demo:21:20:knows",
        entity_a_id=21,
        entity_b_id=20,
        relationship_type="knows",
        observed_relationship_label="knows",
        source_type="Person",
        target_type="Person",
    )
    plan = plan_relationship_reclassification([row], compiled_domain())

    assert plan.changed == 1
    assert plan.changes[0].new_relationship_id == "demo:20:21:knows"
    assert plan.changes[0].new_symmetric is True


@pytest.mark.unit
@pytest.mark.no_network
def test_relationship_reclassification_counts_already_normalized_rows_unchanged():
    canonical_id = relationship_identity(
        "demo", 10, 20, "DEPLOYS_TO", symmetric=False
    )
    plan = plan_relationship_reclassification(
        [
            relationship_row(
                relationship_id=canonical_id,
                relationship_type="DEPLOYS_TO",
                canonical_relationship_type="DEPLOYS_TO",
                domain_status="recognized",
            )
        ],
        compiled_domain(),
    )

    assert plan.changed == 0
    assert plan.unchanged == 1
