from uuid import uuid4

import pytest

from common.conf.domain_config import DomainConfig
from common.conf.relationship_config import normalize_relationship
from common.schema.ingestion.contracts import (
    ContextRelationshipWrite,
    relationship_identity,
)


def compiled_domain():
    return DomainConfig.from_mapping(
        {
            "version": 1,
            "topics": {"Software Development": {}},
            "entity_types": {
                "Project": {
                    "topic": "Software Development",
                    "labels": ["project"],
                },
                "Technology": {
                    "topic": "Software Development",
                    "labels": ["technology"],
                },
            },
            "relationships": {
                "USES": {
                    "source_types": ["Project"],
                    "target_types": ["Technology"],
                },
                "COLLABORATES_WITH": {
                    "source_types": ["Project"],
                    "target_types": ["Project"],
                    "symmetric": True,
                },
            },
        }
    ).compile()


@pytest.mark.unit
@pytest.mark.no_network
def test_relationship_normalization_accepts_canonical_wording_and_constraints():
    result = normalize_relationship(
        compiled_domain(),
        " uses ",
        source_type="project",
        target_type="technology",
    )

    assert result.observed_label == "uses"
    assert result.canonical_type == "USES"
    assert result.domain_status == "recognized"
    assert result.persistence_type == "USES"


@pytest.mark.unit
@pytest.mark.no_network
def test_unknown_or_incompatible_relationships_remain_valid_evidence():
    unknown = normalize_relationship(
        compiled_domain(),
        "deploys to",
        source_type="Project",
        target_type="Technology",
    )
    incompatible = normalize_relationship(
        compiled_domain(),
        "USES",
        source_type="Technology",
        target_type="Project",
    )

    assert unknown.domain_status == "unrecognized"
    assert unknown.canonical_type is None
    assert unknown.persistence_type == "deploys to"
    assert incompatible.reason == "endpoint_type_mismatch"
    assert incompatible.canonical_type is None
    assert not incompatible.symmetric


@pytest.mark.unit
@pytest.mark.no_network
def test_symmetric_relationship_keeps_symmetric_marker_for_identity_handling():
    result = normalize_relationship(
        compiled_domain(),
        "collaborates_with",
        source_type="Project",
        target_type="Project",
    )

    assert result.canonical_type == "COLLABORATES_WITH"
    assert result.symmetric


@pytest.mark.unit
@pytest.mark.no_network
def test_context_relationship_write_preserves_observed_and_canonical_labels():
    write = ContextRelationshipWrite(
        support_block_ids=(uuid4(),),
        entity_a_id=2,
        entity_b_id=3,
        relationship_type="USES",
        observed_label="uses",
        canonical_type="USES",
        source_type="Project",
        target_type="Technology",
    )

    assert write.relationship_type == "uses"
    assert write.canonical_type == "USES"
    assert write.observed_label == "uses"
    assert write.domain_status == "recognized"


@pytest.mark.unit
@pytest.mark.no_network
def test_context_relationship_status_is_derived_from_canonical_presence():
    recognized = ContextRelationshipWrite(
        support_block_ids=(uuid4(),),
        entity_a_id=2,
        entity_b_id=3,
        relationship_type="uses",
        canonical_type="USES",
        domain_status="unrecognized",
    )
    unknown = ContextRelationshipWrite(
        support_block_ids=(uuid4(),),
        entity_a_id=2,
        entity_b_id=3,
        relationship_type="deploys to",
        canonical_type=None,
        domain_status="recognized",
    )

    assert recognized.domain_status == "recognized"
    assert unknown.domain_status == "unrecognized"


@pytest.mark.unit
@pytest.mark.no_network
def test_relationship_identity_is_directional_unless_symmetric_is_explicit():
    assert relationship_identity("p", 3, 2, "USES", symmetric=False) == ("p:3:2:uses")
    assert relationship_identity("p", 3, 2, "USES", symmetric=True) == ("p:2:3:uses")
    directional = ContextRelationshipWrite(
        support_block_ids=(uuid4(),),
        entity_a_id=3,
        entity_b_id=2,
        relationship_type="uses",
        symmetric=False,
    )
    symmetric = ContextRelationshipWrite(
        support_block_ids=(uuid4(),),
        entity_a_id=3,
        entity_b_id=2,
        relationship_type="collaborates_with",
        symmetric=True,
    )
    assert (directional.entity_a_id, directional.entity_b_id) == (3, 2)
    assert (symmetric.entity_a_id, symmetric.entity_b_id) == (2, 3)
