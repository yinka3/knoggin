import pytest

from common.conf.domain_config import DomainConfig
from core.knowledge.entity.reclassification import plan_reclassification


def compiled_domain():
    return DomainConfig.from_mapping(
        {
            "version": 8,
            "topics": {
                "Software Development": {"active": True},
                "Identity": {"active": True},
            },
            "entity_types": {
                "Project": {
                    "topic": "Software Development",
                    "labels": ["software project"],
                },
                "Identity": {"topic": "Identity", "labels": ["person"]},
            },
        }
    ).compile()


@pytest.mark.unit
@pytest.mark.no_network
def test_reclassification_maps_old_labels_to_canonical_type_and_topic():
    plan = plan_reclassification(
        [
            {
                "entity_id": 2,
                "canonical_name": "Knoggin",
                "type": "software project",
                "topic": "General",
            },
            {
                "entity_id": 3,
                "canonical_name": "Ada",
                "type": "person",
                "topic": "General",
            },
            {
                "entity_id": 4,
                "canonical_name": "Unmapped",
                "type": "vehicle",
                "topic": "General",
            },
            {
                "entity_id": 1,
                "canonical_name": "Ada",
                "type": "person",
                "topic": "Identity",
            },
        ],
        compiled_domain(),
    )

    assert plan.domain_version == 8
    assert plan.scanned == 4
    assert plan.changed == 2
    assert plan.unchanged == 1
    assert plan.unmapped == 1
    assert plan.changes[0].to_dict() == {
        "entity_id": 2,
        "canonical_name": "Knoggin",
        "old_type": "software project",
        "old_topic": "General",
        "new_type": "Project",
        "new_topic": "Software Development",
    }


@pytest.mark.unit
@pytest.mark.no_network
def test_reclassification_does_not_guess_from_entity_name():
    plan = plan_reclassification(
        [
            {
                "entity_id": 2,
                "canonical_name": "Project Alpha",
                "type": "",
                "topic": "General",
            }
        ],
        compiled_domain(),
    )

    assert plan.changed == 0
    assert plan.unmapped == 1
