import pytest

from common.conf.domain_config import DomainConfig
from core.knowledge.db.writers.relationship_reclassification_writer import (
    RelationshipReclassificationWriter,
)
from core.knowledge.relationship_reclassification import (
    plan_relationship_reclassification,
)
from tests.fixtures.fakes import RecordingPostgresClient


def compiled_domain():
    return DomainConfig.from_mapping(
        {
            "version": 3,
            "topics": {"Software": {"active": True}},
            "entity_types": {
                "Project": {"topic": "Software", "labels": ["project"]},
                "Technology": {"topic": "Software", "labels": ["technology"]},
            },
            "relationships": {
                "DEPLOYS_TO": {
                    "source_types": ["Project"],
                    "target_types": ["Technology"],
                }
            },
        }
    ).compile()


def unknown_row():
    return {
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


@pytest.mark.unit
@pytest.mark.no_network
async def test_writer_compare_and_updates_relationship_dependents_transactionally():
    domain = compiled_domain()
    row = unknown_row()
    change = plan_relationship_reclassification([row], domain).changes[0]
    client = RecordingPostgresClient(fetch_all_results=[[row]])

    result = await RelationshipReclassificationWriter(client).apply(
        user_name="ada",
        project_id="demo",
        domain=domain,
        changes=[change],
    )

    assert result == type(result)(scanned=1, updated=1, conflicts=0)
    assert client.transaction_enters == 1
    executed = [call[1] for call in client.calls if call[0] == "execute"]
    assert "FOR UPDATE" in executed[0]
    assert "INSERT INTO public.relationships" in executed[1]
    assert "relationship_evidence_refs" in executed[2]
    assert "relationship_observations" in executed[3]
    assert "episode_relationships" in executed[4]
    assert "DELETE FROM public.relationships" in executed[-1]
