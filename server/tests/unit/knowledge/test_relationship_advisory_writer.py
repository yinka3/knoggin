import pytest

from core.knowledge.db.writers.relationship_advisory_writer import (
    RelationshipAdvisoryWriter,
)
from core.knowledge.relationship_advisories import RelationshipAdvisory
from tests.fixtures.fakes import RecordingPostgresClient


@pytest.mark.unit
@pytest.mark.no_network
async def test_writer_persists_current_state_and_audit_for_acceptance():
    client = RecordingPostgresClient(fetch_one_results=[None])

    decision = await RelationshipAdvisoryWriter(client).apply_action(
        user_name="ada",
        project_id="project-1",
        pattern_key="deploys to|project|technology",
        action="accept",
        relationship_type="DEPLOYS_TO",
        decided_by="ada",
    )

    assert decision.disposition == "accepted"
    assert decision.revision == 1
    assert client.transaction_enters == 1
    executed = [call for call in client.calls if call[0] == "execute"]
    assert len(executed) == 4
    assert "FOR UPDATE" in executed[0][1]
    assert "INSERT INTO relationship_advisories" in executed[1][1]
    assert "INSERT INTO relationship_advisory_decisions" in executed[2][1]
    assert "UPDATE public.human_reviews" in executed[3][1]


@pytest.mark.unit
@pytest.mark.no_network
async def test_materializing_pending_advisory_opens_a_linked_review():
    client = RecordingPostgresClient()
    advisory = RelationshipAdvisory(
        pattern_key="deploys to|project|technology",
        observed_label="deploys to",
        source_type="Project",
        target_type="Technology",
        occurrence_count=3,
        distinct_source_entities=2,
        distinct_target_entities=2,
        message_ids=(1, 2, 3),
        first_observed_ms=1,
        last_observed_ms=3,
    )

    await RelationshipAdvisoryWriter(client).materialize_pending(
        user_name="ada", project_id="project-1", advisory=advisory
    )

    executed = [call for call in client.calls if call[0] == "execute"]
    assert len(executed) == 2
    assert "INSERT INTO relationship_advisories" in executed[0][1]
    assert "INSERT INTO public.human_reviews" in executed[1][1]
